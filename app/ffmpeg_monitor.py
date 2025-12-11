# ffmpeg_monitor.py
import psutil
import threading
import time
from pathlib import Path
from config import logger
import os
import signal
from config import FFMPEG_LOG_LEVEL, logger, WATCHERS_LOG_CYCLE, HLS_DIR
import random
import re
import json
import subprocess
import queue
import logging
from datetime import datetime
from typing import Dict, List, Optional, Union, Any, Tuple
from ffmpeg_logger import FFmpegLogger

# Constantes
SUMMARY_CYCLE = 300  # 5 minutes
CLEANING_COOLDOWN = 180  # 3 minutes entre deux nettoyages

class FFmpegMonitor(threading.Thread):
    """
    Moniteur centralisé des processus FFmpeg
    Surveillance passive, moins agressive
    """

    def __init__(self, channels_dict):
        super().__init__(daemon=True)
        self.channels = channels_dict
        self.stop_event = threading.Event()
        self.ffmpeg_log_dir = Path("/app/logs/ffmpeg")
        self.ffmpeg_log_dir.mkdir(parents=True, exist_ok=True)
        
        # État et compteurs
        self.last_clean_time = 0
        self.cleanup_cooldown = CLEANING_COOLDOWN
        self.cleaned_processes = set()  # Pour éviter de nettoyer les mêmes processus trop souvent

    def run(self):
        """Méthode principale du thread"""
        logger.info("🚀 Démarrage du FFmpegMonitor (version simplifiée)")
        self.run_monitor_loop()

    def _check_all_ffmpeg_processes(self):
        """
        Parcourt les processus FFmpeg pour détecter uniquement 
        les cas de processus multiples, avec limitation du nettoyage
        """
        # Limiter la fréquence de vérification (45s)
        current_time = time.time()
        if hasattr(self, "last_check_time") and current_time - self.last_check_time < 45:
            return
        setattr(self, "last_check_time", current_time)

        # Collecte des processus FFmpeg organisés par chaîne
        try:
            ffmpeg_processes = {}
            for proc in psutil.process_iter(attrs=["pid", "name", "cmdline"]):
                try:
                    if "ffmpeg" in proc.info["name"].lower():
                        cmd_args = proc.info["cmdline"] or []
                        cmd_str = " ".join(cmd_args)

                        # Détecter le nom de la chaîne
                        for channel_name in self.channels:
                            if f"/{HLS_DIR.strip('/')}/{channel_name}/" in cmd_str:
                                ffmpeg_processes.setdefault(channel_name, []).append(proc.info["pid"])
                                break
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue

            # Pour chaque chaîne, vérifier seulement les cas où il y a plusieurs processus
            for channel_name, pids in ffmpeg_processes.items():
                # On s'intéresse seulement aux cas où il y a plusieurs processus
                if len(pids) <= 1:
                    continue
                    
                # On log l'information
                logger.warning(f"⚠️ {channel_name}: {len(pids)} processus FFmpeg détectés")
                
                # Vérifier le cooldown avant de procéder au nettoyage
                if current_time - self.last_clean_time < self.cleanup_cooldown:
                    logger.info(f"⏳ Attente du cooldown de nettoyage: {int(self.cleanup_cooldown - (current_time - self.last_clean_time))}s")
                    continue
                
                # On récupère l'objet chaîne
                channel = self.channels.get(channel_name)
                if not channel:
                    continue
                    
                # On demande à l'objet de nettoyer ses processus
                logger.info(f"🧹 Nettoyage des processus multiples pour {channel_name}")
                
                # Éviter de nettoyer trop souvent
                if hasattr(channel, "process_manager") and hasattr(channel.process_manager, "_clean_zombie_processes"):
                    channel.process_manager._clean_zombie_processes()
                    self.last_clean_time = current_time
                    
                    # Une petite pause après un nettoyage
                    time.sleep(1)
                
        except Exception as e:
            logger.error(f"❌ Erreur lors de la vérification des processus: {e}")

    def _log_channels_summary(self):
        """Génère un résumé des chaînes en cours d'exécution"""
        try:
            active_channels = []
            for name, channel in sorted(self.channels.items()):
                if hasattr(channel, "is_running") and channel.is_running():
                    watchers = getattr(channel, "watchers_count", 0)
                    active_channels.append(f"{name}: {watchers} viewers")
            
            if active_channels:
                logger.info(f"📊 Chaînes actives: {', '.join(active_channels)}")
            else:
                logger.info("📊 Aucune chaîne active")
                
        except Exception as e:
            logger.error(f"❌ Erreur lors de la génération du résumé: {e}")

    # MÉTHODE SUPPRIMÉE: _watchers_loop()
    # Cette méthode contenait un health check redondant qui était déjà fait par
    # FFmpegProcessManager._monitor_process(). Le monitoring des viewers est maintenant
    # géré entièrement par StatsCollector.
    
    def run_monitor_loop(self):
        """Boucle principale du monitoring avec intervalle plus long"""
        last_check_time = 0
        last_error_check_time = 0
        check_interval = 30  # Vérifier toutes les 30 secondes
        error_check_interval = 60  # Vérifier les erreurs toutes les 60 secondes
        
        while not self.stop_event.is_set():
            try:
                current_time = time.time()
                
                # Vérification des processus avec intervalle
                if current_time - last_check_time >= check_interval:
                    self._check_all_ffmpeg_processes()
                    last_check_time = current_time
                
                # Vérification des erreurs dans les logs FFmpeg
                if current_time - last_error_check_time >= error_check_interval:
                    self._check_ffmpeg_logs_for_errors()
                    last_error_check_time = current_time
                
                # Pause plus longue
                time.sleep(5)
                
            except Exception as e:
                logger.error(f"❌ Erreur monitoring FFmpeg: {e}")
                time.sleep(10)

    def _check_ffmpeg_logs_for_errors(self):
        """Vérifie périodiquement les logs FFmpeg pour détecter des erreurs"""
        try:
            # Vérifier les logs de chaque chaîne
            for channel_name in self.channels:
                # Créer ou récupérer le logger FFmpeg pour cette chaîne
                ffmpeg_logger = FFmpegLogger(channel_name)
                
                # Vérifier les erreurs dans les logs
                ffmpeg_logger.check_for_errors()
                
        except Exception as e:
            logger.error(f"❌ Erreur lors de la vérification des logs FFmpeg: {e}")

    def _save_stats_periodically(self):
        """Sauvegarde périodiquement les statistiques"""
        if hasattr(self.channels, "stats_collector") and self.channels.stats_collector:
            self.channels.stats_collector.save_stats()

    def _check_process_health(self, channel_name, pids):
        """Vérifie la santé d'un processus ffmpeg"""
        try:
            # Vérifier si le processus existe
            if not pids:
                logger.warning(f"[{channel_name}] ⚠️ Aucun processus ffmpeg trouvé")
                return False

            # Vérifier les erreurs critiques
            if len(pids) > 1:
                logger.error(f"[{channel_name}] ❌ Processus ffmpeg multiples détectés: {pids}")
                return False

            # Vérifier le fichier de log
            if not self._check_ffmpeg_log(channel_name):
                logger.error(f"[{channel_name}] ❌ Erreurs détectées dans les logs ffmpeg")
                return False

            return True

        except Exception as e:
            logger.error(f"[{channel_name}] ❌ Erreur vérification santé: {e}")
            return False

    # MÉTHODE SUPPRIMÉE: check_stream_health()
    # Cette méthode était cassée (référençait self.process qui n'existe pas dans FFmpegMonitor)
    # et redondante avec FFmpegProcessManager.check_stream_health()
    # Le monitoring de la santé des streams est géré par FFmpegProcessManager.

    def ensure_hls_directory(self, channel_name: str = None):
        """Ensure that HLS directory exists for a channel or all channels"""
        self.hls_dir = Path(HLS_DIR)  # Define hls_dir if not already defined
        
        if channel_name:
            # Ensure a specific channel directory exists
            channel_path = Path(self.hls_dir) / channel_name
            channel_path.mkdir(exist_ok=True, parents=True)
            return channel_path
        else:
            # Create/verify main HLS directory
            Path(self.hls_dir).mkdir(exist_ok=True, parents=True)
            return self.hls_dir

    def clean_channel(self, channel_name):
        """Nettoie les segments d'une chaîne spécifique"""
        try:
            # Assure que le répertoire existe avec les bonnes permissions
            self.ensure_hls_directory(channel_name)
            
            channel_dir = os.path.join(self.hls_dir, channel_name)
            if not os.path.exists(channel_dir):
                logger.debug(f"Le dossier {channel_dir} n'existe pas, création...")
                os.makedirs(channel_dir, exist_ok=True)
                return True
            return True
        except Exception as e:
            logger.error(f"❌ Erreur lors du nettoyage du canal {channel_name}: {e}")
            return False

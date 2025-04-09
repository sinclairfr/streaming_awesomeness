# ffmpeg_monitor.py
import psutil
import threading
import time
from pathlib import Path
from config import logger
import os
import signal
from config import FFMPEG_LOG_LEVEL, logger, WATCHERS_LOG_CYCLE
import random

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
                            if f"/hls/{channel_name}/" in cmd_str:
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

    def _watchers_loop(self):
        """Surveille l'activité des viewers plus passivement"""
        last_log_time = 0
        last_health_check = 0
        last_summary_time = 0
        log_cycle = WATCHERS_LOG_CYCLE
        summary_cycle = SUMMARY_CYCLE

        while True:
            try:
                current_time = time.time()

                # Health check espacé (10 minutes)
                if current_time - last_health_check > 600:
                    for channel_name, channel in self.channels.items():
                        if hasattr(channel, "check_stream_health"):
                            try:
                                # On vérifie mais on n'agit pas automatiquement
                                channel.check_stream_health()
                            except Exception as e:
                                logger.error(f"[{channel_name}] ❌ Erreur health check: {e}")
                    last_health_check = current_time

                # Récapitulatif des chaînes
                if current_time - last_summary_time > summary_cycle:
                    self._log_channels_summary()
                    last_summary_time = current_time

                # Log des viewers actifs
                if current_time - last_log_time > log_cycle:
                    active_channels = []
                    for name, channel in sorted(self.channels.items()):
                        if hasattr(channel, "watchers_count") and channel.watchers_count > 0:
                            active_channels.append(f"{name}: {channel.watchers_count}")

                    if active_channels:
                        logger.info(f"👥 Viewers actifs: {', '.join(active_channels)}")
                    last_log_time = current_time

                # Pause plus longue
                time.sleep(15)

            except Exception as e:
                logger.error(f"❌ Erreur watchers_loop: {e}")
                time.sleep(15)
    
    def run_monitor_loop(self):
        """Boucle principale du monitoring avec intervalle plus long"""
        last_check_time = 0
        check_interval = 30  # Vérifier toutes les 30 secondes
        
        while not self.stop_event.is_set():
            try:
                current_time = time.time()
                
                # Vérification des processus avec intervalle
                if current_time - last_check_time >= check_interval:
                    self._check_all_ffmpeg_processes()
                    last_check_time = current_time
                
                # Pause plus longue
                time.sleep(5)
                
            except Exception as e:
                logger.error(f"❌ Erreur monitoring FFmpeg: {e}")
                time.sleep(10)

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

    def check_stream_health(self):
        """Vérifie la santé du stream"""
        if not self.process:
            return False

        # Vérifie si le processus est toujours en cours d'exécution
        if self.process.poll() is not None:
            logger.error(f"Le processus FFmpeg pour {self.channel_name} s'est terminé avec le code {self.process.returncode}")
            return False

        # Vérifie les ressources système
        if not self.check_system_resources():
            return False

        return True

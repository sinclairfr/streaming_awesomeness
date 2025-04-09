# ffmpeg_process_manager.py
import os
import time
import signal
import psutil
import threading
import subprocess
from pathlib import Path
from config import logger
import traceback
from typing import Callable, Optional
from datetime import datetime

# Constante pour le seuil de détection des problèmes
# Plus élevé = plus tolérant aux délais entre segments
CRASH_THRESHOLD = 30  # 30 secondes sans nouveau segment avant d'être considéré comme problématique

class FFmpegProcessManager:
    """
    # Gestion centralisée des processus FFmpeg
    # Démarrage, arrêt, surveillance
    """

    # Registre global des instances
    all_channels = {}

    def __init__(self, channel_name, logger_instance=None):
        self.channel_name = channel_name
        self.process = None
        self.lock = threading.Lock()
        self.monitor_thread = None
        self.stop_monitoring = threading.Event()
        self.last_playback_time = 0
        self.playback_offset = 0
        self.total_duration = 0
        self.logger_instance = logger_instance
        
        # État de santé et compteurs
        self.last_segment_time = time.time()  # Initialisation
        self.crash_count = 0
        self.last_crash_time = 0
        self.health_warnings = 0  # Compteur d'avertissements de santé

        # Callbacks
        self.on_process_died = None
        self.on_position_update = None
        self.on_segment_created = None

        # Enregistrement global
        FFmpegProcessManager.all_channels[channel_name] = self

    def is_running(self) -> bool:
        """Vérifie si le processus est en cours d'exécution"""
        return self.process is not None and self.process.poll() is None

    def get_pid(self) -> Optional[int]:
        """Retourne le PID du processus actuel s'il existe"""
        if self.process:
            return self.process.pid
        return None

    def start_process(self, command, hls_dir) -> bool:
        """Démarre un processus FFmpeg avec la commande spécifiée"""
        with self.lock:
            # Vérifier si un processus est déjà en cours
            if self.is_running():
                logger.warning(f"[{self.channel_name}] Un processus FFmpeg est déjà en cours")
                return False

            # Nettoyer les processus existants
            self._clean_zombie_processes()

            # Créer le dossier HLS si nécessaire
            os.makedirs(hls_dir, exist_ok=True)

            try:
                # Démarrer le processus
                logger.info(f"[{self.channel_name}] 🚀 Démarrage FFmpeg: {' '.join(command[:5])}...")
                self.process = subprocess.Popen(
                    command,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )
                
                # Réinitialiser les compteurs d'état
                self.last_segment_time = time.time()
                self.health_warnings = 0
                
                # Démarrer la surveillance
                self._start_monitoring(hls_dir)
                
                return True
                
            except Exception as e:
                logger.error(f"[{self.channel_name}] ❌ Erreur démarrage FFmpeg: {e}")
                return False

    def stop_process(self, timeout: int = 5) -> bool:
        """Arrête proprement le processus FFmpeg"""
        with self.lock:
            if not self.is_running():
                return True

            try:
                # Arrêter la surveillance
                if self.monitor_thread and self.monitor_thread.is_alive():
                    self.stop_monitoring.set()
                
                pid = self.process.pid if self.process else None
                logger.info(f"[{self.channel_name}] 🛑 Arrêt du processus FFmpeg PID {pid}")

                # Tenter un arrêt propre
                self.process.terminate()
                
                try:
                    self.process.wait(timeout=timeout)
                except subprocess.TimeoutExpired:
                    # Kill forcé si nécessaire
                    logger.warning(f"[{self.channel_name}] ⚠️ Kill forcé du processus FFmpeg")
                    self.process.kill()
                    time.sleep(0.5)

                # Nettoyage final
                self.process = None
                return True

            except Exception as e:
                logger.error(f"[{self.channel_name}] ❌ Erreur arrêt FFmpeg: {e}")
                return False

    def _clean_zombie_processes(self):
        """Nettoie les processus FFmpeg orphelins pour cette chaîne"""
        try:
            pattern = f"/hls/{self.channel_name}/"
            processes_killed = 0

            for proc in psutil.process_iter(["pid", "name", "cmdline"]):
                try:
                    # Vérifier si c'est ffmpeg
                    if "ffmpeg" not in proc.info["name"].lower():
                        continue

                    # Vérifier si c'est pour notre chaîne
                    cmdline = " ".join(proc.info["cmdline"] or [])
                    if pattern not in cmdline:
                        continue

                    # Sauter notre propre processus
                    if self.process and proc.info["pid"] == self.process.pid:
                        continue

                    # Tuer le processus orphelin
                    logger.info(f"[{self.channel_name}] 🧹 Nettoyage processus {proc.info['pid']}")
                    proc.kill()
                    processes_killed += 1

                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
                except Exception as e:
                    logger.warning(f"[{self.channel_name}] ⚠️ Erreur nettoyage: {e}")

            if processes_killed > 0:
                time.sleep(1)  # Attendre que les processus se terminent

        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur nettoyage global: {e}")

    def _start_monitoring(self, hls_dir):
        """Démarre le thread de surveillance"""
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.stop_monitoring.set()
            if self.monitor_thread != threading.current_thread():
                try:
                    self.monitor_thread.join(timeout=3)
                except RuntimeError:
                    pass
            self.stop_monitoring.clear()

        self.monitor_thread = threading.Thread(
            target=self._monitor_process,
            daemon=True
        )
        self.monitor_thread.start()

    def _monitor_process(self):
        """Surveillance du processus FFmpeg"""
        try:
            # Intervalles de vérification
            health_check_interval = 60  # Vérification de santé toutes les 60 secondes (était 30)
            
            last_health_check = time.time()
            
            while not self.stop_monitoring.is_set():
                # 1. Vérification de base: processus toujours en vie?
                if not self.is_running():
                    return_code = self.process.poll() if self.process else -999
                    logger.error(f"[{self.channel_name}] ❌ Processus FFmpeg arrêté (code: {return_code})")
                    if self.on_process_died:
                        self.on_process_died(return_code)
                    break

                # 2. Vérification périodique de la santé
                current_time = time.time()
                if current_time - last_health_check >= health_check_interval:
                    self.check_stream_health()
                    last_health_check = current_time
                
                # Pause courte pour économiser des ressources
                time.sleep(1)
                
        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur surveillance: {e}")
            if self.on_process_died:
                self.on_process_died(-1, str(e))

    def check_stream_health(self) -> bool:
        """
        Vérifie l'état de santé du stream avec une approche plus tolérante
        Retourne False uniquement si problème grave et répété
        """
        # Si le processus n'est pas en cours, pas besoin d'analyser
        if not self.is_running():
            return False
            
        # Vérifier le temps écoulé depuis le dernier segment
        if hasattr(self, "last_segment_time"):
            elapsed = time.time() - self.last_segment_time
            # Augmenter le seuil pour être plus tolérant
            segment_threshold = CRASH_THRESHOLD * 3  # 90 secondes sans segment (était 2x=60s)
            
            if elapsed > segment_threshold:
                # Accumulation d'avertissements plutôt que décision immédiate
                self.health_warnings += 1
                logger.warning(f"[{self.channel_name}] ⚠️ Pas de segment depuis {elapsed:.1f}s (avertissement {self.health_warnings}/3)")
                
                # Décider d'un problème seulement après plusieurs avertissements
                if self.health_warnings >= 3:
                    logger.error(f"[{self.channel_name}] ❌ Problème de santé confirmé après {self.health_warnings} avertissements")
                    # Notifier seulement après confirmation du problème
                    if self.on_process_died:
                        self.on_process_died(-2, "health_check_failed")
                    return False
                
                # Continuer à surveiller si pas assez d'avertissements
                return True
            else:
                # Réinitialiser les avertissements si tout va bien
                if self.health_warnings > 0:
                    logger.info(f"[{self.channel_name}] ✅ Stream de nouveau en bonne santé")
                    self.health_warnings = 0
        
        # Processus en vie et santé OK
        return True

    def on_new_segment(self, segment_path, size):
        """Appelé quand un nouveau segment est créé"""
        # Mise à jour du timestamp de dernier segment
        self.last_segment_time = time.time()
        
        # Réinitialiser le compteur d'avertissements
        self.health_warnings = 0
        
        # Appeler le callback externe si disponible
        if self.on_segment_created:
            self.on_segment_created(segment_path, size)

    def set_playback_offset(self, offset):
        """Définit l'offset de lecture"""
        self.playback_offset = offset

    def get_playback_offset(self):
        """Récupère l'offset de lecture actuel"""
        return self.playback_offset

    def set_total_duration(self, duration):
        """Définit la durée totale de la playlist"""
        self.total_duration = duration
        logger.info(f"[{self.channel_name}] Durée totale définie: {duration:.2f}s")

    def restart_process(self):
        """Redémarre le processus FFmpeg de façon simplifiée"""
        try:
            # Mémoriser l'état et la commande actuelle
            was_running = self.is_running()
            current_offset = self.get_playback_offset() if was_running else 0
            command = self.process.args if self.process else None
            
            if not was_running or not command:
                logger.warning(f"[{self.channel_name}] ⚠️ Impossible de redémarrer: processus inactif ou commande inconnue")
                return False
                
            # Extraction du dossier HLS
            hls_dir = command[-1].rsplit('/', 1)[0]
            
            # Arrêter proprement
            self.stop_process()
            time.sleep(2)
            
            # Redémarrer
            success = self.start_process(command, hls_dir)
            
            if success:
                # Restaurer la position
                self.set_playback_offset(current_offset)
                logger.info(f"[{self.channel_name}] ✅ Redémarrage réussi")
                return True
            else:
                logger.error(f"[{self.channel_name}] ❌ Échec du redémarrage")
                return False
                
        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur lors du redémarrage: {e}")
            return False

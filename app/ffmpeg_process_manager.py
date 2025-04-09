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
            logger.warning(f"[{self.channel_name}] ⚠️ Check de santé: processus inactif")
            return False
            
        # Vérifier le temps écoulé depuis le dernier segment
        if hasattr(self, "last_segment_time"):
            current_time = time.time()
            elapsed = current_time - self.last_segment_time
            # Augmenter le seuil pour être plus tolérant
            segment_threshold = CRASH_THRESHOLD * 3  # 90 secondes sans segment (était 2x=60s)
            
            # Vérifier l'utilisation CPU du processus
            cpu_usage = 0
            memory_usage_mb = 0
            try:
                if self.process:
                    pid = self.process.pid
                    process = psutil.Process(pid)
                    cpu_usage = process.cpu_percent(interval=0.1)
                    memory_usage_mb = process.memory_info().rss / (1024 * 1024)
            except Exception as e:
                logger.debug(f"[{self.channel_name}] Impossible de vérifier les ressources: {e}")
            
            # Vérification des segments dans le dossier HLS et leur date de création
            hls_dir = f"/app/hls/{self.channel_name}"
            ts_files = 0
            latest_segment_time = 0
            oldest_segment_time = current_time
            average_segment_size = 0
            
            try:
                ts_files_list = list(Path(hls_dir).glob("*.ts"))
                ts_files = len(ts_files_list)
                playlist_exists = Path(f"{hls_dir}/playlist.m3u8").exists()
                
                # Analyser les segments pour obtenir plus d'informations
                if ts_files > 0:
                    total_size = 0
                    for segment in ts_files_list:
                        stat = segment.stat()
                        total_size += stat.st_size
                        segment_time = stat.st_mtime
                        if segment_time > latest_segment_time:
                            latest_segment_time = segment_time
                        if segment_time < oldest_segment_time:
                            oldest_segment_time = segment_time
                    
                    average_segment_size = total_size / ts_files if ts_files > 0 else 0
                    segment_age = current_time - latest_segment_time if latest_segment_time > 0 else 0
                    segments_timespan = latest_segment_time - oldest_segment_time if latest_segment_time > oldest_segment_time else 0
                    
                    logger.warning(f"[{self.channel_name}] 📂 Segments HLS: {ts_files} segments, dernier créé il y a {segment_age:.1f}s, taille moyenne: {average_segment_size/1024:.1f}KB")
                    
                    # Vérifier si les segments sont générés mais pas téléchargés
                    if segment_age < 10 and elapsed > segment_threshold:
                        logger.warning(f"[{self.channel_name}] ⚠️ Segments générés mais non téléchargés! Dernier segment créé il y a {segment_age:.1f}s")
                else:
                    logger.warning(f"[{self.channel_name}] 📂 Aucun segment trouvé dans {hls_dir}, playlist: {playlist_exists}")
            except Exception as e:
                logger.warning(f"[{self.channel_name}] Erreur vérification HLS: {e}")
            
            # Collecter des métriques pour les problèmes
            if elapsed > segment_threshold:
                # Log détaillé des métriques système
                logger.warning(f"[{self.channel_name}] ⚠️ Pas de segment traité depuis {elapsed:.1f}s | CPU: {cpu_usage:.1f}% | RAM: {memory_usage_mb:.1f}MB")
                
                # Accumulation d'avertissements plutôt que décision immédiate
                self.health_warnings += 1
                
                # S'assurer que le compteur ne dépasse pas 3 pour la logique de traitement
                health_warnings_for_display = min(self.health_warnings, 3)
                
                logger.warning(f"[{self.channel_name}] 🚨 Avertissement santé {health_warnings_for_display}/3 - Dernier segment il y a {elapsed:.1f}s")
                
                # Vérification des logs FFmpeg pour indices supplémentaires
                try:
                    ffmpeg_log = Path(f"/app/logs/ffmpeg/{self.channel_name}_ffmpeg.log")
                    if ffmpeg_log.exists():
                        # Lire les 5 dernières lignes du log
                        with open(ffmpeg_log, 'r') as f:
                            lines = f.readlines()
                            last_lines = lines[-10:] if len(lines) >= 10 else lines
                            log_excerpt = "".join(last_lines).strip()
                            
                            # Vérifier des problèmes connus dans les logs
                            issues = []
                            if "error" in log_excerpt.lower():
                                issues.append("Erreur FFmpeg détectée")
                            if "dropping" in log_excerpt.lower():
                                issues.append("FFmpeg abandonne des frames")
                            if "delay" in log_excerpt.lower() and "too large" in log_excerpt.lower():
                                issues.append("Délai trop important")
                            if "stalled" in log_excerpt.lower():
                                issues.append("Lecture bloquée")
                            
                            if issues:
                                logger.warning(f"[{self.channel_name}] 📝 Problèmes détectés dans les logs FFmpeg: {', '.join(issues)}")
                                logger.warning(f"[{self.channel_name}] 📝 Extrait des logs FFmpeg:\n{log_excerpt}")
                except Exception as e:
                    logger.debug(f"[{self.channel_name}] Impossible de lire les logs FFmpeg: {e}")
                
                # Décider d'un problème seulement après 3 avertissements
                if self.health_warnings >= 3:
                    # Diagnostic détaillé
                    diagnosis = "Aucun segment traité (génération ou téléchargement)"
                    if ts_files > 0 and (current_time - latest_segment_time) < 30:
                        diagnosis = "Segments générés mais non téléchargés par les clients"
                    elif cpu_usage > 90:
                        diagnosis = f"CPU surchargé ({cpu_usage:.1f}%), FFmpeg ne peut pas générer les segments assez rapidement"
                    elif ts_files == 0:
                        diagnosis = "Aucun segment généré, FFmpeg a peut-être des problèmes avec le fichier source"
                    
                    logger.error(f"[{self.channel_name}] ❌ PROBLÈME DE SANTÉ CONFIRMÉ après {self.health_warnings} avertissements")
                    logger.error(f"[{self.channel_name}] 📊 Résumé: {elapsed:.1f}s sans segment | CPU: {cpu_usage:.1f}% | {ts_files} segments TS")
                    logger.error(f"[{self.channel_name}] 🔍 DIAGNOSTIC: {diagnosis}")
                    
                    # Notifier seulement après confirmation du problème
                    if self.on_process_died:
                        # Structurer les informations de diagnostic pour le callback
                        error_details = {
                            "type": "health_check_failed",
                            "elapsed": elapsed,
                            "cpu_usage": cpu_usage,
                            "segments_count": ts_files,
                            "diagnosis": diagnosis,
                            "average_segment_size": average_segment_size
                        }
                        
                        self.on_process_died(-2, str(error_details))
                    return False
                
                # Continuer à surveiller si pas assez d'avertissements
                return True
            else:
                # Santé normale - log périodique pour surveillance
                if (int(current_time) % 300) < 1:  # Log toutes les ~5 minutes 
                    logger.debug(f"[{self.channel_name}] ✅ Santé OK: dernier segment il y a {elapsed:.1f}s | CPU: {cpu_usage:.1f}%")
                
                # Réinitialiser les avertissements si tout va bien
                if self.health_warnings > 0:
                    logger.info(f"[{self.channel_name}] ✅ Stream de nouveau en bonne santé (dernier segment il y a {elapsed:.1f}s)")
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

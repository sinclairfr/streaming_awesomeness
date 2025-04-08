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


class FFmpegProcessManager:
    """
    # On centralise toute la gestion des processus FFmpeg
    # Démarrage, arrêt, surveillance, nettoyage, etc.
    """

    # Registre de toutes les instances de FFmpegProcessManager (global)
    all_channels = {}

    def __init__(self, channel_name, logger_instance=None):
        self.channel_name = channel_name
        self.process = None
        self.active_pids = set()  # Tous les PIDs FFmpeg associés à cette chaîne
        self.lock = threading.Lock()
        self.monitor_thread = None
        self.stop_monitoring = threading.Event()
        self.last_playback_time = 0
        self.playback_offset = 0
        self.total_duration = 0
        self.logger_instance = logger_instance  # Instance FFmpegLogger, optionnelle
        self.crash_count = 0
        self.last_crash_time = 0

        # Callbacks qu'on peut remplacer depuis l'extérieur
        self.on_process_died = None  # Callback quand le processus meurt
        self.on_position_update = None  # Callback quand la position est mise à jour
        self.on_segment_created = None  # Callback quand un segment est créé

        # Registre global des channels pour retrouver le parent depuis n'importe où
        if not hasattr(FFmpegProcessManager, "all_channels"):
            FFmpegProcessManager.all_channels = {}

    def start_process(self, command, hls_dir):
        """
        # Démarre un processus FFmpeg avec la commande fournie
        # Renvoie True si démarré avec succès, False sinon
        """
        with self.lock:
            logger.debug(f"[{self.channel_name}] 🚀 Démarrage start_process")
            # On nettoie d'abord les processus existants
            self._clean_existing_processes()

            try:
                # On s'assure que le dossier HLS existe
                Path(hls_dir).mkdir(parents=True, exist_ok=True)

                # Préparation du log si disponible
                log_file = None
                log_path = None  # Initialisation de log_path
                if self.logger_instance:
                    log_path = self.logger_instance.get_main_log_file()
                    os.makedirs(os.path.dirname(log_path), exist_ok=True)
                    log_file = open(log_path, "a", buffering=1)
                    logger.info(f"[{self.channel_name}] 📝 Logs FFmpeg -> {log_path}")

                # Configuration de l'environnement
                env = os.environ.copy()
                env["AV_LOG_FORCE_NOCOLOR"] = (
                    "1"  # Évite les problèmes de couleur dans les logs
                )
                if log_path:  # Vérification que log_path est défini
                    env["FFREPORT"] = f"file={log_path}:level=32"  # Log détaillé
                env["TMPDIR"] = (
                    "/tmp"  # S'assure que le dossier temporaire est accessible
                )

                # Lancement du processus
                logger.info(
                    f"[{self.channel_name}] 🚀 Lancement FFmpeg: {' '.join(command)}"
                )

                if log_file:
                    # Avec redirection des logs
                    process = subprocess.Popen(
                        command,
                        stdout=log_file if log_file else subprocess.DEVNULL,
                        stderr=subprocess.STDOUT if log_file else subprocess.DEVNULL,
                        bufsize=1,
                        universal_newlines=True,
                        env=env,  # Ajoute l'environnement
                    )
                else:
                    # Sans redirection (stdout/stderr ignorés)
                    process = subprocess.Popen(
                        command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
                    )

                self.process = process
                self.active_pids.add(process.pid)

                # Vérification rapide du démarrage
                time.sleep(1)
                if self.process.poll() is not None:
                    logger.error(
                        f"[{self.channel_name}] ❌ FFmpeg s'est arrêté immédiatement"
                    )
                    if log_file:
                        log_file.close()
                    return False

                # Démarrage du thread de surveillance
                self._start_monitoring(hls_dir)

                logger.info(
                    f"[{self.channel_name}] ✅ FFmpeg démarré avec PID: {self.process.pid}"
                )
                return True

            except Exception as e:
                logger.error(f"[{self.channel_name}] ❌ Erreur démarrage FFmpeg: {e}")
                logger.error(f"[{self.channel_name}] Détail: {traceback.format_exc()}")
                if "log_file" in locals() and log_file:
                    log_file.close()
                return False

    def stop_process(self):
        """Arrête proprement le processus FFmpeg en cours, sans sauvegarder la position"""
        with self.lock:
            if not self.process:
                return

            try:
                # On arrête la surveillance
                if self.monitor_thread and self.monitor_thread.is_alive():
                    self.stop_monitoring.set()
                    # Évite le join() si c'est le thread courant
                    if self.monitor_thread != threading.current_thread():
                        self.monitor_thread.join(timeout=3)

                pid = self.process.pid
                logger.info(f"[{self.channel_name}] 🛑 Arrêt du processus FFmpeg {pid}")

                # Tentative d'arrêt propre avec SIGTERM
                self.process.terminate()

                # On attend un peu que ça se termine
                for _ in range(5):  # 5 secondes max
                    if self.process.poll() is not None:
                        logger.info(
                            f"[{self.channel_name}] ✅ Processus {pid} terminé proprement"
                        )
                        break
                    time.sleep(1)

                # Si toujours en vie, on force avec SIGKILL
                if self.process.poll() is None:
                    logger.warning(
                        f"[{self.channel_name}] ⚠️ Processus {pid} résistant, envoi de SIGKILL"
                    )
                    self.process.kill()
                    time.sleep(1)

                # On retire ce PID des actifs
                self.active_pids.discard(pid)

                # Nettoyage
                self.process = None

                # On nettoie aussi les autres processus au cas où
                self._clean_orphan_processes()

                logger.info(f"[{self.channel_name}] 🧹 Processus FFmpeg nettoyé")
                return True

            except Exception as e:
                logger.error(f"[{self.channel_name}] ❌ Erreur arrêt FFmpeg: {e}")
                self.process = None
                return False

    def _clean_existing_processes(self):
        """
        # Nettoie tous les processus FFmpeg existants pour cette chaîne
        """
        # Si on a un processus actif, on l'arrête proprement
        if self.process and self.process.poll() is None:
            self.stop_process()

        # On nettoie tous les autres processus qui pourraient traîner
        self._clean_orphan_processes()

    def _clean_orphan_processes(self, force_cleanup=True):
        """Nettoie brutalement les processus FFmpeg orphelins"""
        try:
            # Recherche tous les processus FFmpeg liés à cette chaîne
            pattern = f"/hls/{self.channel_name}/"
            processes_killed = 0

            # IMPROVED: Use a more robust approach to find and kill processes
            for proc in psutil.process_iter(["pid", "name", "cmdline"]):
                try:
                    # Is it ffmpeg?
                    if "ffmpeg" not in proc.info["name"].lower():
                        continue

                    # Is it for our channel?
                    cmdline = " ".join(proc.info["cmdline"] or [])
                    if pattern not in cmdline:
                        continue

                    # Skip our own process if we have one
                    if self.process and proc.info["pid"] == self.process.pid:
                        continue

                    # Kill the orphaned process
                    logger.info(
                        f"[{self.channel_name}] 🔪 Killing orphaned process {proc.info['pid']}"
                    )
                    proc.kill()
                    processes_killed += 1

                    # Small delay between kills
                    if processes_killed > 0:
                        time.sleep(0.5)

                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
                except Exception as e:
                    logger.error(f"[{self.channel_name}] Error killing process: {e}")

            if processes_killed > 0:
                logger.info(
                    f"[{self.channel_name}] 🧹 Cleaned up {processes_killed} orphaned FFmpeg processes"
                )
                # Wait for processes to fully terminate
                time.sleep(1)

        except Exception as e:
            logger.error(
                f"[{self.channel_name}] ❌ Error cleaning orphaned processes: {e}"
            )

    def _start_monitoring(self, hls_dir):
        """Démarre le thread de surveillance du processus FFmpeg"""
        if self.monitor_thread and self.monitor_thread.is_alive():
            # On arrête d'abord le thread existant
            self.stop_monitoring.set()
            # Évite le join sur le thread courant
            if self.monitor_thread != threading.current_thread():
                try:
                    self.monitor_thread.join(timeout=3)
                except RuntimeError:
                    logger.warning(
                        f"[{self.channel_name}] Impossible de joindre le thread (thread courant)"
                    )
            self.stop_monitoring.clear()

        # On crée et démarre le nouveau thread
        self.monitor_thread = threading.Thread(
            target=self._monitor_process, args=(hls_dir,), daemon=True
        )
        self.monitor_thread.start()
        logger.debug(f"[{self.channel_name}] 👀 Surveillance FFmpeg démarrée")

    def _monitor_process(self, hls_dir):
        """Surveille le processus FFmpeg en continu"""
        try:
            # Initialisation des variables de surveillance
            last_health_check = time.time()
            health_check_interval = 30  # Vérification de santé toutes les 30 secondes
            last_segment_check = time.time()
            segment_check_interval = 10  # Vérification des segments toutes les 10 secondes
            
            while not self.stop_monitoring.is_set():
                current_time = time.time()
                
                # 1. Vérification de base du processus
                if not self.process or not self.process.poll() is None:
                    return_code = self.process.poll() if self.process else -999
                    logger.error(f"[{self.channel_name}] ❌ Processus FFmpeg arrêté (code: {return_code})")
                    if self.on_process_died:
                        self.on_process_died(return_code)
                    break

                # 2. Vérification périodique de la santé du stream
                if current_time - last_health_check >= health_check_interval:
                    if not self.check_stream_health():
                        logger.warning(f"[{self.channel_name}] ⚠️ Problème de santé détecté")
                        if self.on_process_died:
                            self.on_process_died(-2)  # Code spécial pour problème de santé
                        break
                    last_health_check = current_time

                # 3. Vérification des segments HLS
                if current_time - last_segment_check >= segment_check_interval:
                    try:
                        segments = list(Path(hls_dir).glob("*.ts"))
                        if segments:
                            # Vérifier l'âge du dernier segment
                            latest_segment = max(segments, key=lambda s: s.stat().st_mtime)
                            segment_age = current_time - latest_segment.stat().st_mtime
                            
                            # Seuil plus tolérant pendant le démarrage
                            startup_duration = current_time - getattr(self, "_startup_time", 0)
                            threshold = 180 if startup_duration < 180 else 300
                            
                            if segment_age > threshold:
                                logger.warning(f"[{self.channel_name}] ⚠️ Dernier segment trop vieux ({segment_age:.1f}s)")
                                if self.on_process_died:
                                    self.on_process_died(-3)  # Code spécial pour segments trop vieux
                                break
                    except Exception as e:
                        logger.error(f"[{self.channel_name}] ❌ Erreur vérification segments: {e}")
                    last_segment_check = current_time

                # 4. Mise à jour de la position de lecture
                if self.on_position_update:
                    self.on_position_update(self.playback_offset)

                # 5. Vérification des ressources système
                try:
                    memory = psutil.virtual_memory()
                    if memory.percent > 90:
                        logger.error(f"[{self.channel_name}] ❌ Mémoire système critique: {memory.percent}%")
                        if self.on_process_died:
                            self.on_process_died(-4)  # Code spécial pour mémoire critique
                        break
                except Exception as e:
                    logger.error(f"[{self.channel_name}] ❌ Erreur vérification mémoire: {e}")

                # Pause entre les vérifications
                time.sleep(1)

        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur surveillance processus: {e}")
            if self.on_process_died:
                self.on_process_died(-1)  # Code d'erreur général

    def set_total_duration(self, duration):
        """
        # Définit la durée totale de la playlist
        """
        self.total_duration = duration

    def set_playback_offset(self, offset):
        """
        # Définit l'offset de lecture avec validation
        """
        # S'assurer que l'offset est valide et dans les limites
        if self.total_duration > 0:
            # Appliquer le modulo pour rester dans les limites
            self.playback_offset = offset % self.total_duration
            logger.debug(
                f"[{self.channel_name}] 🔄 Process manager: offset ajusté à {self.playback_offset:.2f}s (modulo {self.total_duration:.2f}s)"
            )
        else:
            self.playback_offset = offset

        self.last_playback_time = time.time()

    def get_playback_offset(self):
        """
        # Renvoie l'offset de lecture actuel
        """
        if self.total_duration <= 0:
            return 0

        # Calcul de l'offset actuel
        current_time = time.time()
        elapsed = current_time - self.last_playback_time
        current_offset = (self.playback_offset + elapsed) % self.total_duration

        return current_offset

    def is_running(self) -> bool:
        """Vérifie si le processus FFmpeg est en cours d'exécution"""
        # Récupérer le canal IPTV pour ce process manager
        channel = None
        if self.channel_name in FFmpegProcessManager.all_channels:
            channel = FFmpegProcessManager.all_channels[self.channel_name]
            
        # Vérifier si le canal a un processus en cours
        if channel and hasattr(channel, "_current_process") and channel._current_process is not None:
            return channel._current_process.poll() is None
        # Vérifier si le flag de streaming est actif
        elif channel and hasattr(channel, "_streaming_active"):
            return channel._streaming_active
        
        return False

    def restart_process(self):
        """Redémarre proprement le processus FFmpeg"""
        try:
            logger.info(f"[{self.channel_name}] 🔄 Redémarrage propre du processus FFmpeg")
            
            # Sauvegarder l'état actuel
            was_running = self.is_running()
            current_offset = self.get_playback_offset() if was_running else 0
            
            # Arrêter proprement le processus actuel
            if was_running:
                self.stop_process()
                time.sleep(2)  # Attendre que le processus soit bien arrêté
            
            # Vérifier que le processus est bien arrêté
            if self.process and self.process.poll() is None:
                logger.warning(f"[{self.channel_name}] ⚠️ Le processus n'est pas arrêté, tentative de kill forcé")
                try:
                    self.process.kill()
                    time.sleep(1)
                except Exception as e:
                    logger.error(f"[{self.channel_name}] ❌ Erreur lors du kill forcé: {e}")
            
            # Redémarrer avec le même offset
            if self.process:
                command = self.process.args
                hls_dir = command[-1].rsplit('/', 1)[0]  # Extraire le dossier HLS du chemin de sortie
                
                # Vérifier que le dossier HLS existe
                if not os.path.exists(hls_dir):
                    logger.warning(f"[{self.channel_name}] ⚠️ Dossier HLS introuvable, création: {hls_dir}")
                    os.makedirs(hls_dir, exist_ok=True)
                
                # Nettoyer les anciens segments
                for f in os.listdir(hls_dir):
                    if f.endswith('.ts'):
                        try:
                            os.remove(os.path.join(hls_dir, f))
                        except Exception as e:
                            logger.warning(f"[{self.channel_name}] ⚠️ Erreur suppression segment {f}: {e}")
                
                success = self.start_process(command, hls_dir)
                
                if success:
                    # Restaurer la position de lecture
                    self.set_playback_offset(current_offset)
                    logger.info(f"[{self.channel_name}] ✅ Redémarrage réussi, position restaurée: {current_offset:.2f}s")
                    return True
                else:
                    logger.error(f"[{self.channel_name}] ❌ Échec du redémarrage")
                    return False
            
            return False
            
        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur lors du redémarrage: {e}")
            return False

    def check_stream_health(self) -> bool:
        """
        Vérifie la santé du stream et retourne True si tout est OK, False sinon.
        Cette méthode centralise toute la logique de vérification de la santé du stream.
        """
        try:
            # 1. Vérifier si le processus est en cours d'exécution
            if not self.is_running():
                logger.warning(f"[{self.channel_name}] ⚠️ Processus FFmpeg arrêté")
                return False

            # 2. Vérifier les ressources système
            if not self.check_system_resources():
                logger.warning(f"[{self.channel_name}] ⚠️ Problème de ressources système")
                return False

            # 3. Vérifier la présence et la validité des segments HLS
            hls_dir = Path(f"/app/hls/{self.channel_name}")
            if not hls_dir.exists():
                logger.error(f"[{self.channel_name}] ❌ Dossier HLS introuvable")
                return False

            # Vérifier la playlist
            playlist_path = hls_dir / "playlist.m3u8"
            if not playlist_path.exists():
                logger.error(f"[{self.channel_name}] ❌ playlist.m3u8 introuvable")
                return False

            # Vérifier les segments
            segments = list(hls_dir.glob("*.ts"))
            if not segments:
                logger.warning(f"[{self.channel_name}] ⚠️ Aucun segment trouvé")
                return False

            # 4. Vérifier l'âge des segments
            current_time = time.time()
            latest_segment = max(segments, key=lambda s: s.stat().st_mtime)
            segment_age = current_time - latest_segment.stat().st_mtime

            # Seuil plus tolérant pendant la période de démarrage
            startup_duration = current_time - getattr(self, "_startup_time", 0)
            threshold = 180 if startup_duration < 180 else 300  # 3 minutes pendant le démarrage, 5 minutes après

            if segment_age > threshold:
                logger.warning(f"[{self.channel_name}] ⚠️ Segments trop vieux (dernier: {segment_age:.1f}s)")
                return False

            # 5. Vérifier la continuité des segments
            segment_numbers = []
            for seg in segments:
                try:
                    num = int(seg.stem.split('_')[1])
                    segment_numbers.append((num, seg))
                except (ValueError, IndexError):
                    continue

            if segment_numbers:
                segment_numbers.sort(key=lambda x: x[0])
                for i in range(1, len(segment_numbers)):
                    current_num = segment_numbers[i][0]
                    prev_num = segment_numbers[i-1][0]
                    gap = current_num - prev_num
                    if gap > 5:  # Tolérance de 5 segments manquants
                        logger.warning(f"[{self.channel_name}] ⚠️ Saut de segments détecté: {prev_num} -> {current_num}")
                        return False

            # 6. Vérifier les erreurs dans les logs
            if not self._check_ffmpeg_log():
                logger.error(f"[{self.channel_name}] ❌ Erreurs détectées dans les logs FFmpeg")
                return False

            return True

        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur vérification santé: {e}")
            return False

    def _check_ffmpeg_log(self) -> bool:
        """Vérifie les logs FFmpeg pour détecter des erreurs"""
        try:
            log_path = Path(f"/app/logs/ffmpeg/{self.channel_name}_ffmpeg.log")
            if not log_path.exists():
                return True  # Pas de log = pas d'erreur

            with open(log_path, "r") as f:
                content = f.read()
                # Détecter les erreurs critiques
                if "error" in content.lower() or "failed" in content.lower():
                    return False
            return True
        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur lecture log: {e}")
            return False

    def check_system_resources(self) -> bool:
        """Vérifie les ressources système"""
        try:
            # Vérifier la mémoire disponible
            memory = psutil.virtual_memory()
            if memory.percent > 90:  # 90% d'utilisation max
                logger.warning(f"[{self.channel_name}] ⚠️ Mémoire système critique: {memory.percent}%")
                return False

            # Vérifier l'espace disque
            disk = psutil.disk_usage('/')
            if disk.percent > 90:  # 90% d'utilisation max
                logger.warning(f"[{self.channel_name}] ⚠️ Espace disque critique: {disk.percent}%")
                return False

            return True
        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur vérification ressources: {e}")
            return False

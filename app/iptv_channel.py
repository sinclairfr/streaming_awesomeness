# iptv_channel.py
import os
import time
import random
import psutil
import shutil
import subprocess
import threading
import datetime
from pathlib import Path
from typing import Optional
from video_processor import VideoProcessor
from hls_cleaner import HLSCleaner
import json
from ffmpeg_logger import FFmpegLogger
from stream_error_handler import StreamErrorHandler
import signal  # On ajoute l'import signal
import time  # Ajout si nécessaire
from config import (
    TIMEOUT_NO_VIEWERS,
    FFMPEG_LOG_LEVEL,
    logger,
    CONTENT_DIR,
    USE_GPU,
)

class IPTVChannel:
    """
    # On gère une chaîne IPTV, son streaming et sa surveillance
    """
    def __init__(
        self,
        name: str,
        video_dir: str,
        hls_cleaner: HLSCleaner,
        use_gpu: USE_GPU
    ):
        self.name = name
        self.video_dir = video_dir
        self.use_gpu = use_gpu
        self.hls_cleaner = hls_cleaner  # On stocke l'instance partagée du nettoyeur
        self.error_handler = StreamErrorHandler(self.name)

        self.active_ffmpeg_pids = set()  # Pour traquer les PIDs actifs
        self.logger = FFmpegLogger(name)  # Une seule ligne pour gérer tous les logs

        # On initialise le VideoProcessor
        self.processor = VideoProcessor(self.video_dir)

        # Configuration HLS
        self.hls_time = 6
        self.hls_list_size = 20
        self.hls_delete_threshold = 6
        self.target_duration = 8

        # Paramètres d'encodage
        self.video_bitrate = "2800k"
        self.max_bitrate = "2996k"
        self.buffer_size = "4200k"
        self.gop_size = 48
        self.keyint_min = 48
        self.sc_threshold = 0
        self.crf = 22

        # Variables de surveillance
        self.processed_videos = []
        self.restart_count = 0
        self.max_restarts = 5
        self.restart_cooldown = 60
        self.error_count = 0
        self.min_segment_size = 1024
        self.ffmpeg_process = None
        self.stop_event = threading.Event()
        self.lock = threading.Lock()
        self.fallback_mode = False
        self.monitoring_thread = None
        self.last_segment_time = 0
        self.start_offset = 0

        # On scanne les vidéos pour remplir self.processed_videos
        self._scan_videos()
        self.total_duration = None
        
        # On calcule une fois la durée totale
        total_duration = self._calculate_total_duration()
        if total_duration > 0:
            # On définit l'offset aléatoire une seule fois
            self.start_offset = random.uniform(0, total_duration)
            logger.info(f"[{self.name}] Offset initial = {self.start_offset:.2f}s")
        else:
            self.start_offset = 0

        # Gestion de la position de lecture
        self.last_playback_time = time.time()  # Pour calculer le temps écoulé

        # offset
        self.watchers_count = 0
        self.channel_offset = 0.0
        self.last_watcher_time = time.time()  # On initialise au démarrage

        self.current_position = 0  # Pour tracker la position actuelle
        self.last_known_position = 0  # Pour sauvegarder la dernière position connue avant un arrêt

    def _get_duration_with_retry(self, video, max_retries=2) -> float:
        """Tente d'obtenir la durée d'une vidéo via ffprobe, avec plusieurs essais."""
        for i in range(max_retries + 1):
            duration = self._get_duration_once(video)
            if duration > 0:
                return duration
            logger.warning(f"[{self.name}] ⚠️ Tentative {i+1}/{max_retries+1} échouée pour {video}")
            time.sleep(0.5)  # Petit délai entre chaque essai

        return 0.0

    def _get_duration_once(self, video) -> float:
        """Effectue un appel unique à ffprobe pour récupérer la durée d'une vidéo."""
        cmd = [
            "ffprobe", "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            str(video)
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)

        try:
            return float(result.stdout.strip())
        except ValueError:
            return 0.0

    def _calculate_total_duration(self, max_retries=2) -> float:
        """Calcule la durée totale des vidéos traitées. Retente plusieurs fois avant d’abandonner."""
        total_duration = 0.0

        for video in self.processed_videos:
            duration = self._get_duration_with_retry(video, max_retries)
            if duration <= 0:
                logger.error(f"[{self.name}] ❌ Impossible d'obtenir une durée valide pour {video}, on skip.")
            else:
                total_duration += duration

        # Si la durée n'est pas trouvée après plusieurs essais, on met 120s par défaut
        if total_duration <= 0:
            logger.warning(f"[{self.name}] ⚠️ Durée inconnue, utilisation du fallback 120sec.")
            return 120.00

        logger.info(f"[{self.name}] 📊 Durée totale calculée: {total_duration:.2f}s")
        return total_duration

    def _build_input_params(self) -> list:
        """On construit les paramètres d'entrée FFmpeg"""
        params = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel", FFMPEG_LOG_LEVEL,
            "-y",
            "-re",
            "-progress", str(self.logger.get_progress_file()),
            "-fflags", "+genpts+igndts",
        ]

        try:
            # On s'assure que total_duration est valide
            if not self.total_duration:
                self.total_duration = self._calculate_total_duration()
                
            # On vérifie qu'on a bien une durée valide avant de continuer
            if not self.total_duration or self.total_duration <= 0:
                logger.warning(f"[{self.name}] Impossible de calculer la durée totale, on skip l'offset")
                return params

            # On calcule l'offset actuel en tenant compte du temps écoulé
            current_time = time.time()
            elapsed = current_time - self.last_playback_time

            # On s'assure que l'offset est initialisé
            if not hasattr(self, 'playback_offset'):
                logger.info("Offset non intiliaisé, on le définit au hasard.")
                self.playback_offset = random.uniform(0, self.total_duration)

            # On calcule l'offset total
            total_offset = self.playback_offset
            if elapsed > 0:
                total_offset = (self.playback_offset + elapsed) % self.total_duration

            logger.info(f"[{self.name}] Reprise lecture à {total_offset:.2f}s (total: {self.total_duration:.2f}s)")

            if total_offset > 0:
                params.extend(["-ss", f"{total_offset}"])

        except Exception as e:
            logger.error(f"[{self.name}] Erreur calcul offset: {e}, on continue sans.")

        params.extend([
            "-f", "concat",
            "-safe", "0",
            "-stream_loop", "-1",
            "-i", str(self._create_concat_file()),
        ])

        return params
    
    def _build_encoding_params(self) -> list:
            """Paramètres d'encodage simplifiés"""
            if self.fallback_mode:
                return [
                    "-c:v", "h264_vaapi" if self.use_gpu else "libx264",
                    "-profile:v", "main",
                    "-level", "4.1",
                    "-bf", "0",
                    "-bufsize", "5M",
                    "-maxrate", "5M",
                    "-low_power", "1" if self.use_gpu else None,
                    "-c:a", "aac",
                    "-b:a", "192k",
                    "-ar", "48000"
                ]

            # Force conversion for MKV files
            if any(file.suffix.lower() == ".mkv" for file in self.processed_videos):
                logger.info(f"[{self.name}] 📺 MKV détecté, conversion forcée")
                return [
                    "-c:v", "h264_vaapi" if self.use_gpu else "h264",
                    "-profile:v", "main",
                    "-level", "4.1",
                    "-bf", "0",
                    "-bufsize", "5M",
                    "-maxrate", "5M",
                    "-low_power", "1" if self.use_gpu else None,
                    "-c:a", "aac",
                    "-b:a", "192k",
                    "-ar", "48000"
                ]

            return ["-c", "copy"]  # Copy direct pour formats compatibles

    def _build_hls_params(self, hls_dir: str) -> list:
        """On configure les paramètres HLS"""
        params = [
            "-f", "hls",
            "-hls_time", str(self.hls_time),
            "-hls_list_size", "5",  # Réduit la taille de la liste pour éviter l’accumulation de segments
            "-hls_delete_threshold", "1",  # Supprime rapidement les anciens segments
            "-hls_flags", "delete_segments+append_list+program_date_time",
            "-hls_segment_filename", f"{hls_dir}/segment_%d.ts",
            f"{hls_dir}/playlist.m3u8",
        ]

        logger.info(f"[{self.name}] 📜 HLS Params: {' '.join(params)}")
        return params

    def _build_ffmpeg_command(self, hls_dir: str) -> list:
        """
        Builds the complete FFmpeg command by combining input, encoding and HLS parameters.

        This method concatenates the parameters from:
        - _build_input_params(): Input related parameters for FFmpeg
        - _build_encoding_params(): Video/audio encoding parameters
        - _build_hls_params(): HLS streaming specific parameters

        Args:
            hls_dir (str): Directory path where HLS segments will be saved
        Returns:
            list: Complete FFmpeg command as a list of string arguments
        """
        return (
            self._build_input_params() +
            self._build_encoding_params() +
            self._build_hls_params(hls_dir)
        )

    def _monitor_ffmpeg(self, hls_dir: str):
        """Surveille le processus FFmpeg et gère les erreurs"""
        if not self.ffmpeg_process or not self.ffmpeg_process.stderr:
            logger.error(f"[{self.name}] ❌ ffmpeg_process n'est pas initialisé correctement")
            return
    
        last_segment_log = 0
        SEGMENT_LOG_INTERVAL = 30  # Log des segments toutes les 30 secondes
        
        try:
            while self.ffmpeg_process:
                
                current_time = time.time()
                # On met à jour la position de lecture
                if self.logger and self.logger.get_progress_file():
                    self._update_playback_position(self.logger.get_progress_file())
                else:
                    logging.warning("{self.name} - No progress file found for monitoring.")
                      
                # Log périodique des segments
                if current_time - last_segment_log > SEGMENT_LOG_INTERVAL:
                    self._check_segments(hls_dir)
                    last_segment_log = current_time
                
                # Vérifier si le processus est toujours en vie
                if self.ffmpeg_process.poll() is not None:
                    logger.error(f"[{self.name}] ❌ FFmpeg s'est arrêté avec code: {self.ffmpeg_process.returncode}")
                    self.error_handler.add_error("PROCESS_DIED")
                    break

                # Lire la sortie FFmpeg
                for line in iter(self.ffmpeg_process.stderr.readline, b''):
                    if line:
                        line = line.decode('utf-8').strip()
                        if "error" in line.lower():
                            error_type = self._categorize_ffmpeg_error(line)
                            if self.error_handler.add_error(error_type):
                                logger.error(f"[{self.name}] Erreur FFmpeg critique: {line}")
                                self._restart_stream()
                        elif "warning" in line.lower():
                            logger.warning(f"[{self.name}] Warning FFmpeg: {line}")
                        else:
                            logger.debug(f"[{self.name}] FFmpeg: {line}")

                # Vérifier les segments HLS
                self._check_segments(hls_dir)

                # Vérifier les timeouts
                current_time = time.time()
                if self._handle_timeouts(current_time, 300):  # 5 minutes sans nouveaux segments
                    logger.error(f"[{self.name}] ⏱️ Timeout détecté")
                    self._restart_stream()

                # Vérifier l'inactivité des viewers
                if self._check_viewer_inactivity(current_time, 3600):  # 1 heure sans viewer
                    logger.info(f"[{self.name}] 💤 Stream arrêté pour inactivité")
                    self.stop_stream()
                    break

            # Lecture de la sortie FFmpeg
            for line in iter(self.ffmpeg_process.stderr.readline, b''):
                if line:
                    line = line.decode('utf-8').strip()
                    if "error" in line.lower():
                        error_type = self._categorize_ffmpeg_error(line)
                        if self.error_handler.add_error(error_type):
                            logger.error(f"[{self.name}] Erreur FFmpeg critique: {line}")
                            self._restart_stream()
                    elif "warning" in line.lower():
                        logger.warning(f"[{self.name}] Warning FFmpeg: {line}")
                    else:
                        logger.debug(f"[{self.name}] FFmpeg: {line}")

                # Vérifications périodiques
                if self._handle_timeouts(current_time, 300):
                    logger.error(f"[{self.name}] ⏱️ Timeout détecté")
                    self._restart_stream()

                if self._check_viewer_inactivity(current_time, 3600):
                    logger.info(f"[{self.name}] 💤 Stream arrêté pour inactivité")
                    self.stop_stream()
                    break

                time.sleep(1)  # Éviter une utilisation CPU excessive

        except Exception as e:
            logger.error(f"[{self.name}] Erreur monitoring FFmpeg: {e}")
            self.error_handler.add_error(f"MONITOR_ERROR: {str(e)}")
        finally:
            self._clean_processes()

    def _update_playback_position(self, progress_file):
        """Met à jour et log la position de lecture actuelle"""
        if progress_file.exists():
            try:
                with open(progress_file, 'r') as f:
                    content = f.read()
                    if 'out_time_ms=' in content:
                        position_lines = [l for l in content.split('\n') if 'out_time_ms=' in l]
                        if position_lines:
                            ms_value = int(position_lines[-1].split('=')[1])
                            
                            # Correction pour valeurs négatives
                            if ms_value < 0:
                                # Utilise la durée totale calculée (en microsecondes)
                                total_duration_us = self._calculate_total_duration() * 1_000_000
                                # Convertit la position négative en une position valide positive
                                ms_value = total_duration_us + ms_value
                            
                            # Convertit en secondes
                            self.current_position = ms_value / 1_000_000
                            
                            # Log chaque 10 secondes
                            if time.time() % 10 < 1:
                                logger.info(f"⏱️ {self.name} - Position: {self.current_position:.2f}s")
            except Exception as e:
                logger.error(f"Erreur mise à jour position lecture: {e}")

    def _check_segments(self, hls_dir: str) -> bool:
        """Vérifie la génération des segments HLS"""
        try:
            segment_log_path = Path(f"/app/logs/segments/{self.name}_segments.log")
            segment_log_path.parent.mkdir(parents=True, exist_ok=True)
            
            hls_path = Path(hls_dir)
            playlist = hls_path / "playlist.m3u8"
            
            if not playlist.exists():
                logger.error(f"[{self.name}] ❌ playlist.m3u8 introuvable")
                return False
                
            # Lecture de la playlist
            with open(playlist) as f:
                segments = [line.strip() for line in f if line.strip().endswith('.ts')]
                
            if not segments:
                logger.warning(f"[{self.name}] ⚠️ Aucun segment dans la playlist")
                return False
                
            # Log des segments
            current_time = time.strftime("%Y-%m-%d %H:%M:%S")
            log_entry = f"{current_time} - Segments actifs: {len(segments)}\n"
            
            for segment in segments:
                segment_path = hls_path / segment
                if segment_path.exists():
                    size = segment_path.stat().st_size
                    mtime = time.strftime("%H:%M:%S", time.localtime(segment_path.stat().st_mtime))
                    log_entry += f"  - {segment} (Size: {size/1024:.1f}KB, Modified: {mtime})\n"
                else:
                    log_entry += f"  - {segment} (MISSING)\n"
                    
            # Écriture dans le log
            with open(segment_log_path, "a") as f:
                f.write(log_entry)
                f.write("-" * 80 + "\n")
                
            return True
            
        except Exception as e:
            logger.error(f"[{self.name}] Erreur vérification segments: {e}")
            return False

    def _handle_timeouts(self, current_time, crash_threshold):
        """Gère les timeouts et redémarre le stream si nécessaire"""
        if current_time - self.last_segment_time > crash_threshold:
            logger.error(f"🔥 Pas de nouveau segment pour {self.name} depuis {current_time - self.last_segment_time:.1f}s")
            if self.error_handler.add_error("segment_timeout"):
                if self._restart_stream():
                    self.error_handler.reset()
                return True
        return False

    def _check_viewer_inactivity(self, current_time, timeout):
        """Vérifie l'inactivité des viewers et gère l'arrêt du stream"""
        if not hasattr(self, 'last_watcher_time'):
            self.last_watcher_time = current_time
            return False

        # On ne vérifie l'inactivité que si le stream est actif
        if not self.ffmpeg_process:
            return False

        inactivity_duration = current_time - self.last_watcher_time
        
        # On ajoute une marge de sécurité (120s au lieu de 60s)
        if inactivity_duration > timeout + 60:  
            logger.info(f"[{self.name}] ⚠️ Inactivité détectée: {inactivity_duration:.1f}s")
            return True
            
        return False
    
    def _restart_stream(self) -> bool:
        """# On redémarre le stream en forçant le cleanup"""
        try:
            logger.info(f"🔄 Redémarrage du stream {self.name}")

            # On respecte le cooldown
            elapsed = time.time() - getattr(self, "last_restart_time", 0)
            if elapsed < self.restart_cooldown:
                logger.info(
                    f"⏳ Attente du cooldown ({self.restart_cooldown - elapsed:.1f}s)"
                )
                time.sleep(self.restart_cooldown - elapsed)

            self.last_restart_time = time.time()

            # On arrête proprement FFmpeg
            self._clean_processes()
            time.sleep(2)

            # On relance le stream
            return self.start_stream()

        except Exception as e:
            logger.error(f"Erreur lors du redémarrage de {self.name}: {e}")
            return False

    def log_ffmpeg_processes(self):
        """On vérifie et log le nombre de processus FFmpeg uniquement s'il y a un changement"""
        ffmpeg_count = 0
        for proc in psutil.process_iter(attrs=["name", "cmdline"]):
            try:
                if ("ffmpeg" in proc.info["name"].lower() and
                    proc.info.get("cmdline") and  # On vérifie que cmdline existe
                    any(self.name in str(arg) for arg in proc.info["cmdline"])):
                    ffmpeg_count += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        # On stocke le dernier count connu
        if not hasattr(self, '_last_ffmpeg_count'):
            self._last_ffmpeg_count = -1

        # On log uniquement si le nombre a changé
        if ffmpeg_count != self._last_ffmpeg_count:
            logger.warning(f"📊 {self.name}: {ffmpeg_count} processus FFmpeg actifs")
            self._last_ffmpeg_count = ffmpeg_count

    def _clean_processes(self):
        """On nettoie les process avec sauvegarde de l'offset"""
        with self.lock:
            current_pid = self.ffmpeg_process.pid if self.ffmpeg_process else None
            for pid in self.active_ffmpeg_pids.copy():
                if pid != current_pid:  # ⬅️ Ajoute cette vérification
                    try:
                        os.kill(pid, signal.SIGKILL)
                        logger.info(f"🔥 Ancien processus {pid} tué")
                        self.active_ffmpeg_pids.remove(pid)
                    except ProcessLookupError:
                        self.active_ffmpeg_pids.remove(pid)
            try:
                if not self.ffmpeg_process:
                    return
                # Stop FFmpeg process if running
                if self.ffmpeg_process:
                    try:
                        self.ffmpeg_process.terminate()
                        self.ffmpeg_process.wait(timeout=5)
                    except (ProcessLookupError, TimeoutError):
                        if self.ffmpeg_process:
                            self.ffmpeg_process.kill()
                    finally:
                        self.ffmpeg_process = None

                # Wait for monitoring thread to finish if it exists
                if hasattr(self, 'monitor_thread') and self.monitor_thread:
                    try:
                        # Only attempt to join if the thread is not the current thread
                        if self.monitor_thread != threading.current_thread():
                            self.monitor_thread.join(timeout=5)
                    except (RuntimeError, TimeoutError) as e:
                        logger.warning(f"[{self.name}] Could not join monitor thread: {e}")
                    finally:
                        self.monitor_thread = None
                        
                    pid = self.ffmpeg_process.pid
                    logger.info(f"🧹 Nettoyage du process FFmpeg {pid} pour {self.name}")

                # On sauvegarde d'abord la position actuelle
                if hasattr(self, 'current_position') and self.current_position > 0:
                    self.last_known_position = self.current_position
                    logger.info(f"💾 Sauvegarde position {self.name}: {self.last_known_position}s")

                # Dans _clean_processes, avant de kill le process:
                if hasattr(self, 'monitoring_thread') and self.monitoring_thread:
                    self.monitoring_thread.join(timeout=5)
                    self.monitoring_thread = None
                    
                # On essaie d'abord avec sudo kill -15 (SIGTERM)
                try:
                    subprocess.run(['sudo', 'kill', '-15', str(pid)], check=True)
                    time.sleep(2)
                    if not psutil.pid_exists(pid):
                        logger.info(f"✅ Process {pid} terminé proprement avec sudo kill -15")
                        self.ffmpeg_process = None
                        return
                except subprocess.CalledProcessError:
                    pass

                # Si ça ne marche pas, on force avec sudo kill -9 (SIGKILL)
                try:
                    subprocess.run(['sudo', 'kill', '-9', str(pid)], check=True)
                    time.sleep(1)
                    if not psutil.pid_exists(pid):
                        logger.info(f"✅ Process {pid} tué avec sudo kill -9")
                        self.ffmpeg_process = None
                        return
                except subprocess.CalledProcessError:
                    pass

                # En dernier recours, sudo pkill -9 -f
                try:
                    cmd = f"sudo pkill -9 -f 'ffmpeg.*{self.name}'"
                    subprocess.run(cmd, shell=True, check=True)
                    time.sleep(1)
                    logger.info(f"✅ Processus FFmpeg tués avec sudo pkill")
                except subprocess.CalledProcessError as e:
                    logger.error(f"❌ Échec nettoyage final: {e}")

                self.ffmpeg_process = None
                self.active_ffmpeg_pids.clear()

            except Exception as e:
                logger.error(f"Erreur nettoyage pour {self.name}: {e}")
            finally:
                self.ffmpeg_process = None
                
    def _create_concat_file(self) -> Optional[Path]:
        """Crée le fichier de concaténation avec les bons chemins"""
        try:
            logger.info(f"[{self.name}] 🛠️ Création de _playlist.txt")

            processed_dir = Path(CONTENT_DIR) / self.name / "processed"
            concat_file = Path(CONTENT_DIR) / self.name / "_playlist.txt"

            processed_files = sorted(processed_dir.glob("*.mp4")) + sorted(processed_dir.glob("*.mkv"))
            if not processed_files:
                logger.error(f"[{self.name}] ❌ Aucune vidéo dans {processed_dir}")
                return None

            logger.info(f"[{self.name}] 📝 Écriture de _playlist.txt")

            with open(concat_file, "w", encoding="utf-8") as f:
                for video in processed_files:
                    # Escape single quotes in the path
                    escaped_path = str(video.absolute()).replace("'", "'\\''")
                    f.write(f"file '{escaped_path}'\n")
                    logger.info(f"[{self.name}] ✅ Ajout de {video.name}")

            logger.info(f"[{self.name}] 🎥 Playlist créée")
            return concat_file

        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur _playlist.txt: {e}")
            return None
        
    def _verify_playlist(self):
        """On vérifie que le fichier playlist est valide"""
        try:
            playlist_path = Path(f"/app/content/{self.name}/_playlist.txt")
            if not playlist_path.exists():
                logger.error(f"[{self.name}] ❌ _playlist.txt n'existe pas")
                return False

            with open(playlist_path, 'r') as f:
                lines = f.readlines()

            if not lines:
                logger.error(f"[{self.name}] ❌ _playlist.txt est vide")
                return False

            valid_count = 0
            for i, line in enumerate(lines, 1):
                line = line.strip()
                if not line:
                    continue

                if not line.startswith('file'):
                    logger.error(f"[{self.name}] ❌ Ligne {i} invalide: {line}")
                    return False

                try:
                    file_path = line.split("'")[1] if "'" in line else line.split()[1]
                    file_path = Path(file_path)
                    if not file_path.exists():
                        logger.error(f"[{self.name}] ❌ Fichier manquant: {file_path}")
                        return False
                    valid_count += 1
                except Exception as e:
                    logger.error(f"[{self.name}] ❌ Erreur parsing ligne {i}: {e}")
                    return False

            if valid_count == 0:
                logger.error(f"[{self.name}] ❌ Aucun fichier valide dans la playlist")
                return False

            logger.info(f"[{self.name}] ✅ Playlist valide avec {valid_count} fichiers")
            return True

        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur vérification playlist: {e}")
            return False

    def start_stream(self) -> bool:
        """Démarre le stream avec FFmpeg"""
        with self.lock:
            try:
                logger.info(f"[{self.name}] 🚀 Démarrage du stream...")

                # Vérification/création dossier HLS
                hls_dir = Path(f"/app/hls/{self.name}")
                hls_dir.mkdir(parents=True, exist_ok=True)

                # Création playlist
                concat_file = self._create_concat_file()
                if not concat_file or not concat_file.exists():
                    logger.error(f"[{self.name}] ❌ _playlist.txt introuvable")
                    return False

                # Nettoyage anciens processus avec vérification
                current_pid = self.ffmpeg_process.pid if self.ffmpeg_process else None
                for pid in self.active_ffmpeg_pids.copy():
                    if pid != current_pid:
                        try:
                            os.kill(pid, signal.SIGKILL)
                            logger.info(f"🔥 Ancien processus {pid} tué")
                            self.active_ffmpeg_pids.remove(pid)
                        except ProcessLookupError:
                            self.active_ffmpeg_pids.remove(pid)

                time.sleep(3)  # Délai augmenté pour s'assurer du nettoyage

                # Construction et lancement de la commande
                cmd = self._build_ffmpeg_command(hls_dir)
                logger.info(f"[{self.name}] 📝 Commande FFmpeg : {' '.join(cmd)}")

                # Lancement FFmpeg avec redirection des logs
                with open(self.logger.get_main_log_file(), "a", buffering=1) as ffmpeg_log:
                    process = subprocess.Popen(
                        cmd,
                        stdout=ffmpeg_log,
                        stderr=subprocess.STDOUT,
                        bufsize=1,
                        universal_newlines=True
                    )

                self.ffmpeg_process = process  
                self.ffmpeg_pid = process.pid
                self.active_ffmpeg_pids.add(process.pid)
                logger.info(f"[{self.name}] FFmpeg lancé avec PID: {self.ffmpeg_pid}")

                # Vérification rapide du démarrage
                time.sleep(2)
                if self.ffmpeg_process.poll() is not None:
                    logger.error(f"[{self.name}] FFmpeg s'est arrêté immédiatement")
                    self.ffmpeg_process = None
                    return False

                # Attente des premiers segments avec timeout étendu
                start_time = time.time()
                while time.time() - start_time < 60:  # Timeout augmenté à 60s
                    if list(hls_dir.glob("segment_*.ts")):
                        self.stream_start_time = time.time()
                        logger.info(f"✅ FFmpeg démarré pour {self.name} (PID: {self.ffmpeg_pid})")
                        return True
                    else:
                        logger.debug(f"[{self.name}] En attente des segments...")
                    time.sleep(0.5)

                logger.error(f"❌ Timeout en attendant les segments pour {self.name}")
                self.ffmpeg_process = None
                return False

            except Exception as e:
                logger.error(f"Erreur démarrage stream {self.name}: {e}")
                self.ffmpeg_process = None
                return False

    def _scan_videos(self) -> bool:
        """On scanne les fichiers vidéos et met à jour processed_videos en utilisant VideoProcessor"""
        try:
            source_dir = Path(self.video_dir)
            processed_dir = source_dir / "processed"
            processed_dir.mkdir(exist_ok=True)

            video_extensions = (".mp4", ".avi", ".mkv", ".mov")
            source_files = []
            for ext in video_extensions:
                source_files.extend(source_dir.glob(f"*{ext}"))

            if not source_files:
                logger.warning(f"Aucun fichier vidéo dans {self.video_dir}")
                return False

            # On repart d'une liste vide
            self.processed_videos = []

            # Pour chaque vidéo source
            for source in source_files:
                processed_file = processed_dir / f"{source.stem}.mp4"

                # Si déjà dans processed/
                if processed_file.exists():
                    logger.info(f"🔄 Vidéo déjà présente dans processed/: {source.name}")
                    self.processed_videos.append(processed_file)
                    continue

                # On utilise uniquement VideoProcessor pour traiter/vérifier
                if self.processor.is_already_optimized(source):
                    logger.info(f"✅ Vidéo déjà optimisée: {source.name}, copie directe")
                    shutil.copy2(source, processed_file)
                    self.processed_videos.append(processed_file)
                else:
                    processed = self.processor.process_video(source)
                    if processed:
                        self.processed_videos.append(processed)
                    else:
                        logger.error(f"❌ Échec traitement de {source.name}")

            # On vérifie qu'on a des vidéos traitées
            if not self.processed_videos:
                logger.error(f"Aucune vidéo traitée disponible pour {self.name}")
                return False

            self.processed_videos.sort()
            return True

        except Exception as e:
            logger.error(f"Erreur scan des vidéos pour {self.name}: {e}")
            return False

    def stop_stream_if_needed(self):
        """
        Arrête proprement le stream en sauvegardant la position de lecture
        """
        with self.lock:
            if not self.ffmpeg_process and not self.active_ffmpeg_pids:
                return
                
            logger.info(f"[{self.name}] 🛑 Arrêt du stream (dernier watcher: {time.time() - self.last_watcher_time:.1f}s)")
            
            # On sauvegarde la position actuelle
            if hasattr(self, 'playback_offset'):
                current_time = time.time()
                elapsed = current_time - self.last_playback_time
                self.playback_offset = (self.playback_offset + elapsed) % self.total_duration
                self.last_playback_time = current_time
                logger.info(f"[{self.name}] 💾 Position sauvegardée: {self.playback_offset:.1f}s")
            
            # On nettoie d'abord avec _clean_processes
            self._clean_processes()
            
            # On fait un double check des processus par nom
            for proc in psutil.process_iter(attrs=["pid", "name", "cmdline"]):
                try:
                    if "ffmpeg" in proc.info["name"].lower():
                        cmd_str = " ".join(str(arg) for arg in proc.info.get("cmdline", []))
                        if f"/hls/{self.name}/" in cmd_str:
                            logger.warning(f"[{self.name}] 🔥 Process FFmpeg persistant détecté (PID: {proc.pid}), on force l'arrêt")
                            try:
                                os.kill(proc.pid, signal.SIGKILL)
                            except:
                                pass
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
                    
            # On vide le set des PIDs actifs
            self.active_ffmpeg_pids.clear()
            
            # On réinitialise l'attribut ffmpeg_process
            self.ffmpeg_process = None
                    
            # On nettoie les segments HLS si nécessaire
            hls_dir = Path(f"/app/hls/{self.name}")
            if hls_dir.exists():
                for segment in hls_dir.glob("*.ts"):
                    try:
                        segment.unlink()
                    except Exception as e:
                        logger.error(f"[{self.name}] Erreur nettoyage segment {segment}: {e}")
    
    def start_stream_if_needed(self) -> bool:
        with self.lock:
            if self.ffmpeg_process is not None:
                return True  # Déjà en cours

            # On met à jour le moment de reprise
            self.last_playback_time = time.time()
            self.last_watcher_time = time.time()

            # Vérification automatique du dossier HLS
            hls_path = Path(f"/app/hls/{self.name}")
            if not hls_path.exists():
                logger.info(f"[{self.name}] 📂 Création automatique du dossier HLS")
                hls_path.mkdir(parents=True, exist_ok=True)
                os.chmod(hls_path, 0o777)

            # Vérifier la playlist avant de démarrer
            if not self._verify_playlist():
                logger.error(f"[{self.name}] ❌ Vérification de playlist échouée")
                return False

            logger.info(f"[{self.name}] 🔄 Création du fichier _playlist.txt")
            concat_file = self._create_concat_file()

            if not concat_file or not concat_file.exists():
                logger.error(f"[{self.name}] ❌ _playlist.txt est introuvable")
                return False

            return self.start_stream()

    def _check_inactivity(self, current_time: float) -> bool:
        """Vérifie si le flux est réellement inactif"""
        
        # Temps depuis la dernière requête client
        time_since_last_request = current_time - self.last_watcher_time
        
        # Temps depuis le dernier segment demandé
        time_since_last_segment = current_time - getattr(self, 'last_segment_time', 0)
        
        # Si l'un des deux est actif récemment, le flux n'est pas inactif
        if time_since_last_request < TIMEOUT_NO_VIEWERS or time_since_last_segment < TIMEOUT_NO_VIEWERS:
            return False
            
        logger.warning(
            f"⚠️ {self.name} - Inactivité détectée:"
            f"\n- Dernière requête: il y a {time_since_last_request:.1f}s"
            f"\n- Dernier segment: il y a {time_since_last_segment:.1f}s"
        )
        return True
    
    def update_watchers(self, count: int):
        """Mise à jour du nombre de watchers"""
        with self.lock:
            old_count = self.watcher_count
            
            # On ne log et ne fait rien si le count est identique
            if old_count == count:
                # Juste mise à jour du timestamp d'activité
                self.last_watcher_time = time.time()
                return
                
            # Log uniquement si le nombre change
            self.watcher_count = count
            logger.info(f"📊 Mise à jour {self.name}: {old_count} -> {count} watchers")
                
            # Actions basées sur le changement
            if count > 0 and old_count == 0:
                logger.info(f"[{self.name}] 🔥 Premier watcher, démarrage du stream")
                self.start_stream_if_needed()
            elif count == 0 and old_count > 0:
                logger.info(f"[{self.name}] ⚠️ Plus de watchers recensés")
        """Mise à jour du nombre de watchers"""
        with self.lock:
            old_count = self.watcher_count
            self.watcher_count = count
            
            # Mise à jour du timestamp même si le count ne change pas
            self.last_watcher_time = time.time()
            
            if old_count != count:
                logger.info(f"📊 Mise à jour {self.name}: {count} watchers")
                
            # On ne démarre le stream que si c'est le premier watcher
            if count > 0 and old_count == 0:
                logger.info(f"[{self.name}] 🔥 Premier watcher, démarrage du stream")
                self.start_stream_if_needed()
            # On ne stoppe plus automatiquement quand count = 0
            # Le cleanup sera géré par _check_viewer_inactivity
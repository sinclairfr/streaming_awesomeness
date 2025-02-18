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
from config import logger
import json
from ffmpeg_logger import FFmpegLogger
from stream_error_handler import StreamErrorHandler
import signal  # On ajoute l'import signal
import time  # Ajout si nécessaire

class IPTVChannel:
    """
    # On gère une chaîne IPTV, son streaming et sa surveillance
    """

    def __init__(
        self,
        name: str,
        video_dir: str,
        hls_cleaner: HLSCleaner,
        use_gpu: bool = False
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

        # Gestion de la position de lecture
        self.total_duration = self._calculate_total_duration()
        self.playback_offset = random.uniform(0, self.total_duration) if self.total_duration > 0 else 0
        self.last_playback_time = time.time()  # Pour calculer le temps écoulé
        
        # On scanne les vidéos pour remplir self.processed_videos
        self._scan_videos()

        # On calcule une fois la durée totale
        total_duration = self._calculate_total_duration()
        if total_duration > 0:
            # On définit l'offset aléatoire une seule fois
            self.start_offset = random.uniform(0, total_duration)
            logger.info(f"[{self.name}] Offset initial = {self.start_offset:.2f}s")
        else:
            self.start_offset = 0
    
        # offset
        self.watchers_count = 0
        self.channel_offset = 0.0
        self.channel_paused_at = None  # Pour mémoriser le moment où on coupe FFmpeg
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

        # Si la durée n'est pas trouvée après plusieurs essais, on met 3600s par défaut
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
            "-loglevel", "warning",
            "-y",
            "-re",
            "-progress", str(self.logger.get_progress_file()),
            "-fflags", "+genpts+igndts",
        ]
        
        try:
            # On s'assure que total_duration est valide
            if not hasattr(self, 'total_duration') or self.total_duration <= 0:
                self.total_duration = self._calculate_total_duration()
            
            # On calcule l'offset actuel en tenant compte du temps écoulé
            current_time = time.time()
            elapsed = current_time - self.last_playback_time
            
            # On s'assure que l'offset est initialisé
            if not hasattr(self, 'playback_offset'):
                self.playback_offset = random.uniform(0, self.total_duration)
                
            total_offset = self.playback_offset
            if elapsed > 0:
                total_offset = (self.playback_offset + elapsed) % self.total_duration
                
            logger.info(f"[{self.name}] Reprise lecture à {total_offset:.2f}s (total: {self.total_duration:.2f}s)")
            
            if total_offset > 0:
                params.extend(["-ss", f"{total_offset}"])
        except Exception as e:
            logger.error(f"[{self.name}] Erreur calcul offset: {e}")
            # En cas d'erreur, on continue sans offset
        
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
                "-c:v", "h264_nvenc" if self.use_gpu else "libx264",
                "-preset", "medium",
                "-b:v", self.video_bitrate,
                "-c:a", "aac",
                "-b:a", "128k"
            ]
        
        return ["-c", "copy"]  # Copy tout simplement
        
    def _build_hls_params(self, hls_dir: str) -> list:
        """On configure les paramètres HLS"""
        return [
            "-f", "hls",
            "-hls_time", str(self.hls_time),
            "-hls_list_size", str(self.hls_list_size),
            "-hls_delete_threshold", str(self.hls_delete_threshold),
            "-hls_flags", "delete_segments+append_list",
            "-hls_start_number_source", "datetime",
            "-hls_segment_filename", f"{hls_dir}/segment_%d.ts",
            f"{hls_dir}/playlist.m3u8",
        ]

    def _build_ffmpeg_command(self, hls_dir: str) -> list:
        """On construit la commande FFmpeg complète"""
        return (
            self._build_input_params() +
            self._build_encoding_params() +
            self._build_hls_params(hls_dir)
        )
    
    def _monitor_ffmpeg(self, hls_dir: str):
        """On surveille le process FFmpeg et la génération des segments"""
        self.last_segment_time = time.time()
        last_segment_number = -1
        progress_file = self.logger.get_progress_file()
        last_position = 0
        loop_count = 0
        hls_dir = Path(hls_dir)
        crash_threshold = 10
        TIMEOUT_NO_VIEWERS = int(os.getenv("TIMEOUT_NO_VIEWERS", "60"))  # Par défaut 60s
    
        while (
            not self.stop_event.is_set()
            and self.ffmpeg_process
            and self.ffmpeg_process.poll() is None
        ):
            try:
                current_time = time.time()

                # Ajout du monitoring des processus FFmpeg
                ffmpeg_count = 0
                for proc in psutil.process_iter(attrs=["name", "cmdline"]):
                    try:
                        if ("ffmpeg" in proc.info["name"].lower() and 
                            proc.info.get("cmdline") and
                            any(self.name in str(arg) for arg in proc.info["cmdline"])):
                            ffmpeg_count += 1
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        continue
                
                if ffmpeg_count != getattr(self, '_last_ffmpeg_count', -1):
                    logger.warning(f"📊 {self.name}: {ffmpeg_count} processus FFmpeg actifs")
                    self._last_ffmpeg_count = ffmpeg_count

                # Lecture du fichier de progression et log toutes les 10s
                if progress_file.exists():
                    with open(progress_file, 'r') as f:
                        content = f.read()
                        if 'out_time_ms=' in content:
                            position_lines = [l for l in content.split('\n') if 'out_time_ms=' in l]
                            if position_lines:
                                self.current_position = int(position_lines[-1].split('=')[1]) // 1000000
                                if time.time() % 10 < 1:  # Log toutes les ~10s
                                    logger.info(f"⏱️ {self.name} - Position actuelle: {self.current_position}s, Dernière position sauvegardée: {self.last_known_position}s")

                # Vérification segments
                segments = list(Path(hls_dir).glob("*.ts"))
                if segments:
                    newest_segment = max(segments, key=lambda x: x.stat().st_mtime)
                    if newest_segment.stat().st_mtime > self.last_segment_time:
                        self.last_segment_time = newest_segment.stat().st_mtime
                        # On sauvegarde la dernière position connue
                        self.last_known_position = self.current_position
                        logger.debug(f"[{self.name}] 💾 Position mise à jour: {self.last_known_position}s")

                # Vérifications timeout
                if current_time - self.last_segment_time > crash_threshold:
                    logger.error(f"🔥 Pas de nouveau segment pour {self.name} depuis {current_time - self.last_segment_time:.1f}s")
                    if self.error_handler.add_error("segment_timeout"):
                        if self._restart_stream():
                            self.error_handler.reset()

                # Check inactivité viewers
                if hasattr(self, 'last_watcher_time') and (current_time - self.last_watcher_time) > TIMEOUT_NO_VIEWERS:
                    logger.info(f"⏹️ Sauvegarde position {self.current_position}s et arrêt FFmpeg pour {self.name}")
                    self.last_known_position = self.current_position  # On sauvegarde la position
                    self._clean_processes()
                    return

                time.sleep(1)

            except Exception as e:
                logger.error(f"Erreur monitoring {self.name}: {e}")
                time.sleep(1)

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
            try:
                if not self.ffmpeg_process:
                    return
                    
                pid = self.ffmpeg_process.pid
                logger.info(f"🧹 Nettoyage du process FFmpeg {pid} pour {self.name}")

                # On sauvegarde d'abord la position actuelle
                if hasattr(self, 'current_position') and self.current_position > 0:
                    self.last_known_position = self.current_position
                    logger.info(f"💾 Sauvegarde position {self.name}: {self.last_known_position}s")

                # D'abord on essaie gentiment avec SIGTERM
                try:
                    self.ffmpeg_process.terminate()
                    try:
                        self.ffmpeg_process.wait(timeout=5)
                        logger.info(f"✅ Process {pid} terminé proprement")
                        self.ffmpeg_process = None
                        return
                    except subprocess.TimeoutExpired:
                        pass
                except:
                    pass

                # Si ça marche pas, SIGKILL
                try:
                    os.kill(pid, signal.SIGKILL)
                    time.sleep(2)
                    if not psutil.pid_exists(pid):
                        logger.info(f"✅ Process {pid} tué avec SIGKILL")
                        self.ffmpeg_process = None
                        return
                except:
                    pass

                # En dernier recours, on utilise pkill
                try:
                    subprocess.run(f"pkill -9 -f 'ffmpeg.*{self.name}'", shell=True)
                    time.sleep(2)
                    if not psutil.pid_exists(pid):
                        logger.info(f"✅ Process {pid} tué avec pkill")
                        self.ffmpeg_process = None
                        return
                except:
                    pass

                # Si on arrive ici et que c'est un zombie, on l'ignore
                try:
                    process = psutil.Process(pid)
                    if process.status() == "zombie":
                        logger.info(f"💀 Process {pid} est un zombie, on l'ignore")
                        self.ffmpeg_process = None
                        return
                except:
                    pass

                logger.warning(f"⚠️ Impossible de tuer le process {pid} pour {self.name}")
                
            except Exception as e:
                logger.error(f"Erreur nettoyage pour {self.name}: {e}")
            finally:
                self.ffmpeg_process = None                
    
    def _create_concat_file(self) -> Optional[Path]:
        """Crée le fichier de concaténation avec les bons chemins"""
        try:
            logger.info(f"[{self.name}] 🛠️ Création de _playlist.txt")
            
            # On utilise des chemins absolus
            processed_dir = Path("/app/content") / self.name / "processed"
            concat_file = Path("/app/content") / self.name / "_playlist.txt"
            
            processed_files = sorted(processed_dir.glob("*.mp4"))
            if not processed_files:
                logger.error(f"[{self.name}] ❌ Aucune vidéo dans {processed_dir}")
                return None

            logger.info(f"[{self.name}] 📝 Écriture de _playlist.txt")
            
            with open(concat_file, "w", encoding="utf-8") as f:
                for video in processed_files:
                    # On utilise le chemin absolu complet
                    f.write(f"file '{str(video.absolute())}'\n")
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

                # On nettoie d'abord les anciens processus
                for pid in self.active_ffmpeg_pids.copy():
                    try:
                        os.kill(pid, signal.SIGKILL)
                        logger.info(f"🔥 Ancien processus {pid} tué au démarrage")
                        self.active_ffmpeg_pids.remove(pid)
                    except ProcessLookupError:
                        self.active_ffmpeg_pids.remove(pid)

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

                self.ffmpeg_pid = process.pid
                self.active_ffmpeg_pids.add(process.pid)
                logger.info(f"[{self.name}] FFmpeg lancé avec PID: {self.ffmpeg_pid}")

                # Vérification rapide du démarrage
                time.sleep(2)
                if process.poll() is not None:
                    logger.error(f"[{self.name}] FFmpeg s'est arrêté immédiatement")
                    return False

                # Attente des premiers segments
                start_time = time.time()
                while time.time() - start_time < 20:
                    if list(hls_dir.glob("segment_*.ts")):
                        self.stream_start_time = time.time()
                        self.ffmpeg_process = process
                        logger.info(f"✅ FFmpeg démarré pour {self.name} (PID: {self.ffmpeg_pid})")
                        return True
                    time.sleep(0.5)

                logger.error(f"❌ Timeout en attendant les segments pour {self.name}")
                return False

            except Exception as e:
                logger.error(f"Erreur démarrage stream {self.name}: {e}")
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

    def _create_channel_directory(self):
        """# On crée le dossier HLS de la chaîne s'il n'existe pas"""
        Path(f"hls/{self.name}").mkdir(parents=True, exist_ok=True)

    def stop_stream_if_needed(self):
        """
        Arrête le stream si FFmpeg tourne encore.
        """
        with self.lock:
            if self.ffmpeg_process:
                # On mémorise la position actuelle avant l'arrêt
                current_time = time.time()
                elapsed = current_time - self.last_playback_time
                self.playback_offset = (self.playback_offset + elapsed) % self.total_duration
                self.last_playback_time = current_time
                
                logger.info(f"Arrêt de FFmpeg pour {self.name} (offset mémorisé: {self.playback_offset:.2f}s)")
                self._clean_processes()
                
    def start_stream_if_needed(self) -> bool:
        with self.lock:
            if self.ffmpeg_process is not None:
                return True  # Déjà en cours

            # On met à jour le moment de reprise
            self.last_playback_time = time.time()
            # On réinitialise aussi le temps du dernier watcher
            self.last_watcher_time = time.time()  # Ajout de cette ligne
            # 🔹 Vérification automatique du dossier HLS
            hls_path = Path(f"/app/hls/{self.name}")
            if not hls_path.exists():
                logger.info(f"[{self.name}] 📂 Création automatique du dossier HLS")
                hls_path.mkdir(parents=True, exist_ok=True)
                os.chmod(hls_path, 0o777)

            logger.info(f"[{self.name}] 🔄 Création du fichier _playlist.txt AVANT lancement du stream")
            concat_file = self._create_concat_file()
            
            if not concat_file or not concat_file.exists():
                logger.error(f"[{self.name}] ❌ _playlist.txt est introuvable, le stream NE PEUT PAS démarrer")
                return False

            return self.start_stream()

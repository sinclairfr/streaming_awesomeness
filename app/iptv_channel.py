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

        self.active_ffmpeg_pids = set()  # Pour traquer les PIDs actifs

        # On initialise le VideoProcessor
        self.processor = VideoProcessor(self.video_dir)

        # On paramètre les logs FFmpeg
        self.ffmpeg_log_dir = Path("logs/ffmpeg")
        self.ffmpeg_log_dir.mkdir(parents=True, exist_ok=True)
        self.ffmpeg_log_file = self.ffmpeg_log_dir / f"{self.name}_ffmpeg.log"

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
        self.max_restarts = 3
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
        self.last_watcher_time = 0
        self.channel_offset = 0.0
        self.channel_paused_at = None  # Pour mémoriser le moment où on coupe FFmpeg

    def _calculate_total_duration(self) -> float:
        """# Calcule la somme des durées de toutes les vidéos traitées pour cette chaîne."""
        total_duration = 0.0
        for video in self.processed_videos:
            try:
                cmd = [
                    "ffprobe", "-v", "error",
                    "-show_entries", "format=duration",
                    "-of", "default=noprint_wrappers=1:nokey=1",
                    str(video)
                ]
                result = subprocess.run(cmd, capture_output=True, text=True)
                duration = float(result.stdout.strip())
                total_duration += duration
            except Exception as e:
                logger.error(f"[{self.name}] Erreur durant la lecture de {video}: {e}")
        return total_duration
           
    def _build_ffmpeg_command(self, hls_dir: str) -> list:
        """
        # On construit la commande FFmpeg pour streamer
        # hls_dir est le dossier de sortie HLS
        """
        self.ffmpeg_log_file.parent.mkdir(parents=True, exist_ok=True)
        self.ffmpeg_log_file.touch(exist_ok=True)
        
        # Define the progress file path
        progress_file = self.ffmpeg_log_dir / f"{self.name}_progress.log"

        # Ensure the directory for the progress file exists
        progress_file.parent.mkdir(parents=True, exist_ok=True)

        # Create the progress file if it doesn't exist
        progress_file.touch(exist_ok=True)


        base_cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel", "warning",
            "-y",
            "-re",
            "-progress", str(progress_file),  # On ajoute le tracking de progression
            "-fflags", "+genpts+igndts",
        ]

        # On applique l'offset si défini
        if self.start_offset > 0:
            logger.info(f"[{self.name}] Lancement avec un offset de {self.start_offset:.2f}s")
            base_cmd.extend(["-ss", f"{self.start_offset}"])

        base_cmd.extend([
            "-f", "concat",
            "-safe", "0",
            "-stream_loop", "-1",  # Doit rester ici car c'est une option d'entrée
            "-i", str(self._create_concat_file()),
        ])

        # On choisit le mode encodage complet (fallback) ou copie directe
        if self.fallback_mode:
            # Mode ré-encodage
            encoding_params = [
                "-map", "0:v:0",
                "-map", "0:a:0",
                "-vf", "scale=w=1280:h=720:force_original_aspect_ratio=decrease",
                "-c:v", "h264_nvenc" if self.use_gpu else "libx264",
                "-profile:v", "main",
                "-b:v", self.video_bitrate,
                "-maxrate", self.max_bitrate,
                "-bufsize", self.buffer_size,
                "-crf", str(self.crf),
                "-g", str(self.gop_size),
                "-keyint_min", str(self.keyint_min),
                "-sc_threshold", str(self.sc_threshold),
                "-c:a", "aac",
                "-ar", "48000",
                "-b:a", "128k",
            ]
        else:
            # Mode copie
            encoding_params = [
                "-c:v", "copy",
                "-c:a", "copy",
            ]

        hls_params = [
            "-f", "hls",
            "-hls_time", str(self.hls_time),
            "-hls_list_size", str(self.hls_list_size),
            "-hls_delete_threshold", str(self.hls_delete_threshold),
            "-hls_flags", "delete_segments+append_list",
            "-hls_start_number_source", "datetime",
            "-hls_segment_filename", f"{hls_dir}/segment_%d.ts",
            f"{hls_dir}/playlist.m3u8",
        ]

        return base_cmd + encoding_params + hls_params

    def _start_ffmpeg_process(self, cmd: list) -> Optional[subprocess.Popen]:
        """
        # On lance le process FFmpeg et on vérifie l'apparition des segments
        """
        try:
            logger.info(f"🚀 Démarrage FFmpeg pour {self.name}")
            logger.info(f"[{self.name}] FFmpeg command: {' '.join(cmd)}")

            # On nettoie d'abord les anciens processus
            for pid in self.active_ffmpeg_pids.copy():
                try:
                    os.kill(pid, signal.SIGKILL)
                    logger.info(f"🔥 Ancien processus {pid} tué au démarrage")
                    self.active_ffmpeg_pids.remove(pid)
                except ProcessLookupError:
                    self.active_ffmpeg_pids.remove(pid)
                    
            # Vérifier que le dossier des logs FFmpeg existe
            self.ffmpeg_log_file.parent.mkdir(parents=True, exist_ok=True)
            self.ffmpeg_log_file.touch(exist_ok=True)

            # Lancer FFmpeg
            with open(self.ffmpeg_log_file, "a", buffering=1) as ffmpeg_log:
                process = subprocess.Popen(
                    cmd,
                    stdout=ffmpeg_log,
                    stderr=subprocess.STDOUT,  # Capture stderr dans stdout
                    bufsize=1,
                    universal_newlines=True
                )

            self.ffmpeg_pid = process.pid
            self.active_ffmpeg_pids.add(process.pid)
            logger.info(f"[{self.name}] FFmpeg lancé avec PID: {self.ffmpeg_pid}")

            # Vérification du process FFmpeg
            time.sleep(2)  # Attendre un minimum pour voir si FFmpeg survit
            if process.poll() is not None:
                logger.error(f"[{self.name}] ❌ FFmpeg s'est arrêté immédiatement. Vérifie {self.ffmpeg_log_file}")
                return None

            # Vérification de l'apparition des segments HLS
            start_time = time.time()
            hls_dir = Path(f"hls/{self.name}")

            while time.time() - start_time < 20:  # Timeout prolongé à 20 secondes
                if list(hls_dir.glob("segment_*.ts")):
                    self.stream_start_time = time.time()
                    logger.info(f"✅ FFmpeg démarré pour {self.name} (PID: {self.ffmpeg_pid})")
                    return process
                time.sleep(0.5)  # Vérification moins agressive

            # Timeout si aucun segment généré
            logger.error(f"❌ Timeout en attendant les segments pour {self.name}")
            if process.poll() is None:
                logger.warning(f"[{self.name}] FFmpeg tourne encore mais n'a pas généré de segments.")
            return None

        except Exception as e:
            logger.error(f"Erreur démarrage FFmpeg pour {self.name}: {e}")
            return None

    def _monitor_ffmpeg(self, hls_dir: str):
        """# On surveille le process FFmpeg et la génération des segments"""
        self.last_segment_time = time.time()
        last_segment_number = -1
        progress_file = self.ffmpeg_log_dir / f"{self.name}_progress.log"
        last_position = 0
        loop_count = 0
        hls_dir = Path(hls_dir)
        crash_threshold = 10
        timeout_no_viewers = 60  

        while (
            not self.stop_event.is_set()
            and self.ffmpeg_process
            and self.ffmpeg_process.poll() is None
        ):
            try:
                current_time = time.time()

                # On lit le fichier de progression
                if progress_file.exists():
                    with open(progress_file, 'r') as f:
                        content = f.read()
                        if 'out_time_ms=' in content:
                            # On extrait la position courante
                            position_lines = [l for l in content.split('\n') if 'out_time_ms=' in l]
                            if position_lines:
                                current_position = int(position_lines[-1].split('=')[1]) // 1000000  # Conversion en secondes
                                
                                # Si on détecte un retour au début (position actuelle < position précédente)
                                if current_position < last_position:
                                    loop_count += 1
                                    logger.warning(f"🔄 [{self.name}] Boucle #{loop_count} - Redémarrage de la playlist")
                                
                                last_position = current_position

                segments = sorted(hls_dir.glob("segment_*.ts"))

                if segments:
                    newest_segment = max(segments, key=lambda x: x.stat().st_mtime)
                    try:
                        current_segment = int(newest_segment.stem.split("_")[1])
                        segment_size = newest_segment.stat().st_size

                        with open(self.ffmpeg_log_dir / f"{self.name}_segments.log", "a") as seg_log:
                            seg_log.write(
                                f"{datetime.datetime.now()} - "
                                f"Segment {current_segment}: {segment_size} bytes\n"
                            )

                        if segment_size < self.min_segment_size:
                            logger.warning(
                                f"⚠️ Segment {newest_segment.name} trop petit ({segment_size} bytes)"
                            )
                            self.error_count += 1
                        else:
                            if current_segment != last_segment_number:
                                self.last_segment_time = current_time
                                last_segment_number = current_segment
                                self.error_count = 0

                    except ValueError:
                        logger.error(f"Format de segment invalide: {newest_segment.name}")
                        self.error_count += 1
                else:
                    self.error_count += 1

                # Vérification si plus de watchers depuis 60s
                if hasattr(self, 'last_watcher_time') and (current_time - self.last_watcher_time) > timeout_no_viewers:
                    logger.info(f"⏹️ Arrêt FFmpeg pour {self.name} (aucun watcher depuis {timeout_no_viewers}s).")
                    self._clean_processes()
                    return  

                elapsed = current_time - self.last_segment_time
                if elapsed > crash_threshold:
                    logger.error(f"🔥 Pas de nouveau segment pour {self.name} depuis {elapsed:.1f}s")
                    if self.restart_count < self.max_restarts:
                        self.restart_count += 1
                        logger.info(f"🔄 Tentative de redémarrage {self.restart_count}/{self.max_restarts}")
                        if self._restart_stream():
                            self.error_count = 0
                        else:
                            logger.error(f"❌ Échec du redémarrage pour {self.name}")
                    else:
                        logger.critical(f"⛔️ Nombre maximum de redémarrages atteint pour {self.name}")

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
        """Nettoyage plus agressif des processus FFmpeg"""
        with self.lock:
            try:
                # On utilise pkill pour tuer tous les processus FFmpeg de cette chaîne
                cleanup_cmd = f"pkill -9 -f 'ffmpeg.*{self.name}'"
                subprocess.run(cleanup_cmd, shell=True)
                
                # On attend un peu
                time.sleep(1)
                
                # Vérification finale
                ffmpeg_count = 0
                for proc in psutil.process_iter(["name", "cmdline"]):
                    try:
                        if ("ffmpeg" in proc.info["name"].lower() and 
                            proc.info.get("cmdline") and
                            any(self.name in str(arg) for arg in proc.info["cmdline"])):
                            ffmpeg_count += 1
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        continue
                        
                if ffmpeg_count > 0:
                    logger.error(f"⚠️ {ffmpeg_count} processus FFmpeg restants pour {self.name}")
                else:
                    logger.info(f"✅ Tous les processus FFmpeg arrêtés pour {self.name}")
                    
                self.ffmpeg_process = None
                self.ffmpeg_pid = None
                
            except Exception as e:
                logger.error(f"❌ Erreur nettoyage FFmpeg pour {self.name}: {e}")

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

    def start_stream(self) -> bool:
        """ Démarre le stream avec FFmpeg """
        logger.info(f"[{self.name}] 🚀 start_stream() appelé !")
        self.ffmpeg_log_file.parent.mkdir(parents=True, exist_ok=True)
        self.ffmpeg_log_file.touch(exist_ok=True)

        with self.lock:
            logger.info(f"[{self.name}] 🔄 Tentative de démarrage du stream...")

            # Vérification que le dossier HLS existe
            hls_dir = Path(f"/app/hls/{self.name}")
            hls_dir.mkdir(parents=True, exist_ok=True)

            # Vérifier si _playlist.txt est bien généré
            concat_file = self._create_concat_file()
            if not concat_file or not concat_file.exists():
                logger.error(f"[{self.name}] ❌ _playlist.txt introuvable, arrêt du stream.")
                return False

            # Construire la commande FFmpeg
            cmd = self._build_ffmpeg_command(hls_dir)
            logger.debug(f"[{self.name}] 📝 Commande FFmpeg : {' '.join(cmd)}")

            self.ffmpeg_process = self._start_ffmpeg_process(cmd)
            return self.ffmpeg_process is not None

    def _scan_videos(self) -> bool:
        """
        # On scanne les fichiers vidéos et on met à jour processed_videos
        # On effectue la normalisation si nécessaire
        """
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

            # On vérifie les fichiers déjà normalisés
            self.processed_videos = []

            for source in source_files:
                processed_file = processed_dir / f"{source.stem}.mp4"

                # ⚠️ Vérifier si le fichier est déjà dans processed/
                if processed_file.exists():
                    logger.info(f"🔄 Vidéo déjà présente dans processed/, pas besoin de traitement : {source.name}")
                    self.processed_videos.append(processed_file)
                    continue

                # Vérification avec video_processor
                if self.processor.is_already_optimized(source):
                    logger.info(f"✅ Vidéo déjà optimisée : {source.name}, copie directe.")
                    shutil.copy2(source, processed_file)
                    self.processed_videos.append(processed_file)
                else:
                    # Lancer la normalisation via video_processor
                    processed = self.processor.process_video(source)
                    if processed:
                        self.processed_videos.append(processed)


            if not self.processed_videos:
                logger.error(f"Aucune vidéo traitée disponible pour {self.name}")
                return False

            self.processed_videos.sort()
            return True

        except Exception as e:
            logger.error(f"Erreur lors du scan des vidéos pour {self.name}: {e}")
            return False

    def _is_already_normalized(self, video_path: Path) -> bool:
        """
        Vérifie si une vidéo est déjà en H.264, ≤ 1080p, ≤ 30 FPS, et en AAC.
        Si oui, la normalisation est inutile.
        """
        import json

        logger.info(f"🔍 Vérification du format de {video_path.name}")

        cmd = [
            "ffprobe", "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "stream=codec_name,width,height,r_frame_rate",
            "-show_entries", "stream=codec_name:stream=sample_rate",
            "-of", "json",
            str(video_path)
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)

        try:
            video_info = json.loads(result.stdout)
            streams = video_info.get("streams", [])

            if not streams:
                logger.warning(f"⚠️ Impossible de lire {video_path}, normalisation forcée.")
                return False

            video_stream = streams[0]
            codec = video_stream.get("codec_name", "").lower()
            width = int(video_stream.get("width", 0))
            height = int(video_stream.get("height", 0))
            framerate = video_stream.get("r_frame_rate", "0/1").split("/")
            fps = round(int(framerate[0]) / int(framerate[1])) if len(framerate) == 2 else 0

            audio_codec = None
            for stream in streams:
                if stream.get("codec_name") and stream.get("codec_name") != codec:
                    audio_codec = stream.get("codec_name").lower()

            # Vérification des critères
            if codec == "h264" and width <= 1920 and height <= 1080 and fps <= 30 and (audio_codec is None or audio_codec == "aac"):
                return True  # ✅ Vidéo optimisée, pas besoin de normaliser

            return False  # ❌ Vidéo à normaliser

        except json.JSONDecodeError as e:
            logger.error(f"❌ Erreur JSON avec ffprobe: {e}")
            return False

    def _process_video(self, source: Path, dest: Path) -> bool:
        """
        # On normalise/transcode une vidéo source et on vérifie la validité du résultat
        """
        try:
            logger.info(f"Début de la normalisation de {source.name}")

            # On force la normalisation
            cmd = ["ffprobe",
                   "-v", "error",
                   "-select_streams", "v:0",
                   "-show_entries", "stream=codec_name,width,height,r_frame_rate,duration",
                   "-of", "json",
                   str(source)]
            probe_result = subprocess.run(cmd, capture_output=True, text=True)
            if probe_result.returncode != 0:
                logger.error(f"Erreur analyse {source.name}: {probe_result.stderr}")
                return False

            source_info = json.loads(probe_result.stdout)
            if not source_info.get("streams"):
                logger.error(f"Pas de flux vidéo dans {source.name}")
                return False

            # On construit la commande FFmpeg
            temp_output = dest.parent / f"temp_{dest.name}"
            ffmpeg_cmd = ["ffmpeg", "-y"]

            if source.suffix.lower() == ".mkv":
                ffmpeg_cmd.extend([
                    "-fflags", "+genpts+igndts",
                    "-analyzeduration", "100M",
                    "-probesize", "100M"
                ])

            ffmpeg_cmd.extend(["-i", str(source)])

            if self.use_gpu:
                ffmpeg_cmd.extend([
                    "-c:v", "h264_nvenc",
                    "-preset", "p4",
                    "-profile:v", "high",
                ])
            else:
                ffmpeg_cmd.extend(["-c:v", "libx264", "-preset", "medium", "-crf", "23"])

            ffmpeg_cmd.extend([
                "-pix_fmt", "yuv420p",
                "-movflags", "+faststart",
                "-max_muxing_queue_size", "1024",
                "-c:a", "aac",
                "-b:a", "192k",
                "-ar", "48000",
                "-ac", "2",
                str(temp_output)
            ])

            logger.info(f"Démarrage FFmpeg pour {source.name}")
            process = subprocess.run(ffmpeg_cmd, capture_output=True, text=True)
            if process.returncode != 0:
                logger.error(f"Erreur FFmpeg pour {source.name}: {process.stderr}")
                if temp_output.exists():
                    temp_output.unlink()
                return False

            if not self._verify_transcoding(temp_output):
                logger.error(f"Vérification transcoding échouée pour {source.name}")
                temp_output.unlink()
                return False

            # On renomme le fichier temporaire
            temp_output.rename(dest)
            logger.info(f"✅ Normalisation réussie: {source.name} -> {dest.name}")
            return True

        except Exception as e:
            logger.error(f"Erreur traitement {source.name}: {e}")
            if "temp_output" in locals() and temp_output.exists():
                temp_output.unlink()
            return False

    def _verify_transcoding(self, output_file: Path) -> bool:
        """# On vérifie la validité du fichier de sortie"""
        try:
            if output_file.stat().st_size < 1024 * 1024:
                logger.error(f"Fichier trop petit: {output_file.stat().st_size} bytes")
                return False

            probe_cmd = [
                "ffprobe",
                "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=codec_name,width,height",
                "-of", "json",
                str(output_file)
            ]
            probe_result = subprocess.run(probe_cmd, capture_output=True, text=True)
            if probe_result.returncode != 0:
                logger.error(f"Erreur vérification codec: {probe_result.stderr}")
                return False

            output_info = json.loads(probe_result.stdout)
            if not output_info.get("streams"):
                logger.error("Pas de flux vidéo dans le fichier converti")
                return False

            duration_cmd = [
                "ffprobe",
                "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                str(output_file)
            ]
            duration_result = subprocess.run(duration_cmd, capture_output=True, text=True)
            if (
                duration_result.returncode != 0
                or float(duration_result.stdout.strip()) < 1
            ):
                logger.error("Durée de la vidéo invalide")
                return False

            return True

        except Exception as e:
            logger.error(f"Erreur vérification transcoding: {e}")
            return False
    
    def _check_system_resources(self) -> bool:
        """# On vérifie l'état des ressources système"""
        try:
            cpu_percent = psutil.cpu_percent()
            memory_percent = psutil.virtual_memory().percent
            if cpu_percent > 98 and memory_percent > 95:
                logger.warning(
                    f"Ressources critiques - CPU: {cpu_percent}%, RAM: {memory_percent}%"
                )
                return False
            return True
        except Exception as e:
            logger.error(f"Erreur lors de la vérification des ressources : {e}")
            return True

    def _create_channel_directory(self):
        """# On crée le dossier HLS de la chaîne s'il n'existe pas"""
        Path(f"hls/{self.name}").mkdir(parents=True, exist_ok=True)

    def stop_stream_if_needed(self):
        """
        Arrête le stream si FFmpeg tourne encore.
        """
        with self.lock:
            if self.ffmpeg_process:
                logger.info(f"Arrêt de FFmpeg pour {self.name} (plus de watchers).")
                self._clean_processes()

    def start_stream_if_needed(self) -> bool:

        with self.lock:
            if self.ffmpeg_process is not None:
                return True  # Déjà en cours
            
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


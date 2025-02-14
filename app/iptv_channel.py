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

        # On initialise le VideoProcessor
        self.processor = VideoProcessor(self.video_dir, self.use_gpu)

        # On paramètre les logs FFmpeg
        self.ffmpeg_log_dir = Path("logs/ffmpeg")
        self.ffmpeg_log_dir.mkdir(parents=True, exist_ok=True)
        self.ffmpeg_log_file = self.ffmpeg_log_dir / f"{self.name}_ffmpeg.log"

        # Configuration HLS
        self.hls_time = 6
        self.hls_list_size = 10
        self.hls_delete_threshold = 3
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

    def _build_ffmpeg_command(self, hls_dir: str) -> list:
        """
        # On construit la commande FFmpeg pour streamer
        # hls_dir est le dossier de sortie HLS
        """
        base_cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel", "warning",  # On réduit les logs console
            "-y",
            "-re",
            "-fflags", "+genpts+igndts",
        ]

        # On applique l'offset si défini
        if self.start_offset > 0:
            base_cmd.extend(["-ss", f"{self.start_offset}"])
            # On réinitialise l'offset pour éviter les offsets successifs
            self.start_offset = 0

        base_cmd.extend([
            "-f", "concat",
            "-safe", "0",
            "-stream_loop", "-1",
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

            with open(self.ffmpeg_log_file, "a") as ffmpeg_log:
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=ffmpeg_log,
                    universal_newlines=True
                )

            start_time = time.time()
            hls_dir = Path(f"hls/{self.name}")
            while time.time() - start_time < 10:
                if list(hls_dir.glob("segment_*.ts")):
                    self.stream_start_time = time.time()
                    logger.info(f"✅ FFmpeg démarré pour {self.name} (PID: {process.pid})")
                    return process
                time.sleep(0.1)

            logger.error(f"❌ Timeout en attendant les segments pour {self.name}")
            process.kill()
            return None

        except Exception as e:
            logger.error(f"Erreur démarrage FFmpeg pour {self.name}: {e}")
            return None

    def _monitor_ffmpeg(self, hls_dir: str):
        """
        # On surveille FFmpeg et les segments HLS
        """
        self.last_segment_time = time.time()
        last_segment_number = -1
        hls_dir = Path(hls_dir)
        crash_threshold = 10

        while (
            not self.stop_event.is_set()
            and self.ffmpeg_process
            and self.ffmpeg_process.poll() is None
        ):
            try:
                current_time = time.time()
                segments = sorted(hls_dir.glob("segment_*.ts"))

                if segments:
                    newest_segment = max(segments, key=lambda x: x.stat().st_mtime)
                    try:
                        current_segment = int(newest_segment.stem.split("_")[1])
                        segment_size = newest_segment.stat().st_size

                        # On logge des infos de segment
                        with open(
                            self.ffmpeg_log_dir / f"{self.name}_segments.log", "a"
                        ) as seg_log:
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
                                logger.debug(
                                    f"✨ Nouveau segment {current_segment} pour {self.name}"
                                )
                    except ValueError:
                        logger.error(f"Format de segment invalide: {newest_segment.name}")
                        self.error_count += 1
                else:
                    self.error_count += 1

                elapsed = current_time - self.last_segment_time
                if elapsed > crash_threshold:
                    logger.error(
                        f"🔥 Pas de nouveau segment pour {self.name} depuis {elapsed:.1f}s"
                    )
                    if self.restart_count < self.max_restarts:
                        self.restart_count += 1
                        logger.info(
                            f"🔄 Tentative de redémarrage {self.restart_count}/{self.max_restarts}"
                        )
                        if self._restart_stream():
                            self.error_count = 0
                        else:
                            logger.error(f"❌ Échec du redémarrage pour {self.name}")
                    else:
                        logger.critical(
                            f"⛔️ Nombre maximum de redémarrages atteint pour {self.name}"
                        )

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

    def _clean_processes(self):
        """# On arrête proprement le process FFmpeg"""
        with self.lock:
            if self.ffmpeg_process:
                try:
                    self.stop_event.set()
                    self.ffmpeg_process.terminate()
                    try:
                        self.ffmpeg_process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        self.ffmpeg_process.kill()
                    finally:
                        self.ffmpeg_process = None
                except Exception as e:
                    logger.error(f"Erreur lors du nettoyage FFmpeg pour {self.name}: {e}")

    def _create_concat_file(self) -> Optional[Path]:
        """# On crée un fichier de concaténation basé sur les vidéos traitées"""
        try:
            if not self.processed_videos:
                self._scan_videos()

            if not self.processed_videos:
                logger.error(f"Aucune vidéo traitée pour {self.name}")
                return None

            concat_file = Path(self.video_dir) / "_playlist.txt"
            with open(concat_file, "w", encoding="utf-8") as f:
                for video in self.processed_videos:
                    f.write(f"file '{video.absolute()}'\n")

            logger.info(f"Fichier de concaténation créé: {concat_file}")
            return concat_file

        except Exception as e:
            logger.error(
                f"Erreur lors de la création du fichier de concaténation pour {self.name}: {e}"
            )
            return None

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
                if processed_file.exists():
                    self.processed_videos.append(processed_file)
                else:
                    # On lance la normalisation
                    processed = self._process_video(source, processed_file)
                    if processed:
                        self.processed_videos.append(processed_file)

            if not self.processed_videos:
                logger.error(f"Aucune vidéo traitée disponible pour {self.name}")
                return False

            self.processed_videos.sort()
            return True

        except Exception as e:
            logger.error(f"Erreur lors du scan des vidéos pour {self.name}: {e}")
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
                    "-rc", "vbr",
                    "-cq", "23"
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

    def start_stream(self) -> bool:
        """
        # On lance la diffusion HLS de la chaîne
        """
        with self.lock:
            try:
                hls_dir = f"hls/{self.name}"
                Path(hls_dir).mkdir(parents=True, exist_ok=True)

                cmd = self._build_ffmpeg_command(hls_dir)
                self.ffmpeg_process = self._start_ffmpeg_process(cmd)
                if not self.ffmpeg_process:
                    return False

                if not self.monitoring_thread or not self.monitoring_thread.is_alive():
                    self.monitoring_thread = threading.Thread(
                        target=self._monitor_ffmpeg,
                        args=(hls_dir,),
                        daemon=True
                    )
                    self.monitoring_thread.start()

                return True

            except Exception as e:
                logger.error(f"Erreur lors du démarrage du stream pour {self.name}: {e}")
                self._clean_processes()
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

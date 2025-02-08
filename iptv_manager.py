#!/usr/bin/env python3
import os
import sys
import subprocess
import shutil
import logging
import threading
import time
import datetime
from pathlib import Path
import psutil
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from tqdm import tqdm  # Pour la barre de progression CLI
import traceback
import json
from queue import Queue
from typing import Optional, List
import logging
import glob
import random

# -----------------------------------------------------------------------------
# Configuration du logging et constantes globales
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - [%(levelname)s] - %(message)s",
)
logger = logging.getLogger(__name__)
SERVER_URL = os.getenv("SERVER_URL", "192.168.10.183")
# Fichier de log d'Nginx pour connaître les clients (modifie si besoin)
NGINX_ACCESS_LOG = os.getenv("NGINX_ACCESS_LOG", "/var/log/nginx/access.log")

# Paramètres fixes attendus pour la normalisation
NORMALIZATION_PARAMS = {
    "codec_name": "h264",
    "pix_fmt": "yuv420p",
    "r_frame_rate": "25/1",
    "profile": "baseline",
    "level": "3.0",
}

# On configure un logger pour le minuteur de crash
crash_logger = logging.getLogger("CrashTimer")
if not crash_logger.handlers:

    crash_handler = logging.FileHandler("/app/crash_timer.log")
    crash_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
    crash_logger.addHandler(crash_handler)
    crash_logger.setLevel(logging.INFO)


# -----------------------------------------------------------------------------
# Gestionnaire d'événements pour le dossier de contenu
# -----------------------------------------------------------------------------
class ChannelEventHandler(FileSystemEventHandler):
    def __init__(self, manager):
        self.manager = manager
        self.last_event_time = 0
        self.event_delay = 10  # On attend 10 s pour regrouper les événements
        self.pending_events = set()
        self.event_timer = None
        super().__init__()

    def _handle_event(self, event):
        current_time = time.time()
        if current_time - self.last_event_time >= self.event_delay:
            self.pending_events.add(event.src_path)
            if self.event_timer:
                self.event_timer.cancel()
            self.event_timer = threading.Timer(
                self.event_delay, self._process_pending_events
            )
            self.event_timer.start()

    def _process_pending_events(self):
        if self.pending_events:
            logger.info(f"On traite {len(self.pending_events)} événements groupés")
            self.manager.scan_channels()
            self.pending_events.clear()
            self.last_event_time = time.time()

    def on_modified(self, event):
        if not event.is_directory:
            self._handle_event(event)

    def on_created(self, event):
        if event.is_directory:
            logger.info(f"🔍 Nouveau dossier détecté: {event.src_path}")
            # Attendre un peu que les fichiers soient copiés
            time.sleep(2)
            self._handle_event(event)


# -----------------------------------------------------------------------------
# Nettoyage des segments
# -----------------------------------------------------------------------------
class HLSCleaner:
    """Gestionnaire unique de nettoyage des segments HLS"""

    def __init__(
        self,
        base_hls_dir: str,
        max_segment_age: int = 300,
        min_free_space_gb: float = 10.0,
    ):
        self.base_hls_dir = Path(base_hls_dir)
        self.max_segment_age = max_segment_age
        self.min_free_space_gb = min_free_space_gb
        self.cleanup_interval = 60
        self.stop_event = threading.Event()
        self.cleanup_thread = None
        self.lock = threading.Lock()  # Pour les opérations concurrentes

    def initial_cleanup(self):
        """Nettoyage initial au démarrage du système"""
        with self.lock:
            try:
                logger.info("🧹 Nettoyage initial HLS...")
                if self.base_hls_dir.exists():
                    shutil.rmtree(self.base_hls_dir)
                self.base_hls_dir.mkdir(parents=True, exist_ok=True)
                logger.info("✨ Dossier HLS réinitialisé")
            except Exception as e:
                logger.error(f"Erreur nettoyage initial HLS: {e}")

    def cleanup_channel(self, channel_name: str):
        """Nettoie les segments d'une chaîne spécifique"""
        with self.lock:
            channel_dir = self.base_hls_dir / channel_name
            if channel_dir.exists():
                try:
                    shutil.rmtree(channel_dir)
                    channel_dir.mkdir(parents=True, exist_ok=True)
                    logger.info(f"✨ Chaîne {channel_name} nettoyée")
                except Exception as e:
                    logger.error(f"Erreur nettoyage chaîne {channel_name}: {e}")

    def start(self):
        """Démarre la surveillance et le nettoyage périodique"""
        if not self.cleanup_thread or not self.cleanup_thread.is_alive():
            self.stop_event.clear()
            self.cleanup_thread = threading.Thread(
                target=self._cleanup_loop, daemon=True
            )
            self.cleanup_thread.start()
            logger.info("🔄 Démarrage monitoring HLS")

    def stop(self):
        """Arrête la surveillance"""
        if self.cleanup_thread:
            self.stop_event.set()
            self.cleanup_thread.join()
            self.cleanup_thread = None
            logger.info("⏹️ Arrêt monitoring HLS")

    def _cleanup_loop(self):
        while not self.stop_event.is_set():
            try:
                self._check_disk_space()
                self._cleanup_old_segments()
                self._cleanup_orphaned_segments()
                time.sleep(self.cleanup_interval)
            except Exception as e:
                logger.error(f"Erreur boucle nettoyage: {e}")

    def _check_disk_space(self):
        """Vérifie l'espace disque et nettoie si nécessaire"""
        try:
            disk_usage = shutil.disk_usage(self.base_hls_dir)
            free_space_gb = disk_usage.free / (1024**3)

            if free_space_gb < self.min_free_space_gb:
                logger.warning(f"⚠️ Espace disque critique: {free_space_gb:.2f} GB")
                self._aggressive_cleanup()

        except Exception as e:
            logger.error(f"Erreur vérification espace disque: {e}")

    def _aggressive_cleanup(self):
        """Nettoyage agressif quand l'espace disque est critique"""
        try:
            segments = self._get_all_segments()
            # On garde seulement les 3 segments les plus récents par chaîne
            segments_by_channel = {}

            for segment in segments:
                channel = segment.parent.name
                if channel not in segments_by_channel:
                    segments_by_channel[channel] = []
                segments_by_channel[channel].append(segment)

            for channel, segs in segments_by_channel.items():
                # Trie par date de modification
                sorted_segs = sorted(
                    segs, key=lambda x: x.stat().st_mtime, reverse=True
                )
                # Supprime tous sauf les 3 plus récents
                for seg in sorted_segs[3:]:
                    try:
                        seg.unlink()
                        logger.debug(f"Segment supprimé (espace critique): {seg}")
                    except Exception as e:
                        logger.error(f"Erreur suppression segment {seg}: {e}")

            logger.info("Nettoyage agressif des segments terminé")

        except Exception as e:
            logger.error(f"Erreur nettoyage agressif: {e}")

    def _cleanup_old_segments(self):
        """Nettoie les segments plus vieux que max_segment_age"""
        try:
            current_time = time.time()
            old_segments = []

            for segment in self._get_all_segments():
                try:
                    if current_time - segment.stat().st_mtime > self.max_segment_age:
                        old_segments.append(segment)
                except Exception:
                    # Si on ne peut pas lire les stats, on considère le segment comme périmé
                    old_segments.append(segment)

            for segment in old_segments:
                try:
                    segment.unlink()
                    logger.debug(f"Segment périmé supprimé: {segment}")
                except Exception as e:
                    logger.error(f"Erreur suppression segment périmé {segment}: {e}")

            if old_segments:
                logger.info(f"🧹 {len(old_segments)} segments périmés supprimés")

        except Exception as e:
            logger.error(f"Erreur nettoyage segments périmés: {e}")

    def _cleanup_orphaned_segments(self):
        """Nettoie les segments qui ne sont plus référencés dans les playlists"""
        try:
            for channel_dir in self.base_hls_dir.iterdir():
                if not channel_dir.is_dir():
                    continue

                try:
                    playlist_path = channel_dir / "playlist.m3u8"
                    if not playlist_path.exists():
                        # Si pas de playlist, on considère tous les segments comme orphelins
                        for segment in channel_dir.glob("*.ts"):
                            try:
                                segment.unlink()
                                logger.debug(f"Segment orphelin supprimé: {segment}")
                            except Exception as e:
                                logger.error(
                                    f"Erreur suppression segment orphelin {segment}: {e}"
                                )
                        continue

                    # Lecture de la playlist
                    with open(playlist_path, "r") as f:
                        playlist_content = f.read()

                    # Liste des segments référencés
                    referenced_segments = {
                        line.strip()
                        for line in playlist_content.splitlines()
                        if line.strip().endswith(".ts")
                    }

                    # Suppression des segments non référencés
                    for segment in channel_dir.glob("*.ts"):
                        if segment.name not in referenced_segments:
                            try:
                                segment.unlink()
                                logger.debug(
                                    f"Segment non référencé supprimé: {segment}"
                                )
                            except Exception as e:
                                logger.error(
                                    f"Erreur suppression segment non référencé {segment}: {e}"
                                )

                except Exception as e:
                    logger.error(f"Erreur nettoyage chaîne {channel_dir.name}: {e}")

        except Exception as e:
            logger.error(f"Erreur nettoyage segments orphelins: {e}")

    def _get_all_segments(self) -> List[Path]:
        """Retourne tous les segments HLS"""
        segments = []
        try:
            for channel_dir in self.base_hls_dir.iterdir():
                if channel_dir.is_dir():
                    segments.extend(channel_dir.glob("*.ts"))
        except Exception as e:
            logger.error(f"Erreur lecture segments: {e}")
        return segments

    def _clean_hls_directory(self):
        """Délègue le nettoyage au HLSCleaner"""
        self.hls_cleaner._clean_hls_directory(self.name)


# -----------------------------------------------------------------------------
# Traitement et normalisation des vidéos
# -----------------------------------------------------------------------------
class VideoProcessor:

    def __init__(self, channel_dir: str, use_gpu: bool = False):
        self.channel_dir = Path(channel_dir)
        self.use_gpu = use_gpu
        self.video_extensions = (".mp4", ".avi", ".mkv", ".mov")
        self.processed_dir = self.channel_dir / "processed"
        self.processed_dir.mkdir(exist_ok=True)
        self.processing_thread = None
        self.processing_complete = threading.Event()
        self.processed_videos = []  # Initialisation de la liste
        self.current_processing = None
        self.processing_errors = {}

    def process_videos(self):
        """Traite toutes les vidéos sources"""
        try:
            # Scan des fichiers source
            source_files = []
            for ext in self.video_extensions:
                source_files.extend(self.channel_dir.glob(f"*{ext}"))

            source_files = sorted(source_files)

            if not source_files:
                logger.info(
                    f"Aucun fichier vidéo source trouvé dans {self.channel_dir}"
                )
                return []

            processed_files = []
            self.processing_errors.clear()

            # Traitement des fichiers
            for source_file in source_files:
                try:
                    self.current_processing = source_file
                    processed = self._normalize_video(source_file, NORMALIZATION_PARAMS)
                    if processed:
                        processed_files.append(processed)
                        logger.info(f"✅ {source_file.name} traité avec succès")
                    elif self._is_already_normalized(source_file, NORMALIZATION_PARAMS):
                        already_processed = (
                            self.processed_dir / f"{source_file.stem}.mp4"
                        )
                        processed_files.append(already_processed)
                        logger.info(f"👍 {source_file.name} déjà traité")
                    else:
                        self.processing_errors[source_file.name] = (
                            "Échec de normalisation"
                        )
                        logger.error(f"❌ Échec du traitement de {source_file.name}")
                except Exception as e:
                    self.processing_errors[source_file.name] = str(e)
                    logger.error(
                        f"❌ Erreur lors du traitement de {source_file.name}: {e}"
                    )
                finally:
                    self.current_processing = None

            self.processed_videos = processed_files

            if processed_files:
                logger.info(
                    f"✨ {len(processed_files)}/{len(source_files)} fichiers traités avec succès"
                )
            if self.processing_errors:
                logger.warning(f"⚠️ {len(self.processing_errors)} erreurs de traitement")

            return processed_files

        except Exception as e:
            logger.error(f"Erreur traitement vidéos: {e}")
            return []
        finally:
            self.processing_complete.set()

    def process_videos_async(self) -> None:
        """Démarre le traitement asynchrone des vidéos"""
        self.processing_complete.clear()
        self.processed_videos = []  # Reset de la liste
        self.processing_errors.clear()
        self.processing_thread = threading.Thread(target=self.process_videos)
        self.processing_thread.start()

    def wait_for_completion(self) -> list:
        """Attend la fin du traitement et retourne les résultats"""
        if self.processing_thread:
            self.processing_thread.join()
        return self.processed_videos

    def get_processing_status(self) -> dict:
        """Retourne l'état actuel du traitement"""
        return {
            "current_file": (
                str(self.current_processing) if self.current_processing else None
            ),
            "completed": self.processing_complete.is_set(),
            "processed_count": len(self.processed_videos),
            "errors": self.processing_errors,
        }

    def _normalize_video(self, video_path: Path, ref_info: dict) -> "Optional[Path]":
        """Normalise la vidéo avec FFmpeg avec une gestion améliorée des MKV"""
        max_attempts = 3
        attempt = 0
        duration_ms = self._get_duration(video_path)

        while attempt < max_attempts:
            temp_output = self.processed_dir / f"temp_{video_path.stem}.mp4"
            output_path = self.processed_dir / f"{video_path.stem}.mp4"

            if self._is_already_normalized(video_path, ref_info):
                return output_path

            # Construction de la commande FFmpeg optimisée
            cmd = ["ffmpeg", "-y"]

            # Paramètres spéciaux pour MKV
            if video_path.suffix.lower() == ".mkv":
                cmd.extend(
                    [
                        "-fflags",
                        "+genpts+igndts",
                        "-analyzeduration",
                        "100M",
                        "-probesize",
                        "100M",
                    ]
                )

            cmd.extend(["-i", str(video_path)])

            # Paramètres d'encodage vidéo
            if self.use_gpu:
                cmd.extend(
                    [
                        "-c:v",
                        "h264_nvenc",
                        "-preset",
                        "p4",
                        "-profile:v",
                        "high",
                        "-rc",
                        "vbr",
                        "-cq",
                        "23",
                    ]
                )
            else:
                cmd.extend(["-c:v", "libx264", "-preset", "fast", "-crf", "22"])

            # Paramètres communs
            cmd.extend(
                [
                    "-pix_fmt",
                    "yuv420p",
                    "-movflags",
                    "+faststart",
                    "-max_muxing_queue_size",
                    "1024",
                    "-y",
                ]
            )

            # Paramètres audio
            cmd.extend(["-c:a", "aac", "-b:a", "192k", "-ar", "48000", "-ac", "2"])

            cmd.append(str(temp_output))

            logger.info(
                f"On lance FFmpeg pour normaliser {video_path.name} (tentative {attempt+1}/{max_attempts})"
            )

            try:
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True,
                )

                pbar = tqdm(
                    total=duration_ms, unit="ms", desc=video_path.name, leave=False
                )

                error_output = []
                while True:
                    line = process.stderr.readline()
                    if not line and process.poll() is not None:
                        break
                    if line:
                        error_output.append(line.strip())
                        if "time=" in line:
                            try:
                                time_str = line.split("time=")[1].split()[0]
                                h, m, s = time_str.split(":")
                                ms = int(
                                    (float(h) * 3600 + float(m) * 60 + float(s)) * 1000
                                )
                                pbar.update(max(0, ms - pbar.n))
                            except:
                                pass

                pbar.close()
                process.wait()

                if process.returncode == 0 and temp_output.exists():
                    # Vérification du fichier de sortie
                    if self._verify_output_file(temp_output):
                        temp_output.rename(output_path)
                        logger.info(f"Normalisation réussie pour {video_path.name}")
                        return output_path
                    else:
                        logger.error(
                            f"Fichier de sortie invalide pour {video_path.name}"
                        )
                else:
                    logger.error(
                        f"Échec de la normalisation de {video_path.name} (tentative {attempt+1})"
                    )
                    logger.error("\n".join(error_output[-5:]))

                if temp_output.exists():
                    temp_output.unlink()

            except Exception as e:
                logger.error(
                    f"Erreur pendant FFmpeg pour {video_path.name} (tentative {attempt+1}): {e}"
                )
                if temp_output.exists():
                    temp_output.unlink()

            attempt += 1
            time.sleep(2)

        return None

    def _verify_output_file(self, file_path: Path) -> bool:
        """Vérifie que le fichier de sortie est valide"""
        try:
            cmd = [
                "ffprobe",
                "-v",
                "error",
                "-select_streams",
                "v:0",
                "-show_entries",
                "stream=codec_name",
                "-of",
                "json",
                str(file_path),
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                return False

            # Vérifie la taille minimale
            if file_path.stat().st_size < 1024:  # 1 KB minimum
                return False

            return True

        except Exception as e:
            logger.error(f"Erreur lors de la vérification du fichier {file_path}: {e}")
            return False

    def _get_duration(self, video_path: Path) -> float:
        """Retourne la durée de la vidéo en millisecondes."""
        try:
            cmd = [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                str(video_path),
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            duration_sec = float(result.stdout.strip())
            return duration_sec * 1000  # conversion en ms
        except Exception as e:
            logger.error(
                f"Erreur lors de la récupération de la durée de {video_path}: {e}"
            )
            return 0

    def _is_already_normalized(self, video_path: Path, ref_info: dict) -> bool:
        output_path = self.processed_dir / f"{video_path.stem}.mp4"
        if output_path.exists():
            logger.info(
                f"Le fichier {video_path.name} a déjà été normalisé dans {output_path}"
            )
            return True
        return False

    def create_concat_file(self, video_files: list) -> "Optional[Path]":
        try:
            concat_file = self.processed_dir / "concat.txt"
            with open(concat_file, "w", encoding="utf-8") as f:
                for video in video_files:
                    f.write(f"file '{video.absolute()}'\n")
            return concat_file
        except Exception as e:
            logger.error(
                f"Erreur lors de la création du fichier de concaténation : {e}"
            )
            return None


# -----------------------------------------------------------------------------
# Gestion d'une chaîne IPTV (streaming et surveillance)
# -----------------------------------------------------------------------------
class IPTVChannel:
    def __init__(
        self, name: str, video_dir: str, hls_cleaner: HLSCleaner, use_gpu: bool = False
    ):

        self.name = name
        self.video_dir = video_dir
        self.use_gpu = use_gpu
        self.hls_cleaner = hls_cleaner  # Instance partagée du nettoyeur

        self.processed_videos = []
        self.video_extensions = (".mp4", ".avi", ".mkv", ".mov")

        # Initialisation du processeur vidéo
        self.processor = VideoProcessor(self.video_dir, self.use_gpu)

        # Configuration des logs
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

        # Autres initialisations
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
        # Base commune pour tous les modes
        base_cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "warning",  # On réduit les logs console
            "-y",
            "-re",
            "-fflags",
            "+genpts+igndts",
        ]

        # Ajout de l'offset uniquement si défini et non nul
        if self.start_offset > 0:
            base_cmd.extend(["-ss", f"{self.start_offset}"])
            # On réinitialise pour éviter de réutiliser l'offset lors des redémarrages
            self.start_offset = 0

        base_cmd.extend(
            [
                "-f",
                "concat",
                "-safe",
                "0",
                "-stream_loop",
                "-1",
                "-i",
                str(self._create_concat_file()),
            ]
        )

        if self.fallback_mode:
            # Mode ré-encodage complet
            encoding_params = [
                "-map",
                "0:v:0",
                "-map",
                "0:a:0",
                "-vf",
                "scale=w=1280:h=720:force_original_aspect_ratio=decrease",
                "-c:v",
                "h264_nvenc" if self.use_gpu else "libx264",
                "-profile:v",
                "main",
                "-b:v",
                self.video_bitrate,
                "-maxrate",
                self.max_bitrate,
                "-bufsize",
                self.buffer_size,
                "-crf",
                str(self.crf),
                "-g",
                str(self.gop_size),
                "-keyint_min",
                str(self.keyint_min),
                "-sc_threshold",
                str(self.sc_threshold),
                "-c:a",
                "aac",
                "-ar",
                "48000",
                "-b:a",
                "128k",
            ]
        else:
            # Mode copie simple
            encoding_params = ["-c:v", "copy", "-c:a", "copy"]

        # Paramètres HLS
        hls_params = [
            "-f",
            "hls",
            "-hls_time",
            str(self.hls_time),
            "-hls_list_size",
            str(self.hls_list_size),
            "-hls_delete_threshold",
            str(self.hls_delete_threshold),
            "-hls_flags",
            "delete_segments+append_list",
            "-hls_start_number_source",
            "datetime",
            "-hls_segment_filename",
            f"{hls_dir}/segment_%d.ts",
            f"{hls_dir}/playlist.m3u8",
        ]

        return base_cmd + encoding_params + hls_params

    def _start_ffmpeg_process(self, cmd: list) -> Optional[subprocess.Popen]:
        try:
            logger.info(f"🚀 Démarrage FFmpeg pour {self.name}")

            # On redirige les logs détaillés vers un fichier
            with open(self.ffmpeg_log_file, "a") as ffmpeg_log:
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=ffmpeg_log,
                    universal_newlines=True,
                )

            # Attente de la création du premier segment
            start_time = time.time()
            hls_dir = Path(f"hls/{self.name}")
            while time.time() - start_time < 10:
                if list(hls_dir.glob("segment_*.ts")):
                    self.stream_start_time = time.time()
                    logger.info(
                        f"✅ FFmpeg démarré pour {self.name} (PID: {process.pid})"
                    )
                    return process
                time.sleep(0.1)

            logger.error(f"❌ Timeout en attendant les segments pour {self.name}")
            process.kill()
            return None

        except Exception as e:
            logger.error(f"Erreur démarrage FFmpeg pour {self.name}: {e}")
            return None

    def _monitor_ffmpeg(self, hls_dir: str):
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

                        # Log des informations de segment dans un fichier séparé
                        with open(
                            self.ffmpeg_log_dir / f"{self.name}_segments.log", "a"
                        ) as seg_log:
                            seg_log.write(
                                f"{datetime.datetime.now()} - Segment {current_segment}: {segment_size} bytes\n"
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
                        logger.error(
                            f"Format de segment invalide: {newest_segment.name}"
                        )
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
        try:
            logger.info(f"🔄 Redémarrage du stream {self.name}")

            # Attente du cooldown si nécessaire
            elapsed = time.time() - getattr(self, "last_restart_time", 0)
            if elapsed < self.restart_cooldown:
                logger.info(
                    f"⏳ Attente du cooldown ({self.restart_cooldown - elapsed:.1f}s)"
                )
                time.sleep(self.restart_cooldown - elapsed)

            self.last_restart_time = time.time()

            # On arrête proprement FFmpeg
            self._clean_processes()

            # On attend un peu
            time.sleep(2)

            # On redémarre
            return self.start_stream()

        except Exception as e:
            logger.error(f"Erreur lors du redémarrage de {self.name}: {e}")
            return False

    def _clean_processes(self):
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
                    logger.error(
                        f"Erreur lors du nettoyage du processus pour {self.name}: {e}"
                    )

    def _create_concat_file(self) -> Optional[Path]:
        """Crée le fichier de concaténation pour FFmpeg"""
        try:
            if not self.processed_videos:
                self._scan_videos()

            if not self.processed_videos:
                logger.error(f"Aucune vidéo traitée disponible pour {self.name}")
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
        """Scanne les fichiers vidéos disponibles"""
        try:
            source_dir = Path(self.video_dir)
            processed_dir = source_dir / "processed"
            processed_dir.mkdir(exist_ok=True)

            # Liste tous les fichiers vidéo source
            source_files = []
            for ext in self.video_extensions:
                source_files.extend(source_dir.glob(f"*{ext}"))

            if not source_files:
                logger.warning(f"Aucun fichier vidéo trouvé dans {self.video_dir}")
                return False

            # Vérifie les fichiers déjà traités
            self.processed_videos = []
            for source in source_files:
                processed_file = processed_dir / f"{source.stem}.mp4"
                if processed_file.exists():
                    self.processed_videos.append(processed_file)
                else:
                    # Si besoin de normalisation, l'implémenter ici
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
        """Traite une vidéo source avec vérification de la transcoding"""
        try:
            # On ne fait plus de simple copie
            logger.info(f"Début de la normalisation de {source.name}")

            # Analyse du fichier source
            probe_cmd = [
                "ffprobe",
                "-v",
                "error",
                "-select_streams",
                "v:0",
                "-show_entries",
                "stream=codec_name,width,height,r_frame_rate,duration",
                "-of",
                "json",
                str(source),
            ]

            probe_result = subprocess.run(probe_cmd, capture_output=True, text=True)
            if probe_result.returncode != 0:
                logger.error(
                    f"Erreur analyse source {source.name}: {probe_result.stderr}"
                )
                return False

            source_info = json.loads(probe_result.stdout)
            if not source_info.get("streams"):
                logger.error(f"Pas de flux vidéo trouvé dans {source.name}")
                return False

            # Construction de la commande FFmpeg
            cmd = ["ffmpeg", "-y"]
            if source.suffix.lower() == ".mkv":
                cmd.extend(
                    [
                        "-fflags",
                        "+genpts+igndts",
                        "-analyzeduration",
                        "100M",
                        "-probesize",
                        "100M",
                    ]
                )

            cmd.extend(["-i", str(source)])

            # Paramètres d'encodage
            if self.use_gpu:
                cmd.extend(
                    [
                        "-c:v",
                        "h264_nvenc",
                        "-preset",
                        "p4",
                        "-profile:v",
                        "high",
                        "-rc",
                        "vbr",
                        "-cq",
                        "23",
                    ]
                )
            else:
                cmd.extend(["-c:v", "libx264", "-preset", "medium", "-crf", "23"])

            # Paramètres communs
            cmd.extend(
                [
                    "-pix_fmt",
                    "yuv420p",
                    "-movflags",
                    "+faststart",
                    "-max_muxing_queue_size",
                    "1024",
                ]
            )

            # Paramètres audio
            cmd.extend(["-c:a", "aac", "-b:a", "192k", "-ar", "48000", "-ac", "2"])

            # Fichier de sortie temporaire
            temp_output = dest.parent / f"temp_{dest.name}"
            cmd.append(str(temp_output))

            # Lancement de la conversion
            logger.info(f"Démarrage FFmpeg pour {source.name}")
            process = subprocess.run(cmd, capture_output=True, text=True)

            if process.returncode != 0:
                logger.error(f"Erreur FFmpeg pour {source.name}: {process.stderr}")
                if temp_output.exists():
                    temp_output.unlink()
                return False

            # Vérification du fichier de sortie
            if not self._verify_transcoding(temp_output):
                logger.error(f"Vérification transcoding échouée pour {source.name}")
                temp_output.unlink()
                return False

            # Déplacement du fichier temporaire
            temp_output.rename(dest)
            logger.info(f"✅ Normalisation réussie: {source.name} -> {dest.name}")
            return True

        except Exception as e:
            logger.error(f"Erreur traitement {source.name}: {e}")
            if "temp_output" in locals() and temp_output.exists():
                temp_output.unlink()
            return False

    def _verify_transcoding(self, output_file: Path) -> bool:
        """Vérifie que la transcoding a bien été effectuée"""
        try:
            # Vérification de la taille
            if output_file.stat().st_size < 1024 * 1024:  # 1MB minimum
                logger.error(f"Fichier trop petit: {output_file.stat().st_size} bytes")
                return False

            # Vérification du codec
            probe_cmd = [
                "ffprobe",
                "-v",
                "error",
                "-select_streams",
                "v:0",
                "-show_entries",
                "stream=codec_name,width,height",
                "-of",
                "json",
                str(output_file),
            ]

            probe_result = subprocess.run(probe_cmd, capture_output=True, text=True)
            if probe_result.returncode != 0:
                logger.error(f"Erreur vérification codec: {probe_result.stderr}")
                return False

            output_info = json.loads(probe_result.stdout)
            if not output_info.get("streams"):
                logger.error("Pas de flux vidéo trouvé dans le fichier converti")
                return False

            # Vérification de la durée (au moins quelques secondes)
            duration_cmd = [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                str(output_file),
            ]

            duration_result = subprocess.run(
                duration_cmd, capture_output=True, text=True
            )
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
                        target=self._monitor_ffmpeg, args=(hls_dir,), daemon=True
                    )
                    self.monitoring_thread.start()

                return True

            except Exception as e:
                logger.error(
                    f"Erreur lors du démarrage du stream pour {self.name}: {e}"
                )
                self._clean_processes()
                return False

    def _check_system_resources(self) -> bool:
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
        Path(f"hls/{self.name}").mkdir(parents=True, exist_ok=True)


# -----------------------------------------------------------------------------
# Gestionnaire de monitoring des clients (IP des viewers)
# -----------------------------------------------------------------------------
class ClientMonitor(threading.Thread):

    def __init__(self, log_file: str):
        super().__init__(daemon=True)
        self.log_file = log_file

    def run(self):
        if not os.path.exists(self.log_file):
            logger.warning(
                "Fichier log introuvable pour le monitoring des clients : %s",
                self.log_file,
            )
            return
        with open(self.log_file, "r") as f:
            # On se positionne à la fin du fichier
            f.seek(0, 2)
            while True:
                line = f.readline()
                if not line:
                    time.sleep(0.5)
                    continue
                # On considère que l'IP est le premier champ de la ligne (format commun)
                ip = line.split()[0]
                logger.info(f"Client connecté : {ip}")


# -----------------------------------------------------------------------------
# Gestionnaire principal IPTV
# -----------------------------------------------------------------------------
class IPTVManager:
    # Définition des constantes de classe
    VIDEO_EXTENSIONS = (".mp4", ".avi", ".mkv", ".mov")
    CPU_THRESHOLD = 85
    SEGMENT_AGE_THRESHOLD = 30  # secondes

    def __init__(self, content_dir: str, use_gpu: bool = False):
        self.content_dir = content_dir
        self.use_gpu = use_gpu
        self.channels = {}

        # Instance unique du nettoyeur HLS
        self.hls_cleaner = HLSCleaner("./hls")
        # Nettoyage initial
        self.hls_cleaner.initial_cleanup()
        # Démarrage du monitoring
        self.hls_cleaner.start()

        self.scan_lock = threading.Lock()
        self.failing_channels = set()
        self.channel_queue = Queue()

        logger.info("Initialisation du gestionnaire IPTV")
        self._clean_startup()

        # Démarrage du thread de traitement de la file d'attente
        self.queue_processor = threading.Thread(
            target=self._process_channel_queue, daemon=True
        )
        self.queue_processor.start()

        # Configuration habituelle
        self.observer = Observer()
        event_handler = ChannelEventHandler(self)
        self.observer.schedule(event_handler, self.content_dir, recursive=True)

        # Scan initial
        logger.info(f"Démarrage du scan initial dans {self.content_dir}")
        self.scan_channels(initial=True, force=True)

        # Démarrage des moniteurs
        self.client_monitor = ClientMonitor(NGINX_ACCESS_LOG)
        self.client_monitor.start()
        self.resource_monitor = ResourceMonitor()
        self.resource_monitor.start()

        # Démarrage de l'updater de playlist
        self.master_playlist_updater = threading.Thread(
            target=self._master_playlist_loop, daemon=True
        )
        self.master_playlist_updater.start()

    def _is_channel_ready(self, channel_name: str) -> bool:
        """Vérifie si une chaîne est prête à être diffusée"""
        try:
            channel_dir = Path(f"./content/{channel_name}")
            processed_dir = channel_dir / "processed"

            if not processed_dir.exists():
                return False

            # Vérifie les fichiers vidéo normalisés (sans les fichiers temp_)
            processed_videos = [
                f for f in processed_dir.glob("*.mp4") if not f.name.startswith("temp_")
            ]

            if not processed_videos:
                return False

            # Vérifie si tous les fichiers source sont traités
            source_videos = [
                f
                for f in channel_dir.glob("*.*")
                if f.suffix.lower() in self.VIDEO_EXTENSIONS
                and not f.name.startswith("temp_")
                and f.parent == channel_dir
            ]

            processed_names = {v.stem for v in processed_videos}
            source_names = {v.stem for v in source_videos}

            # Si tous les fichiers source sont traités, la chaîne est prête
            return processed_names >= source_names and len(processed_videos) > 0

        except Exception as e:
            logger.error(f"Erreur vérification état {channel_name}: {e}")
            return False

    def _verify_channel(self, channel: IPTVChannel) -> bool:
        """Vérifie qu'une chaîne est active et produit des segments"""
        try:
            if not self._is_channel_ready(channel.name):
                return False

            hls_dir = Path(f"./hls/{channel.name}")
            if not hls_dir.exists():
                return False

            # Vérifie la présence et l'âge des segments
            ts_files = list(hls_dir.glob("*.ts"))
            if not ts_files:
                return False

            # Vérifie que le dernier segment n'est pas trop vieux
            newest_ts = max(ts_files, key=lambda x: x.stat().st_mtime)
            if time.time() - newest_ts.stat().st_mtime > self.SEGMENT_AGE_THRESHOLD:
                return False

            return True

        except Exception as e:
            logger.error(f"Erreur vérification {channel.name}: {e}")
            return False

    def generate_master_playlist(self):
        """Génère la playlist principale uniquement avec les chaînes prêtes"""
        try:
            playlist_path = os.path.abspath("./hls/playlist.m3u")
            logger.info(f"On génère la master playlist à {playlist_path}")

            ready_channels = []

            # Vérifie chaque chaîne
            for name, channel in sorted(self.channels.items()):
                if self._verify_channel(channel):
                    ready_channels.append(name)
                    logger.info(f"Chaîne {name} prête et active")
                else:
                    logger.debug(f"Chaîne {name} pas encore prête")
                    # On remet la chaîne en file d'attente pour traitement
                    if name not in self.failing_channels:
                        self.channel_queue.put(channel)

            # Génération de la playlist
            with open(playlist_path, "w", encoding="utf-8") as f:
                f.write("#EXTM3U\n")
                for name in ready_channels:
                    f.write(f'#EXTINF:-1 tvg-id="{name}" tvg-name="{name}",{name}\n')
                    f.write(f"http://{SERVER_URL}/hls/{name}/playlist.m3u8\n")

            logger.info(
                f"Master playlist mise à jour avec {len(ready_channels)} chaînes"
            )
            logger.info(f"Chaînes actives: {', '.join(ready_channels)}")

        except Exception as e:
            logger.error(f"Erreur lors de la génération de la master playlist : {e}")
            logger.error(traceback.format_exc())

    def _clean_startup(self):
        """Nettoyage initial plus approfondi"""
        try:
            logger.info("🧹 Nettoyage initial...")

            # Nettoyage du dossier HLS et des fichiers temporaires
            patterns_to_clean = [
                ("./hls/**/*", "Fichiers HLS"),
                ("./content/**/_playlist.txt", "Playlists"),
                ("./content/**/*.vtt", "Fichiers VTT"),
                ("./content/**/temp_*", "Fichiers temporaires"),
            ]

            for pattern, desc in patterns_to_clean:
                count = 0
                for f in glob.glob(pattern, recursive=True):
                    try:
                        if os.path.isfile(f):
                            os.remove(f)
                        elif os.path.isdir(f):
                            shutil.rmtree(f)
                        count += 1
                    except Exception as e:
                        logger.error(f"Erreur nettoyage {f}: {e}")
                logger.info(f"✨ {count} {desc} supprimés")

            # Recréation des dossiers nécessaires
            os.makedirs("./hls", exist_ok=True)

        except Exception as e:
            logger.error(f"Erreur nettoyage initial: {e}")

    def _process_channel_queue(self):
        """Traite la file d'attente des chaînes en tenant compte des ressources"""
        while True:
            try:
                channel = self.channel_queue.get()

                # Vérification CPU avant traitement
                cpu_usage = psutil.cpu_percent()
                if cpu_usage > self.CPU_THRESHOLD:
                    logger.warning(
                        f"CPU trop chargé ({cpu_usage}%), mise en attente de {channel.name}"
                    )
                    time.sleep(10)  # Attente avant nouvelle tentative
                    self.channel_queue.put(channel)
                    continue

                if channel.name in self.failing_channels:
                    logger.info(
                        f"Chaîne {channel.name} précédemment en échec, nettoyage approfondi"
                    )
                    self._clean_channel(channel.name)

                success = self._start_channel(channel)
                if not success:
                    self.failing_channels.add(channel.name)
                else:
                    self.failing_channels.discard(channel.name)

            except Exception as e:
                logger.error(f"Erreur traitement file d'attente: {e}")
            finally:
                self.channel_queue.task_done()

    def _start_channel(self, channel: IPTVChannel) -> bool:
        """Démarre une chaîne avec vérification des ressources"""
        try:
            # Vérification CPU
            if psutil.cpu_percent() > self.CPU_THRESHOLD:
                logger.warning(f"CPU trop chargé pour démarrer {channel.name}")
                return False

            # Vérification des fichiers source
            if not channel._scan_videos():
                logger.error(f"Aucune vidéo valide pour {channel.name}")
                return False

            # Démarrage avec timeout
            start_time = time.time()
            if not channel.start_stream():
                return False

            # Vérification des segments
            while time.time() - start_time < 10:
                if list(Path(f"./hls/{channel.name}").glob("*.ts")):
                    logger.info(f"✅ Chaîne {channel.name} démarrée avec succès")
                    return True
                time.sleep(0.5)

            logger.error(f"❌ Timeout démarrage {channel.name}")
            return False

        except Exception as e:
            logger.error(f"Erreur démarrage {channel.name}: {e}")
            return False

    def _clean_channel(self, channel_name: str):
        """Nettoyage approfondi d'une chaîne en échec"""
        try:
            self.hls_cleaner.cleanup_channel(channel_name)
            # Nettoyage des fichiers HLS
            hls_dir = Path(f"./hls/{channel_name}")
            if hls_dir.exists():
                shutil.rmtree(hls_dir)
            hls_dir.mkdir(parents=True, exist_ok=True)

            # Nettoyage des fichiers temporaires
            channel_dir = Path(f"./content/{channel_name}")
            if channel_dir.exists():
                for pattern in ["_playlist.txt", "*.vtt", "temp_*"]:
                    for f in channel_dir.glob(pattern):
                        try:
                            f.unlink()
                        except Exception as e:
                            logger.error(f"Erreur suppression {f}: {e}")

            logger.info(f"Nettoyage de la chaîne {channel_name} terminé")

        except Exception as e:
            logger.error(f"Erreur nettoyage chaîne {channel_name}: {e}")

    def _update_master_playlist(self):
        """Mise à jour de la playlist principale avec vérification des chaînes"""
        try:
            active_channels = []
            for name, channel in self.channels.items():
                if self._verify_channel(channel):
                    active_channels.append(name)
                else:
                    # Requeue les chaînes inactives
                    if name not in self.failing_channels:
                        self.channel_queue.put(channel)

            self.generate_master_playlist(active_channels)

        except Exception as e:
            logger.error(f"Erreur mise à jour master playlist: {e}")

    def _clean_directory(self, directory: Path):
        """Nettoie récursivement un dossier"""
        if not directory.exists():
            return
        for item in directory.glob("**/*"):
            try:
                if item.is_file():
                    item.unlink()
                elif item.is_dir():
                    shutil.rmtree(item)
            except Exception as e:
                logger.error(f"Erreur lors de la suppression de {item}: {e}")

    def scan_channels(self, force: bool = False, initial: bool = False):
        with self.scan_lock:
            try:
                content_path = Path(self.content_dir)
                if not content_path.exists():
                    logger.error(f"Le dossier {content_path} n'existe pas!")
                    return

                channel_dirs = [d for d in content_path.iterdir() if d.is_dir()]
                processing_queue = []

                # Première phase : démarrage immédiat des chaînes prêtes
                for channel_dir in channel_dirs:
                    try:
                        channel_name = channel_dir.name
                        processed_dir = channel_dir / "processed"

                        if processed_dir.exists() and list(processed_dir.glob("*.mp4")):
                            if channel_name not in self.channels:
                                # On passe le hls_cleaner à la création de la chaîne
                                channel = IPTVChannel(
                                    channel_name,
                                    str(channel_dir),
                                    hls_cleaner=self.hls_cleaner,  # Ajout ici
                                    use_gpu=self.use_gpu,
                                )
                                self.channels[channel_name] = channel
                                threading.Thread(
                                    target=self._start_ready_channel, args=(channel,)
                                ).start()
                            else:
                                if force or initial or self._needs_update(channel_dir):
                                    channel = self.channels[channel_name]
                                    threading.Thread(
                                        target=self._update_channel_playlist,
                                        args=(channel, channel_dir),
                                    ).start()
                        else:
                            processing_queue.append(channel_dir)

                    except Exception as e:
                        logger.error(f"Erreur traitement {channel_dir.name}: {e}")
                        continue

                if processing_queue:
                    threading.Thread(
                        target=self._process_new_channels, args=(processing_queue,)
                    ).start()

            except Exception as e:
                logger.error(f"Erreur scan des chaînes: {e}")

    def _start_ready_channel(self, channel: IPTVChannel):
        """Démarre une chaîne avec un offset aléatoire"""
        try:
            logger.info(f"Démarrage rapide de la chaîne {channel.name}")
            if channel._scan_videos():
                # Calcul de la durée totale des vidéos
                total_duration = 0
                for video in channel.processed_videos:
                    try:
                        cmd = [
                            "ffprobe",
                            "-v",
                            "error",
                            "-show_entries",
                            "format=duration",
                            "-of",
                            "default=noprint_wrappers=1:nokey=1",
                            str(video),
                        ]
                        result = subprocess.run(cmd, capture_output=True, text=True)
                        duration = float(result.stdout.strip())
                        total_duration += duration
                    except Exception as e:
                        logger.error(f"Erreur lecture durée {video}: {e}")
                        continue

                # Génération d'un offset aléatoire
                if total_duration > 0:
                    channel.start_offset = random.uniform(0, total_duration)
                    logger.info(
                        f"Démarrage de {channel.name} à {channel.start_offset:.2f}s"
                    )

                if channel.start_stream():
                    logger.info(f"✅ Chaîne {channel.name} démarrée avec succès")
                    self.generate_master_playlist()
                else:
                    logger.error(f"❌ Échec démarrage {channel.name}")
            else:
                logger.error(f"❌ Échec scan vidéos {channel.name}")
        except Exception as e:
            logger.error(f"Erreur démarrage {channel.name}: {e}")

    def _process_new_channels(self, channel_dirs: List[Path]):
        """Traite les nouvelles chaînes avec démarrage aléatoire"""
        for channel_dir in channel_dirs:
            try:
                channel_name = channel_dir.name
                logger.info(f"Traitement de {channel_name}")

                # Création du canal
                channel = IPTVChannel(
                    channel_name,
                    str(channel_dir),
                    hls_cleaner=self.hls_cleaner,
                    use_gpu=self.use_gpu,
                )
                self.channels[channel_name] = channel

                # On attend la fin du traitement des vidéos
                channel.start_processing()
                channel.wait_for_processing()

                if channel.processed_videos:
                    # Calcul de la durée totale
                    total_duration = 0
                    for video in channel.processed_videos:
                        try:
                            cmd = [
                                "ffprobe",
                                "-v",
                                "error",
                                "-show_entries",
                                "format=duration",
                                "-of",
                                "default=noprint_wrappers=1:nokey=1",
                                str(video),
                            ]
                            result = subprocess.run(cmd, capture_output=True, text=True)
                            duration = float(result.stdout.strip())
                            total_duration += duration
                        except Exception as e:
                            logger.error(f"Erreur lecture durée {video}: {e}")
                            continue

                    # Génération d'un offset aléatoire
                    if total_duration > 0:
                        random_offset = random.uniform(0, total_duration)
                        # On stocke l'offset dans le canal pour utilisation dans _build_ffmpeg_command
                        channel.start_offset = random_offset
                        logger.info(
                            f"Offset initial pour {channel_name}: {random_offset:.2f}s"
                        )

                    if channel.start_stream():
                        logger.info(f"✅ Chaîne {channel_name} démarrée")
                        self.generate_master_playlist()
                    else:
                        logger.error(f"❌ Échec démarrage {channel_name}")

            except Exception as e:
                logger.error(f"Erreur traitement {channel_dir.name}: {e}")
                continue

    def _start_playlist_updater(self):
        # Cancelle l'ancien updater si existant
        if hasattr(self, "playlist_timer"):
            self.playlist_timer.cancel()

        # Démarre le nouveau timer
        self.playlist_timer = threading.Timer(10, self._update_master_playlist)
        self.playlist_timer.daemon = True
        self.playlist_timer.start()

    def _update_channel_playlist(self, channel: IPTVChannel, channel_dir: Path):
        """Update channel playlist without interrupting stream"""
        try:
            # Check for new content
            new_videos = self._scan_new_videos(channel_dir)
            if not new_videos:
                logger.info(f"Pas de nouveau contenu pour {channel.name}")
                return

            # Process new videos
            processor = VideoProcessor(str(channel_dir), use_gpu=self.use_gpu)
            processor.process_videos_async()

            # Wait for processing to complete
            new_processed = processor.wait_for_completion()
            if not new_processed:
                logger.error(f"Échec traitement nouveaux fichiers: {channel.name}")
                return

            # Update channel's video list and concat file
            channel.processed_videos.extend(new_processed)
            channel.processed_videos.sort()

            # Create new concat file
            concat_file = channel._create_concat_file()
            if not concat_file:
                logger.error(f"Échec création concat file: {channel.name}")
                return

            logger.info(f"Playlist mise à jour: {channel.name}")

            # Optionally check stream health
            if not channel._check_stream_health():
                logger.warning(f"Problème détecté stream {channel.name}")
                if not channel.start_stream():
                    logger.error(f"Échec redémarrage {channel.name}")

        except Exception as e:
            logger.error(f"Erreur mise à jour {channel.name}: {e}")
            logger.error(traceback.format_exc())

    def _scan_new_videos(self, channel_dir: Path) -> list:
        """Scan for new videos in channel directory"""
        try:
            processed_dir = channel_dir / "processed"
            if not processed_dir.exists():
                return []

            current_videos = {f.stem for f in processed_dir.glob("*.mp4")}
            all_videos = {f.stem for f in channel_dir.glob("*.mp4")}
            new_videos = all_videos - current_videos

            if new_videos:
                logger.info(f"Nouveaux fichiers trouvés: {len(new_videos)}")
                for video in new_videos:
                    logger.debug(f"Nouveau: {video}")

            return [channel_dir / f"{video}.mp4" for video in new_videos]

        except Exception as e:
            logger.error(f"Erreur scan nouveaux fichiers: {e}")
            return []

    def _master_playlist_loop(self):
        """Boucle de mise à jour continue de la master playlist."""
        while True:

            try:
                self.generate_master_playlist()
                time.sleep(10)
            except Exception as e:
                logger.error(f"Erreur mise à jour master playlist: {e}")

    def _clean_directory(self, directory: Path):
        if not directory.exists():
            return
        for item in directory.glob("**/*"):
            try:
                if item.is_file():
                    item.unlink()
                elif item.is_dir():
                    shutil.rmtree(item)
            except Exception as e:
                logger.error(f"Erreur lors de la suppression de {item} : {e}")

    def cleanup(self):
        logger.info("Début du nettoyage...")
        # Arrêt du nettoyeur HLS
        if hasattr(self, "hls_cleaner"):
            logger.info("Arrêt du nettoyeur HLS...")
            self.hls_cleaner.stop()

        if hasattr(self, "observer"):
            logger.info("On arrête l'observer...")
            self.observer.stop()
            self.observer.join()
        for name, channel in self.channels.items():
            logger.info(f"Nettoyage de la chaîne {name}...")
            channel._clean_processes()
        logger.info("Nettoyage terminé")

    def _signal_handler(self, signum, frame):
        logger.info(f"Signal {signum} reçu, nettoyage en cours...")
        self.cleanup()
        sys.exit(0)

    def _needs_update(self, channel_dir: Path) -> bool:
        try:
            logger.debug(f"Vérification des mises à jour pour {channel_dir.name}")
            video_files = list(channel_dir.glob("*.mp4"))
            if not video_files:
                logger.debug(f"Aucun fichier vidéo dans {channel_dir.name}")
                return True
            for video_file in video_files:
                mod_time = video_file.stat().st_mtime
                logger.debug(
                    f"Fichier vidéo : {video_file.name}, Modifié : {datetime.datetime.fromtimestamp(mod_time)}"
                )
            return True
        except Exception as e:
            logger.error(
                f"Erreur lors de la vérification des mises à jour pour {channel_dir} : {e}"
            )
            return True

    def _cpu_monitor(self):
        """Surveille l'utilisation CPU seconde par seconde et alerte si la moyenne sur 1 minute > 90%"""
        usage_samples = []
        threshold = 90
        while True:
            try:
                usage = psutil.cpu_percent(interval=1)
                usage_samples.append(usage)
                if len(usage_samples) >= 60:
                    avg_usage = sum(usage_samples) / len(usage_samples)
                    if avg_usage > threshold:
                        logger.warning(
                            f"ALERTE CPU : Utilisation moyenne de {avg_usage:.1f}% sur 1 minute"
                        )
                    usage_samples = []
            except Exception as e:
                logger.error(f"Erreur lors du monitoring CPU : {e}")

    def run(self):
        try:
            self.scan_channels()
            self.generate_master_playlist()
            self.observer.start()
            # Lancement du monitoring CPU dans un thread dédié
            cpu_thread = threading.Thread(target=self._cpu_monitor, daemon=True)
            cpu_thread.start()
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.cleanup()
        except Exception as e:
            logger.error(f"🔥 Erreur dans le gestionnaire IPTV : {e}")
            self.cleanup()


# -----------------------------------------------------------------------------
# Moniteur des ressources système
# -----------------------------------------------------------------------------
class ResourceMonitor(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.interval = 20  # secondes

    def run(self):
        while True:
            try:
                cpu_percent = psutil.cpu_percent(interval=1)
                ram = psutil.virtual_memory()
                ram_used_gb = ram.used / (1024 * 1024 * 1024)
                ram_total_gb = ram.total / (1024 * 1024 * 1024)

                # Tenter de récupérer les infos GPU si nvidia-smi est disponible
                gpu_info = ""
                try:
                    result = subprocess.run(
                        [
                            "nvidia-smi",
                            "--query-gpu=utilization.gpu,memory.used",
                            "--format=csv,noheader,nounits",
                        ],
                        capture_output=True,
                        text=True,
                    )
                    if result.returncode == 0:
                        gpu_util, gpu_mem = result.stdout.strip().split(",")
                        gpu_info = f", GPU: {gpu_util}%, MEM GPU: {gpu_mem}MB"
                except FileNotFoundError:
                    pass  # Pas de GPU NVIDIA

                logger.info(
                    f"💻 Ressources - CPU: {cpu_percent}%, "
                    f"RAM: {ram_used_gb:.1f}/{ram_total_gb:.1f}GB ({ram.percent}%)"
                    f"{gpu_info}"
                )
            except Exception as e:
                logger.error(f"Erreur monitoring ressources: {e}")

            time.sleep(self.interval)


def run(self):
    try:
        self.scan_channels()
        self.generate_master_playlist()
        self.observer.start()
        cpu_thread = threading.Thread(target=self._cpu_monitor, daemon=True)
        cpu_thread.start()
        while True:
            time.sleep(1)
    except Exception as e:
        logger.error(f"🔥 Erreur fatale dans le gestionnaire IPTV : {e}")
        import traceback

        logger.error(traceback.format_exc())
        self.cleanup()


if __name__ == "__main__":
    manager = IPTVManager("./content", use_gpu=False)
    try:
        manager.run()
    except KeyboardInterrupt:
        logger.info("Interruption utilisateur détectée")
        manager.cleanup()
    except Exception as e:
        logger.error(f"Erreur fatale : {e}")
        manager.cleanup()
        raise

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
    crash_handler = logging.FileHandler("/crash_timer.log")
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

    def process_videos_async(self) -> None:
        """Start asynchronous video processing"""
        self.processing_thread = threading.Thread(target=self.process_videos)
        self.processing_thread.start()

    def wait_for_completion(self) -> list:
        """Wait for processing to complete and return results"""
        if self.processing_thread:
            self.processing_thread.join()
        return self.processed_videos

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
        """
        Si le fichier traité existe déjà dans le dossier processed,
        on considère qu'il est normalisé.
        """
        output_path = self.processed_dir / f"{video_path.stem}.mp4"
        if output_path.exists():
            logger.info(
                f"Le fichier {video_path.name} a déjà été normalisé dans {output_path}"
            )
            return True
        return False

    def _normalize_video(self, video_path: Path, ref_info: dict) -> "Optional[Path]":
        """Normalise la vidéo avec FFmpeg en affichant une barre de progression CLI."""
        max_attempts = 3
        attempt = 0
        duration_ms = self._get_duration(video_path)
        while attempt < max_attempts:
            temp_output = self.processed_dir / f"temp_{video_path.stem}.mp4"
            output_path = self.processed_dir / f"{video_path.stem}.mp4"

            if self._is_already_normalized(video_path, ref_info):
                return output_path

            # Construction de la commande FFmpeg avec l'option -progress pour le suivi
            cmd = ["ffmpeg", "-y", "-i", str(video_path)]

            if self.use_gpu:
                cmd.extend(
                    [
                        "-c:v",
                        "h264_nvenc",
                        "-preset",
                        "p4",
                        "-profile:v",
                        "baseline",
                        "-level:v",
                        "3.0",
                        "-pix_fmt",
                        "yuv420p",
                        "-rc",
                        "cbr",
                        "-b:v",
                        "2000k",
                        "-maxrate",
                        "2500k",
                        "-bufsize",
                        "3000k",
                    ]
                )
            else:
                cmd.extend(
                    [
                        "-c:v",
                        "libx264",
                        "-preset",
                        "medium",
                        "-profile:v",
                        "baseline",
                        "-level:v",
                        "3.0",
                        "-pix_fmt",
                        "yuv420p",
                        "-b:v",
                        "2000k",
                        "-maxrate",
                        "2500k",
                        "-bufsize",
                        "3000k",
                    ]
                )

            cmd.extend(
                [
                    "-c:a",
                    "aac",
                    "-b:a",
                    "192k",
                    "-ar",
                    "48000",
                    "-ac",
                    "2",
                    "-max_muxing_queue_size",
                    "1024",
                    "-movflags",
                    "+faststart",
                    str(temp_output),
                ]
            )

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
                # Lecture en continu des lignes de progression
                while True:
                    line = process.stdout.readline()
                    if not line:
                        break
                    line = line.strip()
                    if line.startswith("out_time_ms="):
                        try:
                            out_time_ms = int(line.split("=")[1])
                            pbar.update(max(0, out_time_ms - pbar.n))
                        except Exception:
                            pass
                    if line == "progress=end":
                        break
                pbar.close()
                process.wait()
                if process.returncode == 0 and temp_output.exists():
                    temp_output.rename(output_path)
                    logger.info(f"Normalisation réussie pour {video_path.name}")
                    return output_path
                else:
                    stderr = process.stderr.read()
                    logger.error(
                        f"Échec de la normalisation de {video_path.name} (tentative {attempt+1}): {stderr}"
                    )
                    if temp_output.exists():
                        temp_output.unlink()
            except Exception as e:
                logger.error(
                    f"Erreur pendant FFmpeg pour {video_path.name} (tentative {attempt+1}): {e}"
                )
                if temp_output.exists():
                    temp_output.unlink()
            attempt += 1
            time.sleep(2)  # Petite pause avant nouvelle tentative
        return None

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

    def process_videos(self):
        """Traite toutes les vidéos sources"""
        try:
            # Scan des fichiers source
            source_files = []
            for ext in self.video_extensions:
                source_files.extend(self.channel_dir.glob(f"*{ext}"))

            source_files = sorted(source_files)

            if not source_files:
                logger.info("Aucun fichier vidéo source trouvé")
                return []

            processed_files = []

            # Traitement des fichiers
            for source_file in source_files:
                processed = self._normalize_video(source_file, NORMALIZATION_PARAMS)
                if processed:
                    processed_files.append(processed)
                elif self._is_already_normalized(source_file, NORMALIZATION_PARAMS):
                    processed_files.append(
                        self.processed_dir / f"{source_file.stem}.mp4"
                    )

            self.processed_videos = processed_files
            return processed_files

        except Exception as e:
            logger.error(f"Erreur traitement vidéos: {e}")
            return []


# -----------------------------------------------------------------------------
# Gestion d'une chaîne IPTV (streaming et surveillance)
# -----------------------------------------------------------------------------
class IPTVChannel:
    def __init__(self, name: str, video_dir: str, use_gpu: bool = False):
        self.use_gpu = use_gpu
        self.name = name
        self.video_dir = video_dir
        self.processor = VideoProcessor(self.video_dir, use_gpu=use_gpu)
        self.processed_videos = []
        self.ffmpeg_process = None
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self._create_channel_directory()
        self.monitoring_thread = None
        self.last_segment_time = 0
        self.error_count = 0
        self.max_errors = 3
        self.restart_delay = 5  # en secondes
        self.last_successful_segment = 0
        self.segment_check_interval = 2  # Vérifier toutes les 2 secondes
        self.segment_timeout = 10  # Réduire le timeout à 10s au lieu de 30s
        self.target_duration = 4  # Durée cible des segments
        self.wrap_threshold = 20  # Seuil de rotation des segments
        self.media_sequence = 0  # Séquence média initiale
        self.segment_buffer = 15
        self.min_segment_size = 1024  # 1KB minimum
        self.current_segments = {}  # Pour tracker les segments actifs
        self.last_playlist = None  # Garder une copie de la dernière playlist valide
        self.stream_start_time = None
        self.media_sequence = 0
        self.segment_count = 0
        self.wrap_count = 20
        self.fallback_mode = False  # False = mode stream copy, True = mode ré-encodage

    def start_stream(self) -> bool:
        with self.lock:
            try:
                if not self.processed_videos:
                    logger.error(f"❌ Aucune vidéo traitée pour {self.name}")
                    return False
                if not self._check_system_resources():
                    logger.error(
                        f"❌ Ressources système insuffisantes pour {self.name}"
                    )
                    return False
                hls_dir = f"hls/{self.name}"
                self._clean_hls_directory()  # On vide le dossier HLS avant de démarrer
                cmd = self._build_ffmpeg_command(hls_dir)
                self.ffmpeg_process = self._start_ffmpeg_process(cmd)
                if not self.ffmpeg_process:
                    return False
                self._start_monitoring(hls_dir)
                return True
            except Exception as e:
                logger.error(
                    f"Erreur lors du démarrage du stream pour {self.name} : {e}"
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
                        f"Erreur lors du nettoyage du processus pour {self.name} : {e}"
                    )

    def _create_channel_directory(self):
        Path(f"hls/{self.name}").mkdir(parents=True, exist_ok=True)

    def scan_videos(self) -> bool:
        try:
            processor = VideoProcessor(self.video_dir)
            self.processed_videos = processor.process_videos()
            return len(self.processed_videos) > 0
        except Exception as e:
            logger.error(f"Erreur lors du scan des vidéos pour {self.name} : {e}")
            return False

    def _clean_hls_directory(self):
        try:
            hls_dir = Path(f"hls/{self.name}")
            if hls_dir.exists():
                for item in hls_dir.iterdir():
                    try:
                        if item.is_file():
                            item.unlink()
                        elif item.is_dir():
                            shutil.rmtree(item)
                    except OSError:
                        pass
        except Exception as e:
            logger.error(
                f"Erreur lors du nettoyage du dossier HLS pour {self.name} : {e}"
            )

    def _start_monitoring(self, hls_dir: str):
        if self.monitoring_thread is None:
            self.monitoring_thread = threading.Thread(
                target=self._monitor_ffmpeg, args=(hls_dir,), daemon=True
            )
            self.monitoring_thread.start()

    def _create_concat_file(self) -> "Optional[Path]":
        try:
            concat_file = Path(self.video_dir) / "_playlist.txt"
            if not self.processed_videos:
                logger.error("Aucune vidéo traitée disponible")
                return None
            with open(concat_file, "w", encoding="utf-8") as f:
                for video in self.processed_videos:
                    f.write(f"file '{video.absolute()}'\n")
            logger.info(f"Fichier de concaténation créé : {concat_file}")
            return concat_file
        except Exception as e:
            logger.error(
                f"Erreur lors de la création du fichier de concaténation pour {self.name} : {e}"
            )
            return None

    def _start_ffmpeg_process(self, cmd: list) -> "Optional[subprocess.Popen]":
        try:
            logger.info(f"🖥️ Commande FFmpeg pour {self.name} : {' '.join(cmd)}")
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
            )
            # Attendre la création du premier segment
            start_time = time.time()
            hls_dir = Path(f"hls/{self.name}")
            while time.time() - start_time < 10:  # 10s timeout
                if list(hls_dir.glob("segment_*.ts")):
                    self.stream_start_time = time.time()
                    logger.info(
                        f"✅ FFmpeg démarré pour {self.name} avec PID {process.pid}"
                    )
                    return process
                time.sleep(0.1)

            logger.error(f"❌ Timeout en attendant les segments pour {self.name}")
            process.kill()
            return None
        except Exception as e:
            logger.error(f"Erreur lors du démarrage de FFmpeg pour {self.name} : {e}")
            return None

    def _build_ffmpeg_command(self, hls_dir: str) -> list:
        audio_codec = "aac" if self.fallback_mode else "copy"

        if self.fallback_mode:
            if self.use_gpu:
                video_codec = "h264_nvenc"
                preset = [
                    "-preset",
                    "p4",
                    "-profile:v",
                    "baseline",
                    "-level:v",
                    "3.0",
                    "-pix_fmt",
                    "yuv420p",
                    "-rc",
                    "cbr",
                    "-g",
                    "50",
                    "-sc_threshold",
                    "0",
                ]
            else:
                video_codec = "libx264"
                preset = [
                    "-preset",
                    "medium",
                    "-profile:v",
                    "baseline",
                    "-level:v",
                    "3.0",
                    "-pix_fmt",
                    "yuv420p",
                    "-r",
                    "25",
                    "-g",
                    "50",
                    "-keyint_min",
                    "25",
                    "-sc_threshold",
                    "0",
                ]
        else:
            video_codec = "copy"
            preset = []

        cmd = (
            [
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "info",
                "-y",
                "-re",
                "-fflags",
                "+genpts+igndts",
                "-f",
                "concat",
                "-safe",
                "0",
                "-stream_loop",
                "-1",
                "-i",
                str(self._create_concat_file()),
            ]
            + preset
            + [
                "-c:v",
                video_codec,
                "-c:a",
                audio_codec,
                "-f",
                "hls",
                "-hls_time",
                "4",
                "-hls_list_size",
                str(self.segment_buffer),
                "-hls_flags",
                "delete_segments+append_list+program_date_time",
                "-hls_segment_type",
                "mpegts",
                "-hls_init_time",
                "4",
                "-hls_segment_filename",
                f"{hls_dir}/segment_%d.ts",
                f"{hls_dir}/playlist.m3u8",
            ]
        )

        logger.info(
            f"Commande FFmpeg ({'fallback' if self.fallback_mode else 'normal'}) pour {self.name} : {' '.join(cmd)}"
        )
        return cmd

    def _monitor_ffmpeg(self, hls_dir: str):
        self.last_segment_time = time.time()
        last_segment_number = -1
        hls_dir = Path(hls_dir)
        crash_threshold = 10  # Seuil en secondes avant de logger le bug

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
                    except ValueError:
                        current_segment = -1
                    if current_segment != last_segment_number:
                        self.last_segment_time = current_time
                        last_segment_number = current_segment
                        self.error_count = 0

                        # Mise à jour de la playlist HLS
                        playlist_content = self._generate_playlist(segments)
                        playlist_path = hls_dir / "playlist.m3u8"
                        temp_playlist = playlist_path.with_suffix(".m3u8.tmp")
                        temp_playlist.write_text(playlist_content)
                        temp_playlist.replace(playlist_path)

                        # (Optionnel) Nettoyage des anciens segments ici...

                elapsed = current_time - self.last_segment_time
                if elapsed > crash_threshold:
                    message = f"[{self.name}] Aucune mise à jour de segment depuis {elapsed:.1f} secondes."
                    print(message)
                    crash_logger.info(message)

                time.sleep(1)
            except Exception as e:
                logger.error(f"Erreur monitoring {self.name} : {e}")
                time.sleep(1)

    def _generate_playlist(self, segments: list) -> str:
        """Génère une playlist HLS avec PDT et séquence média."""
        segments = sorted(segments, key=lambda x: int(x.stem.split("_")[1]))

        if not segments:
            return ""

        # Calcul des paramètres de base
        first_segment = min(int(s.stem.split("_")[1]) for s in segments)
        segment_count = len(segments)

        # En-tête de la playlist
        playlist = [
            "#EXTM3U",
            "#EXT-X-VERSION:3",
            f"#EXT-X-TARGETDURATION:{self.target_duration}",
            f"#EXT-X-MEDIA-SEQUENCE:{first_segment}",
            "#EXT-X-ALLOW-CACHE:NO",
        ]

        # Calcul du temps de base pour PDT
        base_time = datetime.datetime.utcnow() - datetime.timedelta(
            seconds=segment_count * self.target_duration
        )

        # Génération des segments
        for idx, segment in enumerate(segments):
            segment_time = (
                base_time + datetime.timedelta(seconds=idx * self.target_duration)
            ).strftime("%Y-%m-%dT%H:%M:%S.000Z")

            playlist.extend(
                [
                    f"#EXT-X-PROGRAM-DATE-TIME:{segment_time}",
                    f"#EXTINF:{self.target_duration:.3f},",
                    segment.name,
                ]
            )

        return "\n".join(playlist)

    def _restart_stream(self):
        try:
            logger.info(f"🔄 Redémarrage du stream {self.name}")
            # On sauvegarde l'état actuel (séquence média et nombre de segments)
            current_state = {
                "media_sequence": self.media_sequence,
                "segment_count": self.segment_count,
            }

            # On arrête proprement le processus FFmpeg en cours
            self._clean_processes()

            # On prépare un dossier temporaire pour sauvegarder la playlist et quelques segments
            hls_dir = Path(f"hls/{self.name}")
            temp_dir = hls_dir / "temp"
            temp_dir.mkdir(exist_ok=True)

            if hls_dir.exists():
                # On sauvegarde la playlist actuelle si elle existe
                playlist = hls_dir / "playlist.m3u8"
                if playlist.exists():
                    shutil.copy2(playlist, temp_dir / "playlist.m3u8")
                # On sauvegarde les 3 derniers segments
                segments = sorted(
                    list(hls_dir.glob("segment_*.ts")),
                    key=lambda x: x.stat().st_mtime,
                    reverse=True,
                )[:3]
                for seg in segments:
                    shutil.copy2(seg, temp_dir / seg.name)

            # On nettoie le dossier HLS
            self._clean_hls_directory()

            # On restaure la playlist et les segments sauvegardés
            if temp_dir.exists():
                for item in temp_dir.iterdir():
                    shutil.copy2(item, hls_dir / item.name)
                shutil.rmtree(temp_dir)

            # On restaure l'état de la séquence média
            self.media_sequence = current_state["media_sequence"]
            self.segment_count = current_state["segment_count"]
            self.error_count = 0

            # Activation du mode fallback si ce n'est pas déjà fait
            if not self.fallback_mode:
                logger.info(
                    "Activation du mode ré-encodage (fallback mode) pour garantir la compatibilité"
                )
                self.fallback_mode = True

            # On recrée le fichier de concaténation pour FFmpeg
            concat_file = self._create_concat_file()
            if not concat_file:
                logger.error("Échec de la recréation du fichier de concaténation")
                return False

            # Petite pause avant de redémarrer
            time.sleep(self.restart_delay)

            # On lance le stream avec la nouvelle configuration
            stream_started = self.start_stream()

            # Optionnel : on attend que le fichier playlist.m3u8 soit bien créé (timeout 10s)
            timeout = 10
            start_time = time.time()
            playlist_path = hls_dir / "playlist.m3u8"
            while not playlist_path.exists() and (time.time() - start_time < timeout):
                time.sleep(0.5)
            if not playlist_path.exists():
                logger.error(
                    f"La playlist {playlist_path} n'a pas été créée dans le délai imparti"
                )
                return False

            return stream_started
        except Exception as e:
            logger.error(f"Erreur lors du redémarrage du stream {self.name}: {e}")
            return False

    def start_processing(self):
        """Start asynchronous video processing"""
        self.processor.process_videos_async()

    def wait_for_processing(self):
        """Wait for video processing to complete"""
        self.processed_videos = self.processor.wait_for_completion()

    def _check_stream_health(self) -> bool:
        """Vérifie l'état du stream"""
        try:
            if not self.ffmpeg_process:
                return False
            return self.ffmpeg_process.poll() is None
        except Exception as e:
            logger.error(f"Erreur vérification santé {self.name}: {e}")
            return False


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
                f"Fichier log introuvable pour le monitoring des clients : {self.log_file}"
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
    def __init__(self, content_dir: str, use_gpu: bool = False):
        self.content_dir = content_dir
        self.use_gpu = use_gpu
        self.channels = {}
        self.scan_lock = threading.Lock()

        logger.info("Initialisation du gestionnaire IPTV")

        # Clean HLS directory
        hls_dir = Path("./hls")
        if hls_dir.exists():
            logger.info("Nettoyage du dossier HLS")
            self._clean_directory(hls_dir)
        hls_dir.mkdir(exist_ok=True)

        # Set up file monitoring
        self.observer = Observer()
        event_handler = ChannelEventHandler(self)
        self.observer.schedule(event_handler, self.content_dir, recursive=True)

        # Initial channel scan
        logger.info(f"Démarrage du scan initial dans {self.content_dir}")
        self.scan_channels(initial=True, force=True)

        # Start monitors
        self.client_monitor = ClientMonitor(NGINX_ACCESS_LOG)
        self.client_monitor.start()
        self.resource_monitor = ResourceMonitor()
        self.resource_monitor.start()

        # Start playlist updater
        self.master_playlist_updater = threading.Thread(
            target=self._master_playlist_loop, daemon=True
        )
        self.master_playlist_updater.start()

    def _start_playlist_updater(self):
        # Cancelle l'ancien updater si existant
        if hasattr(self, "playlist_timer"):
            self.playlist_timer.cancel()

        # Démarre le nouveau timer
        self.playlist_timer = threading.Timer(10, self._update_master_playlist)
        self.playlist_timer.daemon = True
        self.playlist_timer.start()

    def scan_channels(self, force: bool = False, initial: bool = False):
        with self.scan_lock:
            try:
                content_path = Path(self.content_dir)
                if not content_path.exists():
                    logger.error(f"Le dossier {content_path} n'existe pas!")
                    return

                # Initial playlist generation
                self.generate_master_playlist()
                self._start_playlist_updater()

                channel_dirs = [d for d in content_path.iterdir() if d.is_dir()]
                processing_channels = []
                processed_channels = set()

                # Rest of the existing code stays the same
                for channel_dir in channel_dirs:
                    try:
                        channel_name = channel_dir.name
                        video_files = list(channel_dir.glob("*.mp4"))

                        if not video_files:
                            logger.warning(f"Aucun fichier MP4 dans {channel_name}")
                            continue

                        if channel_name in self.channels:
                            channel = self.channels[channel_name]
                            if force or initial or self._needs_update(channel_dir):
                                logger.info(f"Mise à jour du contenu: {channel_name}")
                                self._update_channel_playlist(channel, channel_dir)
                        else:
                            logger.info(f"Nouvelle chaîne: {channel_name}")
                            channel = IPTVChannel(
                                channel_name, str(channel_dir), use_gpu=self.use_gpu
                            )
                            self.channels[channel_name] = channel
                            channel.start_processing()
                            processing_channels.append(channel)

                    except Exception as e:
                        logger.error(f"Erreur traitement {channel_dir.name}: {e}")
                        continue

                # Second pass: Start streams for already processed channels
                for name, channel in self.channels.items():
                    if channel not in processing_channels and channel.processed_videos:
                        if channel.start_stream():
                            processed_channels.add(name)

                # Third pass: Wait for processing and start remaining streams
                for channel in processing_channels:
                    channel.wait_for_processing()
                    if channel.start_stream():
                        processed_channels.add(channel.name)

                # Final status update
                logger.info(f"\nRésumé du traitement:")
                logger.info(f"Total dossiers: {len(channel_dirs)}")
                logger.info(f"Chaînes actives: {len(processed_channels)}")
                logger.info(f"Chaînes: {', '.join(processed_channels)}")

                # Update master playlist
                self.generate_master_playlist()

                # Verify channel health
                self._verify_channels_health()

            except Exception as e:
                logger.error(f"Erreur scan des chaînes: {e}")
                logger.error(traceback.format_exc())

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

    def _verify_channels_health(self):
        """Verify all channels are running properly"""
        active_channels = []
        for name, channel in self.channels.items():
            if not channel._check_stream_health():
                logger.warning(f"Chaîne inactive: {name}, tentative redémarrage")
                if channel.start_stream():
                    active_channels.append(name)
            else:
                active_channels.append(name)

        if not active_channels:
            logger.error("CRITIQUE: Aucune chaîne active!")
            self._clean_directory(Path("./hls"))
            time.sleep(5)
            self.scan_channels(force=True)
        else:
            logger.info(f"Chaînes actives: {', '.join(active_channels)}")

    def _master_playlist_loop(self):
        """Boucle de mise à jour continue de la master playlist."""
        while True:

            try:
                self.generate_master_playlist()
                time.sleep(10)
            except Exception as e:
                logger.error(f"Erreur mise à jour master playlist: {e}")

    def _update_master_playlist(self):
        self.generate_master_playlist()
        self.master_playlist_timer = threading.Timer(10, self._update_master_playlist)
        self.master_playlist_timer.start()

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

    def generate_master_playlist(self):
        try:
            playlist_path = os.path.abspath("./hls/playlist.m3u")
            logger.info(f"On génère la master playlist à {playlist_path}")
            with open(playlist_path, "w", encoding="utf-8") as f:
                f.write("#EXTM3U\n")
                for name, channel in sorted(self.channels.items()):
                    hls_playlist = f"./hls/{name}/playlist.m3u8"
                    if not os.path.exists(hls_playlist):
                        logger.warning(
                            f"Playlist HLS manquante pour {name}, tentative de redémarrage"
                        )
                        channel.start_stream()
                    logger.info(f"Ajout de la chaîne {name} à la master playlist")
                    f.write(f'#EXTINF:-1 tvg-id="{name}" tvg-name="{name}",{name}\n')
                    f.write(f"http://{SERVER_URL}/hls/{name}/playlist.m3u8\n")
            logger.info(
                f"Master playlist mise à jour avec {len(self.channels)} chaînes"
            )
        except Exception as e:
            logger.error(f"Erreur lors de la génération de la master playlist : {e}")
            import traceback

            logger.error(traceback.format_exc())

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

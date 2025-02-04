#!/usr/bin/env python3
import os
import sys
import subprocess
import json
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
            self._handle_event(event)


# -----------------------------------------------------------------------------
# Traitement et normalisation des vidéos
# -----------------------------------------------------------------------------
class VideoProcessor:
    def __init__(self, channel_dir: str):
        self.channel_dir = Path(channel_dir)
        self.video_extensions = (".mp4", ".avi", ".mkv", ".mov")
        # On crée le dossier pour les vidéos normalisées
        self.processed_dir = self.channel_dir / "processed"
        self.processed_dir.mkdir(exist_ok=True)

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
            cmd = [
                "ffmpeg",
                "-y",
                "-i",
                str(video_path),
                "-c:v",
                "libx264",
                "-preset",
                "ultrafast",
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
                "-b:v",
                "2000k",
                "-maxrate",
                "2500k",
                "-bufsize",
                "3000k",
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
                "-progress",
                "pipe:1",
                str(temp_output),
            ]
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

    def process_videos(self) -> list:
        """
        Traite tous les fichiers vidéo du dossier source et retourne la liste
        des fichiers normalisés dans processed.
        """
        try:
            source_files = sorted(
                [
                    f
                    for f in self.channel_dir.glob("*.*")
                    if f.suffix.lower() in self.video_extensions
                ]
            )
            if not source_files:
                logger.error("Aucun fichier vidéo trouvé")
                return []
            processed_files = []
            ref_info = NORMALIZATION_PARAMS
            for source_file in source_files:
                processed_path = self._normalize_video(source_file, ref_info)
                if processed_path:
                    processed_files.append(processed_path)
                else:
                    logger.error(f"Échec du traitement pour {source_file}")
                    return []  # On arrête en cas d'erreur
            return processed_files
        except Exception as e:
            logger.error(f"Erreur dans le traitement des vidéos : {e}")
            return []

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
    def __init__(self, name: str, video_dir: str):
        self.name = name
        self.video_dir = video_dir
        self.ffmpeg_process = None
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self._create_channel_directory()
        self.processed_videos = []
        self.monitoring_thread = None
        self.last_segment_time = 0
        self.error_count = 0
        self.max_errors = 3
        self.restart_delay = 5  # en secondes

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

    def _build_ffmpeg_command(self, hls_dir: str) -> list:
        concat_file = self._create_concat_file()
        if not concat_file:
            raise Exception("Impossible de créer le fichier de concaténation")
        return [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "info",
            "-y",
            "-re",
            "-f",
            "concat",
            "-safe",
            "0",
            "-stream_loop",
            "-1",
            "-i",
            str(concat_file),
            "-c:v",
            "copy",
            "-c:a",
            "copy",
            "-f",
            "hls",
            "-hls_time",
            "2",
            "-hls_list_size",
            "10",
            "-hls_flags",
            "delete_segments+append_list+independent_segments",
            "-hls_segment_type",
            "mpegts",
            "-hls_segment_filename",
            f"{hls_dir}/segment_%d.ts",
            f"{hls_dir}/playlist.m3u8",
        ]

    def _monitor_ffmpeg(self, hls_dir: str):
        self.last_segment_time = time.time()
        segment_timeout = 10  # secondes sans nouveau segment
        while (
            not self.stop_event.is_set()
            and self.ffmpeg_process
            and self.ffmpeg_process.poll() is None
        ):
            try:
                current_time = time.time()
                segments = list(Path(hls_dir).glob("segment_*.ts"))
                if segments:
                    last_segment = max(segments, key=lambda f: f.stat().st_mtime)
                    self.last_segment_time = last_segment.stat().st_mtime
                if current_time - self.last_segment_time > segment_timeout:
                    self.error_count += 1
                    logger.error(
                        f"⚠️ Aucun nouveau segment depuis {segment_timeout}s pour {self.name}"
                    )
                    if self.error_count >= self.max_errors:
                        logger.error(
                            f"🔄 Redémarrage du stream pour {self.name} après {self.error_count} erreurs"
                        )
                        self._restart_stream()
                        break
                if not self._check_system_resources():
                    logger.warning(f"⚠️ Ressources système faibles pour {self.name}")
                time.sleep(1)
            except Exception as e:
                logger.error(f"Erreur dans la surveillance de {self.name} : {e}")
                self._restart_stream()
                break

    def _check_system_resources(self) -> bool:
        try:
            cpu_percent = psutil.cpu_percent()
            memory_percent = psutil.virtual_memory().percent
            if cpu_percent > 95 and memory_percent > 90:
                logger.warning(
                    f"Ressources critiques - CPU: {cpu_percent}%, RAM: {memory_percent}%"
                )
                return False
            return True
        except Exception as e:
            logger.error(f"Erreur lors de la vérification des ressources : {e}")
            return True

    def _restart_stream(self):
        try:
            logger.info(f"🔄 Redémarrage du stream {self.name}")
            self._clean_processes()
            time.sleep(self.restart_delay)
            self.error_count = 0
            self.start_stream()
        except Exception as e:
            logger.error(f"Erreur lors du redémarrage du stream {self.name} : {e}")

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
            time.sleep(2)
            if process.poll() is not None:
                stderr = process.stderr.read()
                logger.error(
                    f"❌ FFmpeg s'est arrêté immédiatement pour {self.name}. Erreur : {stderr}"
                )
                return None
            logger.info(f"✅ FFmpeg démarré pour {self.name} avec PID {process.pid}")
            return process
        except Exception as e:
            logger.error(f"Erreur lors du démarrage de FFmpeg pour {self.name} : {e}")
            return None


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
    def __init__(self, content_dir: str):
        self.content_dir = content_dir
        self.channels = {}
        self.scan_lock = threading.Lock()
        logger.info("On initialise le gestionnaire IPTV")
        # On vide le dossier hls au démarrage
        hls_dir = Path("./hls")
        if hls_dir.exists():
            logger.info("On vide le dossier hls au démarrage")
            self._clean_directory(hls_dir)
        hls_dir.mkdir(exist_ok=True)
        logger.info("Le dossier hls est prêt")
        self.observer = Observer()
        event_handler = ChannelEventHandler(self)
        self.observer.schedule(event_handler, self.content_dir, recursive=True)
        logger.info(f"Démarrage du scan initial dans {self.content_dir}")
        self.scan_channels(initial=True, force=True)
        self.generate_master_playlist()

        # Lancer le monitoring des clients via le log d'Nginx
        self.client_monitor = ClientMonitor(NGINX_ACCESS_LOG)
        self.client_monitor.start()

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

    def scan_channels(self, force: bool = False, initial: bool = False):
        with self.scan_lock:
            try:
                content_path = Path(self.content_dir)
                if not content_path.exists():
                    logger.error(f"Le dossier {content_path} n'existe pas !")
                    return
                channel_dirs = [d for d in content_path.iterdir() if d.is_dir()]
                logger.info(f"{len(channel_dirs)} dossiers de chaînes trouvés :")
                for d in channel_dirs:
                    logger.info(f"- {d.name}")
                processed_channels = set()
                for channel_dir in channel_dirs:
                    try:
                        channel_name = channel_dir.name
                        logger.info(f"\nOn traite le dossier : {channel_name}")
                        all_files = list(channel_dir.glob("*.*"))
                        logger.info(f"Fichiers dans {channel_name} :")
                        for f in all_files:
                            logger.info(f"  - {f.name} ({f.stat().st_size} octets)")
                        video_files = [
                            f for f in all_files if f.suffix.lower() == ".mp4"
                        ]
                        if not video_files:
                            logger.warning(
                                f"Aucun fichier MP4 dans {channel_name}, on passe"
                            )
                            continue
                        logger.info(
                            f"{len(video_files)} fichiers vidéo trouvés dans {channel_name}"
                        )
                        if channel_name in self.channels:
                            channel = self.channels[channel_name]
                            if force or initial or self._needs_update(channel_dir):
                                logger.info(
                                    f"Mise à jour de la chaîne existante : {channel_name}"
                                )
                                success = channel.scan_videos()
                                logger.info(
                                    f"Scan vidéo {'réussi' if success else 'échoué'} pour {channel_name}"
                                )
                                if success:
                                    if not channel.start_stream():
                                        logger.error(
                                            f"Échec du démarrage du stream pour {channel_name}"
                                        )
                                    else:
                                        processed_channels.add(channel_name)
                        else:
                            logger.info(
                                f"Configuration d'une nouvelle chaîne : {channel_name}"
                            )
                            channel = IPTVChannel(channel_name, str(channel_dir))
                            success = channel.scan_videos()
                            logger.info(
                                f"Scan vidéo {'réussi' if success else 'échoué'} pour {channel_name}"
                            )
                            if success:
                                self.channels[channel_name] = channel
                                if not channel.start_stream():
                                    logger.error(
                                        f"Échec du démarrage du stream pour {channel_name}"
                                    )
                                else:
                                    processed_channels.add(channel_name)
                        hls_dir = Path(f"./hls/{channel_name}")
                        if hls_dir.exists():
                            logger.info(f"Le dossier HLS existe pour {channel_name}")
                            hls_files = list(hls_dir.glob("*.*"))
                            logger.info(f"Fichiers HLS pour {channel_name} :")
                            for f in hls_files:
                                logger.info(f"  - {f.name}")
                        else:
                            logger.error(f"Dossier HLS manquant pour {channel_name}")
                    except Exception as e:
                        logger.error(
                            f"Erreur lors du traitement de {channel_dir.name} : {e}"
                        )
                        import traceback

                        logger.error(traceback.format_exc())
                        continue
                logger.info("\nRésumé du traitement des chaînes :")
                logger.info(f"Total de dossiers trouvés : {len(channel_dirs)}")
                logger.info(f"Chaînes traitées avec succès : {len(processed_channels)}")
                logger.info(f"Chaînes actives : {', '.join(self.channels.keys())}")
                self.generate_master_playlist()
            except Exception as e:
                logger.error(f"Erreur lors du scan des chaînes : {e}")
                import traceback

                logger.error(traceback.format_exc())

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


if __name__ == "__main__":
    manager = IPTVManager("./content")
    try:
        manager.run()
    except KeyboardInterrupt:
        logger.info("Interruption utilisateur détectée")
        manager.cleanup()
    except Exception as e:
        logger.error(f"Erreur fatale : {e}")
        manager.cleanup()
        raise

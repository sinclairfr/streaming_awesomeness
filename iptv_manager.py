import os
from pathlib import Path
import subprocess
import json
import hashlib
import shutil
import logging
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor
import threading
import time
import psutil
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Configuration du logging
logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='%(asctime)s - %(name)s - [%(levelname)s] - %(message)s'
)
logger = logging.getLogger(__name__)

SERVER_URL = os.getenv('SERVER_URL', '192.168.10.183')

class ChannelEventHandler(FileSystemEventHandler):
    def __init__(self, manager):
        self.manager = manager
        self.last_event_time = 0
        self.event_delay = 5  # Délai minimum entre deux événements (en secondes)
        super().__init__()

    def on_modified(self, event):
        if not event.is_directory:
            current_time = time.time()
            if current_time - self.last_event_time >= self.event_delay:
                logger.debug(f"Modification détectée: {event.src_path}")
                self.manager.scan_channels()
                self.last_event_time = current_time

    def on_created(self, event):
        if event.is_directory:
            current_time = time.time()
            if current_time - self.last_event_time >= self.event_delay:
                logger.info(f"Nouvelle chaîne détectée: {event.src_path}")
                self.manager.scan_channels()
                self.last_event_time = current_time
                
class IPTVChannel:
    def __init__(self, name: str, video_dir: str, cache_dir: str):
        self.name = name
        self.video_dir = video_dir
        self.cache_dir = cache_dir
        self.videos: List[Dict] = []
        self.current_video = 0
        self.total_duration = 0
        self.active_streams = 0
        self.ffmpeg_process = None
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.executor = ThreadPoolExecutor(max_workers=2)
        self.last_mtime = 0

    def start_stream(self):
        """Démarre le flux IPTV"""
        with self.lock:
            try:
                if not self.videos:
                    logger.error(f"🚫 Aucune vidéo disponible pour {self.name}")
                    return False

                # Vérifier si le flux est déjà en cours
                if self.ffmpeg_process and self.ffmpeg_process.poll() is None:
                    logger.debug(f"⚡ Flux déjà en cours pour {self.name}")
                    return True

                self.active_streams += 1
                if self.active_streams == 1:  # Démarrer le stream uniquement pour le premier spectateur
                    self.stop_event.clear()
                    self._start_ffmpeg()
                    logger.info(f"📡 Stream démarré pour {self.name}")
                return True
            except Exception as e:
                logger.error(f"🔥 Erreur lors du démarrage du stream pour {self.name}: {e}")
                return False

    def _get_video_duration(self, video_path: str) -> float:
        """Obtient la durée d'une vidéo en secondes"""
        try:
            cmd = [
                "ffprobe",
                "-v", "quiet",
                "-print_format", "json",
                "-show_format",
                video_path
            ]

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )

            data = json.loads(result.stdout)
            duration = float(data["format"]["duration"])
            logger.debug(f"Durée de {video_path}: {duration} secondes")
            return duration

        except subprocess.CalledProcessError as e:
            logger.error(f"Erreur ffprobe pour {video_path}: {e.stderr}")
            return 0
        except json.JSONDecodeError as e:
            logger.error(f"Erreur de parsing JSON pour {video_path}: {e}")
            return 0
        except KeyError as e:
            logger.error(f"Format de données incorrect pour {video_path}: {e}")
            return 0
        except Exception as e:
            logger.error(f"Erreur inattendue pour {video_path}: {e}")
            return 0

    def scan_videos(self) -> bool:
        """Scan les vidéos avec cache basé sur mtime"""
        try:
            current_mtime = os.stat(self.video_dir).st_mtime
            if current_mtime == self.last_mtime:
                logger.debug(f"Pas de changement dans {self.name} depuis le dernier scan")
                return False

            videos = []
            video_extensions = ('.mp4', '.avi', '.mkv', '.mov')

            for file in sorted(Path(self.video_dir).glob('*.*')):
                if file.suffix.lower() in video_extensions:
                    try:
                        duration = self._get_video_duration(str(file))
                        if duration > 0:
                            videos.append({
                                "path": str(file),
                                "duration": duration,
                                "mtime": file.stat().st_mtime
                            })
                    except (FileNotFoundError, json.JSONDecodeError) as e:
                        logger.error(f"Erreur lors de l'analyse de {file}: {e}")

            if videos != self.videos:
                self.videos = videos
                self.total_duration = sum(v["duration"] for v in videos)
                self._create_channel_directory()
                self._create_initial_playlist()
                self.last_mtime = current_mtime
                return True

            return False

        except Exception as e:
            logger.error(f"Erreur lors du scan des vidéos pour {self.name}: {e}")
            return False

    def _create_channel_directory(self):
        """Crée le répertoire HLS pour la chaîne"""
        try:
            channel_dir = f"/hls/{self.name}"
            os.makedirs(channel_dir, exist_ok=True)
            logger.debug(f"Répertoire créé pour {self.name}: {channel_dir}")
            return True
        except Exception as e:
            logger.error(f"Erreur lors de la création du répertoire pour {self.name}: {e}")
            return False

    def _create_initial_playlist(self):
        """Crée la playlist initiale vide pour la chaîne"""
        try:
            playlist_path = f"/hls/{self.name}/playlist.m3u8"
            with open(playlist_path, 'w') as f:
                f.write("#EXTM3U\n#EXT-X-VERSION:3\n")
            logger.debug(f"Playlist initiale créée pour {self.name}")
            return True
        except Exception as e:
            logger.error(f"Erreur lors de la création de la playlist pour {self.name}: {e}")
            return False

    def _get_cached_path(self, video_path: str) -> str:
        """Obtient ou crée une version optimisée du fichier dans le cache"""
        original_file = Path(video_path)
        file_hash = hashlib.md5(f"{video_path}_{original_file.stat().st_mtime}".encode()).hexdigest()
        cached_path = Path(self.cache_dir) / f"{file_hash}.mp4"

        if cached_path.exists():
            logger.info(f"Utilisation du cache pour {original_file.name}")
            return str(cached_path)

        logger.info(f"Optimisation et mise en cache de {original_file.name}")

        file_extension = original_file.suffix.lower()
        if file_extension == '.mp4':
            logger.info(f"Le fichier {original_file.name} est déjà MP4, copie simple")
            shutil.copy2(video_path, cached_path)
        else:
            logger.info(f"Conversion de {original_file.name} en MP4")
            cmd = [
                "ffmpeg", "-y",
                "-i", video_path,
                "-c:v", "libx264",
                "-preset", "medium",
                "-crf", "23",
                "-c:a", "aac",
                "-movflags", "+faststart",
                str(cached_path)
            ]

            try:
                subprocess.run(cmd, check=True, capture_output=True, text=True)
                logger.info(f"Fichier mis en cache avec succès: {cached_path}")
            except subprocess.CalledProcessError as e:
                logger.error(f"Erreur lors du transcodage: {e.stderr}")
                return video_path

        return str(cached_path)

    def _clean_processes(self):
        """Nettoie proprement les processus FFmpeg"""
        try:
            if self.ffmpeg_process:
                process = psutil.Process(self.ffmpeg_process.pid)
                for child in process.children(recursive=True):
                    child.terminate()
                process.terminate()
                process.wait(timeout=5)
                self.ffmpeg_process = None  # Réinitialiser le processus
                logger.info(f"Processus FFmpeg arrêté pour {self.name}")
        except psutil.NoSuchProcess:
            logger.debug(f"Aucun processus FFmpeg trouvé pour {self.name}")
        except Exception as e:
            logger.error(f"Erreur lors du nettoyage des processus pour {self.name}: {e}")
            
    def _is_process_running(self):
        """Vérifie si le processus FFmpeg est en cours d'exécution"""
        return (
            self.ffmpeg_process is not None and 
            self.ffmpeg_process.poll() is None
        )

    def _should_continue_streaming(self):
        """Vérifie si le streaming doit continuer"""
        return (
            not self.stop_event.is_set() and 
            self.active_streams > 0 and 
            bool(self.videos)
        )

    def _reset_stream_state(self):
        """Réinitialise l'état du stream"""
        with self.lock:
            self.ffmpeg_process = None
            self.active_streams = 0
            self.stop_event.set()  
            
    def _start_ffmpeg(self):
        """Démarre FFmpeg et assure la rotation des vidéos"""
        try:
            with self.lock:  # Use the existing lock for thread safety
                if self.ffmpeg_process and self.ffmpeg_process.poll() is None:
                    logger.debug(f"⚡ FFmpeg déjà en cours pour {self.name}")
                    return

                if self.stop_event.is_set():
                    logger.debug(f"🛑 Stop event is set for {self.name}, skipping start")
                    return

                logger.info(f"🚀 Démarrage du flux pour {self.name}")

                if not self.videos:
                    logger.error(f"🚫 Aucune vidéo trouvée pour {self.name}")
                    return

                current_video = self.videos[self.current_video]["path"]
                cached_video = self._get_cached_path(current_video)

                cmd = [
                    "ffmpeg", "-y",
                    "-re",
                    "-i", cached_video,
                    "-c:v", "copy",
                    "-c:a", "copy",
                    "-f", "hls",
                    "-hls_time", "6",
                    "-hls_list_size", "10",
                    "-hls_flags", "delete_segments+append_list",
                    "-hls_segment_filename", f"/hls/{self.name}/segment_%03d.ts",
                    f"/hls/{self.name}/playlist.m3u8",
                    "-threads", "1"
                ]

                logger.info(f"🖥 Commande FFmpeg: {' '.join(cmd)}")

                self.ffmpeg_process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True
                )

                def monitor():
                    process_ended = False
                    try:
                        while not self.stop_event.is_set() and self.ffmpeg_process and self.ffmpeg_process.poll() is None:
                            line = self.ffmpeg_process.stderr.readline()
                            if line.strip():
                                logger.debug(f"📡 FFmpeg: {line.strip()}")
                            time.sleep(0.1)  # Prevent excessive CPU usage

                        process_ended = True
                    except Exception as e:
                        logger.error(f"🔥 Erreur dans le monitoring: {e}")
                    finally:
                        # Only proceed with video rotation if the process actually ended and we should continue
                        if process_ended and not self.stop_event.is_set() and self.active_streams > 0:
                            with self.lock:
                                if self.active_streams > 0:  # Double check under lock
                                    self.current_video = (self.current_video + 1) % len(self.videos)
                                    logger.info(f"Passage à la vidéo suivante pour {self.name}")
                                    # Use executor to start new ffmpeg process to avoid recursion
                                    self.executor.submit(self._start_ffmpeg)

                # Start the monitor in the executor
                self.executor.submit(monitor)

        except Exception as e:
            logger.error(f"🚨 Erreur démarrage FFmpeg: {e}")
            
class IPTVManager:
    def __init__(self, content_dir: str, cache_dir: str = "/cache"):
        self.content_dir = content_dir
        self.cache_dir = cache_dir
        self.channels: Dict[str, IPTVChannel] = {}
        self.last_update_time = time.time()
        self.last_scan_time = 0  # Dernier temps de scan
        self.scan_delay = 10  # Délai minimum entre deux scans (en secondes)

        self.observer = Observer()
        self.observer.schedule(ChannelEventHandler(self), self.content_dir, recursive=True)

        # Création des dossiers nécessaires
        os.makedirs(cache_dir, exist_ok=True)
        os.makedirs("/hls", exist_ok=True)

        logger.info("📡 IPTV Manager initialisé avec Watchdog")

    def scan_channels(self):
        """Scanne et met à jour les chaînes"""
        current_time = time.time()
        if current_time - self.last_scan_time < self.scan_delay:
            logger.debug("Scan ignoré (trop fréquent)")
            return

        self.last_scan_time = current_time
        changes_detected = False
        current_channels = set(self.channels.keys())
        found_channels = set()

        # Parcours tous les dossiers dans content_dir
        for item in Path(self.content_dir).iterdir():
            if item.is_dir():
                channel_name = item.name
                found_channels.add(channel_name)

                # Nouvelle chaîne trouvée
                if channel_name not in self.channels:
                    logger.info(f"Nouvelle chaîne trouvée: {channel_name}")
                    channel = IPTVChannel(channel_name, str(item), self.cache_dir)
                    channel.scan_videos()
                    if channel.videos:
                        self.channels[channel_name] = channel
                        changes_detected = True
                        channel.start_stream()
                        logger.info(f"Nouvelle chaîne {channel_name} ajoutée avec succès")
                else:
                    # Chaîne existante - vérifie les modifications
                    channel = self.channels[channel_name]
                    old_videos = set(v["path"] for v in channel.videos)
                    channel.scan_videos()
                    new_videos = set(v["path"] for v in channel.videos)

                    if old_videos != new_videos:
                        logger.info(f"Changements détectés dans la chaîne {channel_name}")
                        changes_detected = True
                        # Redémarre le stream si les vidéos ont changé
                        if channel.active_streams > 0:
                            channel.start_stream()

        # Vérifie les chaînes supprimées
        removed_channels = current_channels - found_channels
        if removed_channels:
            for channel_name in removed_channels:
                logger.info(f"Chaîne supprimée: {channel_name}")
                if self.channels[channel_name].ffmpeg_process:
                    self.channels[channel_name].ffmpeg_process.terminate()
                del self.channels[channel_name]
                changes_detected = True

        # Met à jour la playlist si des changements sont détectés
        if changes_detected:
            logger.info("Changements détectés, mise à jour des playlists...")
            self.generate_master_playlist()
            logger.info("Playlists mises à jour avec succès")
            
    def generate_master_playlist(self):
        """Génère la playlist pour Tivimate"""
        if time.time() - self.last_update_time < 10:  # ⏳ Mise à jour max toutes les 10s
            return

        self.last_update_time = time.time()
        playlist_path = "/hls/playlist.m3u"

        with open(playlist_path, "w") as f:
            f.write("#EXTM3U\n")
            for name, channel in self.channels.items():
                f.write(f'#EXTINF:-1 tvg-id="{name}" tvg-name="{name}" tvg-logo="" group-title="Movies",{name}\n')
                f.write(f'http://{SERVER_URL}/hls/{name}/playlist.m3u8\n')

        logger.info("🎬 Playlist M3U mise à jour")

    def run(self):
        """Démarre le gestionnaire IPTV avec Watchdog"""
        try:
            self.observer.start()
            self.scan_channels()  # Scan initial

            while True:
                time.sleep(1)  # Boucle infinie pour maintenir le service actif

        except KeyboardInterrupt:
            self.cleanup()  # Nettoyage en cas d'interruption

        except Exception as e:
            logger.error(f"🔥 Erreur dans le gestionnaire IPTV: {e}")
            self.cleanup()

    def cleanup(self):
        """Nettoyage propre à l'arrêt"""
        logger.info("Arrêt du gestionnaire...")
        self.observer.stop()
        self.observer.join()

        for channel in self.channels.values():
            channel._clean_processes()
            channel.executor.shutdown(wait=True)


if __name__ == "__main__":
    manager = IPTVManager("/content")
    manager.run()
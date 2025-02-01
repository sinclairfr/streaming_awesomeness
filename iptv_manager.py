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
        """Méthode de démarrage du stream avec plus de logs"""
        with self.lock:
            try:
                if not self.videos:
                    logger.error(f"🚫 Aucune vidéo disponible pour {self.name}")
                    return False

                logger.info(f"🎬 Vérification des vidéos pour {self.name}: {self.videos}")

                if self.ffmpeg_process and self.ffmpeg_process.poll() is None:
                    logger.debug(f"⚡ Flux déjà en cours pour {self.name}")
                    return True

                self.active_streams += 1
                if self.active_streams == 1:
                    logger.info(f"🚀 Démarrage du stream pour {self.name}")
                    self.stop_event.clear()
                    return self._start_ffmpeg()  # Retourner le résultat de _start_ffmpeg

                return True
            except Exception as e:
                logger.error(f"🔥 Erreur dans start_stream() pour {self.name}: {e}")
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
            
            logger.info(f"📄 Fichiers trouvés dans {self.video_dir} : {list(Path(self.video_dir).glob('*.*'))}")

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
        channel_dir = f"hls/{self.name}"  # Utilisation d'un chemin relatif
        try:
            os.makedirs(channel_dir, exist_ok=True)
            logger.info(f"📁 Répertoire créé pour {self.name}: {channel_dir}")
        except PermissionError:
            logger.error(f"🚨 Permission refusée : impossible de créer {channel_dir}")
        except Exception as e:
            logger.error(f"⚠️ Erreur inattendue lors de la création de {channel_dir}: {e}")

    def _create_initial_playlist(self):
        """Crée la playlist initiale pour la chaîne"""
        try:
            playlist_path = f"hls/{self.name}/playlist.m3u8"  # Utilisation d'un chemin relatif
            with open(playlist_path, 'w') as f:
                f.write("#EXTM3U\n")
                f.write("#EXT-X-VERSION:3\n")
                f.write("#EXT-X-TARGETDURATION:6\n")
                f.write("#EXT-X-START:TIME-OFFSET=0\n")
                f.write("#EXT-X-MEDIA-SEQUENCE:0\n")
                f.write("#EXT-X-PLAYLIST-TYPE:EVENT\n")
            logger.debug(f"Playlist initiale créée pour {self.name}")
            return True
        except Exception as e:
            logger.error(f"Erreur lors de la création de la playlist pour {self.name}: {e}")
            return False    

    def _get_cached_path(self, video_path: str) -> str:
        """Obtient ou crée une version optimisée du fichier dans le cache.
        Si le fichier n'est pas en MP4, il est converti, puis supprimé après conversion réussie.
        """
        try:
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
                shutil.copy2(video_path, str(cached_path))
            else:
                logger.info(f"Conversion de {original_file.name} en MP4")
                cmd = [
                    "ffmpeg", "-y",
                    "-i", video_path,
                    "-c:v", "libx264",
                    "-preset", "medium",
                    "-crf", "23",
                    "-c:a", "aac",
                    "-b:a", "192k",
                    "-ac", "2",
                    "-ar", "48000",
                    "-movflags", "+faststart",
                    "-progress", "pipe:1",
                    str(cached_path)
                ]

                proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1
                )

                # Lecture et log de la progression en temps réel
                for line in proc.stdout:
                    line = line.strip()
                    if line:
                        logger.info(f"FFmpeg conversion: {line}")

                proc.wait()
                if proc.returncode != 0:
                    raise Exception(f"Erreur FFmpeg, code {proc.returncode}")

            if not cached_path.exists():
                raise Exception("Le fichier cache n'a pas été créé")
            if cached_path.stat().st_size == 0:
                raise Exception("Le fichier cache est vide")
            logger.info(f"Fichier mis en cache avec succès: {cached_path}")

            # Suppression du fichier source si ce n'était pas déjà un MP4 (conversion effectuée)
            if file_extension != '.mp4':
                try:
                    original_file.unlink()
                    logger.info(f"Fichier source {original_file.name} supprimé après conversion.")
                except Exception as e:
                    logger.error(f"Erreur lors de la suppression du fichier source {original_file.name}: {e}")

            return str(cached_path)

        except Exception as e:
            logger.error(f"Erreur lors de la mise en cache de {video_path}: {e}")
            return video_path  # En cas d'erreur, retourner le chemin original

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
        """Méthode de démarrage FFmpeg avec debug amélioré"""
        try:
            cached_video = self._get_cached_path(self.videos[self.current_video]["path"])
            if cached_video is None:
                logger.warning("Le fichier cache n'a pas été créé, utilisation du fichier original")
                cached_video = self.videos[self.current_video]["path"]

            logger.info(f"🟢 Initialisation FFmpeg pour {self.name}")
            
            # 1. Vérification des prérequis
            if not shutil.which('ffmpeg'):
                logger.error("❌ FFmpeg n'est pas installé ou n'est pas dans le PATH")
                return False

            # 2. Vérification des fichiers et dossiers
            current_video = self.videos[self.current_video]["path"]
            if not os.path.exists(current_video):
                logger.error(f"❌ Fichier vidéo introuvable: {current_video}")
                return False

            hls_dir = f"hls/{self.name}"
            os.makedirs(hls_dir, exist_ok=True)
            logger.info(f"📁 Dossier HLS vérifié: {hls_dir}")

            # 3. Configuration de la commande FFmpeg
            cmd = [
                "ffmpeg", "-y",
                "-stream_loop", "-1",
                "-re",
                "-fflags", "+genpts",  # 🔹 Ajout de cette option pour fixer les timestamps
                "-i", cached_video,
                "-c:v", "copy",
                "-c:a", "copy",
                "-f", "hls",
                "-hls_time", "6",
                "-hls_list_size", "10",
                "-hls_flags", "delete_segments+append_list",
                "-hls_segment_filename", f"hls/{self.name}/segment_%03d.ts",
                f"hls/{self.name}/playlist.m3u8",
                "-threads", "1"
            ]

            logger.info(f"🖥️ Commande FFmpeg: {' '.join(cmd)}")

            # 4. Lancement de FFmpeg avec capture des erreurs
            try:
                self.ffmpeg_process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True,
                    bufsize=1
                )
            except subprocess.SubprocessError as e:
                logger.error(f"❌ Erreur lors du lancement de FFmpeg: {e}")
                return False

            # 5. Vérification immédiate du processus
            if self.ffmpeg_process.poll() is not None:
                stderr = self.ffmpeg_process.stderr.read()
                logger.error(f"❌ FFmpeg s'est arrêté immédiatement. Erreur: {stderr}")
                return False

            logger.info(f"✅ FFmpeg démarré avec PID {self.ffmpeg_process.pid}")

            # 6. Monitoring dans un thread séparé
            def monitor():
                try:
                    while not self.stop_event.is_set():
                        if self.ffmpeg_process.poll() is not None:
                            stderr = self.ffmpeg_process.stderr.read()
                            logger.error(f"❌ FFmpeg s'est arrêté. Code: {self.ffmpeg_process.poll()}, Erreur: {stderr}")
                            break

                        # Vérification de la playlist toutes les 5 secondes
                        playlist_path = f"{hls_dir}/playlist.m3u8"
                        if os.path.exists(playlist_path):
                            with open(playlist_path, 'r') as f:
                                content = f.read()
                                if '.ts' in content:
                                    logger.info(f"✅ Segments HLS détectés dans la playlist")
                        else:
                            logger.warning(f"⚠️ Playlist non trouvée: {playlist_path}")

                        time.sleep(5)

                except Exception as e:
                    logger.error(f"🔥 Erreur dans le monitoring: {e}")
                finally:
                    logger.info("Monitoring FFmpeg terminé")

            self.executor.submit(monitor)
            return True

        except Exception as e:
            logger.error(f"🚨 Erreur grave dans _start_ffmpeg: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

class IPTVManager:
    def __init__(self, content_dir: str, cache_dir: str = "./cache"):
        self.content_dir = content_dir
        self.cache_dir = cache_dir
        self.channels: Dict[str, IPTVChannel] = {}
        self.last_update_time = 0
        self.last_scan_time = 0
        self.scan_delay = 10

        # Création des dossiers nécessaires
        os.makedirs(cache_dir, exist_ok=True)
        os.makedirs("./hls", exist_ok=True)

        # Configuration du Watchdog
        self.observer = Observer()
        self.observer.schedule(ChannelEventHandler(self), self.content_dir, recursive=True)

        # Scanner les chaînes immédiatement et forcer la génération de playlist
        self.scan_channels(force=True)  # Ajout du paramètre force
        self.generate_master_playlist()
        
        logger.info("📡 IPTV Manager initialisé avec Watchdog")

    def _clean_hls_segments(self):
        """Nettoie les segments HLS anciens"""
        try:
            channel_dir = Path(f"hls/{self.name}")  # Utilisation d'un chemin relatif
            for segment in channel_dir.glob("segment_*.ts"):
                if segment.stat().st_mtime < time.time() - 3600:  # Supprimer les segments de plus d'une heure
                    segment.unlink()
                    logger.debug(f"Segment supprimé: {segment}")
        except Exception as e:
            logger.error(f"Erreur lors du nettoyage des segments HLS pour {self.name}: {e}")   
            
    def generate_master_playlist(self):
        """Génère la playlist principale au format M3U"""
        if time.time() - self.last_update_time < 5:  # Réduit à 5 secondes au lieu de 10
            return

        self.last_update_time = time.time()
        playlist_path = os.path.abspath("./hls/playlist.m3u")

        try:
            with open(playlist_path, "w", encoding='utf-8') as f:
                f.write("#EXTM3U\n")
                
                for name, channel in sorted(self.channels.items()):
                    logger.debug(f"Ajout de la chaîne {name} à la playlist")
                    total_duration = -1
                    f.write(f'#EXTINF:{total_duration} tvg-id="{name}" tvg-name="{name}",{name}\n')
                    f.write(f'http://{SERVER_URL}./hls/{name}/playlist.m3u8\n')

            logger.info(f"🎬 Playlist M3U mise à jour avec succès - {len(self.channels)} chaînes")
                
        except Exception as e:
            logger.error(f"Erreur lors de la génération de la playlist: {e}")
 
    def run(self):
        """Démarre le gestionnaire IPTV avec Watchdog"""
        try:
            # Forcer un scan et une génération de playlist au démarrage
            self.scan_channels()
            self.generate_master_playlist()
            
            self.observer.start()

            while True:
                time.sleep(1)

        except KeyboardInterrupt:
            self.cleanup()

        except Exception as e:
            logger.error(f"🔥 Erreur dans le gestionnaire IPTV: {e}")
            self.cleanup()
            
    def _create_empty_playlist(self):
        """Crée une playlist M3U vide initiale"""
        try:
            playlist_path = "./hls/playlist.m3u"
            with open(playlist_path, "w", encoding='utf-8') as f:
                f.write("#EXTM3U\n")
                f.write("#EXTINF-TVG-URL=\"http://localhost\"\n")
                f.write("#EXTINF-TVG-NAME=\"IPTV Local\"\n")
            logger.info("Playlist M3U initiale créée")
        except Exception as e:
            logger.error(f"Erreur lors de la création de la playlist initiale: {e}")

    def scan_channels(self, force=False):
        """Scanne et met à jour les chaînes"""
        current_time = time.time()
        if not force and current_time - self.last_scan_time < self.scan_delay:
            logger.debug("Scan ignoré (trop fréquent)")
            return

        self.last_scan_time = current_time
        changes_detected = False
        current_channels = set(self.channels.keys())
        found_channels = set()

        # Parcours tous les dossiers dans content_dir
        try:
            content_path = Path(self.content_dir)
            logger.info(f"Scanning directory: {content_path}")
            
            for item in content_path.iterdir():
                if item.is_dir():
                    channel_name = item.name
                    found_channels.add(channel_name)
                    logger.info(f"Found directory: {channel_name}")

                    # Nouvelle chaîne trouvée
                    if channel_name not in self.channels:
                        logger.info(f"Nouvelle chaîne trouvée: {channel_name}")
                        channel = IPTVChannel(channel_name, str(item), self.cache_dir)
                        channel.scan_videos()
                        if channel.videos:
                            self.channels[channel_name] = channel
                            changes_detected = True
                            channel.start_stream()

            # Log des chaînes détectées
            logger.info(f"Chaînes détectées : {list(self.channels.keys())}")

            # Force la génération de la playlist
            self.generate_master_playlist()
            
        except Exception as e:
            logger.error(f"Erreur lors du scan des chaînes: {e}")   
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
    manager = IPTVManager("./content")
    manager.run()
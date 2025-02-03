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
import datetime
import math, itertools

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
        self.event_delay = 10  # Augmenté à 10 secondes
        self.pending_events = set()
        self.event_timer = None
        super().__init__()

    def _handle_event(self, event):
        current_time = time.time()
        if current_time - self.last_event_time >= self.event_delay:
            self.pending_events.add(event.src_path)
            
            # Annuler le timer existant
            if self.event_timer:
                self.event_timer.cancel()
            
            # Créer un nouveau timer
            self.event_timer = threading.Timer(
                self.event_delay, 
                self._process_pending_events
            )
            self.event_timer.start()

    def _process_pending_events(self):
        """Traitement groupé des événements en attente"""
        if self.pending_events:
            logger.info(f"Traitement de {len(self.pending_events)} événements groupés")
            self.manager.scan_channels()
            self.pending_events.clear()
            self.last_event_time = time.time()

    def on_modified(self, event):
        if not event.is_directory:
            self._handle_event(event)

    def on_created(self, event):
        if event.is_directory:
            self._handle_event(event)


class VideoProcessor:
    def __init__(self, channel_dir: str):
        self.channel_dir = Path(channel_dir)
        self.video_extensions = ('.mp4', '.avi', '.mkv', '.mov')
        self.processed_dir = self.channel_dir / "processed"
        self.processed_dir.mkdir(exist_ok=True)
        
    def _is_already_normalized(self, video_path: Path, ref_info: dict = None) -> bool:
        """Vérifie si une vidéo est déjà normalisée selon les paramètres de référence"""
        try:
            # Si pas de ref_info fourni, utiliser les paramètres par défaut
            if not ref_info:
                ref_info = {
                    'codec_name': 'h264',
                    'pix_fmt': 'yuv420p',
                    'r_frame_rate': '25/1',
                    'profile': 'high',
                    'level': 41
                }

            # Vérifier le fichier de sortie existant
            output_path = self.processed_dir / f"{video_path.stem}.mp4"
            if output_path.exists():
                output_info = self._get_video_info(output_path)
                if (output_info.get('codec_name') == ref_info['codec_name'] and
                    output_info.get('pix_fmt') == ref_info['pix_fmt'] and
                    output_info.get('r_frame_rate') == ref_info['r_frame_rate']):
                    logger.info(f"Le fichier {video_path.name} est déjà normalisé")
                    return True

            # Vérifier le fichier source
            current_info = self._get_video_info(video_path)
            if (current_info.get('codec_name') == ref_info['codec_name'] and
                current_info.get('pix_fmt') == ref_info['pix_fmt'] and
                current_info.get('r_frame_rate') == ref_info['r_frame_rate']):
                logger.info(f"Le fichier source {video_path.name} est déjà au bon format")
                return True

            return False

        except Exception as e:
            logger.error(f"Erreur lors de la vérification de normalisation pour {video_path}: {e}")
            return False

    def _normalize_video(self, video_path: Path, ref_info: dict = None) -> Optional[Path]:
        """Normaliser une vidéo pour correspondre aux paramètres de référence"""
        try:
            if not ref_info:
                ref_info = {
                    'codec_name': 'h264',
                    'pix_fmt': 'yuv420p',
                    'r_frame_rate': '25/1',
                    'profile': 'high',
                    'level': 41
                }

            temp_output = self.processed_dir / f"temp_{video_path.stem}.mp4"
            output_path = self.processed_dir / f"{video_path.stem}.mp4"

            # Vérifier si le fichier est déjà normalisé
            if self._is_already_normalized(video_path, ref_info):
                if not output_path.exists():
                    try:
                        shutil.copy2(video_path, output_path)
                        logger.info(f"Copied {video_path.name} to processed directory")
                    except Exception as e:
                        logger.error(f"Error copying {video_path.name}: {e}")
                        return None
                return output_path

            cmd = [
                "ffmpeg", "-y",
                "-i", str(video_path),
                "-c:v", "libx264",
                "-preset", "ultrafast",
                "-profile:v", "baseline",
                "-level:v", "3.0",
                "-pix_fmt", "yuv420p",
                "-r", "25",
                "-g", "50",
                "-keyint_min", "25",
                "-sc_threshold", "0",
                "-b:v", "2000k",
                "-maxrate", "2500k",
                "-bufsize", "3000k",
                "-c:a", "aac",
                "-b:a", "192k",
                "-ar", "48000",
                "-ac", "2",
                "-max_muxing_queue_size", "1024",
                "-movflags", "+faststart",
                str(temp_output)
            ]

            logger.info(f"Running FFmpeg command for {video_path.name}")
            logger.debug(f"FFmpeg command: {' '.join(cmd)}")
            
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )

            logger.info(f"Normalizing {video_path.name}")
            try:
                stdout, stderr = process.communicate(timeout=1800)
                
                if process.returncode == 0 and temp_output.exists():
                    if self._get_video_info(temp_output):
                        temp_output.rename(output_path)
                        logger.info(f"Successfully normalized {video_path.name}")
                        return output_path
                    else:
                        logger.error(f"Output file verification failed for {video_path.name}")
                else:
                    logger.error(f"Failed to normalize {video_path.name}")
                    if stderr:
                        logger.error(f"FFmpeg error: {stderr}")
                
                if temp_output.exists():
                    temp_output.unlink()
                return None
                
            except subprocess.TimeoutExpired:
                process.kill()
                logger.error(f"Timeout during normalization of {video_path.name}")
                if temp_output.exists():
                    temp_output.unlink()
                return None
                
            except Exception as e:
                logger.error(f"Error during FFmpeg execution for {video_path.name}: {e}")
                if process.poll() is None:
                    process.kill()
                if temp_output.exists():
                    temp_output.unlink()
                return None

        except Exception as e:
            logger.error(f"Error normalizing {video_path}: {e}")
            if 'temp_output' in locals() and temp_output.exists():
                temp_output.unlink()
            return None

    def _get_video_info(self, video_path: Path) -> dict:
        """Obtenir les informations détaillées d'une vidéo"""
        try:
            cmd = [
                "ffprobe",
                "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=width,height,r_frame_rate,pix_fmt,codec_name,profile,level",
                "-of", "json",
                str(video_path)
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0:
                data = json.loads(result.stdout)
                return data.get('streams', [{}])[0]
            return {}
        except Exception as e:
            logger.error(f"Error getting video info for {video_path}: {e}")
            return {}

    def process_videos(self) -> list[Path]:
        """Traiter toutes les vidéos et retourner la liste des fichiers normalisés"""
        try:
            source_files = sorted([
                f for f in self.channel_dir.glob('*.*')
                if f.suffix.lower() in self.video_extensions
            ])

            if not source_files:
                logger.error("No video files found")
                return []

            processed_files = []
            
            # Utiliser le premier fichier comme référence
            ref_info = self._get_video_info(source_files[0])
            
            # Traiter chaque vidéo
            for source_file in source_files:
                processed_path = self._normalize_video(source_file, ref_info)
                if processed_path:
                    processed_files.append(processed_path)
                else:
                    logger.error(f"Failed to process {source_file}")
                    return []  # En cas d'échec, on arrête tout

            return processed_files

        except Exception as e:
            logger.error(f"Error in video processing: {e}")
            return []
    
    def create_concat_file(self, video_files: list[Path]) -> Optional[Path]:
        """Créer le fichier de concaténation pour FFmpeg"""
        try:
            concat_file = self.processed_dir / "concat.txt"
            with open(concat_file, 'w', encoding='utf-8') as f:
                for video in video_files:
                    f.write(f"file '{video.absolute()}'\n")
            return concat_file
        except Exception as e:
            logger.error(f"Error creating concat file: {e}")
            return None

class IPTVChannel:
    def __init__(self, name: str, video_dir: str):  # Supprimer cache_dir
        self.name = name
        self.video_dir = video_dir
        self.ffmpeg_process = None
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self._create_channel_directory()
        self.processed_videos = []
        # Nouveaux attributs pour la surveillance
        self.monitoring_thread = None
        self.last_segment_time = 0
        self.error_count = 0
        self.max_errors = 3
        self.restart_delay = 5  # délai entre les redémarrages en secondes

    def start_stream(self):
        """Méthode principale de démarrage du stream avec surveillance intégrée"""
        with self.lock:
            try:
                if not self.processed_videos:
                    logger.error(f"❌ Aucune vidéo traitée disponible pour {self.name}")
                    return False

                # Vérification des ressources système
                if not self._check_system_resources():
                    logger.error(f"❌ Ressources système insuffisantes pour {self.name}")
                    return False

                hls_dir = f"hls/{self.name}"
                self._clean_hls_directory()

                # Configuration FFmpeg
                cmd = self._build_ffmpeg_command(hls_dir)
                
                # Démarrage du processus FFmpeg
                self.ffmpeg_process = self._start_ffmpeg_process(cmd)
                if not self.ffmpeg_process:
                    return False

                # Démarrage de la surveillance
                self._start_monitoring(hls_dir)
                return True

            except Exception as e:
                logger.error(f"Erreur lors du démarrage du stream: {e}")
                self._clean_processes()
                return False

    def _build_ffmpeg_command(self, hls_dir: str) -> list:
        """Construction de la commande FFmpeg optimisée"""
        concat_file = self._create_concat_file()
        if not concat_file:
            raise Exception("Impossible de créer le fichier de concaténation")

        return [
            "ffmpeg",
            "-hide_banner",
            "-loglevel", "info",
            "-y",
            "-re",
            "-f", "concat",
            "-safe", "0",
            "-stream_loop", "-1",
            "-i", str(concat_file),
            "-c:v", "copy",
            "-c:a", "copy",
            "-f", "hls",
            "-hls_time", "2",
            "-hls_list_size", "10",
            "-hls_flags", "delete_segments+append_list+independent_segments",
            "-hls_segment_type", "mpegts",
            "-hls_segment_filename", f"{hls_dir}/segment_%d.ts",
            f"{hls_dir}/playlist.m3u8"
        ]

    def _monitor_ffmpeg(self, hls_dir: str):
        """Surveillance améliorée du processus FFmpeg"""
        self.last_segment_time = time.time()
        segment_timeout = 10  # secondes
        
        while not self.stop_event.is_set() and self.ffmpeg_process and self.ffmpeg_process.poll() is None:
            try:
                # Vérification des segments
                current_time = time.time()
                segments = list(Path(hls_dir).glob("segment_*.ts"))
                
                if segments:
                    last_segment = max(segments, key=lambda f: f.stat().st_mtime)
                    self.last_segment_time = last_segment.stat().st_mtime
                
                if current_time - self.last_segment_time > segment_timeout:
                    self.error_count += 1
                    logger.error(f"⚠️ Pas de nouveau segment depuis {segment_timeout}s pour {self.name}")
                    
                    if self.error_count >= self.max_errors:
                        logger.error(f"🔄 Redémarrage du stream pour {self.name} après {self.error_count} erreurs")
                        self._restart_stream()
                        break
                
                # Surveillance des ressources
                if not self._check_system_resources():
                    logger.warning(f"⚠️ Ressources système faibles pour {self.name}")
                
                time.sleep(1)

            except Exception as e:
                logger.error(f"Erreur dans la surveillance de {self.name}: {e}")
                self._restart_stream()
                break

    def _check_system_resources(self) -> bool:
        """Vérification des ressources système avec des seuils plus élevés"""
        try:
            cpu_percent = psutil.cpu_percent()
            memory_percent = psutil.virtual_memory().percent
            
            # Only warn if both CPU and memory are very high
            if cpu_percent > 95 and memory_percent > 90:
                logger.warning(f"Ressources critiques - CPU: {cpu_percent}%, RAM: {memory_percent}%")
                return False
            
            return True
        except Exception as e:
            logger.error(f"Erreur lors de la vérification des ressources: {e}")
            return True  # En cas d'erreur, on continue quand même
    def _restart_stream(self):
        """Redémarrage contrôlé du stream"""
        try:
            logger.info(f"🔄 Redémarrage du stream {self.name}")
            self._clean_processes()
            time.sleep(self.restart_delay)
            self.error_count = 0
            self.start_stream()
        except Exception as e:
            logger.error(f"Erreur lors du redémarrage de {self.name}: {e}")

    def _clean_processes(self):
        """Nettoyage amélioré des processus"""
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
                    logger.error(f"Erreur lors du nettoyage des processus: {e}")       

    def _create_channel_directory(self):
        """Crée les répertoires nécessaires pour la chaîne"""
        hls_dir = f"hls/{self.name}"
        os.makedirs(hls_dir, exist_ok=True)

    def scan_videos(self) -> bool:
        """Process and prepare videos for streaming"""
        try:
            processor = VideoProcessor(self.video_dir)
            self.processed_videos = processor.process_videos()
            return len(self.processed_videos) > 0
        except Exception as e:
            logger.error(f"Error scanning videos for {self.name}: {e}")
            return False

    def _clean_hls_directory(self):
        """Nettoie le répertoire HLS"""
        try:
            hls_dir = f"hls/{self.name}"
            for pattern in ["*.ts", "*.m3u8"]:
                for file in Path(hls_dir).glob(pattern):
                    try:
                        file.unlink()
                    except OSError:
                        pass
        except Exception as e:
            logger.error(f"Error cleaning HLS directory: {e}")
       
    def _start_monitoring(self, hls_dir: str):
        """Démarre le thread de surveillance du processus FFmpeg"""
        if self.monitoring_thread is not None:
            return

        self.monitoring_thread = threading.Thread(
            target=self._monitor_ffmpeg,
            args=(hls_dir,),
            daemon=True
        )
        self.monitoring_thread.start()

    def _create_concat_file(self) -> Optional[Path]:
        """Crée le fichier de concaténation pour FFmpeg"""
        try:
            concat_file = Path(self.video_dir) / "_playlist.txt"
            if not self.processed_videos:
                logger.error("Aucune vidéo traitée disponible")
                return None
                
            with open(concat_file, 'w', encoding='utf-8') as f:
                for video in self.processed_videos:
                    f.write(f"file '{video.absolute()}'\n")
                    
            logger.info(f"Fichier de concaténation créé : {concat_file}")
            return concat_file
            
        except Exception as e:
            logger.error(f"Erreur lors de la création du fichier de concaténation : {e}")
            return None

    def _start_ffmpeg_process(self, cmd: list) -> Optional[subprocess.Popen]:
        """Démarre le processus FFmpeg avec la commande donnée"""
        try:
            logger.info(f"🖥️ Commande FFmpeg: {' '.join(cmd)}")
            
            # Démarrer FFmpeg
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )
            
            # Vérification du démarrage
            time.sleep(2)
            if process.poll() is not None:
                stderr = process.stderr.read()
                logger.error(f"❌ FFmpeg s'est arrêté immédiatement. Erreur: {stderr}")
                return None
            
            logger.info(f"✅ FFmpeg démarré avec PID {process.pid}")
            return process

        except Exception as e:
            logger.error(f"Erreur lors du démarrage de FFmpeg: {e}")
            return None

class IPTVManager:
    @staticmethod
    def _clean_directory(directory: Path):
        """Nettoie le contenu d'un répertoire sans supprimer le répertoire lui-même"""
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
                continue
    
    def __init__(self, content_dir: str):
        self.content_dir = content_dir
        self.channels: Dict[str, IPTVChannel] = {}
        self.last_update_time = 0
        self.last_scan_time = 0
        self.scan_delay = 30
        self.scan_lock = threading.Lock()

        # Setup directories with logging
        logger.info("Initializing IPTV Manager")
        
        # Clean HLS directory at startup
        hls_dir = Path("./hls")
        logger.info("Cleaning HLS directory...")
        self._clean_directory(hls_dir)  # Now using the instance method
        
        # Recreate directories
        os.makedirs("./hls", exist_ok=True)
        logger.info(f"Ensured directory exists: ./hls")
        
        # Configure watchdog
        self.observer = Observer()
        event_handler = ChannelEventHandler(self)
        self.observer.schedule(event_handler, self.content_dir, recursive=True)

        # Initial scan with forced processing
        logger.info(f"Starting initial scan of {self.content_dir}")
        self.scan_channels(initial=True, force=True)
        self.generate_master_playlist()
        
    def cleanup(self):
        """Nettoyage simplifié - juste arrêter les processus"""
        logger.info("Début du nettoyage...")
        
        # Arrêter l'observer
        if hasattr(self, 'observer'):
            logger.info("Arrêt de l'observer...")
            self.observer.stop()
            self.observer.join()

        # Nettoyer les processus des chaînes
        for name, channel in self.channels.items():
            logger.info(f"Nettoyage de la chaîne {name}...")
            channel._clean_processes()
            if hasattr(channel, 'executor'):
                channel.executor.shutdown(wait=True)

        logger.info("Nettoyage terminé")

    def _signal_handler(self, signum, frame):
        """Gestionnaire de signal pour un arrêt propre"""
        logger.info(f"Signal {signum} reçu, nettoyage en cours...")
        self.cleanup()
        sys.exit(0)

    def scan_channels(self, force=False, initial=False):
        """Enhanced channel scanning with detailed debugging"""
        with self.scan_lock:
            try:
                content_path = Path(self.content_dir)
                if not content_path.exists():
                    logger.error(f"Content directory {content_path} does not exist!")
                    return

                # List all directories and their contents
                channel_dirs = [d for d in content_path.iterdir() if d.is_dir()]
                logger.info(f"Found {len(channel_dirs)} channel directories:")
                for d in channel_dirs:
                    logger.info(f"- {d.name}")

                processed_channels = set()
                
                for channel_dir in channel_dirs:
                    try:
                        channel_name = channel_dir.name
                        logger.info(f"\nProcessing directory: {channel_name}")
                        
                        # List all files in directory
                        all_files = list(channel_dir.glob('*.*'))
                        logger.info(f"Files in {channel_name}:")
                        for f in all_files:
                            logger.info(f"  - {f.name} ({f.stat().st_size} bytes)")
                        
                        # Check for video files
                        video_files = [f for f in all_files if f.suffix.lower() == '.mp4']
                        if not video_files:
                            logger.warning(f"No MP4 files found in {channel_name}, skipping")
                            continue
                        
                        logger.info(f"Found {len(video_files)} video files in {channel_name}")
                        
                        # Process channel
                        if channel_name in self.channels:
                            channel = self.channels[channel_name]
                            if force or initial or self._needs_update(channel_dir):
                                logger.info(f"Updating existing channel: {channel_name}")
                                success = channel.scan_videos()
                                logger.info(f"Video scan {'successful' if success else 'failed'} for {channel_name}")
                                if success:
                                    if not channel.start_stream():
                                        logger.error(f"Failed to start stream for {channel_name}")
                                    else:
                                        processed_channels.add(channel_name)
                        else:
                            logger.info(f"Setting up new channel: {channel_name}")
                            channel = IPTVChannel(channel_name, str(channel_dir))  # Supprimer self.cache_dir
                            success = channel.scan_videos()
                            logger.info(f"Video scan {'successful' if success else 'failed'} for {channel_name}")
                            if success:
                                self.channels[channel_name] = channel
                                if not channel.start_stream():
                                    logger.error(f"Failed to start stream for {channel_name}")
                                else:
                                    processed_channels.add(channel_name)

                        # Verify HLS directory creation
                        hls_dir = Path(f"./hls/{channel_name}")
                        if hls_dir.exists():
                            logger.info(f"HLS directory exists for {channel_name}")
                            # List HLS directory contents
                            hls_files = list(hls_dir.glob('*.*'))
                            logger.info(f"HLS files for {channel_name}:")
                            for f in hls_files:
                                logger.info(f"  - {f.name}")
                        else:
                            logger.error(f"HLS directory missing for {channel_name}")

                    except Exception as e:
                        logger.error(f"Error processing channel {channel_dir.name}: {e}")
                        import traceback
                        logger.error(traceback.format_exc())
                        continue

                # Report results
                logger.info("\nChannel processing summary:")
                logger.info(f"Total directories found: {len(channel_dirs)}")
                logger.info(f"Successfully processed: {len(processed_channels)}")
                logger.info(f"Processed channels: {', '.join(processed_channels)}")
                logger.info(f"Active channels: {', '.join(self.channels.keys())}")

                # Update master playlist
                self.generate_master_playlist()

            except Exception as e:
                logger.error(f"Error during channel scan: {e}")
                import traceback
                logger.error(traceback.format_exc())

    def generate_master_playlist(self):
        """Enhanced master playlist generation"""
        try:
            playlist_path = os.path.abspath("./hls/playlist.m3u")
            logger.info(f"Generating master playlist at {playlist_path}")

            with open(playlist_path, "w", encoding='utf-8') as f:
                f.write("#EXTM3U\n")
                
                for name, channel in sorted(self.channels.items()):
                    hls_playlist = f"./hls/{name}/playlist.m3u8"
                    if not os.path.exists(hls_playlist):
                        logger.warning(f"HLS playlist missing for {name}, attempting restart")
                        channel.start_stream()
                    
                    logger.info(f"Adding channel {name} to master playlist")
                    f.write(f'#EXTINF:-1 tvg-id="{name}" tvg-name="{name}",{name}\n')
                    f.write(f'http://{SERVER_URL}/hls/{name}/playlist.m3u8\n')

            logger.info(f"Master playlist updated with {len(self.channels)} channels")
                
        except Exception as e:
            logger.error(f"Error generating master playlist: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    def _needs_update(self, channel_dir: Path) -> bool:
        """Check if the channel needs to be updated based on file modifications"""
        try:
            logger.debug(f"Checking updates for channel: {channel_dir.name}")
            
            # Only check video files
            video_files = list(channel_dir.glob('*.mp4'))
            if not video_files:
                logger.debug(f"No video files found in {channel_dir.name}")
                return True

            # Log video files and their timestamps
            for video_file in video_files:
                mod_time = video_file.stat().st_mtime
                logger.debug(f"Video file: {video_file.name}, Modified: {datetime.datetime.fromtimestamp(mod_time)}")

            # If we get here, there are video files to process
            return True

        except Exception as e:
            logger.error(f"Error checking updates for {channel_dir}: {e}")
            return True
    
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
            

if __name__ == "__main__":
    manager = IPTVManager("./content")  # Supprimer le cache_dir par défaut "./cache"
    try:
        manager.run()
    except KeyboardInterrupt:
        logger.info("Interruption utilisateur détectée")
        manager.cleanup()
    except Exception as e:
        logger.error(f"Erreur fatale: {e}")
        manager.cleanup()
        raise
        logger.error(f"Erreur fatale: {e}")
        manager.cleanup()
        raise
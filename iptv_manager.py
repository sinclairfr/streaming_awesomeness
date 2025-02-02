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

class VideoProcessor:
    def __init__(self, channel_dir: str):
        self.channel_dir = Path(channel_dir)
        self.diffusion_dir = self.channel_dir / "diffusion"
        self.diffusion_dir.mkdir(exist_ok=True)
        self.video_extensions = ('.mp4', '.avi', '.mkv', '.mov')
        self.tracking_file = self.diffusion_dir / "processed_files.json"

    def _get_file_hash(self, file_path: Path) -> str:
        """Calculer un hash unique pour un fichier basé sur son nom, taille et date de modification"""
        stats = file_path.stat()
        return hashlib.md5(f"{file_path.name}_{stats.st_size}_{stats.st_mtime}".encode()).hexdigest()

    def _load_tracking_data(self) -> dict:
        """Charger les données de tracking des fichiers traités"""
        if self.tracking_file.exists():
            try:
                with open(self.tracking_file, 'r') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                logger.error("Corrupted tracking file, resetting")
        return {'processed_files': {}, 'bigfile_composition': []}

    def _save_tracking_data(self, data: dict):
        """Sauvegarder les données de tracking"""
        try:
            with open(self.tracking_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving tracking data: {e}")

    def analyze_video(self, video_path: str) -> dict:
        """Analyser les paramètres d'une vidéo"""
        try:
            cmd = [
                "ffprobe",
                "-v", "quiet",
                "-print_format", "json",
                "-show_streams",
                "-show_format",
                str(video_path)
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            data = json.loads(result.stdout)
            
            # Trouver le stream vidéo
            video_stream = next((s for s in data['streams'] if s['codec_type'] == 'video'), None)
            if not video_stream:
                raise ValueError("No video stream found")
                
            return {
                'width': int(video_stream.get('width', 1920)),
                'height': int(video_stream.get('height', 1080)),
                'fps': eval(video_stream.get('r_frame_rate', '25/1')),
                'duration': float(data['format'].get('duration', 0)),
                'bitrate': int(data['format'].get('bit_rate', 0))
            }
        except Exception as e:
            logger.error(f"Error analyzing video {video_path}: {e}")
            return {}

    def _verify_bigfile(self, source_files: List[Path]) -> bool:
        """Vérifier si le bigfile existant correspond aux fichiers sources"""
        bigfile_path = self.diffusion_dir / "bigfile.mp4"
        if not bigfile_path.exists():
            return False

        tracking_data = self._load_tracking_data()
        current_composition = [self._get_file_hash(f) for f in source_files]
        
        # Vérifier si la composition correspond
        if current_composition != tracking_data.get('bigfile_composition', []):
            logger.info("Source files changed, bigfile needs rebuild")
            return False

        # Vérifier l'intégrité du bigfile
        try:
            cmd = ["ffprobe", "-v", "error", str(bigfile_path)]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                logger.error("Corrupted bigfile detected")
                return False
        except Exception as e:
            logger.error(f"Error verifying bigfile: {e}")
            return False

        return True

    def _normalize_video(self, video_path: Path, output_dir: Path, target_width: int, target_height: int) -> Optional[Path]:
        """Normaliser une vidéo avec des paramètres stricts"""
        try:
            output_path = output_dir / f"norm_{video_path.stem}.mp4"
            
            cmd = [
                "ffmpeg", "-y",
                "-i", str(video_path),
                "-vf", f"scale={target_width}:{target_height}:force_original_aspect_ratio=decrease,"
                       f"pad={target_width}:{target_height}:(ow-iw)/2:(oh-ih)/2",
                "-c:v", "libx264",
                "-preset", "slow",
                "-profile:v", "high",
                "-crf", "23",
                "-movflags", "+faststart",
                "-pix_fmt", "yuv420p",
                "-r", "25",  # Force 25fps
                "-g", "50",  # GOP size = 2*fps
                "-keyint_min", "50",
                "-sc_threshold", "0",  # Désactive les changements de scène
                "-bf", "2",  # Nombre de B-frames
                "-c:a", "aac",
                "-b:a", "192k",
                "-ar", "48000",
                "-ac", "2",
                str(output_path)
            ]

            logger.info(f"Normalizing {video_path.name}")
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0 and output_path.exists():
                logger.info(f"Successfully normalized {video_path.name}")
                return output_path
            else:
                logger.error(f"Normalization failed for {video_path}: {result.stderr}")
                return None

        except Exception as e:
            logger.error(f"Error normalizing {video_path}: {e}")
            return None

    def _safe_concatenate(self, video_files: List[Path], output_path: Path) -> bool:
        """Concaténation sécurisée avec vérification"""
        try:
            # Créer le fichier de concaténation
            concat_file = self.diffusion_dir / "concat.txt"
            with open(concat_file, 'w', encoding='utf-8') as f:
                for video in video_files:
                    f.write(f"file '{video.absolute()}'\n")

            cmd = [
                "ffmpeg", "-y",
                "-f", "concat",
                "-safe", "0",
                "-i", str(concat_file),
                "-c", "copy",  # Use copy since files are already normalized
                "-movflags", "+faststart",
                str(output_path)
            ]

            logger.info("Concatenating normalized videos...")
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            success = result.returncode == 0 and output_path.exists()
            if success:
                logger.info("Concatenation successful")
            else:
                logger.error(f"Concatenation failed: {result.stderr}")
            
            return success

        except Exception as e:
            logger.error(f"Error in concatenation: {e}")
            return False

    def _verify_video(self, video_path: Path) -> bool:
        """Vérifier qu'un fichier vidéo est valide"""
        try:
            cmd = [
                "ffprobe",
                "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=codec_name,width,height,r_frame_rate",
                "-of", "json",
                str(video_path)
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                data = json.loads(result.stdout)
                stream = data.get('streams', [{}])[0]
                
                # Vérifier les paramètres
                if (stream.get('codec_name') == 'h264' and
                    stream.get('height') == 1080 and
                    stream.get('r_frame_rate') == '25/1'):
                    return True
                    
                logger.error(f"Invalid video parameters in {video_path}")
            return False
                
        except Exception as e:
            logger.error(f"Error verifying video {video_path}: {e}")
            return False

    def process_videos(self) -> Optional[str]:
        """Process all videos with caching and verification"""
        try:
            # 1. Collecter tous les fichiers vidéo sources
            source_files = sorted([
                f for f in self.channel_dir.glob('*.*') 
                if f.suffix.lower() in self.video_extensions
            ])
            
            if not source_files:
                logger.error("No video files found")
                return None

            # 2. Vérifier si le bigfile existant est valide
            bigfile_path = self.diffusion_dir / "bigfile.mp4"
            if self._verify_bigfile(source_files):
                logger.info("Existing bigfile is valid, skipping processing")
                return str(bigfile_path)

            # 3. Charger les données de tracking
            tracking_data = self._load_tracking_data()
            
            # 4. Traiter les fichiers
            temp_dir = self.diffusion_dir / "temp"
            temp_dir.mkdir(exist_ok=True)
            
            processed_files = []
            target_height = 1080
            
            # Déterminer le target_width à partir du premier fichier
            first_video_info = self.analyze_video(str(source_files[0]))
            if not first_video_info:
                return None
            target_width = int((target_height * first_video_info['width']) / first_video_info['height'])
            target_width = (target_width // 2) * 2

            for source_file in source_files:
                file_hash = self._get_file_hash(source_file)
                cached_path = tracking_data['processed_files'].get(file_hash)
                
                if cached_path and Path(cached_path).exists():
                    # Vérifier que le fichier en cache est valide
                    if self._verify_video(Path(cached_path)):
                        logger.info(f"Using cached version of {source_file.name}")
                        processed_files.append(Path(cached_path))
                        continue
                
                # Normaliser le fichier
                normalized_path = self._normalize_video(
                    source_file,
                    temp_dir,
                    target_width,
                    target_height
                )
                
                if normalized_path:
                    # Déplacer vers le dossier final
                    final_path = self.diffusion_dir / f"norm_{source_file.stem}.mp4"
                    if final_path.exists():
                        final_path.unlink()
                    shutil.move(normalized_path, final_path)
                    
                    # Mettre à jour le tracking
                    tracking_data['processed_files'][file_hash] = str(final_path)
                    processed_files.append(final_path)
                else:
                    logger.error(f"Failed to normalize {source_file}")
                    return None

            # 5. Concaténer en bigfile
            if bigfile_path.exists():
                bigfile_path.unlink()

            if self._safe_concatenate(processed_files, bigfile_path):
                # Mettre à jour la composition du bigfile
                tracking_data['bigfile_composition'] = [
                    self._get_file_hash(f) for f in source_files
                ]
                self._save_tracking_data(tracking_data)
                
                # Nettoyer les fichiers temporaires
                if temp_dir.exists():
                    shutil.rmtree(temp_dir)
                    
                return str(bigfile_path)
            
            return None

        except Exception as e:
            logger.error(f"Error in video processing: {e}")
            return None

class IPTVChannel:
    def __init__(self, name: str, video_dir: str, cache_dir: str):
        self.name = name
        self.video_dir = video_dir
        self.cache_dir = cache_dir
        self.ffmpeg_process = None
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.active_streams = 0
        self._create_channel_directory()

    def _create_channel_directory(self):
        """Crée les répertoires nécessaires pour la chaîne"""
        hls_dir = f"hls/{self.name}"
        os.makedirs(hls_dir, exist_ok=True)

    def _start_ffmpeg(self):
        """Start FFmpeg with optimized HLS streaming settings"""
        try:
            if not hasattr(self, 'bigfile_path'):
                logger.error(f"No bigfile available for {self.name}")
                return False

            hls_dir = f"hls/{self.name}"
            
            # Nettoyer les anciens segments si nécessaire
            self._clean_hls_directory()

            cmd = [
                "ffmpeg",
                "-hide_banner",
                "-loglevel", "warning",  # Réduire les logs
                "-re",
                "-stream_loop", "-1",
                "-i", self.bigfile_path,
                
                # Copier directement les streams sans réencodage
                "-c", "copy",
                
                # Configuration HLS optimisée
                "-f", "hls",
                "-hls_time", "2",
                "-hls_list_size", "10",
                "-hls_segment_type", "mpegts",
                "-hls_flags", "delete_segments+append_list+omit_endlist+split_by_time",
                "-hls_segment_filename", f"{hls_dir}/segment_%d.ts",
                f"{hls_dir}/playlist.m3u8"
            ]

            logger.info(f"Starting FFmpeg for {self.name}")
            
            self.ffmpeg_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )

            # Monitoring thread
            def monitor_output():
                while not self.stop_event.is_set() and self.ffmpeg_process and self.ffmpeg_process.poll() is None:
                    stderr_line = self.ffmpeg_process.stderr.readline()
                    if stderr_line:
                        line = stderr_line.strip()
                        if "error" in line.lower():
                            logger.error(f"FFmpeg [{self.name}]: {line}")
                        else:
                            logger.debug(f"FFmpeg [{self.name}]: {line}")

            # Start monitoring in a separate thread
            monitor_thread = threading.Thread(target=monitor_output, daemon=True)
            monitor_thread.start()

            # Initial check with timeout
            end_time = time.time() + 5  # 5 seconds timeout
            while time.time() < end_time:
                if self.ffmpeg_process.poll() is not None:
                    stderr = self.ffmpeg_process.stderr.read()
                    logger.error(f"FFmpeg failed to start: {stderr}")
                    return False
                if os.path.exists(f"{hls_dir}/playlist.m3u8"):
                    logger.info(f"HLS stream started successfully for {self.name}")
                    return True
                time.sleep(0.1)

            logger.error(f"Timeout waiting for playlist creation for {self.name}")
            return False

        except Exception as e:
            logger.error(f"Error starting FFmpeg: {e}")
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
            logger.error(f"Error cleaning HLS directory for {self.name}: {e}")

    def scan_videos(self) -> bool:
        """Process and prepare videos for streaming"""
        try:
            processor = VideoProcessor(self.video_dir)
            bigfile_path = processor.process_videos()
            
            if bigfile_path:
                logger.info(f"Successfully processed videos for {self.name}")
                self.bigfile_path = bigfile_path
                return True
            return False

        except Exception as e:
            logger.error(f"Error scanning videos for {self.name}: {e}")
            return False

    def _clean_processes(self):
        """Clean up FFmpeg processes properly"""
        with self.lock:  # Protection contre les nettoyages simultanés
            if not self.ffmpeg_process:
                return

            try:
                logger.info(f"Stopping FFmpeg for {self.name}")
                self.stop_event.set()
                
                # Arrêt propre
                self.ffmpeg_process.terminate()
                
                try:
                    # Attendre l'arrêt propre
                    for _ in range(50):  # 5 secondes max
                        if self.ffmpeg_process.poll() is not None:
                            break
                        time.sleep(0.1)
                    else:
                        # Force kill si nécessaire
                        self.ffmpeg_process.kill()
                        self.ffmpeg_process.wait(timeout=1)
                except (subprocess.TimeoutExpired, ProcessLookupError):
                    pass

                # Nettoyage des segments HLS
                hls_dir = f"hls/{self.name}"
                if os.path.exists(hls_dir):
                    for entry in os.scandir(hls_dir):
                        if entry.name.endswith(('.ts', '.m3u8')):
                            try:
                                os.unlink(entry.path)
                            except OSError:
                                pass

                self.ffmpeg_process = None
                logger.info(f"FFmpeg stopped for {self.name}")

            except Exception as e:
                logger.error(f"Error cleaning processes for {self.name}: {e}")
                self.ffmpeg_process = None

    def start_stream(self):
        """Start streaming with enhanced error handling and logging"""
        with self.lock:
            try:
                logger.info(f"Starting stream for channel: {self.name}")
                
                # Ensure HLS directory exists
                hls_dir = f"hls/{self.name}"
                os.makedirs(hls_dir, exist_ok=True)
                logger.info(f"Ensured HLS directory: {hls_dir}")

                # Clean old files
                self._clean_hls_directory()
                
                # Verify bigfile exists
                if not hasattr(self, 'bigfile_path') or not os.path.exists(self.bigfile_path):
                    logger.error(f"No valid bigfile for {self.name}")
                    return False
                
                logger.info(f"Using bigfile: {self.bigfile_path}")

                # Start FFmpeg process
                cmd = [
                    "ffmpeg",
                    "-hide_banner",
                    "-loglevel", "info",
                    "-re",
                    "-stream_loop", "-1",
                    "-i", self.bigfile_path,
                    
                    # Video settings
                    "-c:v", "copy",
                    
                    # Audio settings
                    "-c:a", "copy",
                    
                    # HLS settings
                    "-f", "hls",
                    "-hls_time", "2",
                    "-hls_list_size", "10",
                    "-hls_flags", "delete_segments+append_list",
                    "-hls_segment_filename", f"{hls_dir}/segment_%d.ts",
                    f"{hls_dir}/playlist.m3u8"
                ]

                logger.info(f"Starting FFmpeg for {self.name}")
                logger.debug(f"FFmpeg command: {' '.join(cmd)}")

                self.ffmpeg_process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True
                )

                # Wait for initial playlist creation
                max_wait = 10
                playlist_file = Path(f"{hls_dir}/playlist.m3u8")
                start_time = time.time()
                
                while time.time() - start_time < max_wait:
                    if self.ffmpeg_process.poll() is not None:
                        error = self.ffmpeg_process.stderr.read()
                        logger.error(f"FFmpeg failed to start for {self.name}: {error}")
                        return False
                    
                    if playlist_file.exists() and playlist_file.stat().st_size > 0:
                        logger.info(f"HLS stream started successfully for {self.name}")
                        return True
                    
                    time.sleep(0.5)

                logger.error(f"Timeout waiting for HLS playlist creation for {self.name}")
                self._clean_processes()
                return False

            except Exception as e:
                logger.error(f"Error starting stream for {self.name}: {e}")
                import traceback
                logger.error(traceback.format_exc())
                self._clean_processes()
                return False
    def ensure_hls_conversion(video_dir: str, hls_dir: str) -> bool:
        """Ensure all videos in the directory are converted to HLS format."""
        try:
            video_extensions = ('.mp4', '.avi', '.mkv', '.mov')
            for file in Path(video_dir).glob('*.*'):
                if file.suffix.lower() in video_extensions:
                    hls_output_dir = Path(hls_dir) / file.stem
                    hls_output_dir.mkdir(parents=True, exist_ok=True)
                    if not convert_to_hls(str(file), str(hls_output_dir)):
                        return False
            return True
        except Exception as e:
            logger.error(f"Error ensuring HLS conversion: {e}")
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
            
    def _convert_video(self, video_path: str) -> str:
        """Convertit une vidéo en MP4 dans son dossier d'origine et remplace l'original si nécessaire."""
        try:
            original_file = Path(video_path)
            output_path = original_file.with_suffix('.mp4')  # Même répertoire, extension .mp4

            # Si c'est déjà un MP4 valide, pas besoin de conversion
            if original_file.suffix.lower() == '.mp4':
                probe_cmd = [
                    "ffprobe",
                    "-v", "error",
                    "-select_streams", "v:0",
                    "-show_entries", "stream=codec_name",
                    "-of", "json",
                    str(original_file)
                ]
                try:
                    probe_result = subprocess.run(probe_cmd, capture_output=True, text=True)
                    if probe_result.returncode == 0:
                        video_info = json.loads(probe_result.stdout)
                        if video_info.get('streams', [{}])[0].get('codec_name') == 'h264':
                            logger.info(f"✅ Le fichier {original_file.name} est déjà au bon format")
                            return str(original_file.absolute())
                except Exception as e:
                    logger.warning(f"Impossible de vérifier le format de {original_file.name}: {e}")

            # Conversion nécessaire
            logger.info(f"🔄 Conversion de {original_file.name} en MP4/H264")

            # Créer un fichier temporaire pour la conversion
            temp_output = output_path.with_suffix('.temp.mp4')

            cmd = [
                "ffmpeg", "-y",
                "-i", str(original_file),
                "-c:v", "libx264",
                "-preset", "superfast",
                "-crf", "23",
                "-c:a", "aac",
                "-b:a", "192k",
                "-ac", "2",
                "-ar", "48000",
                "-movflags", "+faststart",
                str(temp_output)
            ]

            try:
                logger.info(f"Commande de conversion: {' '.join(cmd)}")
                process = subprocess.run(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    check=True
                )
                
                # Vérifier que le fichier temporaire est valide
                if temp_output.exists() and temp_output.stat().st_size > 0:
                    # Supprimer l'ancien fichier s'il n'est pas un MP4
                    if original_file.suffix.lower() != '.mp4':
                        original_file.unlink()
                        logger.info(f"🗑️ Ancien fichier supprimé : {original_file.name}")
                    
                    # Renommer le fichier temporaire en fichier final
                    temp_output.rename(output_path)
                    
                    logger.info(f"✅ Conversion réussie: {output_path.name}")
                    return str(output_path.absolute())
                else:
                    logger.error(f"❌ Fichier converti invalide pour {original_file.name}")
                    if temp_output.exists():
                        temp_output.unlink()
                    return None

            except subprocess.CalledProcessError as e:
                logger.error(f"❌ Erreur lors de la conversion de {original_file.name}: {e.stderr}")
                if temp_output.exists():
                    temp_output.unlink()
                return None

            except Exception as e:
                logger.error(f"❌ Erreur inattendue lors de la conversion: {e}")
                if temp_output.exists():
                    temp_output.unlink()
                return None

        except Exception as e:
            logger.error(f"❌ Erreur lors de la conversion: {e}")
            return None
    
    def _convert_all_videos(self):  
        """Convertit tous les fichiers vidéo de la chaîne sur place."""
        try:
            logger.info(f"🔄 Début de la conversion des vidéos pour {self.name}")
            converted_files = []
            
            for video in self.videos:
                converted_path = self._convert_video(video["path"])
                if converted_path:
                    # Mettre à jour le chemin dans la liste des vidéos
                    video["path"] = converted_path
                    converted_files.append(converted_path)
                else:
                    logger.error(f"❌ Échec de la conversion pour {video['path']}")

            return converted_files

        except Exception as e:
            logger.error(f"❌ Erreur lors de la conversion des vidéos: {e}")
            return [] 

        try:
            logger.info(f"🟢 Initialisation FFmpeg pour {self.name}")
            
            if not self.videos:
                logger.error(f"❌ Aucune vidéo disponible pour {self.name}")
                return False

            hls_dir = f"hls/{self.name}"
            
            # Créer le fichier de concaténation
            concat_file = Path(self.video_dir) / "_playlist.txt"
            try:
                with open(concat_file, 'w', encoding='utf-8') as f:
                    for video in self.videos:
                        f.write(f"file '{video['path']}'\n")
            except Exception as e:
                logger.error(f"Erreur lors de la création du fichier de concaténation: {e}")
                return False

            cmd = [
                "ffmpeg",
                "-hide_banner",
                "-loglevel", "info",
                "-y",
                "-re",
                "-f", "concat",
                "-safe", "0",
                "-stream_loop", "-1",
                "-i", str(concat_file.absolute()),
                
                # Paramètres vidéo
                "-c:v", "copy",  # Copie directe du codec vidéo pour réduire la charge CPU
                
                # Paramètres audio
                "-c:a", "copy",  # Copie directe du codec audio
                
                # Configuration HLS
                "-f", "hls",
                "-hls_time", "2",
                "-hls_list_size", "15",
                "-hls_flags", "delete_segments+append_list",
                "-hls_segment_filename", f"{hls_dir}/segment_%d.ts",
                f"{hls_dir}/playlist.m3u8"
            ]

            logger.info(f"🖥️ Commande FFmpeg: {' '.join(cmd)}")

            # Démarrer FFmpeg
            self.ffmpeg_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )
            
            # Vérification immédiate du processus
            time.sleep(2)
            if self.ffmpeg_process.poll() is not None:
                stderr = self.ffmpeg_process.stderr.read()
                logger.error(f"❌ FFmpeg s'est arrêté immédiatement. Erreur: {stderr}")
                return False
                
            logger.info(f"✅ FFmpeg démarré avec PID {self.ffmpeg_process.pid}")
            return True

        except Exception as e:
            logger.error(f"Erreur lors du démarrage de FFmpeg: {e}")
            return False
        try:
            logger.info(f"🟢 Initialisation FFmpeg pour {self.name}")
            
            if not self.videos:
                logger.error(f"❌ Aucune vidéo disponible pour {self.name}")
                return False

            hls_dir = f"hls/{self.name}"
            
            # Initialiser le gestionnaire de playlist
            self.playlist_manager = HLSPlaylistManager(self.videos, hls_dir)
            
            # Créer le fichier de concaténation
            concat_file = Path(self.video_dir) / "_playlist.txt"
            try:
                with open(concat_file, 'w', encoding='utf-8') as f:
                    for video in self.videos:
                        f.write(f"file '{video['path']}'\n")
            except Exception as e:
                logger.error(f"Erreur lors de la création du fichier de concaténation: {e}")
                return False

            cmd = [
                "ffmpeg",
                "-hide_banner",
                "-loglevel", "info",
                "-y",
                "-re",
                "-f", "concat",
                "-safe", "0",
                "-stream_loop", "-1",
                "-i", str(concat_file.absolute()),
                
                # Paramètres vidéo
                "-c:v", "libx264",
                "-preset", "veryfast",
                "-tune", "zerolatency",
                "-profile:v", "main",
                "-b:v", "2000k",
                "-bufsize", "4000k",
                "-r", "25",
                
                # Paramètres audio
                "-c:a", "aac",
                "-b:a", "128k",
                "-ar", "44100",
                
                # Configuration HLS
                "-f", "hls",
                "-hls_time", "2",
                "-hls_list_size", "0",  # Pas de limite, géré par notre playlist manager
                "-hls_flags", "delete_segments",
                "-hls_segment_filename", f"{hls_dir}/segment_%d.ts",
                f"{hls_dir}/playlist.m3u8"
            ]

            logger.info(f"🖥️ Commande FFmpeg: {' '.join(cmd)}")

            # Démarrer FFmpeg
            self.ffmpeg_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )

            # Démarrer le thread de mise à jour de la playlist
            def update_playlist_thread():
                while self.ffmpeg_process and self.ffmpeg_process.poll() is None:
                    self.playlist_manager.update_playlist()
                    time.sleep(1)

            threading.Thread(target=update_playlist_thread, daemon=True).start()
            
            return True

        except Exception as e:
            logger.error(f"Erreur lors du démarrage de FFmpeg: {e}")
            return False
        try:
            logger.info(f"🟢 Initialisation FFmpeg pour {self.name}")
            
            if not self.videos:
                logger.error(f"❌ Aucune vidéo disponible pour {self.name}")
                return False

            hls_dir = f"hls/{self.name}"
            
            # Nettoyage complet avant de démarrer
            if not self._clean_hls_directory(hls_dir):
                logger.error("Échec du nettoyage du répertoire HLS")
                return False

            # Convertir les vidéos si nécessaire
            converted_files = self._convert_all_videos()
            if not converted_files:
                logger.error(f"❌ Aucun fichier converti disponible pour {self.name}")
                return False

            # Créer un fichier de concaténation temporaire
            concat_file = Path(self.video_dir) / "_playlist.txt"
            try:
                with open(concat_file, 'w', encoding='utf-8') as f:
                    for file_path in converted_files:
                        f.write(f"file '{file_path}'\n")
            except Exception as e:
                logger.error(f"Erreur lors de la création du fichier de concaténation: {e}")
                return False

            # Configuration FFmpeg optimisée pour timeshift
            # Au lieu d'utiliser concat, on utilise input_loop avec un seul fichier
            input_files = [str(Path(video["path"]).absolute()) for video in self.videos]
            cmd = [
                "ffmpeg",
                "-hide_banner",
                "-loglevel", "info",
                "-y",
                "-stream_loop", "-1",  # Boucle infinie sur l'input
                "-re",  # Lecture en temps réel
                "-i", "concat:" + "|".join(input_files),  # Concaténation directe des fichiers
                # Paramètres vidéo optimisés
                "-c:v", "libx264",
                "-preset", "ultrafast",
                "-tune", "zerolatency",
                "-profile:v", "baseline",
                "-level", "3.0",
                "-b:v", "2000k",
                "-maxrate", "2500k",
                "-bufsize", "4000k",
                "-g", "60",
                "-keyint_min", "60",
                "-sc_threshold", "0",
                # Paramètres audio
                "-c:a", "aac",
                "-b:a", "128k",
                "-ac", "2",
                "-ar", "44100",
                # Configuration HLS
                "-f", "hls",
                "-hls_time", "2",
                "-hls_list_size", "10",
                "-hls_flags", "delete_segments+append_list+program_date_time+independent_segments",
                "-hls_segment_type", "mpegts",
                "-start_number", "0",
                "-hls_allow_cache", "1",
                "-hls_segment_filename", f"{hls_dir}/segment_%d.ts",
                f"{hls_dir}/playlist.m3u8"
            ]

            logger.info(f"🖥️ Commande FFmpeg: {' '.join(cmd)}")

            try:
                # Démarrage avec pipe pour monitorer la sortie
                self.ffmpeg_process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True
                )
                
                # Vérification de démarrage
                time.sleep(2)
                if self.ffmpeg_process.poll() is not None:
                    stderr = self.ffmpeg_process.stderr.read()
                    logger.error(f"❌ FFmpeg s'est arrêté immédiatement. Erreur: {stderr}")
                    return False
                
                # Démarrer un thread pour monitorer la sortie FFmpeg
                def monitor_output():
                    error_count = 0
                    startup_grace_period = 10  # Grace period de 10 secondes au démarrage
                    last_segment_check = time.time()
                    segment_check_interval = 30  # Vérifie les segments toutes les 30 secondes
                    
                    # Attendre le démarrage initial
                    time.sleep(startup_grace_period)
                    
                    while self.ffmpeg_process and self.ffmpeg_process.poll() is None:
                        stderr_line = self.ffmpeg_process.stderr.readline()
                        if stderr_line:
                            line = stderr_line.strip()
                            if "error" in line.lower():
                                error_count += 1
                                logger.error(f"FFmpeg [{self.name}]: {line}")
                            else:
                                logger.debug(f"FFmpeg [{self.name}]: {line}")
                        
                        current_time = time.time()
                        if current_time - last_segment_check >= segment_check_interval:
                            last_segment_check = current_time
                            segments = list(Path(hls_dir).glob("segment_*.ts"))
                            if not segments and error_count > 0:  # Ne redémarre que s'il y a eu des erreurs
                                logger.error(f"Aucun segment trouvé pour {self.name} après {segment_check_interval}s, redémarrage")
                                self._clean_processes()
                                self.start_stream()
                                break

                    # Si le processus s'est arrêté sans erreur explicite
                    if self.ffmpeg_process and self.ffmpeg_process.poll() is not None:
                        logger.error(f"FFmpeg s'est arrêté pour {self.name}, redémarrage")
                        self._clean_processes()
                        self.start_stream()
                
                threading.Thread(target=monitor_output, daemon=True).start()
                
                logger.info(f"✅ FFmpeg démarré avec PID {self.ffmpeg_process.pid}")
                return True

            except Exception as e:
                logger.error(f"Erreur lors du démarrage de FFmpeg: {e}")
                return False

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
        self.scan_delay = 30
        self.scan_lock = threading.Lock()

        # Setup directories with logging
        logger.info("Initializing IPTV Manager")
        for dir_path in [cache_dir, "./hls"]:
            os.makedirs(dir_path, exist_ok=True)
            logger.info(f"Ensured directory exists: {dir_path}")

        # Configure watchdog
        self.observer = Observer()
        event_handler = ChannelEventHandler(self)
        self.observer.schedule(event_handler, self.content_dir, recursive=True)

        # Initial scan with forced processing
        logger.info(f"Starting initial scan of {self.content_dir}")
        self.scan_channels(initial=True, force=True)
        self.generate_master_playlist()

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
                            channel = IPTVChannel(channel_name, str(channel_dir), self.cache_dir)
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
        """Enhanced update check with debug logging"""
        try:
            logger.debug(f"Checking updates for channel: {channel_dir.name}")
            
            diffusion_dir = channel_dir / "diffusion"
            if not diffusion_dir.exists():
                logger.debug(f"Diffusion directory missing for {channel_dir.name}")
                return True

            bigfile = diffusion_dir / "bigfile.mp4"
            if not bigfile.exists():
                logger.debug(f"Bigfile missing for {channel_dir.name}")
                return True

            # Check video files with logging
            video_files = list(channel_dir.glob('*.mp4'))
            if not video_files:
                logger.debug(f"No video files found in {channel_dir.name}")
                return True

            # Log all video files and their timestamps
            for video_file in video_files:
                mod_time = video_file.stat().st_mtime
                logger.debug(f"Video file: {video_file.name}, Modified: {datetime.fromtimestamp(mod_time)}")

            last_source_mod = max(f.stat().st_mtime for f in video_files)
            bigfile_mod = bigfile.stat().st_mtime
            
            needs_update = last_source_mod > bigfile_mod
            if needs_update:
                logger.debug(f"Source files newer than bigfile for {channel_dir.name}")
            
            return needs_update

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
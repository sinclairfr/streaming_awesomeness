import json
import shutil
import subprocess
from pathlib import Path
from config import logger
from queue import Queue
import threading
import os
import logging
import re

class VideoProcessor:
    def __init__(self, channel_dir: str):
        self.channel_dir = Path(channel_dir)
        self.processed_dir = self.channel_dir / "processed"
        self.processed_dir.mkdir(exist_ok=True)
        
        # Queue et threading
        self.processing_queue = Queue()
        self.processed_files = []
        self.processing_thread = None
        self.processing_lock = threading.Lock()
        
        # Configuration GPU
        self.USE_GPU = os.getenv('FFMPEG_HARDWARE_ACCELERATION', '').lower() == 'vaapi'
        self.logger = logging.getLogger('config')
        
        # Vérification du support GPU au démarrage
        if self.USE_GPU:
            self.check_gpu_support()

    def check_gpu_support(self):
        """Vérifie si le support GPU est disponible"""
        try:
            result = subprocess.run(['vainfo'], capture_output=True, text=True)
            if result.returncode == 0 and 'VAEntrypointVLD' in result.stdout:
                self.logger.info("✅ Support VAAPI détecté et activé")
                return True
            else:
                self.logger.warning("⚠️ VAAPI configuré mais non fonctionnel, retour au mode CPU")
                self.USE_GPU = False
                return False
        except Exception as e:
            self.logger.error(f"❌ Erreur vérification VAAPI: {str(e)}")
            self.USE_GPU = False
            return False
        
    def get_gpu_filters(self, video_path: Path = None, is_streaming: bool = False) -> list:
        """Génère les filtres vidéo pour le GPU"""
        filters = []
        
        if not is_streaming:
            if video_path and self.is_large_resolution(video_path):
                filters.append("scale_vaapi=w=1920:h=1080")
        
        filters.extend(["format=nv12|vaapi", "hwupload"])
        return filters

    def get_gpu_args(self, is_streaming: bool = False) -> list:
        """Génère les arguments de base pour le GPU"""
        args = [
            "-vaapi_device", "/dev/dri/renderD128",
            "-hwaccel", "vaapi",
            "-hwaccel_output_format", "vaapi"
        ]
        
        if is_streaming:
            args.extend(["-init_hw_device", "vaapi=va:/dev/dri/renderD128"])
            
        return args

    def get_encoding_args(self, is_streaming: bool = False) -> list:
            """Génère les arguments d'encodage selon le mode GPU/CPU"""
            if self.USE_GPU:
                args = [
                    "-c:v", "h264_vaapi",
                    "-profile:v", "main",
                    "-level", "4.1",
                    "-bf", "0",
                    "-bufsize", "5M",
                    "-maxrate", "5M",
                    "-low_power", "1",
                    "-c:a", "aac",
                    "-b:a", "192k",
                    "-ar", "48000"
                ]
                
                if is_streaming:
                    args.extend([
                        "-g", "60",  # GOP size for streaming
                        "-maxrate", "5M",
                        "-bufsize", "10M",
                        "-flags", "+cgop",  # Closed GOP for streaming
                        "-sc_threshold", "0"  # Disable scene change detection for smoother streaming
                    ])
            else:
                args = [
                    "-c:v", "libx264",
                    "-profile:v", "main",
                    "-preset", "fast" if not is_streaming else "ultrafast",
                    "-crf", "23"
                ]
                
                if is_streaming:
                    args.extend([
                        "-tune", "zerolatency",
                        "-maxrate", "5M",
                        "-bufsize", "10M",
                        "-g", "60"
                    ])
                    
            return args
    
    def sanitize_filename(self, filename: str) -> str:
        """Sanitize filename to remove problematic characters"""
        # Remove or replace problematic characters
        sanitized = filename.replace("'", "").replace('"', "")
        # Replace spaces with underscores
        sanitized = sanitized.replace(" ", "_")
        # Remove any other special characters except letters, numbers, dots, and underscores
        sanitized = re.sub(r'[^a-zA-Z0-9._-]', '', sanitized)
        return sanitized
    
    def process_video(self, video_path: Path) -> Path:
            """Process a video file with sanitized filename"""
            try:
                # Sanitize the source filename
                sanitized_name = self.sanitize_filename(video_path.name)
                sanitized_path = video_path.parent / sanitized_name
                
                # Rename the source file if needed
                if video_path.name != sanitized_name:
                    logger.info(f"Renaming source file: {video_path.name} -> {sanitized_name}")
                    video_path.rename(sanitized_path)
                    video_path = sanitized_path
                
                # Create sanitized output path
                output_path = self.processed_dir / sanitized_name
                
                # Skip if already processed and optimized
                if output_path.exists() and self.is_already_optimized(output_path):
                    logger.info(f"✅ {video_path.name} déjà optimisé")
                    return output_path

                # Prepare FFmpeg command
                command = ["ffmpeg", "-y"]  # Force overwrite if exists
                
                # Add GPU-specific arguments if enabled
                if self.USE_GPU:
                    command.extend(self.get_gpu_args())
                
                # Input file
                command.extend(["-i", str(video_path)])
                
                # Add video filters if using GPU
                if self.USE_GPU:
                    filters = self.get_gpu_filters(video_path)
                    if filters:
                        command.extend(["-vf", ",".join(filters)])
                
                # Add encoding parameters
                command.extend(self.get_encoding_args())
                
                # Audio encoding (same for GPU and CPU)
                command.extend([
                    "-c:a", "aac",
                    "-b:a", "192k",
                    "-ar", "48000"
                ])
                
                # Output file
                command.append(str(output_path))

                # Execute FFmpeg
                logger.info(f"🎬 Traitement de {video_path.name}")
                logger.debug(f"Commande: {' '.join(command)}")
                
                result = subprocess.run(
                    command,
                    capture_output=True,
                    text=True
                )

                if result.returncode != 0:
                    logger.error(f"❌ Erreur FFmpeg: {result.stderr}")
                    return None

                logger.info(f"✅ {video_path.name} traité avec succès")
                return output_path

            except Exception as e:
                logger.error(f"Error processing video {video_path}: {e}")
                logger.error(traceback.format_exc())
                return None

    def process_videos_async(self, videos: list = None):
        """
        Lance le traitement asynchrone des vidéos.
        Si videos est None, scanne le dossier pour trouver les vidéos à traiter.
        """
        if self.processing_thread and self.processing_thread.is_alive():
            logger.warning("🏃 Traitement déjà en cours")
            return False

        if videos is None:
            videos = [
                f for f in self.channel_dir.glob("*.mp4")
                if not f.name.startswith("temp_")
                and f.parent == self.channel_dir
            ]

        if not videos:
            logger.warning("⚠️ Aucune vidéo à traiter")
            return False

        # On vide la queue et la liste des fichiers traités
        while not self.processing_queue.empty():
            self.processing_queue.get()
        self.processed_files.clear()

        # On remplit la queue
        for video in videos:
            self.processing_queue.put(video)

        # On lance le thread de traitement
        self.processing_thread = threading.Thread(
            target=self._process_queue,
            daemon=True
        )
        self.processing_thread.start()
        return True

    def _process_queue(self):
        """Traite les vidéos dans la queue"""
        while not self.processing_queue.empty():
            video = self.processing_queue.get()
            try:
                processed = self.process_video(video)
                if processed:
                    with self.processing_lock:
                        self.processed_files.append(processed)
            except Exception as e:
                logger.error(f"❌ Erreur traitement {video.name}: {e}")
            finally:
                self.processing_queue.task_done()

    def wait_for_completion(self, timeout: int = 300) -> list:
        """
        Attend la fin du traitement des vidéos.
        Retourne la liste des fichiers traités.
        
        Args:
            timeout (int): Timeout en secondes
            
        Returns:
            list: Liste des fichiers traités
        """
        if not self.processing_thread:
            return []

        start_time = time.time()
        while self.processing_thread.is_alive():
            if time.time() - start_time > timeout:
                logger.error("⏰ Timeout pendant le traitement des vidéos")
                return []
            time.sleep(1)

        with self.processing_lock:
            processed = self.processed_files.copy()
        return processed

    def is_already_optimized(self, video_path: Path) -> bool:
        """
        Vérifie si une vidéo est déjà en H.264, ≤ 1080p, ≤ 30 FPS, et en AAC.
        Ajoute un log détaillé expliquant la raison si la normalisation est nécessaire.
        """
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

            logger.info(f"🎥 Codec: {codec}, Résolution: {width}x{height}, FPS: {fps}, Audio: {audio_codec}")

            # Vérification des critères
            needs_transcoding = False
            if codec != "h264":
                logger.info(f"🚨 Codec vidéo non H.264 ({codec}), conversion nécessaire")
                needs_transcoding = True
            if width > 1920 or height > 1080:
                logger.info(f"🚨 Résolution supérieure à 1080p ({width}x{height}), réduction nécessaire")
                needs_transcoding = True
            if fps > 30:
                logger.info(f"🚨 FPS supérieur à 30 ({fps}), réduction nécessaire")
                needs_transcoding = True
            if audio_codec and audio_codec != "aac":
                logger.info(f"🚨 Codec audio non AAC ({audio_codec}), conversion nécessaire")
                needs_transcoding = True

            if needs_transcoding:
                logger.info(f"⚠️ Normalisation nécessaire pour {video_path.name}")
            else:
                logger.info(f"✅ Vidéo déjà optimisée, pas besoin de normalisation pour {video_path.name}")

            return not needs_transcoding

        except json.JSONDecodeError as e:
            logger.error(f"❌ Erreur JSON avec ffprobe: {e}")
            return False

    def is_large_resolution(self, video_path: Path) -> bool:
        """
        Vérifie si une vidéo a une résolution significativement supérieure à 1080p.
        On tolère une marge de 10% pour éviter des conversions inutiles.
        """
        try:
            cmd = [
                "ffprobe", "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=width,height",
                "-of", "json",
                str(video_path)
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            video_info = json.loads(result.stdout)
            
            if 'streams' not in video_info or not video_info['streams']:
                return False
                
            stream = video_info['streams'][0]
            width = int(stream.get('width', 0))
            height = int(stream.get('height', 0))
            
            # On tolère une marge de 10% au-dessus de 1080p
            max_height = int(1080 * 1.1)  # ~1188p
            max_width = int(1920 * 1.1)   # ~2112px
            
            return width > max_width or height > max_height
            
        except (subprocess.SubprocessError, json.JSONDecodeError, ValueError, KeyError) as e:
            logger.error(f"❌ Erreur vérification résolution {video_path.name}: {e}")
            return False
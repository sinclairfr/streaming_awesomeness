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
        """Sanitize le nom de fichier en retirant TOUS les caractères problématiques"""
        # On nettoie plus agressivement les caractères problématiques pour FFmpeg
        sanitized = filename.replace("'", "").replace('"', "").replace(",", "_")
        sanitized = sanitized.replace("-", "_").replace(" ", "_")
        # On supprime les caractères spéciaux et on garde uniquement lettres, chiffres, points et underscore
        sanitized = re.sub(r'[^a-zA-Z0-9._]', '', sanitized)
        # Limitation longueur max à 100 caractères pour éviter problèmes de buffer
        if len(sanitized) > 100:
            base, ext = os.path.splitext(sanitized)
            sanitized = base[:96] + ext  # On garde l'extension
        return sanitized
    

    def process_video(self, video_path: Path) -> Path:
        """Traite un fichier vidéo avec gestion spéciale pour HEVC 10-bit"""
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
            output_path = self.processed_dir / sanitized_name.replace(".mkv", ".mp4")
            
            # Skip if already processed and optimized
            if output_path.exists() and self.is_already_optimized(output_path):
                logger.info(f"✅ {video_path.name} déjà optimisé")
                return output_path

            # Vérifier si le fichier est HEVC 10-bit pour adapter l'approche
            is_hevc_10bit = self._check_hevc_10bit(video_path)
            
            # Prepare FFmpeg command
            command = ["ffmpeg", "-y"]  # Force overwrite if exists
            
            # Paramètres d'entrée améliorés pour fichiers problématiques
            command.extend([
                "-analyzeduration", "100M",
                "-probesize", "100M",
                "-fflags", "+igndts+discardcorrupt"
            ])
            
            # Input file
            command.extend(["-i", str(video_path)])
            
            # Désactive explicitement les sous-titres et les données métadonnées
            command.extend(["-sn", "-dn", "-map_chapters", "-1"])
            
            # Conserve uniquement la première piste audio et vidéo
            command.extend(["-map", "0:v:0", "-map", "0:a:0"])
            
            # Pour HEVC 10-bit, on utilise une approche différente sans GPU
            if is_hevc_10bit:
                logger.info(f"🔄 Détection HEVC 10-bit, utilisation d'un encodage CPU optimisé")
                command.extend([
                    "-c:v", "libx264", 
                    "-crf", "22",
                    "-preset", "fast",
                    "-pix_fmt", "yuv420p",  # Assure la compatibilité maximale
                    "-profile:v", "high",
                    "-level", "4.1",
                    "-maxrate", "5M",
                    "-bufsize", "10M",
                    "-g", "48",
                    "-keyint_min", "48",
                    "-sc_threshold", "0",  # Désactive la détection de scène
                    "-c:a", "aac",
                    "-b:a", "192k",
                    "-ar", "48000",
                    "-ac", "2"  # Force stereo
                ])
            else:
                # Si on peut utiliser le GPU
                if self.USE_GPU:
                    command.extend(self.get_gpu_args())
                    filters = self.get_gpu_filters(video_path)
                    if filters:
                        command.extend(["-vf", ",".join(filters)])
                
                # Paramètres d'encodage standard
                command.extend(self.get_encoding_args())
                
                # Audio encoding
                command.extend([
                    "-c:a", "aac",
                    "-b:a", "192k",
                    "-ar", "48000",
                    "-ac", "2"  # Force stereo
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

            # Si ça échoue encore avec le mode HEVC, on essaie une approche en deux étapes
            if result.returncode != 0 and is_hevc_10bit:
                logger.warning(f"⚠️ Première approche échouée, tentative avec méthode en deux étapes")
                return self._two_pass_hevc_conversion(video_path, output_path)

            if result.returncode != 0:
                logger.error(f"❌ Erreur FFmpeg: {result.stderr}")
                return None

            logger.info(f"✅ {video_path.name} traité avec succès")
            return output_path

        except Exception as e:
            logger.error(f"Error processing video {video_path}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def _check_hevc_10bit(self, video_path: Path) -> bool:
        """Vérifie si un fichier est encodé en HEVC 10-bit"""
        try:
            cmd = [
                "ffprobe", "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=codec_name,pix_fmt",
                "-of", "json",
                str(video_path)
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                return False
                
            video_info = json.loads(result.stdout)
            if 'streams' not in video_info or not video_info['streams']:
                return False
                
            stream = video_info['streams'][0]
            codec = stream.get('codec_name', '').lower()
            pix_fmt = stream.get('pix_fmt', '').lower()
            
            # Détecte HEVC/H.265 avec format de pixel 10-bit
            return (codec in ['hevc', 'h265'] and ('10' in pix_fmt or 'p10' in pix_fmt))
        except Exception as e:
            logger.error(f"❌ Erreur détection HEVC 10-bit: {e}")
            return False

    def _two_pass_hevc_conversion(self, input_path: Path, output_path: Path) -> Path:
        """Conversion HEVC en deux étapes avec fichier intermédiaire"""
        try:
            # Créer un fichier temporaire
            temp_file = input_path.parent / f"temp_{input_path.stem}.mp4"
            
            # Première étape: Extraction simple sans réencodage vidéo
            extract_cmd = [
                "ffmpeg", "-y",
                "-i", str(input_path),
                "-map", "0:v:0",            # Premier flux vidéo
                "-map", "0:a:0",            # Premier flux audio
                "-c:v", "copy",             # Copie sans réencodage
                "-c:a", "aac",              # Conversion audio en AAC
                "-b:a", "192k",
                "-ac", "2",                 # Stéréo
                "-sn", "-dn",               # Pas de sous-titres ni données
                "-map_chapters", "-1",      # Pas de chapitres
                str(temp_file)
            ]
            
            logger.info(f"🔄 Étape 1/2: Extraction en {temp_file}")
            extract_result = subprocess.run(extract_cmd, capture_output=True, text=True)
            
            if extract_result.returncode != 0:
                logger.error(f"❌ Échec extraction: {extract_result.stderr}")
                return None
                
            # Deuxième étape: Réencodage en H.264
            encode_cmd = [
                "ffmpeg", "-y",
                "-i", str(temp_file),
                "-c:v", "libx264",
                "-preset", "fast",
                "-crf", "23",
                "-pix_fmt", "yuv420p",
                "-profile:v", "high",
                "-level", "4.1",
                "-c:a", "copy",
                str(output_path)
            ]
            
            logger.info(f"🔄 Étape 2/2: Réencodage en {output_path}")
            encode_result = subprocess.run(encode_cmd, capture_output=True, text=True)
            
            # Nettoyage du fichier temporaire
            if temp_file.exists():
                try:
                    temp_file.unlink()
                except:
                    pass
                    
            if encode_result.returncode != 0:
                logger.error(f"❌ Échec réencodage: {encode_result.stderr}")
                return None
                
            logger.info(f"✅ Conversion en deux étapes réussie pour {input_path.name}")
            return output_path
        
        except Exception as e:
            logger.error(f"❌ Erreur conversion deux étapes: {e}")
            return None
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

    def wait_for_completion(self, timeout: int = 600) -> list:
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
        Vérifie si une vidéo est déjà optimisée pour le streaming.
        Détecte également les sous-titres et chapitres qui pourraient causer des problèmes.
        """
        logger.info(f"🔍 Vérification du format de {video_path.name}")

        cmd = [
            "ffprobe", "-v", "error",
            "-show_entries", "stream=codec_name,width,height,r_frame_rate,codec_type",
            "-of", "json",
            str(video_path)
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)

        try:
            video_info = json.loads(result.stdout)
            streams = video_info.get("streams", [])

            # On compte les types de flux
            video_streams = [s for s in streams if s['codec_type'] == 'video']
            audio_streams = [s for s in streams if s['codec_type'] == 'audio']
            subtitle_streams = [s for s in streams if s['codec_type'] == 'subtitle']
            
            # Si on a plus d'un flux vidéo ou plus de sous-titres, normalisation requise
            if len(video_streams) > 1 or subtitle_streams:
                logger.warning(f"⚠️ {video_path.name} contient {len(video_streams)} flux vidéo et {len(subtitle_streams)} sous-titres")
                logger.info(f"🚨 Multiples flux détectés, normalisation nécessaire pour garantir la compatibilité HLS")
                return False

            # On vérifie maintenant les caractéristiques vidéo/audio standard
            video_stream = video_streams[0] if video_streams else None
            audio_stream = audio_streams[0] if audio_streams else None

            if not video_stream:
                logger.warning(f"⚠️ Aucun flux vidéo détecté dans {video_path.name}, normalisation forcée.")
                return False

            codec = video_stream.get("codec_name", "").lower()
            width = int(video_stream.get("width", 0))
            height = int(video_stream.get("height", 0))
            framerate = video_stream.get("r_frame_rate", "0/1").split("/")
            fps = round(int(framerate[0]) / int(framerate[1])) if len(framerate) == 2 else 0

            audio_codec = audio_stream.get("codec_name").lower() if audio_stream else "none"

            logger.info(f"🎥 Codec: {codec}, Résolution: {width}x{height}, FPS: {fps}, Audio: {audio_codec}")

            # Vérifions aussi les chapitres qui peuvent causer des problèmes
            chapters_cmd = [
                "ffprobe", "-v", "error",
                "-show_chapters",
                "-of", "json",
                str(video_path)
            ]
            chapters_result = subprocess.run(chapters_cmd, capture_output=True, text=True)
            chapters_info = json.loads(chapters_result.stdout)
            has_chapters = 'chapters' in chapters_info and len(chapters_info['chapters']) > 0
            
            if has_chapters:
                logger.warning(f"⚠️ {video_path.name} contient {len(chapters_info['chapters'])} chapitres")
                logger.info(f"🚨 Chapitres détectés, normalisation nécessaire pour garantir la compatibilité HLS")
                return False

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
            if audio_codec == "none":
                logger.info(f"🚨 Pas de piste audio détectée, normalisation nécessaire pour garantir la présence d'audio")
                needs_transcoding = True
            elif audio_codec != "aac":
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
        except Exception as e:
            logger.error(f"❌ Erreur générale vérification optimisation: {str(e)}")
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
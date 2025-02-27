import json
import shutil
import subprocess
from pathlib import Path
import threading
import os
import logging
import re
from queue import Queue
import time
from typing import List, Dict, Optional, Set
from config import logger

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("video_processor")

class VideoProcessor:
    def __init__(self, channel_dir: str):
        self.channel_dir = Path(channel_dir)
        
        # Création des nouveaux dossiers avec des noms plus explicites
        self.ready_to_stream_dir = self.channel_dir / "ready_to_stream"
        self.ready_to_stream_dir.mkdir(exist_ok=True)
        
        self.processing_dir = self.channel_dir / "processing"
        self.processing_dir.mkdir(exist_ok=True)
        
        self.already_processed_dir = self.channel_dir / "already_processed"
        self.already_processed_dir.mkdir(exist_ok=True)
        
        # Migration des fichiers de l'ancien dossier "processed" si nécessaire
        self._migrate_from_processed()
        
        # Queue et threading pour le traitement asynchrone
        self.processing_queue = Queue()
        self.processed_files = []
        self.currently_processing: Set[Path] = set()  # Pour suivre les fichiers en cours de traitement
        self.processing_lock = threading.Lock()
        
        # Configuration GPU
        self.USE_GPU = os.getenv('FFMPEG_HARDWARE_ACCELERATION', '').lower() == 'vaapi'
        
        # Vérification du support GPU au démarrage
        if self.USE_GPU:
            self.check_gpu_support()
            
        # Démarrage du thread de traitement en arrière-plan
        self.stop_processing = threading.Event()
        self.processing_thread = threading.Thread(target=self._background_processor, daemon=True)
        self.processing_thread.start()
        
    def _migrate_from_processed(self):
        """Migration des fichiers de l'ancien dossier 'processed' vers la nouvelle structure"""
        old_processed = self.channel_dir / "processed"
        if not old_processed.exists():
            return
            
        logger.info(f"Migration des fichiers de 'processed' vers la nouvelle structure pour {self.channel_dir.name}")
        
        # On déplace tous les fichiers .mp4 vers ready_to_stream
        for video in old_processed.glob("*.mp4"):
            dest = self.ready_to_stream_dir / video.name
            if not dest.exists():
                try:
                    shutil.move(str(video), str(dest))
                    logger.info(f"Fichier migré vers ready_to_stream: {video.name}")
                except Exception as e:
                    logger.error(f"Erreur migration {video.name}: {e}")
        
        # On déplace les .mkv vers already_processed (ils ne devraient pas être dans ready_to_stream)
        for video in old_processed.glob("*.mkv"):
            dest = self.already_processed_dir / video.name
            if not dest.exists():
                try:
                    shutil.move(str(video), str(dest))
                    logger.info(f"Fichier MKV migré vers already_processed: {video.name}")
                except Exception as e:
                    logger.error(f"Erreur migration MKV {video.name}: {e}")

    def check_gpu_support(self):
        """Vérifie si le support GPU est disponible"""
        try:
            result = subprocess.run(['vainfo'], capture_output=True, text=True)
            if result.returncode == 0 and 'VAEntrypointVLD' in result.stdout:
                logger.info("✅ Support VAAPI détecté et activé")
                return True
            else:
                logger.warning("⚠️ VAAPI configuré mais non fonctionnel, retour au mode CPU")
                self.USE_GPU = False
                return False
        except Exception as e:
            logger.error(f"❌ Erreur vérification VAAPI: {str(e)}")
            self.USE_GPU = False
            return False

    def _background_processor(self):
        """Thread d'arrière-plan qui traite les vidéos en file d'attente"""
        while not self.stop_processing.is_set():
            try:
                # Scan pour les nouveaux fichiers
                self.scan_for_new_videos()
                
                # Traitement des fichiers en queue
                if not self.processing_queue.empty():
                    video = self.processing_queue.get()
                    
                    # Vérifie si le fichier n'est pas déjà en cours de traitement
                    with self.processing_lock:
                        if video in self.currently_processing:
                            self.processing_queue.task_done()
                            continue
                        self.currently_processing.add(video)
                    
                    try:
                        # Déplacement vers processing
                        temp_path = self.processing_dir / video.name
                        if not temp_path.exists() and video.exists():
                            shutil.copy2(video, temp_path)
                        
                        # Traitement
                        processed = self.process_video(temp_path)
                        
                        if processed:
                            with self.processing_lock:
                                self.processed_files.append(processed)
                                
                            # Déplacer l'original dans already_processed
                            if video.exists():
                                already_path = self.already_processed_dir / video.name
                                if not already_path.exists():
                                    try:
                                        shutil.move(str(video), str(already_path))
                                        logger.info(f"Original déplacé vers already_processed: {video.name}")
                                    except Exception as e:
                                        logger.error(f"Erreur déplacement original {video.name}: {e}")
                            
                            # Supprimer le fichier temporaire de processing
                            if temp_path.exists():
                                temp_path.unlink()
                                
                    except Exception as e:
                        logger.error(f"❌ Erreur traitement {video.name}: {e}")
                    finally:
                        with self.processing_lock:
                            self.currently_processing.discard(video)
                        self.processing_queue.task_done()
                        
                # Pause entre les cycles
                time.sleep(5)
                
            except Exception as e:
                logger.error(f"❌ Erreur dans le thread de traitement: {e}")
                time.sleep(5)

    def scan_for_new_videos(self) -> int:
        """
        Scanne les nouveaux fichiers à traiter
        Retourne le nombre de nouveaux fichiers détectés
        """
        try:
            count = 0
            video_extensions = (".mp4", ".avi", ".mkv", ".mov")
            
            # Liste des fichiers déjà traités ou en cours de traitement pour éviter les doublons
            with self.processing_lock:
                already_processed_names = {f.name for f in self.already_processed_dir.glob("*.*")}
                ready_to_stream_names = {f.name for f in self.ready_to_stream_dir.glob("*.*")}
                processing_names = {f.name for f in self.processing_dir.glob("*.*")}
                currently_processing_names = {f.name for f in self.currently_processing}
            
            # Parcours des fichiers source
            for ext in video_extensions:
                for source in self.channel_dir.glob(f"*{ext}"):
                    # Si déjà traité ou en cours, on ignore
                    if (source.name in already_processed_names or 
                        source.name in ready_to_stream_names or
                        source.name in processing_names or
                        source.name in currently_processing_names):
                        continue
                    
                    # Ajout à la queue de traitement
                    self.processing_queue.put(source)
                    count += 1
                    logger.info(f"Nouveau fichier détecté pour traitement: {source.name}")
            
            return count
            
        except Exception as e:
            logger.error(f"❌ Erreur scan nouveaux fichiers: {e}")
            return 0

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

    def process_video(self, video_path: Path) -> Optional[Path]:
        """Traite un fichier vidéo avec gestion spéciale pour HEVC 10-bit"""
        try:
            # Sanitize the source filename
            sanitized_name = self.sanitize_filename(video_path.name)
            sanitized_path = video_path.parent / sanitized_name
            
            # Rename the source file if needed
            if video_path.name != sanitized_name:
                logger.info(f"Renaming source file: {video_path.name} -> {sanitized_name}")
                if sanitized_path.exists():
                    sanitized_path.unlink()
                video_path.rename(sanitized_path)
                video_path = sanitized_path
            
            # Force l'extension en .mp4 pour le fichier de sortie
            output_name = video_path.stem
            if not output_name.endswith('.mp4'):
                output_name = f"{output_name}.mp4"
            else:
                output_name = video_path.name
                
            # Create sanitized output path dans ready_to_stream
            output_path = self.ready_to_stream_dir / output_name
            
            # Skip if already processed and optimized
            if output_path.exists() and self.is_already_optimized(output_path):
                logger.info(f"✅ {output_name} déjà optimisé dans ready_to_stream")
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
            command.extend(["-map", "0:v:0", "-map", "0:a:0?"])
            
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

            logger.info(f"✅ {video_path.name} traité avec succès -> {output_path}")
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
            temp_file = self.processing_dir / f"temp_{input_path.stem}.mp4"
            
            # Première étape: Extraction simple sans réencodage vidéo
            extract_cmd = [
                "ffmpeg", "-y",
                "-i", str(input_path),
                "-map", "0:v:0",            # Premier flux vidéo
                "-map", "0:a:0?",           # Premier flux audio s'il existe
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

    def get_processed_files(self) -> List[Path]:
        """Renvoie la liste des fichiers traités et prêts pour le streaming"""
        try:
            mp4_files = list(self.ready_to_stream_dir.glob("*.mp4"))
            return sorted(mp4_files) if mp4_files else []
        except Exception as e:
            logger.error(f"❌ Erreur récupération des fichiers traités: {e}")
            return []

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

    def stop(self):
        """Arrête proprement le thread de traitement"""
        self.stop_processing.set()
        if self.processing_thread and self.processing_thread.is_alive():
            self.processing_thread.join(timeout=5)
            logger.info("Thread de traitement vidéo arrêté")
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
        
        # Extraction du nom de la chaîne à partir du chemin du dossier
        self.channel_name = self.channel_dir.name
        
        # Création des nouveaux dossiers avec des noms plus explicites
        self.ready_to_stream_dir = self.channel_dir / "ready_to_stream"
        self.ready_to_stream_dir.mkdir(exist_ok=True)
        
        self.processing_dir = self.channel_dir / "processing"
        self.processing_dir.mkdir(exist_ok=True)
        
        # Création du dossier pour les fichiers ignorés
        self.ignored_dir = self.channel_dir / "ignored"
        self.ignored_dir.mkdir(exist_ok=True)
        
        self._clean_processing_dir()
        
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
        
        # Nom du canal pour les logs
        self.channel_name = self.channel_dir.name

    def _clean_processing_dir(self):
        """Vide le dossier processing au démarrage"""
        try:
            if self.processing_dir.exists():
                for file in self.processing_dir.glob("*.*"):
                    try:
                        file.unlink()
                        logger.info(f"Fichier temporaire supprimé: {file.name}")
                    except Exception as e:
                        logger.error(f"Erreur suppression {file.name}: {e}")
        except Exception as e:
            logger.error(f"Erreur nettoyage dossier processing: {e}")      
            
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

    def _wait_for_file_stability(self, file_path: Path, timeout=60) -> bool:
        """
        Attend que le fichier soit stable (taille constante) avant de le traiter
        
        Args:
            file_path: Chemin du fichier à vérifier
            timeout: Délai maximum d'attente en secondes
            
        Returns:
            bool: True si le fichier est stable, False sinon
        """
        if not file_path.exists():
            logger.error(f"Fichier introuvable: {file_path}")
            return False
        
        start_time = time.time()
        last_size = -1
        stable_count = 0
        
        logger.info(f"Vérification de la stabilité du fichier: {file_path.name}")
        
        while time.time() - start_time < timeout:
            try:
                current_size = file_path.stat().st_size
                
                # Si c'est un petit fichier, on fait une vérification moins stricte
                min_stable_seconds = 3
                if current_size > 100 * 1024 * 1024:  # > 100 MB
                    min_stable_seconds = 5
                
                # Log l'évolution de la taille
                if last_size != -1:
                    if current_size == last_size:
                        stable_count += 1
                        logger.info(f"Fichier stable depuis {stable_count}s: {file_path.name} ({current_size/1024/1024:.1f} MB)")
                    else:
                        stable_count = 0
                        logger.info(f"{file_path.name} Taille en évolution: {current_size/1024/1024:.1f} MB (était {last_size/1024/1024:.1f} MB)")
                
                # Si la taille est stable pendant le temps requis
                if current_size == last_size and stable_count >= min_stable_seconds:
                    logger.info(f"✅ Fichier {file_path.name} stable depuis {stable_count}s, prêt pour traitement")
                    return True
                    
                last_size = current_size
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Erreur vérification stabilité {file_path.name}: {e}")
                time.sleep(1)
                
        logger.warning(f"⏰ Timeout en attendant la stabilité de {file_path.name}")
        return False 
    
    def notify_file_processed(self, file_path):
        """Notifie que le fichier a été traité avec succès et met à jour le statut de la chaîne"""
        try:
            # Obtient le nom de la chaîne à partir du chemin
            channel_name = Path(self.channel_dir).name
            
            # Trouve l'instance de IPTVManager dans la pile d'appels
            import inspect
            frame = inspect.currentframe()
            manager = None
            
            # Cherche un objet qui contient 'channels' et 'channel_ready_status'
            while frame:
                if 'self' in frame.f_locals:
                    obj = frame.f_locals['self']
                    if hasattr(obj, 'channels') and hasattr(obj, 'channel_ready_status'):
                        manager = obj
                        break
                frame = frame.f_back
            
            if manager:
                # Met à jour le statut de préparation de la chaîne
                if channel_name in manager.channels:
                    channel = manager.channels[channel_name]
                    
                    # Marque la chaîne comme prête
                    channel.ready_for_streaming = True
                    
                    # Met à jour le statut dans le manager
                    with manager.scan_lock:
                        manager.channel_ready_status[channel_name] = True
                    
                    logger.info(f"[{channel_name}] ✅ Chaîne marquée comme prête après traitement de {Path(file_path).name}")
                    
                    # Force un rafraîchissement de la playlist maître
                    if hasattr(manager, '_manage_master_playlist'):
                        threading.Thread(target=manager._manage_master_playlist, daemon=True).start()
        
        except Exception as e:
            logger.error(f"❌ Erreur notification traitement: {e}")
    
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
                        # Attendre que le fichier source soit stable avant de le copier
                        if not self._wait_for_file_stability(video, timeout=120):
                            logger.warning(f"⚠️ Fichier {video.name} instable, on reporte le traitement")
                            # Remet le fichier dans la queue pour traitement ultérieur
                            self.processing_queue.put(video)
                            with self.processing_lock:
                                self.currently_processing.discard(video)
                            self.processing_queue.task_done()
                            time.sleep(30)  # Attente plus longue avant de réessayer
                            continue
                        
                        # Déplacement vers processing
                        temp_path = self.processing_dir / video.name
                        if not temp_path.exists() and video.exists():
                            logger.info(f"Copie de {video.name} vers le dossier processing...")
                            shutil.copy2(video, temp_path)
                            
                            # Attendre que la copie soit stable
                            if not self._wait_for_file_stability(temp_path, timeout=60):
                                logger.warning(f"⚠️ Copie de {video.name} instable, on annule le traitement")
                                if temp_path.exists():
                                    temp_path.unlink()
                                with self.processing_lock:
                                    self.currently_processing.discard(video)
                                self.processing_queue.task_done()
                                continue
                        
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
                                
                            # Notifie que le traitement est terminé et met à jour le statut de la chaîne
                            if hasattr(self, 'notify_file_processed'):
                                self.notify_file_processed(processed)
                                
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
            video_extensions = (".mp4", ".avi", ".mkv", ".mov", ".m4v")
            
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
        # Options d'accélération GPU pour l'entrée uniquement
        args = [
            "-hwaccel", "vaapi",
            "-hwaccel_output_format", "vaapi",
            "-vaapi_device", "/dev/dri/renderD128"
        ]
        
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
        
    def _verify_output_file(self, file_path: Path) -> bool:
        """
        Vérifie que le fichier de sortie est valide après transcodage
        
        Args:
            file_path: Chemin du fichier à vérifier
            
        Returns:
            bool: True si le fichier est valide, False sinon
        """
        if not file_path.exists():
            logger.error(f"❌ Fichier introuvable: {file_path}")
            return False
            
        # Vérification de la taille du fichier
        try:
            file_size = file_path.stat().st_size
            if file_size < 10000:  # Moins de 10KB est suspicieux pour une vidéo
                logger.error(f"❌ Fichier trop petit: {file_path} ({file_size} bytes)")
                return False
        except Exception as e:
            logger.error(f"❌ Erreur accès fichier {file_path}: {e}")
            return False
            
        # Vérification basique avec ffprobe
        try:
            cmd = [
                "ffprobe",
                "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=codec_name,width,height",
                "-of", "json",
                str(file_path)
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            
            if result.returncode != 0:
                logger.error(f"❌ Erreur ffprobe: {result.stderr}")
                return False
                
            # Essai de parse du JSON
            try:
                data = json.loads(result.stdout)
                if 'streams' in data and len(data['streams']) > 0:
                    stream = data['streams'][0]
                    logger.info(f"✅ Vidéo validée: {file_path.name} ({stream.get('width')}x{stream.get('height')}, codec: {stream.get('codec_name')})")
                    return True
                else:
                    logger.error(f"❌ Pas de flux vidéo dans {file_path}")
                    return False
            except json.JSONDecodeError:
                logger.error(f"❌ Sortie JSON invalide: {result.stdout}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error(f"❌ Timeout ffprobe pour {file_path}")
            return False
        except Exception as e:
            logger.error(f"❌ Erreur vérification {file_path}: {e}")
            return False

    def _get_video_duration(self, video_path, max_retries=2):
        """
        # Obtient la durée d'un fichier vidéo avec retries
        """
        for i in range(max_retries + 1):
            try:
                cmd = [
                    "ffprobe",
                    "-v", "error",
                    "-show_entries", "format=duration",
                    "-of", "default=noprint_wrappers=1:nokey=1",
                    str(video_path)
                ]

                result = subprocess.run(cmd, capture_output=True, text=True)
                
                if result.returncode == 0:
                    try:
                        duration = float(result.stdout.strip())
                        if duration > 0:
                            return duration
                    except ValueError:
                        pass
                
                # Si on arrive ici, c'est que ça a échoué
                logger.warning(f"[{self.channel_name}] ⚠️ Tentative {i+1}/{max_retries+1} échouée pour {video_path}")
                time.sleep(0.5)  # Petite pause avant la prochaine tentative
                
            except Exception as e:
                logger.error(f"[{self.channel_name}] ❌ Erreur ffprobe: {e}")
        
        # Si on a échoué après toutes les tentatives, on renvoie une durée par défaut
        logger.error(f"[{self.channel_name}] ❌ Impossible d'obtenir la durée pour {video_path}")
        return 0
 
    def _format_time(self, seconds: float) -> str:
        """Formate un temps en secondes au format HH:MM:SS.mmm"""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        seconds_part = seconds % 60
        return f"{hours:02d}:{minutes:02d}:{seconds_part:06.3f}"

    def _concatenate_segments(self, segment_dir: Path, output_path: Path) -> Optional[Path]:
        """Concatène les segments en un seul fichier MP4"""
        try:
            # Liste tous les segments
            segments = sorted(segment_dir.glob("segment_*.mp4"))
            
            if not segments:
                logger.error(f"❌ Aucun segment trouvé dans {segment_dir}")
                self._cleanup_temp_dir(segment_dir)
                return None
                
            logger.info(f"🔄 Concaténation de {len(segments)} segments")
            
            # Création du fichier de concaténation
            concat_file = segment_dir / "concat.txt"
            with open(concat_file, "w") as f:
                for segment in segments:
                    f.write(f"file '{segment.name}'\n")
            
            # Concaténation avec ffmpeg
            cmd = [
                "ffmpeg", "-y",
                "-f", "concat",
                "-safe", "0",
                "-i", str(concat_file),
                "-c", "copy",  # Copie sans réencodage
                "-movflags", "+faststart",  # Optimisation streaming
                str(output_path)
            ]
            
            result = subprocess.run(cmd, 
                                cwd=str(segment_dir),
                                capture_output=True, 
                                text=True)
            
            if result.returncode != 0:
                logger.error(f"❌ Erreur concaténation: {result.stderr}")
                self._cleanup_temp_dir(segment_dir)
                return None
                
            logger.info(f"✅ Concaténation réussie -> {output_path}")
            
            # Nettoyer les segments temporaires
            self._cleanup_temp_dir(segment_dir)
            
            # Notifier la fin du traitement
            self.notify_file_processed(output_path)
            
            return output_path
            
        except Exception as e:
            logger.error(f"❌ Erreur concaténation: {e}")
            self._cleanup_temp_dir(segment_dir)
            return None

    def _cleanup_temp_dir(self, temp_dir: Path):
        """Nettoie un répertoire temporaire"""
        try:
            if temp_dir.exists():
                shutil.rmtree(temp_dir)
                logger.info(f"🧹 Répertoire temporaire nettoyé: {temp_dir}")
        except Exception as e:
            logger.error(f"❌ Erreur nettoyage répertoire temporaire: {e}")

    def process_video(self, video_path: Path) -> Optional[Path]:
        """Traite un fichier vidéo avec gestion simplifiée et améliorée"""
        filename = video_path.name  # Extraction du nom du fichier
        try:
            # Sanitize the source filename
            sanitized_name = self.sanitize_filename(filename)
            sanitized_path = video_path.parent / sanitized_name
            
            # Rename the source file if needed
            if filename != sanitized_name:
                logger.info(f"[{self.channel_name}] Renommage: {filename} -> {sanitized_name}")
                if sanitized_path.exists():
                    sanitized_path.unlink()
                video_path.rename(sanitized_path)
                video_path = sanitized_path
                filename = sanitized_name  # Mise à jour du nom du fichier
            
            # Force l'extension en .mp4 pour le fichier de sortie
            output_name = video_path.stem
            if not output_name.endswith('.mp4'):
                output_name = f"{output_name}.mp4"
            else:
                output_name = filename
                
            # Create sanitized output path dans ready_to_stream
            output_path = self.ready_to_stream_dir / output_name
            
            # Skip if already processed and optimized
            if output_path.exists() and self.is_already_optimized(output_path):
                logger.info(f"[{self.channel_name}] ✅ {output_name} déjà optimisé")
                # Notifie même si déjà traité
                self.notify_file_processed(output_path)
                return output_path

            # On vérifie pourquoi ce fichier nécessite une normalisation
            cmd_probe = [
                "ffprobe", "-v", "error",
                "-show_entries", "stream=codec_name,width,height,r_frame_rate,codec_type",
                "-of", "json",
                str(video_path)
            ]
            probe_result = subprocess.run(cmd_probe, capture_output=True, text=True)
            
            try:
                video_info = json.loads(probe_result.stdout)
                streams = video_info.get("streams", [])
                
                # Récupération des infos vidéo
                video_streams = [s for s in streams if s['codec_type'] == 'video']
                audio_streams = [s for s in streams if s['codec_type'] == 'audio']
                
                reasons = []
                
                if video_streams:
                    video = video_streams[0]
                    codec = video.get("codec_name", "").lower()
                    width = int(video.get("width", 0))
                    height = int(video.get("height", 0))
                    framerate = video.get("r_frame_rate", "0/1").split("/")
                    fps = round(int(framerate[0]) / int(framerate[1])) if len(framerate) == 2 else 0
                    
                    if codec not in ["h264", "hevc", "h265"]:
                        reasons.append(f"codec vidéo {codec} non supporté")
                    if width > 1920 or height > 1080:
                        reasons.append(f"résolution {width}x{height} supérieure à 1080p")
                    if fps > 60:
                        reasons.append(f"FPS de {fps} supérieur à 60")
                else:
                    reasons.append("pas de flux vidéo détecté")
                    
                if not audio_streams:
                    reasons.append("pas de flux audio détecté")
                elif audio_streams:
                    audio = audio_streams[0]
                    audio_codec = audio.get("codec_name", "").lower()
                    if audio_codec not in ["aac", "mp3"]:
                        reasons.append(f"codec audio {audio_codec} non supporté")
                
                # Log détaillé des raisons de normalisation
                if reasons:
                    reasons_str = ", ".join(reasons)
                    logger.info(f"[{self.channel_name}] 🔄 Normalisation de {filename} nécessaire: {reasons_str}")
            except:
                logger.warning(f"[{self.channel_name}] ⚠️ Impossible d'analyser les raisons de normalisation pour {filename}")
            
            # Déterminer la durée totale de la vidéo source
            total_duration = self._get_video_duration(str(video_path))
            if total_duration <= 0:
                total_duration = None
                logger.warning(f"[{self.channel_name}] ⚠️ Impossible de déterminer la durée de {filename}")
            else:
                logger.info(f"[{self.channel_name}] 📊 Durée de {filename}: {self._format_time(total_duration)}")
            
            # Vérifier si le fichier est HEVC 10-bit pour adapter l'approche
            is_hevc_10bit = self._check_hevc_10bit(video_path)
            
            # Utilisation d'un chemin temporaire pour le fichier de sortie
            temp_output_path = self.processing_dir / f"tmp_{output_name}"
            if temp_output_path.exists():
                temp_output_path.unlink()
                
            # Construction de la commande FFmpeg - Approche simplifiée sans segmentation
            if is_hevc_10bit:
                command = [
                    "ffmpeg", "-y",
                    "-i", str(video_path),
                    "-c:v", "libx264", "-crf", "22", "-preset", "fast",
                    "-c:a", "aac", "-b:a", "192k", "-ac", "2",
                    "-sn", "-dn", "-map_chapters", "-1",
                    "-map", "0:v:0", "-map", "0:a:0?",
                    "-movflags", "+faststart",
                    "-max_muxing_queue_size", "4096",
                    "-progress", "pipe:1",
                    str(output_path)
                ]
            elif self.USE_GPU:
                command = [
                    "ffmpeg", "-y",
                    # Options hwaccel AVANT l'input
                    "-hwaccel", "vaapi", 
                    "-hwaccel_output_format", "vaapi",
                    "-vaapi_device", "/dev/dri/renderD128",
                    "-i", str(video_path),
                    "-c:v", "h264_vaapi", "-profile:v", "main",
                    "-c:a", "aac", "-b:a", "192k", "-ac", "2",
                    "-sn", "-dn", "-map_chapters", "-1",
                    "-map", "0:v:0", "-map", "0:a:0?",
                    "-movflags", "+faststart",
                    "-max_muxing_queue_size", "4096",
                    "-progress", "pipe:1",
                    str(output_path)
                ]
            else:
                command = [
                    "ffmpeg", "-y",
                    "-i", str(video_path),
                    "-c:v", "libx264", "-preset", "fast", "-crf", "23",
                    "-c:a", "aac", "-b:a", "192k", "-ac", "2",
                    "-sn", "-dn", "-map_chapters", "-1",
                    "-map", "0:v:0", "-map", "0:a:0?",
                    "-movflags", "+faststart",
                    "-max_muxing_queue_size", "4096",
                    "-progress", "pipe:1",
                    str(output_path)
                ]
            
            # Log de la commande complète
            logger.info(f"[{self.channel_name}] 🎬 Commande pour {filename}:")
            logger.info(f"$ {' '.join(command)}")
            
            # Exécution de la commande
            try:
                process = subprocess.Popen(
                    command,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True,
                    bufsize=1
                )
                
                # Suivi de la progression
                start_time = time.time()
                last_progress_time = start_time
                progress_data = {}
                
                while process.poll() is None:
                    # Vérification du timeout global (2 heures)
                    current_time = time.time()
                    if current_time - start_time > 2 * 60 * 60:
                        logger.error(f"[{self.channel_name}] ⏰ Timeout de 2h pour {filename}")
                        process.kill()
                        return None
                    
                    # Lecture de la progression
                    try:
                        import select
                        ready_to_read, _, _ = select.select([process.stdout], [], [], 1.0)
                        if ready_to_read:
                            stdout_line = process.stdout.readline().strip()
                            if stdout_line and '=' in stdout_line:
                                key, value = stdout_line.split('=', 1)
                                progress_data[key] = value
                                
                                # Affichage de la progression
                                if key == 'out_time' and current_time - last_progress_time >= 10:
                                    if total_duration:
                                        # Calcul du pourcentage et temps restant
                                        time_parts = value.split(':')
                                        if len(time_parts) == 3:
                                            hours, minutes, seconds = time_parts
                                            seconds_parts = seconds.split('.')
                                            seconds = float(f"{seconds_parts[0]}.{seconds_parts[1]}" if len(seconds_parts) > 1 else seconds_parts[0])
                                            out_time_seconds = int(hours) * 3600 + int(minutes) * 60 + seconds
                                            
                                            # Pourcentage et vitesse
                                            elapsed = current_time - start_time
                                            if elapsed > 0:
                                                speed = out_time_seconds / elapsed
                                                percent_done = (out_time_seconds / total_duration) * 100
                                                remaining = (total_duration - out_time_seconds) / max(0.1, speed)
                                                eta = time.strftime("%H:%M:%S", time.gmtime(remaining))
                                                
                                                logger.info(f"[{self.channel_name}] 🔄 {filename}: {value} / {self._format_time(total_duration)} "
                                                        f"({percent_done:.1f}%) - ETA: {eta} - Vitesse: {speed:.2f}x")
                                    else:
                                        logger.info(f"[{self.channel_name}] 🔄 {filename}: {value}")
                                    
                                    last_progress_time = current_time
                    except Exception as e:
                        logger.debug(f"[{self.channel_name}] Erreur lecture progression: {e}")
                    
                    time.sleep(0.1)
                
                # Vérification du résultat
                return_code = process.wait()
                stderr_output = process.stderr.read()
                
                if return_code != 0:
                    logger.error(f"[{self.channel_name}] ❌ Erreur FFmpeg pour {filename}: {stderr_output}")
                    return None
                
                # Vérification que le fichier de sortie existe et est valide
                if output_path.exists() and output_path.stat().st_size > 0:
                    logger.info(f"[{self.channel_name}] ✅ Transcodage réussi: {output_name}")
                    self.notify_file_processed(output_path)
                    return output_path
                else:
                    logger.error(f"[{self.channel_name}] ❌ Fichier de sortie invalide: {output_name}")
                    return None
                    
            except Exception as e:
                logger.error(f"[{self.channel_name}] ❌ Exception transcodage {filename}: {e}")
                if process and process.poll() is None:
                    process.kill()
                return None
        
        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur processing {filename}: {e}")
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
        Critères assouplis pour TiviMate.
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
            
            # Si on a plus d'un flux vidéo, normalisation requise
            if len(video_streams) > 1:
                logger.warning(f"⚠️ {video_path.name} contient {len(video_streams)} flux vidéo")
                logger.info(f"🚨 Multiples flux vidéo détectés, normalisation nécessaire")
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
            
            # Critères de normalisation - Version assouplie pour TiviMate
            reasons = []
            needs_transcoding = False

            # Vérification des codecs vidéo
            if codec not in ["h264", "hevc", "h265"]:
                reasons.append(f"codec vidéo {codec} non supporté (besoin de H.264 ou H.265)")
                needs_transcoding = True

            # Vérification de la résolution (toujours limiter à 1080p)
            if width > 1920 or height > 1080:
                reasons.append(f"résolution {width}x{height} supérieure à 1080p")
                needs_transcoding = True

            # Vérification du FPS (maintenant augmenté à 60)
            if fps > 60:
                reasons.append(f"FPS de {fps} supérieur à 60")
                needs_transcoding = True

            # Vérification de l'audio
            if audio_codec == "none":
                reasons.append("pas de piste audio détectée")
                needs_transcoding = True
            elif audio_codec not in ["aac", "mp3"]:
                reasons.append(f"codec audio {audio_codec} non compatible (besoin de AAC ou MP3)")
                needs_transcoding = True

            if has_chapters and len(chapters_info['chapters']) > 15:
                # On ne considère problématiques que les fichiers avec beaucoup de chapitres
                reasons.append(f"contient trop de chapitres ({len(chapters_info['chapters'])})")
                needs_transcoding = True

            # Sortie des résultats de l'analyse
            if needs_transcoding:
                reasons_str = ", ".join(reasons)
                logger.info(f"⚠️ Normalisation nécessaire pour {video_path.name}: {reasons_str}")
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
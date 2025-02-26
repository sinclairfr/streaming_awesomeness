# iptv_channel.py
import os
import time
import threading
from pathlib import Path
from typing import Optional
import shutil
import json
from video_processor import VideoProcessor
from hls_cleaner import HLSCleaner
from ffmpeg_logger import FFmpegLogger
from stream_error_handler import StreamErrorHandler
from mkv_handler import MKVHandler
from ffmpeg_command_builder import FFmpegCommandBuilder
from ffmpeg_process_manager import FFmpegProcessManager
from playback_position_manager import PlaybackPositionManager

from config import (
    TIMEOUT_NO_VIEWERS,
    logger,
    CONTENT_DIR,
    USE_GPU,
)

class IPTVChannel:
    """Gère une chaîne IPTV, son streaming et sa surveillance"""

    def __init__(
        self,
        name: str,
        video_dir: str,
        hls_cleaner: HLSCleaner,
        use_gpu: USE_GPU
    ):
        self.name = name
        self.video_dir = video_dir
        self.use_gpu = use_gpu
        self.hls_cleaner = hls_cleaner
        self.error_handler = StreamErrorHandler(self.name)
        self.lock = threading.Lock()

        # Initialisation des managers
        self.logger = FFmpegLogger(name)
        self.command_builder = FFmpegCommandBuilder(name, use_gpu=use_gpu) 
        self.process_manager = FFmpegProcessManager(name, self.logger)
        self.position_manager = PlaybackPositionManager(name)
        
        # Configuration des callbacks
        self.process_manager.on_process_died = self._handle_process_died
        self.process_manager.on_position_update = self._handle_position_update
        self.process_manager.on_segment_created = self._handle_segment_created

        # Autres composants
        self.processor = VideoProcessor(self.video_dir)
        self.mkv_handler = MKVHandler(self.name, logger)
        
        # Variables de surveillance
        self.processed_videos = []
        self.watchers_count = 0
        self.last_watcher_time = time.time()
        self.last_segment_time = time.time()

        # Scan initial des vidéos
        self._scan_videos()
        
        # Calcul de la durée totale
        total_duration = self._calculate_total_duration()
        self.position_manager.set_total_duration(total_duration)
        self.process_manager.set_total_duration(total_duration)

    def _calculate_total_duration(self) -> float:
        """Calcule la durée totale en utilisant le PositionManager"""
        try:
            total_duration = self.position_manager.calculate_durations(self.processed_videos)
            if total_duration <= 0:
                logger.warning(f"[{self.name}] ⚠️ Durée totale invalide, fallback à 120s")
                return 120.0
                
            return total_duration
        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur calcul durée: {e}")
            return 120.0

    def _check_segments(self, hls_dir: str) -> bool:
        """Vérifie la génération des segments HLS"""
        try:
            segment_log_path = Path(f"/app/logs/segments/{self.name}_segments.log")
            segment_log_path.parent.mkdir(parents=True, exist_ok=True)
            
            hls_path = Path(hls_dir)
            playlist = hls_path / "playlist.m3u8"
            
            if not playlist.exists():
                logger.error(f"[{self.name}] ❌ playlist.m3u8 introuvable")
                return False
                
            with open(playlist) as f:
                segments = [line.strip() for line in f if line.strip().endswith('.ts')]
                
            if not segments:
                logger.warning(f"[{self.name}] ⚠️ Aucun segment dans la playlist")
                return False
                
            current_time = time.strftime("%Y-%m-%d %H:%M:%S")
            log_entry = f"{current_time} - Segments actifs: {len(segments)}\n"
            
            for segment in segments:
                segment_path = hls_path / segment
                if segment_path.exists():
                    size = segment_path.stat().st_size
                    mtime = time.strftime("%H:%M:%S", time.localtime(segment_path.stat().st_mtime))
                    log_entry += f"  - {segment} (Size: {size/1024:.1f}KB, Modified: {mtime})\n"
                else:
                    log_entry += f"  - {segment} (MISSING)\n"
                    
            with open(segment_log_path, "a") as f:
                f.write(log_entry)
                f.write("-" * 80 + "\n")
                
            return True
            
        except Exception as e:
            logger.error(f"[{self.name}] Erreur vérification segments: {e}")
            return False

    def _handle_timeouts(self, current_time, crash_threshold):
        """Gère les timeouts et redémarre le stream si nécessaire"""
        if current_time - self.last_segment_time > crash_threshold:
            logger.error(f"🔥 Pas de nouveau segment pour {self.name} depuis {current_time - self.last_segment_time:.1f}s")
            if self.error_handler.add_error("segment_timeout"):
                if self._restart_stream():
                    self.error_handler.reset()
                return True
        return False

    def _check_viewer_inactivity(self, current_time, timeout):
        """Vérifie l'inactivité des viewers et gère l'arrêt du stream"""
        if not self.process_manager.is_running():
            return False

        inactivity_duration = current_time - self.last_watcher_time
        
        if inactivity_duration > timeout + 60:  
            logger.info(f"[{self.name}] ⚠️ Inactivité détectée: {inactivity_duration:.1f}s")
            return True
            
        return False
    
    def _clean_processes(self):
        """Nettoie les processus en utilisant le ProcessManager"""
        try:
            self.position_manager.save_position()
            self.process_manager.stop_process()
            logger.info(f"[{self.name}] 🧹 Nettoyage des processus terminé")
        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur nettoyage processus: {e}")              
    
    def _create_concat_file(self) -> Optional[Path]:
        """Crée le fichier de concaténation avec les bons chemins"""
        try:
            logger.info(f"[{self.name}] 🛠️ Création de _playlist.txt")

            processed_dir = Path(CONTENT_DIR) / self.name / "processed"
            concat_file = Path(CONTENT_DIR) / self.name / "_playlist.txt"

            processed_files = sorted(processed_dir.glob("*.mp4")) + sorted(processed_dir.glob("*.mkv"))
            if not processed_files:
                logger.error(f"[{self.name}] ❌ Aucune vidéo dans {processed_dir}")
                return None

            logger.info(f"[{self.name}] 📝 Écriture de _playlist.txt")

            with open(concat_file, "w", encoding="utf-8") as f:
                for video in processed_files:
                    escaped_path = str(video.absolute()).replace("'", "'\\''")
                    f.write(f"file '{escaped_path}'\n")
                    logger.info(f"[{self.name}] ✅ Ajout de {video.name}")

            logger.info(f"[{self.name}] 🎥 Playlist créée")
            return concat_file

        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur _playlist.txt: {e}")
            return None
        
    def _verify_playlist(self):
        """Vérifie que le fichier playlist est valide"""
        try:
            playlist_path = Path(f"/app/content/{self.name}/_playlist.txt")
            if not playlist_path.exists():
                logger.error(f"[{self.name}] ❌ _playlist.txt n'existe pas")
                return False

            with open(playlist_path, 'r') as f:
                lines = f.readlines()

            if not lines:
                logger.error(f"[{self.name}] ❌ _playlist.txt est vide")
                return False

            valid_count = 0
            for i, line in enumerate(lines, 1):
                line = line.strip()
                if not line:
                    continue

                if not line.startswith('file'):
                    logger.error(f"[{self.name}] ❌ Ligne {i} invalide: {line}")
                    return False

                try:
                    file_path = line.split("'")[1] if "'" in line else line.split()[1]
                    file_path = Path(file_path)
                    if not file_path.exists():
                        logger.error(f"[{self.name}] ❌ Fichier manquant: {file_path}")
                        return False
                    valid_count += 1
                except Exception as e:
                    logger.error(f"[{self.name}] ❌ Erreur parsing ligne {i}: {e}")
                    return False

            if valid_count == 0:
                logger.error(f"[{self.name}] ❌ Aucun fichier valide dans la playlist")
                return False

            logger.info(f"[{self.name}] ✅ Playlist valide avec {valid_count} fichiers")
            return True

        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur vérification playlist: {e}")
            return False

    def start_stream(self) -> bool:
        """Démarre le stream avec FFmpeg en utilisant les nouvelles classes"""
        try:
            logger.info(f"[{self.name}] 🚀 Démarrage du stream...")

            hls_dir = Path(f"/app/hls/{self.name}")
            logger.info(f"[{self.name}] Création du répertoire HLS: {hls_dir}")
            hls_dir.mkdir(parents=True, exist_ok=True)
        
            concat_file = self._create_concat_file()
            if not concat_file or not concat_file.exists():
                logger.error(f"[{self.name}] ❌ _playlist.txt introuvable")
                return False
            else:
                logger.info(f"[{self.name}] ✅ _playlist.txt trouvé")
            

            start_offset = self.position_manager.get_start_offset()
            logger.info(f"[{self.name}] Décalage de démarrage: {start_offset}")
            
            logger.info(f"[{self.name}] Optimisation pour le matériel...")
            self.command_builder.optimize_for_hardware()
            logger.info(f"[{self.name}] Vérification mkv...")
            has_mkv = self.command_builder.detect_mkv_in_playlist(concat_file)

            logger.info(f"[{self.name}] Construction de la commande FFmpeg...")
            command = self.command_builder.build_command(
                input_file=concat_file,
                output_dir=hls_dir,
                playback_offset=start_offset,
                progress_file=self.logger.get_progress_file(),
                has_mkv=has_mkv
            )
            
            if not self.process_manager.start_process(command, hls_dir):
                logger.error(f"[{self.name}] ❌ Échec démarrage FFmpeg")
                return False
                
            self.position_manager.set_playing(True)
            
            logger.info(f"[{self.name}] ✅ Stream démarré avec succès")
            return True

        except Exception as e:
            logger.error(f"Erreur démarrage stream {self.name}: {e}")
            return False
    
    def _restart_stream(self) -> bool:
        """Redémarre le stream en cas de problème"""
        try:
            logger.info(f"🔄 Redémarrage du stream {self.name}")

            elapsed = time.time() - getattr(self, "last_restart_time", 0)
            if elapsed < self.error_handler.restart_cooldown:
                logger.info(
                    f"⏳ Attente du cooldown ({self.error_handler.restart_cooldown - elapsed:.1f}s)"
                )
                time.sleep(self.error_handler.restart_cooldown - elapsed)

            self.last_restart_time = time.time()

            self.process_manager.stop_process()
            time.sleep(2)

            return self.start_stream()

        except Exception as e:
            logger.error(f"Erreur lors du redémarrage de {self.name}: {e}")
            return False    
    
    def start_stream_if_needed(self) -> bool:
        """Démarre le stream si nécessaire"""
        if not hasattr(self, 'lock'):
            self.lock = threading.Lock()
            
        with self.lock:
            if self.process_manager.is_running():
                return True  

            self.last_watcher_time = time.time()

            hls_path = Path(f"/app/hls/{self.name}")
            if not hls_path.exists():
                logger.info(f"[{self.name}] 📂 Création automatique du dossier HLS")
                hls_path.mkdir(parents=True, exist_ok=True)
                os.chmod(hls_path, 0o777)

            if not self._verify_playlist():
                logger.error(f"[{self.name}] ❌ Vérification de playlist échouée")
                return False

            concat_file = self._create_concat_file()
            if not concat_file or not concat_file.exists():
                logger.error(f"[{self.name}] ❌ _playlist.txt est introuvable")
                return False

            return self.start_stream()
    
    def stop_stream_if_needed(self):
        """Arrête proprement le stream en utilisant les managers"""
        try:
            if not self.process_manager.is_running():
                return
                
            logger.info(f"[{self.name}] 🛑 Arrêt du stream (dernier watcher: {time.time() - self.last_watcher_time:.1f}s)")
            
            self.position_manager.set_playing(False)
            self.position_manager.save_position()
            
            self.process_manager.stop_process(save_position=True)
            
            if self.hls_cleaner:
                self.hls_cleaner.cleanup_channel(self.name)
                
            logger.info(f"[{self.name}] ✅ Stream arrêté avec succès")
                
        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur arrêt stream: {e}")
    
    def _scan_videos(self) -> bool:
        """Scanne les fichiers vidéos et met à jour processed_videos"""
        try:
            source_dir = Path(self.video_dir)
            processed_dir = source_dir / "processed"
            processed_dir.mkdir(exist_ok=True)
            
            self._verify_processor()
            
            if not source_dir.exists():
                logger.error(f"[{self.name}] ❌ Dossier source introuvable: {source_dir}")
                return False

            video_extensions = (".mp4", ".avi", ".mkv", ".mov")
            source_files = []
            for ext in video_extensions:
                source_files.extend(source_dir.glob(f"*{ext}"))

            if not source_files:
                logger.warning(f"[{self.name}] ⚠️ Aucun fichier vidéo dans {self.video_dir}")
                return False

            self.processed_videos = []

            for source in source_files:
                try:
                    processed_file = processed_dir / f"{source.stem}.mp4"
                    
                    if processed_file.exists():
                        logger.info(f"[{self.name}] ✅ Vidéo déjà présente: {source.name}")
                        self.processed_videos.append(processed_file)
                        continue

                    if not self.processor:
                        logger.error(f"[{self.name}] ❌ VideoProcessor non initialisé")
                        continue

                    try:
                        is_optimized = self.processor.is_already_optimized(source)
                    except Exception as e:
                        logger.error(f"[{self.name}] ❌ Erreur vérification optimisation {source.name}: {e}")
                        continue

                    if is_optimized:
                        logger.info(f"[{self.name}] ✅ Vidéo déjà optimisée: {source.name}")
                        try:
                            shutil.copy2(source, processed_file)
                            self.processed_videos.append(processed_file)
                        except Exception as e:
                            logger.error(f"[{self.name}] ❌ Erreur copie {source.name}: {e}")
                            continue
                    else:
                        try:
                            processed = self.processor.process_video(source)
                            if processed and processed.exists():
                                self.processed_videos.append(processed)
                                logger.info(f"[{self.name}] ✅ Vidéo traitée: {source.name}")
                            else:
                                logger.error(f"[{self.name}] ❌ Échec traitement: {source.name}")
                        except Exception as e:
                            logger.error(f"[{self.name}] ❌ Erreur traitement {source.name}: {e}")
                            continue

                except Exception as e:
                    logger.error(f"[{self.name}] ❌ Erreur traitement fichier {source.name}: {e}")
                    continue

            if not self.processed_videos:
                logger.error(f"[{self.name}] ❌ Aucune vidéo traitée disponible")
                return False

            self.processed_videos.sort()
            logger.info(f"[{self.name}] ✅ Scan terminé: {len(self.processed_videos)} vidéos traitées")
            return True

        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur scan des vidéos: {str(e)}")
            return False
    
    def update_watchers(self, count: int):
        """Mise à jour du nombre de watchers"""
        with self.lock:
            old_count = getattr(self, 'watchers_count', 0)
            self.watchers_count = count
            
            self.last_watcher_time = time.time()
            
            if old_count != count:
                logger.info(f"📊 Mise à jour {self.name}: {count} watchers")
                
            if count > 0 and old_count == 0:
                logger.info(f"[{self.name}] 🔥 Premier watcher, démarrage du stream")
                self.start_stream_if_needed()
  
    def _contains_mkv(self) -> bool:
        """Détecte la présence de fichiers MKV dans la playlist"""
        try:
            for video in self.processed_videos:
                path = str(video)
                if path.lower().endswith('.mkv'):
                    logger.info(f"[{self.name}] ✅ MKV détecté dans processed_videos: {path}")
                    return True
                    
            source_dir = Path(self.video_dir)
            mkv_files = list(source_dir.glob("*.mkv"))
            if mkv_files:
                logger.info(f"[{self.name}] ✅ MKV détectés dans source: {[f.name for f in mkv_files]}")
                return True

            playlist_path = Path(CONTENT_DIR) / self.name / "_playlist.txt"
            if playlist_path.exists():
                with open(playlist_path, 'r') as f:
                    content = f.read()
                    if '.mkv' in content.lower():
                        logger.info(f"[{self.name}] ✅ MKV détecté dans la playlist")
                        return True
                    
            return False

        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur détection MKV: {e}")
            return False
    
    def _verify_processor(self) -> bool:
        """Vérifie que le VideoProcessor est correctement initialisé"""
        try:
            if not self.processor:
                logger.error(f"[{self.name}] ❌ VideoProcessor non initialisé")
                return False
                
            video_dir = Path(self.video_dir)
            if not video_dir.exists():
                logger.error(f"[{self.name}] ❌ Dossier vidéo introuvable: {video_dir}")
                return False
                
            if not os.access(video_dir, os.R_OK | os.W_OK):
                logger.error(f"[{self.name}] ❌ Permissions insuffisantes sur {video_dir}")
                return False
                
            processed_dir = video_dir / "processed"
            try:
                processed_dir.mkdir(exist_ok=True)
            except Exception as e:
                logger.error(f"[{self.name}] ❌ Impossible de créer {processed_dir}: {e}")
                return False
                
            logger.info(f"[{self.name}] ✅ VideoProcessor correctement initialisé")
            return True
            
        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur vérification VideoProcessor: {e}")
            return False

    def _handle_process_died(self, return_code):
        """Gère la mort du processus FFmpeg"""
        logger.error(f"[{self.name}] ❌ Processus FFmpeg terminé avec code: {return_code}")
        self.error_handler.add_error("PROCESS_DIED")
        self._restart_stream()
        
    def _handle_position_update(self, position):
        """Reçoit les mises à jour de position du ProcessManager"""
        self.position_manager.update_from_progress(self.logger.get_progress_file())
        
    def _handle_segment_created(self, segment_path, size):
        """Notifié quand un nouveau segment est créé"""
        self.last_segment_time = time.time()
        if self.logger:
            self.logger.log_segment(segment_path, size)
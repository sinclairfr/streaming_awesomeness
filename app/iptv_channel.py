# iptv_channel.py
import os
import time
import threading
from pathlib import Path
from typing import Optional, List
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
import subprocess
import re
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
        use_gpu: bool = False
    ):
        self.name = name
        self.channel_name = name
        self.video_dir = video_dir
        self.use_gpu = use_gpu
        self.hls_cleaner = hls_cleaner
        self.error_handler = StreamErrorHandler(self.name)
        self.lock = threading.Lock()
        self.ready_for_streaming = False  # Indique si la chaîne est prête

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
        
        # État du scan initial
        self.initial_scan_complete = False
        self.scan_lock = threading.Lock()
        
        # Créer le _playlist.txt immédiatement pour être prêt quand un spectateur arrive
        logger.info(f"[{self.name}] 🔄 Préparation initiale de la chaîne")
        self._scan_videos()
        self._create_concat_file()
        self._verify_playlist()
        
        # Calcul de la durée totale
        total_duration = self._calculate_total_duration()
        self.position_manager.set_total_duration(total_duration)
        self.process_manager.set_total_duration(total_duration)
        
        self.initial_scan_complete = True
        self.ready_for_streaming = len(self.processed_videos) > 0
        
        logger.info(f"[{self.name}] ✅ Initialisation complète. Chaîne prête: {self.ready_for_streaming}")
        
        # Maintenant que tout est initialisé, on lance le scan asynchrone pour mettre à jour en arrière-plan
        threading.Thread(target=self._scan_videos_async, daemon=True).start() 
    def _verify_file_ready(self, file_path: Path) -> bool:
        """
        Vérifie qu'un fichier MP4 est complet et utilisable
        Version améliorée avec détection des atomes MOOV
        
        Args:
            file_path: Chemin du fichier MP4 à vérifier
            
        Returns:
            bool: True si le fichier est valide, False sinon
        """
        filename = file_path.name  # Extraction du nom du fichier
        try:
            # Vérification que le fichier existe et est de taille non nulle
            if not file_path.exists():
                logger.warning(f"[{self.channel_name}] ⚠️ Fichier introuvable: {filename}")
                return False
                
            file_size = file_path.stat().st_size
            if file_size == 0:
                logger.warning(f"[{self.channel_name}] ⚠️ Fichier vide: {filename}")
                self._move_to_ignored(file_path, "fichier vide")
                return False
                
            # Vérifications supplémentaires pour les fichiers MP4
            if file_path.suffix.lower() == '.mp4':
                # 1. Première tentative avec ffprobe pour les métadonnées
                cmd1 = [
                    "ffprobe",
                    "-v", "error",
                    "-select_streams", "v:0",
                    "-show_entries", "format=duration",
                    "-of", "default=noprint_wrappers=1:nokey=1",
                    str(file_path)
                ]
                
                result1 = subprocess.run(cmd1, capture_output=True, text=True, timeout=10)
                
                # Vérification spécifique pour l'erreur "moov atom not found"
                if result1.returncode != 0:
                    if "moov atom not found" in result1.stderr:
                        logger.warning(f"[{self.channel_name}] ⚠️ Atome MOOV manquant dans {filename}, fichier incomplet")
                        self._move_to_ignored(file_path, f"fichier MP4 incomplet: atome MOOV manquant")
                        return False
                        
                    # Autres erreurs ffprobe
                    logger.warning(f"[{self.channel_name}] ⚠️ Erreur ffprobe pour {filename}: {result1.stderr}")
                    
                    # Tentative supplémentaire avec un autre type de vérification
                    cmd2 = [
                        "ffmpeg", 
                        "-v", "error",
                        "-i", str(file_path),
                        "-f", "null",
                        "-t", "5",  # On teste juste les 5 premières secondes
                        "-"
                    ]
                    
                    result2 = subprocess.run(cmd2, capture_output=True, text=True, timeout=15)
                    
                    if result2.returncode != 0:
                        logger.warning(f"[{self.channel_name}] ⚠️ Validation secondaire échouée pour {filename}: {result2.stderr}")
                        self._move_to_ignored(file_path, f"erreur ffprobe: {result1.stderr}")
                        return False
                        
                    # Si on arrive ici, le fichier est lisible malgré l'erreur ffprobe
                    logger.info(f"[{self.channel_name}] ✅ {filename} lisible malgré l'erreur ffprobe")
                    return True
                    
                # Vérification que la durée est valide
                try:
                    duration = float(result1.stdout.strip())
                    if duration <= 0:
                        logger.warning(f"[{self.channel_name}] ⚠️ Durée invalide pour {filename}: {duration}s")
                        self._move_to_ignored(file_path, f"durée invalide: {duration}s")
                        return False
                        
                    logger.info(f"[{self.channel_name}] ✅ Fichier MP4 valide: {filename}, durée: {duration:.2f}s")
                    return True
                except ValueError:
                    logger.warning(f"[{self.channel_name}] ⚠️ Durée non numérique pour {filename}: {result1.stdout}")
                    self._move_to_ignored(file_path, f"durée non numérique: {result1.stdout}")
                    return False
                    
            # Pour les fichiers non-MP4, vérification standard
            cmd = [
                "ffprobe",
                "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                str(file_path)
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            
            if result.returncode != 0:
                logger.warning(f"[{self.channel_name}] ⚠️ Erreur ffprobe pour {filename}: {result.stderr}")
                self._move_to_ignored(file_path, f"erreur ffprobe: {result.stderr}")
                return False
                
            # Vérification que la durée est valide
            try:
                duration = float(result.stdout.strip())
                if duration <= 0:
                    logger.warning(f"[{self.channel_name}] ⚠️ Durée invalide pour {filename}: {duration}s")
                    self._move_to_ignored(file_path, f"durée invalide: {duration}s")
                    return False
                    
                logger.info(f"[{self.channel_name}] ✅ Fichier valide: {filename}, durée: {duration:.2f}s")
                return True
            except ValueError:
                logger.warning(f"[{self.channel_name}] ⚠️ Durée non numérique pour {filename}: {result.stdout}")
                self._move_to_ignored(file_path, f"durée non numérique: {result.stdout}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.warning(f"[{self.channel_name}] ⚠️ Timeout ffprobe pour {filename}")
            self._move_to_ignored(file_path, "timeout ffprobe")
            return False
        except Exception as e:
            logger.warning(f"[{self.channel_name}] ⚠️ Erreur vérification {filename}: {e}")
            self._move_to_ignored(file_path, f"erreur: {str(e)}")
            return False
    
    def _move_to_ignored(self, file_path: Path, reason: str):
        """
        Déplace un fichier invalide vers le dossier 'ignored'
        
        Args:
            file_path: Chemin du fichier à déplacer
            reason: Raison de l'invalidité du fichier
        """
        try:
            # S'assurer que le dossier ignored existe
            ignored_dir = Path(self.video_dir) / "ignored"
            ignored_dir.mkdir(parents=True, exist_ok=True)
                
            # Créer le chemin de destination
            dest_path = ignored_dir / file_path.name
            
            # Si le fichier de destination existe déjà, ajouter un suffixe
            if dest_path.exists():
                base_name = dest_path.stem
                suffix = dest_path.suffix
                timestamp = time.strftime("%Y%m%d_%H%M%S")
                dest_path = ignored_dir / f"{base_name}_{timestamp}{suffix}"
                
            # Déplacer le fichier
            if file_path.exists():
                shutil.move(str(file_path), str(dest_path))
                
                # Créer un fichier de log à côté avec la raison
                log_path = ignored_dir / f"{dest_path.stem}_reason.txt"
                with open(log_path, "w") as f:
                    f.write(f"Fichier ignoré le {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write(f"Raison: {reason}\n")
                    
                logger.info(f"[{self.channel_name}] 🚫 Fichier {file_path.name} déplacé vers ignored: {reason}")
                
        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur déplacement fichier vers ignored: {e}")

    def _scan_videos(self) -> bool:
        """Scanne les fichiers vidéos et met à jour processed_videos"""
        try:
            source_dir = Path(self.video_dir)
            ready_to_stream_dir = source_dir / "ready_to_stream"
            
            # Création du dossier s'il n'existe pas
            ready_to_stream_dir.mkdir(exist_ok=True)
            
            self._verify_processor()
            
            # On réinitialise la liste des vidéos traitées
            self.processed_videos = []
            
            # On scanne d'abord les vidéos dans ready_to_stream
            mp4_files = list(ready_to_stream_dir.glob("*.mp4"))
            
            if not mp4_files:
                logger.warning(f"[{self.name}] ⚠️ Aucun fichier MP4 dans {ready_to_stream_dir}")
                
                # On vérifie s'il y a des fichiers à traiter
                video_extensions = (".mp4", ".avi", ".mkv", ".mov", "m4v")
                source_files = []
                for ext in video_extensions:
                    source_files.extend(source_dir.glob(f"*{ext}"))

                if not source_files:
                    logger.warning(f"[{self.name}] ⚠️ Aucun fichier vidéo dans {self.video_dir}")
                    self.ready_for_streaming = False
                    return False
                    
                logger.info(f"[{self.name}] 🔄 {len(source_files)} fichiers sources à traiter")
                self.ready_for_streaming = False
                return False
                
            # Vérification que les fichiers sont valides
            valid_files = []
            for video_file in mp4_files:
                if self._verify_file_ready(video_file):
                    valid_files.append(video_file)
                else:
                    logger.warning(f"[{self.name}] ⚠️ Fichier {video_file.name} ignoré car non valide")
            
            if valid_files:
                self.processed_videos.extend(valid_files)
                logger.info(f"[{self.name}] ✅ {len(valid_files)} vidéos valides trouvées dans ready_to_stream")
                
                # La chaîne est prête si on a des vidéos valides
                self.ready_for_streaming = True
                return True
            else:
                logger.warning(f"[{self.name}] ⚠️ Aucun fichier MP4 valide trouvé dans ready_to_stream")
                self.ready_for_streaming = False
                return False

        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur scan des vidéos: {str(e)}")
            return False

    def _scan_videos_async(self):
        """Scanne les vidéos en tâche de fond pour les mises à jour ultérieures"""
        try:
            time.sleep(30)  # Attente initiale pour laisser le système se stabiliser
            
            with self.scan_lock:
                logger.info(f"[{self.name}] 🔍 Scan de mise à jour des vidéos en cours...")
                self._scan_videos()
                
                # Mise à jour de la durée totale
                total_duration = self._calculate_total_duration()
                self.position_manager.set_total_duration(total_duration)
                self.process_manager.set_total_duration(total_duration)
                
                # Mise à jour de la playlist
                self._create_concat_file()
                
                logger.info(f"[{self.name}] ✅ Scan de mise à jour terminé. Chaîne prête: {self.ready_for_streaming}")
        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur scan de mise à jour: {e}")

    def _calculate_total_duration(self) -> float:
        """Calcule la durée totale en utilisant le PositionManager"""
        try:
            # Vérification que la liste processed_videos n'est pas vide
            if not self.processed_videos:
                logger.warning(f"[{self.name}] ⚠️ Aucune vidéo à analyser pour le calcul de durée")
                return 120.0  # Valeur par défaut
                
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
    
    def _get_accurate_duration(self, video_path: Path) -> float:
        """
        Obtient la durée précise d'un fichier vidéo avec plusieurs tentatives
        et cache des résultats pour performance
        """
        # Utilisation d'un cache interne
        if not hasattr(self, '_duration_cache'):
            self._duration_cache = {}
            
        video_str = str(video_path)
        if video_str in self._duration_cache:
            return self._duration_cache[video_str]
        
        # Vérification que le fichier est stable avant de lire la durée
        if hasattr(self, 'processor') and hasattr(self.processor, '_wait_for_file_stability'):
            is_stable = self.processor._wait_for_file_stability(video_path, timeout=15)
            if not is_stable:
                logger.warning(f"[{self.name}] ⚠️ Fichier instable pour mesure de durée: {video_path.name}")
                time.sleep(2)  # Pause supplémentaire
                
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # Paramètres plus robustes pour ffprobe
                cmd = [
                    "ffprobe",
                    "-v", "error",
                    "-analyzeduration", "20M",  # Réduit le temps d'analyse pour un équilibre entre performance et précision
                    "-probesize", "50M",        # Réduit la taille analysée pour un équilibre similaire
                    "-i", str(video_path),      # Explicite le fichier d'entrée
                    "-show_entries", "format=duration",
                    "-of", "default=noprint_wrappers=1:nokey=1"
                ]
            
                logger.warning(f"[{self.name}] ⚠️ Tentative {attempt+1}/{max_retries} pour {video_path}")
                
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
                
                if result.returncode == 0 and result.stdout.strip():
                    try:
                        duration = float(result.stdout.strip())
                        if duration > 0:
                            # Arrondir à 3 décimales pour éviter erreurs d'imprécision
                            duration = round(duration, 3)
                            self._duration_cache[video_str] = duration
                            return duration
                    except ValueError:
                        logger.error(f"[{self.name}] ❌ Valeur non numérique: {result.stdout}")
                else:
                    # Log plus verbeux de l'erreur
                    if result.stderr:
                        logger.error(f"[{self.name}] ❌ Erreur ffprobe: {result.stderr}")
                    else:
                        logger.error(f"[{self.name}] ❌ Échec sans message d'erreur, code: {result.returncode}")
                
                # Alternative avec approche différente si échoue
                if attempt == max_retries - 2:  # Avant-dernière tentative
                    try:
                        alternate_cmd = [
                            "ffmpeg", "-i", str(video_path), 
                            "-f", "null", "-"
                        ]
                        
                        # Exécute ffmpeg pour lire le fichier entier
                        alt_result = subprocess.run(
                            alternate_cmd,
                            capture_output=True,
                            text=True,
                            timeout=20
                        )
                        
                        # Cherche la durée dans la sortie d'erreur
                        if alt_result.stderr:
                            duration_match = re.search(r'Duration: (\d{2}):(\d{2}):(\d{2})\.(\d{2})', alt_result.stderr)
                            if duration_match:
                                h, m, s, ms = map(int, duration_match.groups())
                                duration = h * 3600 + m * 60 + s + ms / 100
                                self._duration_cache[video_str] = duration
                                return duration
                    except Exception as e:
                        logger.error(f"[{self.name}] ❌ Erreur méthode alternative: {e}")
                
                # Pause entre les tentatives
                time.sleep(2)
                
            except subprocess.TimeoutExpired:
                logger.error(f"[{self.name}] ❌ Timeout ffprobe pour {video_path.name}")
                time.sleep(2)
            except Exception as e:
                logger.error(f"[{self.name}] ❌ Erreur ffprobe durée {video_path.name}: {e}")
                time.sleep(2)
        
        # Valeur par défaut si impossible de déterminer la durée
        logger.error(f"[{self.name}] ❌ Impossible d'obtenir la durée pour {video_path.name}, utilisation valeur par défaut")
        default_duration = 3600.0  # 1 heure par défaut
        self._duration_cache[video_str] = default_duration
        return default_duration
    
    def _create_concat_file(self) -> Optional[Path]:
        """Crée le fichier de concaténation avec les bons chemins"""
        try:
            logger.info(f"[{self.name}] 🛠️ Création de _playlist.txt")

            # Utiliser ready_to_stream au lieu de processed
            ready_to_stream_dir = Path(self.video_dir) / "ready_to_stream"
            if not ready_to_stream_dir.exists():
                logger.error(f"[{self.name}] ❌ Dossier ready_to_stream introuvable")
                return None
                
            concat_file = Path(self.video_dir) / "_playlist.txt"

            # Ne prendre que les fichiers .mp4 (pas de .mkv)
            ready_files = sorted(ready_to_stream_dir.glob("*.mp4"))
            if not ready_files:
                logger.error(f"[{self.name}] ❌ Aucune vidéo dans {ready_to_stream_dir}")
                return None

            logger.info(f"[{self.name}] 📝 Écriture de _playlist.txt")

            with open(concat_file, "w", encoding="utf-8") as f:
                for video in ready_files:
                    escaped_path = str(video.absolute()).replace("'", "'\\''")
                    f.write(f"file '{escaped_path}'\n")
                    logger.info(f"[{self.name}] ✅ Ajout de {video.name}")

            logger.info(f"[{self.name}] 🎥 Playlist créée avec {len(ready_files)} fichiers")
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
            # Si le scan initial n'est pas terminé ou si la chaîne n'est pas prête, on attend
            if not self.initial_scan_complete:
                logger.info(f"[{self.name}] ⏳ Attente de la fin du scan initial...")
                # On attend au maximum 10 secondes
                for _ in range(10):
                    time.sleep(1)
                    if self.initial_scan_complete:
                        break
                        
            if not self.ready_for_streaming:
                logger.warning(f"[{self.name}] ⚠️ Chaîne non prête pour le streaming (pas de vidéos)")
                return False

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
    
    def start_stream(self) -> bool:
        """Démarre le stream avec FFmpeg en utilisant les nouvelles classes"""
        try:
            # Vérification rapide que la chaîne est prête
            if not self.ready_for_streaming:
                logger.warning(f"[{self.name}] ⚠️ Chaîne non prête pour le streaming (pas de vidéos)")
                return False

            logger.info(f"[{self.name}] 🚀 Démarrage du stream...")

            hls_dir = Path(f"/app/hls/{self.name}")
            logger.info(f"[{self.name}] Création du répertoire HLS: {hls_dir}")
            hls_dir.mkdir(parents=True, exist_ok=True)
            
            # Utilisation du fichier playlist déjà créé
            concat_file = Path(self.video_dir) / "_playlist.txt"
            if not concat_file.exists():
                # Recréation en cas d'absence
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
    
    def refresh_videos(self):
        """Force un nouveau scan des vidéos et notifie le manager"""
        def scan_and_notify():
            try:
                # Exécute le scan
                self._scan_videos_async()
                
                # S'assure que le statut est correctement reporté au manager
                # Attend un peu que le scan asynchrone progresse
                time.sleep(2)
                
                # Vérification directe si des vidéos ont été traitées
                ready_files = list((Path(self.video_dir) / "ready_to_stream").glob("*.mp4"))
                if ready_files:
                    self.ready_for_streaming = True
                    
                    # Trouve le manager parent pour mettre à jour le statut
                    import inspect
                    frame = inspect.currentframe()
                    while frame:
                        if 'self' in frame.f_locals and hasattr(frame.f_locals['self'], 'channel_ready_status'):
                            manager = frame.f_locals['self']
                            with manager.scan_lock:
                                manager.channel_ready_status[self.name] = True
                            logger.info(f"[{self.name}] ✅ Statut 'prêt' mis à jour dans le manager")
                            break
                        frame = frame.f_back
                    
                logger.info(f"[{self.name}] 🔄 Rafraîchissement terminé, prêt: {self.ready_for_streaming}")
            except Exception as e:
                logger.error(f"[{self.name}] ❌ Erreur dans scan_and_notify: {e}")
        
        # Lance le scan dans un thread séparé
        threading.Thread(target=scan_and_notify, daemon=True).start()
        return True  
     
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
                
            ready_to_stream_dir = video_dir / "ready_to_stream"
            try:
                ready_to_stream_dir.mkdir(exist_ok=True)
            except Exception as e:
                logger.error(f"[{self.name}] ❌ Impossible de créer {ready_to_stream_dir}: {e}")
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
            
    # Méthode à ajouter à la classe IPTVChannel pour améliorer la gestion des sauts de segments
    def report_segment_jump(self, prev_segment: int, curr_segment: int):
        """
        Gère les sauts détectés dans les segments HLS avec une meilleure logique
        
        Args:
            prev_segment: Le segment précédent
            curr_segment: Le segment actuel (avec un saut)
        """
        try:
            jump_size = curr_segment - prev_segment
            
            # On ne s'inquiète que des sauts importants et récurrents
            if jump_size <= 5:
                return
                
            logger.warning(f"[{self.name}] 🚨 Saut de segment détecté: {prev_segment} → {curr_segment} (delta: {jump_size})")
            
            # On stocke l'historique des sauts si pas déjà fait
            if not hasattr(self, 'jump_history'):
                self.jump_history = []
                
            # Ajout du saut à l'historique avec timestamp
            self.jump_history.append((time.time(), prev_segment, curr_segment, jump_size))
            
            # On ne garde que les 5 derniers sauts
            if len(self.jump_history) > 5:
                self.jump_history = self.jump_history[-5:]
                
            # On vérifie si on a des sauts fréquents et similaires (signe d'un problème systémique)
            recent_jumps = [j for j in self.jump_history if time.time() - j[0] < 300]  # Sauts des 5 dernières minutes
            
            if len(recent_jumps) >= 3:
                # Si on a au moins 3 sauts récents avec des tailles similaires, on considère que c'est un problème systémique
                similar_sizes = any(abs(j[3] - jump_size) < 10 for j in recent_jumps[:-1])  # Tailles de saut similaires
                
                if similar_sizes and self.error_handler and self.error_handler.add_error("segment_jump"):
                    logger.warning(f"[{self.name}] 🔄 Redémarrage après {len(recent_jumps)} sauts similaires récents")
                    
                    # On vérifie si on a encore des spectateurs actifs
                    watchers = getattr(self, 'watchers_count', 0)
                    if watchers > 0:
                        # Sauvegarde de la position avant le redémarrage
                        if hasattr(self, 'position_manager'):
                            self.position_manager.save_position()
                        return self._restart_stream()
                    else:
                        logger.info(f"[{self.name}] ℹ️ Pas de redémarrage: aucun watcher actif")
        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur gestion saut de segment: {e}")
            return False
    
    def refresh_videos(self):
        """Force un nouveau scan des vidéos"""
        threading.Thread(target=self._scan_videos_async, daemon=True).start()
        return True
# iptv_channel.py
import os
import time
import threading
import random
from pathlib import Path
from typing import Optional, List
import shutil
from video_processor import VideoProcessor
from hls_cleaner import HLSCleaner
from ffmpeg_logger import FFmpegLogger
from ffmpeg_command_builder import FFmpegCommandBuilder
from ffmpeg_process_manager import FFmpegProcessManager
from playback_position_manager import PlaybackPositionManager
import subprocess
from config import (
    logger,
    CRASH_THRESHOLD,
    HLS_DIR,
)
from video_processor import get_accurate_duration
import datetime
from error_handler import ErrorHandler
import psutil
import traceback


class IPTVChannel:
    """Gère une chaîne IPTV, son streaming et sa surveillance"""
    
    # Initialisation des variables de classe (statiques)
    # _playlist_creation_timestamps = {} # Removed - No longer needed for single file playback

    def __init__(
        self,
        name: str,
        video_dir: str,
        hls_cleaner: HLSCleaner,
        use_gpu: bool = False,
        stats_collector=None,
    ):
        """Initialise une chaîne IPTV"""
        self.name = name
        self.video_dir = video_dir
        self.hls_cleaner = hls_cleaner
        self.use_gpu = use_gpu

        # Configuration du logger
        self.logger = FFmpegLogger(name)

        # Stats collector optionnel
        self.stats_collector = stats_collector

        # Gestion des erreurs et des arrêts d'urgence
        self.error_handler = ErrorHandler(
            channel_name=self.name,
            max_restarts=5,
            restart_cooldown=60
        )
        
        # Chemin du fichier de log
        self.crash_log_path = Path(f"/app/logs/crashes_{self.name}.log")
        self.crash_log_path.parent.mkdir(exist_ok=True)

        self.lock = threading.Lock()
        self.ready_for_streaming = False
        self.total_duration = 0
        self.processed_videos = [] # List to hold video file Paths
        self.current_video_index = 0 # Index for the current video

        # Initialiser les composants dans le bon ordre
        self.position_manager = PlaybackPositionManager(name)
        self.command_builder = FFmpegCommandBuilder(name, use_gpu=use_gpu)
        self.process_manager = FFmpegProcessManager(
            self.name, self.logger
        )

        # Ajouter cette chaîne au registre global
        if hasattr(FFmpegProcessManager, "all_channels"):
            FFmpegProcessManager.all_channels[name] = self

        # Configuration des callbacks
        self.process_manager.on_process_died = self._handle_process_died
        self.process_manager.on_position_update = self._handle_position_update
        self.process_manager.on_segment_created = self._handle_segment_created

        # Autres composants
        self.processor = VideoProcessor(self.video_dir)

        # Variables de surveillance
        self.watchers_count = 0
        self.last_watcher_time = time.time()
        self.last_segment_time = time.time()

        # Flag pour la navigation manuelle (éviter l'auto-avancement pendant previous/next)
        self.manual_navigation = False

        # État du scan initial
        self.initial_scan_complete = False
        self.scan_lock = threading.Lock()

        # Initial scan to populate processed_videos
        logger.info(f"[{self.name}] 🔄 Préparation initiale de la chaîne")
        if not self._scan_videos(): # Scan and check if successful
            logger.error(f"[{self.name}] ❌ Scan initial échoué, impossible d'initialiser")
            return # Prevent further initialization if scan fails
        
        # No longer need concat file creation here
        # self._create_concat_file()

        # No longer need total duration calculation here, maybe later per-file
        # total_duration = self._calculate_total_duration()
        # self.position_manager.set_total_duration(total_duration)
        # self.process_manager.set_total_duration(total_duration)

        self.initial_scan_complete = True
        self.ready_for_streaming = len(self.processed_videos) > 0

        logger.info(
            f"[{self.name}] ✅ Initialisation complète. Chaîne prête: {self.ready_for_streaming} avec {len(self.processed_videos)} vidéos."
        )

    def _safe_start_stream(self):
        """Wrapper sécurisé pour start_stream() appelé depuis un Timer thread"""
        try:
            logger.debug(f"[{self.name}] 🔄 Démarrage du stream depuis Timer thread...")
            result = self.start_stream()
            if not result:
                logger.error(f"[{self.name}] ❌ Échec du démarrage du stream depuis Timer thread")
        except Exception as e:
            logger.error(f"[{self.name}] ❌ Exception critique dans Timer thread: {e}")
            logger.error(traceback.format_exc())
            # Tenter un redémarrage d'urgence après un délai
            logger.warning(f"[{self.name}] 🔄 Tentative de récupération automatique dans 10 secondes...")
            threading.Timer(10.0, self._safe_start_stream).start()

    def _handle_position_update(self, position):
        """Reçoit les mises à jour de position du ProcessManager"""
        try:
            # Détecter les sauts de position
            if hasattr(self, "last_logged_position"):
                if abs(position - self.last_logged_position) > 30:
                    logger.info(f"[{self.name}] 📊 Saut détecté: {self.last_logged_position:.2f}s → {position:.2f}s")
                    
                    # Vérifier si on a des erreurs de DTS
                    if position < self.last_logged_position:
                        logger.warning(f"[{self.name}] ⚠️ DTS non-monotone détecté")
                        self.last_dts_error_time = time.time()
                        
                        # Si on a trop d'erreurs DTS, on force un redémarrage
                        if hasattr(self, "dts_error_count"):
                            self.dts_error_count += 1
                            if self.dts_error_count >= 3:
                                logger.error(f"[{self.name}] ❌ Trop d'erreurs DTS, redémarrage forcé")
                                self.process_manager.restart_process()
                                self.dts_error_count = 0
                        else:
                            self.dts_error_count = 1
            
            # Mettre à jour la position
            self.last_logged_position = position
            
            # Vérifier si on a calculé la durée du fichier actuel
            if not hasattr(self, "current_file_duration") and position > 0:
                # Estimer la durée en fonction de la position (approximation)
                duration = get_accurate_duration(self.current_video_file) if hasattr(self, "current_video_file") else 0
                if duration > 0:
                    self.current_file_duration = duration
                    logger.info(f"[{self.name}] ℹ️ Durée du fichier actuel: {duration:.2f}s")
            
        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur dans _handle_position_update: {e}")

    def _scan_videos_async(self):
        """Scanne les vidéos en tâche de fond pour les mises à jour ultérieures"""
        # Éviter les exécutions multiples concurrentes
        if hasattr(self, "_scan_in_progress") and self._scan_in_progress:
            logger.debug(f"[{self.name}] ⏭️ Scan déjà en cours, ignoré")
            return

        # Vérifier si un scan a été fait récemment
        current_time = time.time()
        if hasattr(self, "_last_scan_time") and (current_time - self._last_scan_time) < 60:
            logger.debug(f"[{self.name}] ⏭️ Dernier scan trop récent, ignoré")
            return

        self._scan_in_progress = True
        try:
            with self.scan_lock:
                logger.info(f"[{self.name}] 🔍 Scan de mise à jour des vidéos en cours...")
                old_videos = set(self.processed_videos)
                self._scan_videos()

                # Ne continuer que si des changements ont été détectés
                new_videos = set(self.processed_videos)
                if old_videos == new_videos:
                    logger.debug(f"[{self.name}] ℹ️ Aucun changement détecté dans les vidéos")
                    return

                # Mise à jour de la durée totale - Removed as total duration isn't used for single file playback
                # total_duration = self._calculate_total_duration()
                # self.position_manager.set_total_duration(total_duration) # Removed
                # self.process_manager.set_total_duration(total_duration) # Removed

                # Mise à jour de la playlist - Removed as concat file isn't used
                # self._create_concat_file()

                logger.info(f"[{self.name}] ✅ Scan de mise à jour terminé. Chaîne prête: {self.ready_for_streaming}")
                self._last_scan_time = current_time

        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur scan de mise à jour: {e}")
        finally:
            self._scan_in_progress = False

    # MÉTHODE SUPPRIMÉE: _segment_monitor_loop()
    # Cette boucle est REDONDANTE avec FFmpegProcessManager._monitor_process()
    # qui appelle déjà check_stream_health() toutes les 60 secondes.
    # Avoir deux boucles qui appellent la même vérification est inefficace et peut
    # causer des race conditions.

    def _handle_segment_created(self, segment_path, size):
        """Gère la création d'un nouveau segment HLS"""
        if self.logger:
            self.logger.log_segment(segment_path, size)

        # MAJ des stats de segments
        if hasattr(self, "stats_collector") and self.stats_collector:
            # Extraction de l'ID du segment depuis le nom
            segment_id = Path(segment_path).stem.split("_")[-1]
            self.stats_collector.update_segment_stats(self.name, segment_id, size)
        
        # Mise à jour du timestamp du dernier segment
        self.last_segment_time = time.time()
        logger.debug(f"[{self.name}] ⏱️ Segment créé: {Path(segment_path).name}")

    def _handle_process_died(self, exit_code, stderr=None):
        """
        Gère la mort du processus FFmpeg et décide des actions à prendre.

        Cette méthode a été simplifiée en déléguant l'analyse d'erreurs à ErrorHandler.
        """
        try:
            # Log initial
            logger.info(f"[{self.name}] ℹ️ Processus FFmpeg terminé avec code: {exit_code}")
            if stderr and len(stderr) > 0:
                logger.debug(f"[{self.name}] FFmpeg stderr: {stderr[:200]}...")

            # Analyser l'erreur avec ErrorHandler (méthode statique)
            from error_handler import ErrorHandler
            error_type, diagnosis = ErrorHandler.analyze_ffmpeg_error(exit_code, stderr)

            # --- Handle Successful Completion (Advance to Next Video) ---
            if error_type == "success":
                # Vérifier si on est en navigation manuelle
                if self.manual_navigation:
                    logger.info(f"[{self.name}] 🔄 Navigation manuelle en cours, pas d'auto-avancement")
                    self.manual_navigation = False
                    return

                logger.info(f"[{self.name}] ✅ Fichier vidéo terminé avec succès.")

                # NOUVEAU: Marquer qu'on est en transition pour désactiver les checks de santé
                self.process_manager.transitioning = True

                next_video_index = 0 # Default index
                num_videos = 0

                with self.lock:
                    if not self.processed_videos: # Should not happen if started, but check
                        logger.warning(f"[{self.name}] ⚠️ Liste de vidéos vide après fin de lecture.")
                        self.process_manager.transitioning = False
                        return # Cannot proceed

                    # Check if series.txt exists for sequential playback
                    channel_root_dir = Path(self.video_dir)
                    series_file = channel_root_dir / "series.txt"
                    use_sequential_order = series_file.exists()

                    num_videos = len(self.processed_videos)
                    old_index = self.current_video_index

                    if num_videos > 1:
                        if use_sequential_order:
                            # Sequential: advance to next video
                            next_video_index = (old_index + 1) % num_videos
                            logger.info(f"[{self.name}] ➡️ Passage à la vidéo suivante (mode série): Index {next_video_index}")
                        else:
                            # Random: pick a new random index, different from the old one
                            next_video_index = random.randrange(num_videos)
                            while next_video_index == old_index:
                                logger.debug(f"[{self.name}] 🔀 Vidéo suivante identique ({next_video_index}), re-tirage...")
                                next_video_index = random.randrange(num_videos)
                            logger.info(f"[{self.name}] 🔀 Sélection aléatoire de la prochaine vidéo: Index {next_video_index}")
                    elif num_videos == 1:
                        # Only one video, index must be 0
                        next_video_index = 0
                        logger.info(f"[{self.name}] ℹ️ Une seule vidéo disponible, lecture en boucle.")
                    else: # Should be caught above, but safety check
                         logger.error(f"[{self.name}] ❌ Incohérence: 0 vidéo mais blocage non déclenché plus tôt.")
                         self.process_manager.transitioning = False
                         return

                    self.current_video_index = next_video_index

                # Schedule the start of the next video with a delay to ensure proper cleanup
                # Use the updated index for logging
                logger.info(f"[{self.name}] ⏱️ Planification du démarrage du prochain fichier ({self.current_video_index + 1}/{num_videos}) dans 2 secondes...")
                threading.Timer(2.0, self._safe_start_stream).start()
                return # Don't proceed to error handling
            # --- End Successful Completion Handling ---

            # --- Error Handling (Simplifié et délégu\u00e9 à ErrorHandler) ---
            logger.warning(f"[{self.name}] ⚠️ Processus terminé anormalement: {error_type}")
            if diagnosis:
                logger.info(f"[{self.name}] 📋 Diagnostic: {diagnosis}")

            # Gérer les problèmes de santé avec ErrorHandler
            if error_type in ["health_check_failed", "health_check_detailed"]:
                current_time = time.time()
                elapsed = current_time - getattr(self.process_manager, "last_segment_time", current_time)

                # Déléguer à ErrorHandler
                should_restart = self.error_handler.handle_health_warning(
                    diagnosis=diagnosis,
                    elapsed_since_segment=elapsed
                )

                if should_restart:
                    time.sleep(random.uniform(0.5, 2.0))
                    self._restart_stream(diagnostic=diagnosis)
                # Sinon, on attend le prochain warning

            # Gérer les autres erreurs
            else:
                # Ajouter l'erreur à ErrorHandler
                should_restart = self.error_handler.add_error(error_type)

                if should_restart:
                    logger.warning(f"[{self.name}] ❗ Redémarrage nécessaire: {error_type}")
                    logger.info(f"[{self.name}] 📊 {self.error_handler.get_errors_summary()}")

                    time.sleep(random.uniform(0.5, 3.0))
                    self._restart_stream(diagnostic=error_type)

                elif self.error_handler.has_critical_errors():
                    logger.error(f"[{self.name}] ❌ Erreurs critiques détectées, arrêt du stream")
                    # Attendre un peu avant d'arrêter pour éviter les actions trop rapprochées
                    time.sleep(2)
                    self.stop_stream_if_needed()
                
        except Exception as e:
            logger.error(f"[{self.name}] Erreur lors de la gestion du processus: {e}")
            logger.error(traceback.format_exc())

    def _restart_stream(self, diagnostic=None, reset_to_first=False) -> bool:
        """Redémarre le stream en choisissant un NOUVEAU fichier VIDÉO (séquentiel si series.txt existe, sinon aléatoire)

        Args:
            diagnostic: Raison du redémarrage
            reset_to_first: Si True, repart du premier épisode (index 0) au lieu de passer au suivant
        """
        try:
            restart_reason = diagnostic or "Raison inconnue"
            logger.info(f"[{self.name}] 🔄 Tentative de redémarrage du stream - Raison: {restart_reason}")

            # Arrêter proprement les processus FFmpeg
            self.process_manager.stop_process()

            # Nettoyer le dossier HLS
            self.hls_cleaner.cleanup_channel(self.name)

            # Attendre un peu avant de redémarrer
            time.sleep(random.uniform(1.5, 3.0))

            # Sélectionner un nouveau fichier (séquentiel ou aléatoire selon series.txt)
            with self.lock:
                if not self.processed_videos:
                    logger.warning(f"[{self.name}] ⚠️ Liste de vidéos vide, impossible de redémarrer.")
                    return False

                # Si la navigation manuelle est active, ne PAS changer l'index
                if self.manual_navigation:
                    logger.info(f"[{self.name}] 🎯 Navigation manuelle: index déjà défini à {self.current_video_index}")
                    # Le flag sera réinitialisé dans _handle_process_died ou start_stream
                else:
                    # Check if series.txt exists for sequential playback
                    channel_root_dir = Path(self.video_dir)
                    series_file = channel_root_dir / "series.txt"
                    use_sequential_order = series_file.exists()

                    num_videos = len(self.processed_videos)

                    # Si demandé, réinitialiser au premier épisode
                    if reset_to_first:
                        self.current_video_index = 0
                        logger.info(f"[{self.name}] ⏮️ Réinitialisation au premier épisode (index 0)")
                    elif num_videos > 1:
                        old_index = self.current_video_index

                        if use_sequential_order:
                            # Mode série: passer à la vidéo suivante dans l'ordre
                            next_video_index = (old_index + 1) % num_videos
                            logger.info(f"[{self.name}] ➡️ Passage à la vidéo suivante (mode série): Index {next_video_index}")
                        else:
                            # Mode aléatoire: sélectionner une nouvelle vidéo aléatoire
                            next_video_index = random.randrange(num_videos)
                            while next_video_index == old_index:
                                next_video_index = random.randrange(num_videos)
                            logger.info(f"[{self.name}] 🔀 Sélection d'un nouveau fichier aléatoire: Index {next_video_index}")

                        self.current_video_index = next_video_index
                    else:
                        self.current_video_index = 0

            # Redémarrer le stream
            success = self.start_stream()
            if success:
                logger.info(f"[{self.name}] ✅ Stream redémarré avec succès sur un nouveau fichier.")
            else:
                logger.error(f"[{self.name}] ❌ Échec du redémarrage sur un nouveau fichier.")

            return success
        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur majeure lors du redémarrage: {e}", exc_info=True)
            return False

    def stop_stream_if_needed(self):
        """Arrête le stream si nécessaire"""
        try:
            # Utiliser le process manager pour arrêter proprement les processus FFmpeg
            self.process_manager.stop_process()

            # Nettoyer le dossier HLS avec le HLSCleaner
            self.hls_cleaner.cleanup_channel(self.name)

            logger.info(f"[{self.name}] ✅ Stream arrêté avec succès")
            return True

        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur arrêt stream: {e}")
            return False

    def start_stream(self) -> bool:
        """Démarre le stream pour le fichier vidéo actuel de cette chaîne"""
        try:
            with self.lock: # Lock to prevent race conditions with index/list
                # Vérifier que la chaîne est prête et a des vidéos
                if not self.ready_for_streaming or not self.processed_videos:
                    logger.error(f"[{self.name}] ❌ La chaîne n'est pas prête ou n'a pas de vidéos.")
                    return False

                # Vérifier la validité de l'index
                if not (0 <= self.current_video_index < len(self.processed_videos)):
                    logger.warning(f"[{self.name}] ⚠️ Index vidéo invalide ({self.current_video_index}), réinitialisation à 0.")
                    self.current_video_index = 0
                    if not self.processed_videos: # Double check after reset
                         logger.error(f"[{self.name}] ❌ Aucune vidéo à lire après réinitialisation de l'index.")
                         return False
                
                # Ensure permissions on all content files before starting
                self._ensure_permissions()
                         
                # Sélectionner le fichier vidéo actuel
                video_file = self.processed_videos[self.current_video_index]
                logger.info(f"[{self.name}] 🎥 Processing file ({self.current_video_index + 1}/{len(self.processed_videos)}): {video_file.name}")
                
                # Check if file still exists and is accessible
                if not video_file.exists() or not os.access(video_file, os.R_OK):
                    logger.error(f"[{self.name}] ❌ Fichier vidéo inaccessible: {video_file}. Tentative de rescan...")

                    # CORRECTION: Vérifier le succès du rescan et ajouter de la robustesse
                    try:
                        scan_success = self._scan_videos() # Try to refresh the list
                        if not scan_success:
                            logger.error(f"[{self.name}] ❌ Échec du rescan de la liste de vidéos.")
                            return False

                        logger.info(f"[{self.name}] ✅ Rescan réussi, {len(self.processed_videos)} vidéos trouvées")

                        # Check index validity again after rescan
                        if not (0 <= self.current_video_index < len(self.processed_videos)):
                            logger.warning(f"[{self.name}] ⚠️ Index {self.current_video_index} invalide après rescan, réinitialisation à 0")
                            self.current_video_index = 0

                        if not self.processed_videos:
                            logger.error(f"[{self.name}] ❌ Aucune vidéo valide trouvée après rescan.")
                            return False

                        # Try to get the file again
                        video_file = self.processed_videos[self.current_video_index]
                        logger.info(f"[{self.name}] 🔄 Nouveau fichier sélectionné après rescan: {video_file.name}")

                        if not video_file.exists() or not os.access(video_file, os.R_OK):
                            logger.error(f"[{self.name}] ❌ Fichier toujours inaccessible après rescan: {video_file}. Abandon.")
                            return False # Give up if still inaccessible

                        logger.info(f"[{self.name}] 🎥 Reprise avec fichier ({self.current_video_index + 1}/{len(self.processed_videos)}): {video_file.name}")

                    except Exception as rescan_error:
                        logger.error(f"[{self.name}] ❌ Exception lors du rescan: {rescan_error}")
                        logger.error(traceback.format_exc())
                        return False


                # Créer le dossier HLS
                hls_dir = Path(f"{HLS_DIR}/{self.name}")
                hls_dir.mkdir(parents=True, exist_ok=True)

                # Nettoyer les anciens segments AVANT de démarrer un nouveau fichier
                self.hls_cleaner.cleanup_channel(self.name)

                # Check if it's an MKV file
                has_mkv = ('.mkv' in video_file.name.lower())

                # Construire la commande FFmpeg pour le fichier unique
                command = self.command_builder.build_command(
                    input_file=str(video_file), # Pass the single video file path
                    output_dir=str(hls_dir),
                    progress_file=f"/app/logs/ffmpeg/{self.name}_progress.log",
                    has_mkv=has_mkv, # Pass the MKV check result for this specific file
                    # is_playlist=False # Default or remove parameter
                )

                if not command:
                    logger.error(f"[{self.name}] ❌ Impossible de construire la commande FFmpeg pour {video_file.name}")
                    
                    # Tentative avec le prochain fichier de la playlist
                    if len(self.processed_videos) > 1:
                        logger.warning(f"[{self.name}] 🔄 Tentative avec le prochain fichier dans la playlist...")
                        old_index = self.current_video_index
                        
                        # Select a new random index different from the current one
                        next_video_index = random.randrange(len(self.processed_videos))
                        while next_video_index == old_index:
                            next_video_index = random.randrange(len(self.processed_videos))
                            
                        logger.info(f"[{self.name}] 🔀 Passage au fichier suivant: {old_index} → {next_video_index}")
                        self.current_video_index = next_video_index

                        # Lancer un nouveau thread pour redémarrer le stream avec le nouvel index
                        threading.Timer(1.0, self._safe_start_stream).start()
                        return False
                    
                    return False

                logger.debug(f"[{self.name}] ⚙️ Commande FFmpeg: {' '.join(command)}")

                # Démarrer le processus FFmpeg
                success = self.process_manager.start_process(command, str(hls_dir))

                if success:
                    logger.info(f"[{self.name}] ✅ Processus FFmpeg démarré avec succès pour {video_file.name}")
                    self.error_handler.reset() # Reset errors on successful start
                    # NOUVEAU: Réactiver les checks de santé après un démarrage réussi
                    self.process_manager.transitioning = False
                    # Réinitialiser le flag de navigation manuelle après un démarrage réussi
                    self.manual_navigation = False
                else:
                    logger.error(f"[{self.name}] ❌ Échec du démarrage du processus FFmpeg pour {video_file.name}")
                    
                    # Tentative avec le prochain fichier de la playlist en cas d'échec du démarrage
                    if len(self.processed_videos) > 1:
                        logger.warning(f"[{self.name}] 🔄 Échec du démarrage, tentative avec le prochain fichier...")
                        old_index = self.current_video_index
                        
                        # Select a new random index different from the current one
                        next_video_index = random.randrange(len(self.processed_videos))
                        while next_video_index == old_index:
                            next_video_index = random.randrange(len(self.processed_videos))
                            
                        logger.info(f"[{self.name}] 🔀 Passage au fichier suivant après échec: {old_index} → {next_video_index}")
                        self.current_video_index = next_video_index

                        # Lancer un nouveau thread pour redémarrer le stream avec le nouvel index
                        threading.Timer(1.0, self._safe_start_stream).start()

                return success # Return success status outside the lock

        except Exception as e:
            logger.error(f"[{self.name}] Erreur lors de la démarrage du stream: {e}")
            logger.error(traceback.format_exc())
            return False

    def _scan_videos(self) -> bool:
        """Scanne le dossier ready_to_stream, valide les fichiers, les mélange et met à jour self.processed_videos. Renvoie True si réussi et au moins une vidéo trouvée, False sinon."""
        try:
            with self.lock: # Use lock as we modify shared state
                ready_to_stream_dir = Path(self.video_dir) / "ready_to_stream"
                if not ready_to_stream_dir.exists():
                    logger.error(f"[{self.name}] ❌ Dossier ready_to_stream introuvable: {ready_to_stream_dir}")
                    self.processed_videos = []
                    return False

                # Check if series.txt exists in the channel folder root
                channel_root_dir = Path(self.video_dir)
                series_file = channel_root_dir / "series.txt"
                use_alphabetic_order = series_file.exists()

                if use_alphabetic_order:
                    logger.info(f"[{self.name}] 📄 Fichier series.txt détecté - ordre alphabétique activé")

                # Scanner le dossier ready_to_stream (removed sorted())
                all_video_files = list(ready_to_stream_dir.glob("*.mp4"))

                # IMPORTANT: Filtrer les fichiers macOS (._*) et autres fichiers cachés
                video_files = [v for v in all_video_files if not v.name.startswith('._') and not v.name.startswith('.')]

                if len(all_video_files) != len(video_files):
                    filtered_count = len(all_video_files) - len(video_files)
                    logger.info(f"[{self.name}] 🗑️ {filtered_count} fichiers cachés/métadonnées ignorés")

                if not video_files:
                    logger.warning(f"[{self.name}] ⚠️ Aucun fichier MP4 valide dans {ready_to_stream_dir}")
                    self.processed_videos = []
                    return False

                logger.info(f"[{self.name}] 🔍 {len(video_files)} fichiers valides trouvés dans ready_to_stream")

                # Vérifier que tous les fichiers sont valides
                valid_files = []
                for video in video_files:
                    if video.exists() and os.access(video, os.R_OK):
                        # Optional: Add duration check if needed
                        # try:
                        #     duration = get_accurate_duration(video)
                        #     if duration and duration > 0:
                        #         valid_files.append(video)
                        #     else:
                        #         logger.warning(f"[{self.name}] ⚠️ Fichier ignoré: {video.name} (durée invalide)")
                        # except Exception as e:
                        #     logger.warning(f"[{self.name}] ⚠️ Fichier ignoré: {video.name} (erreur validation: {e})")
                        valid_files.append(video) # Simpler validation for now
                    else:
                        logger.warning(f"[{self.name}] ⚠️ Fichier ignoré: {video.name} (non accessible)")

                if not valid_files:
                    logger.error(f"[{self.name}] ❌ Aucun fichier MP4 valide trouvé après vérification")
                    self.processed_videos = []
                    return False

                # Sort alphabetically or shuffle based on series.txt presence
                if use_alphabetic_order:
                    valid_files.sort(key=lambda x: x.name.lower())
                    logger.info(f"[{self.name}] 🔤 Liste de vidéos triée alphabétiquement.")
                else:
                    random.shuffle(valid_files)
                    logger.info(f"[{self.name}] 🔀 Liste de vidéos mélangée.")

                logger.info(f"[{self.name}] ✅ {len(valid_files)} vidéos valides trouvées.")
                self.processed_videos = valid_files # Update the list
                # Reset index if it's now out of bounds OR if the list changed significantly
                # (safer to reset to 0 on any successful scan with videos)
                if not (0 <= self.current_video_index < len(self.processed_videos)):
                     logger.info(f"[{self.name}] 🔄 Réinitialisation de l'index vidéo à 0 après scan.")
                     self.current_video_index = 0
                elif len(self.processed_videos) > 0 and self.current_video_index >= len(self.processed_videos):
                    # Handle case where list shrank and index is now invalid
                    logger.info(f"[{self.name}] 🔄 Liste de vidéos réduite, réinitialisation de l'index vidéo à 0.")
                    self.current_video_index = 0


                return True # Success

        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur _scan_videos: {e}")
            logger.error(traceback.format_exc())
            self.processed_videos = []
            return False # Failure 

    def is_running(self) -> bool:
        """Vérifie si la chaîne est actuellement en streaming"""
        return self.process_manager.is_running()

    def is_ready_for_streaming(self) -> bool:
        """Vérifie si la chaîne est prête à être ajoutée à la playlist principale."""
        return self.ready_for_streaming and self.initial_scan_complete and len(self.processed_videos) > 0

    def _clean_processes(self):
        """Nettoie tous les processus FFmpeg associés à cette chaîne."""
        try:
            if self.process_manager:
                self.process_manager.stop_process()
            logger.info(f"[{self.name}] Processus FFmpeg nettoyés.")
        except Exception as e:
            logger.error(f"[{self.name}] Erreur lors du nettoyage des processus: {e}")

    def _ensure_permissions(self):
        """S'assure que tous les fichiers et dossiers de la chaîne ont les bonnes permissions."""
        # Cette fonction est conservée pour la structure mais les appels chmod sont désactivés.
        if not hasattr(self, 'video_extensions'):
            self.video_extensions = (".mp4", ".avi", ".mkv", ".mov", ".m4v")
        return True
            
    def refresh_videos(self):
        """
        Rafraîchit la liste des vidéos et redémarre le stream si nécessaire.
        Cette méthode est appelée quand des fichiers sont ajoutés/supprimés dans ready_to_stream.
        """
        logger.info(f"[{self.name}] 🔄 Rafraîchissement des vidéos suite à un changement")
        
        # Sauvegarder la liste actuelle pour détecter les changements
        old_videos = set()
        if hasattr(self, "processed_videos") and self.processed_videos:
            old_videos = set(str(v) for v in self.processed_videos)
        
        # Scanner les vidéos
        with self.lock:  # Verrouiller pour modifier l'état partagé
            scan_success = self._scan_videos()
            if not scan_success:
                logger.warning(f"[{self.name}] ⚠️ Échec du scan lors du rafraîchissement des vidéos")
                # Si échec du scan et qu'on était en cours de lecture, il faut arrêter
                if self.is_running() and not self.processed_videos:
                    logger.warning(f"[{self.name}] ⚠️ Plus de vidéos disponibles, arrêt du stream")
                    self.process_manager.stop_process()
                return False
        
        # Vérifier si la liste des vidéos a changé
        new_videos = set(str(v) for v in self.processed_videos)
        if old_videos == new_videos:
            logger.info(f"[{self.name}] ℹ️ Aucun changement détecté dans la liste des vidéos")
            return True
            
        # La liste a changé, vérifier si le stream est en cours
        if self.is_running():
            # Si on lit actuellement un fichier qui a été supprimé
            if self.current_video_index < len(self.processed_videos):
                current_file = str(self.processed_videos[self.current_video_index])
                # Vérifier si le fichier actuel existe toujours
                if current_file not in old_videos or not Path(current_file).exists():
                    logger.warning(f"[{self.name}] ⚠️ Le fichier actuel a été supprimé ou modifié, redémarrage nécessaire")
                    # Redémarrer le stream avec un nouveau fichier
                    return self._restart_stream(diagnostic="file_deleted")
            else:
                # Index invalide, nécessite un redémarrage
                logger.warning(f"[{self.name}] ⚠️ Index vidéo ({self.current_video_index}) invalide après changement de liste")
                return self._restart_stream(diagnostic="index_invalid")
        
        # Le stream n'est pas en cours, ou le fichier actuel existe toujours
        logger.info(f"[{self.name}] ✅ Liste de vidéos mise à jour: {len(self.processed_videos)} fichiers")
        return True


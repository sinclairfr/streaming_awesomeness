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
)
from video_processor import verify_file_ready, get_accurate_duration
import datetime
from error_handler import ErrorHandler
from time_tracker import TimeTracker
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
        
        # Gestion centralisée du temps de visionnage
        self.time_tracker = TimeTracker(stats_collector) if stats_collector else None

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

    def _segment_monitor_loop(self):
        """Boucle de surveillance des segments pour vérifier la santé du stream"""
        try:
            # Ajouter un délai initial pour laisser le temps à FFmpeg de démarrer
            if not hasattr(self, "segment_monitor_started"):
                self.segment_monitor_started = time.time()
                logger.info(f"[{self.name}] 🔍 Surveillance du stream démarrée")
                time.sleep(5)
                if not hasattr(self, "_startup_time"):
                    self._startup_time = time.time() - 5
            
            # Initialiser le timestamp du dernier health check
            if not hasattr(self, "last_health_check"):
                self.last_health_check = time.time()
            
            # La boucle s'exécute tant que le processus est en cours
            while self.process_manager.is_running():
                # Vérification périodique de la santé du stream
                if time.time() - self.last_health_check >= 30:
                    self.process_manager.check_stream_health()
                    self.last_health_check = time.time()
                
                # Pause entre les vérifications
                time.sleep(1)

        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur dans la boucle de surveillance: {e}")

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
        """Gère la mort du processus FFmpeg et décide des actions à prendre"""
        try:
            # Log the exit code and stderr for debugging
            logger.info(f"[{self.name}] ℹ️ Processus FFmpeg terminé avec code: {exit_code}")
            if stderr:
                logger.info(f"[{self.name}] ℹ️ FFmpeg stderr (premières lignes):\n{stderr[:500]}")

            # --- Handle Successful Completion (Advance to Random Next Video) ---
            if exit_code == 0:
                logger.info(f"[{self.name}] ✅ Fichier vidéo terminé avec succès.")
                next_video_index = 0 # Default index
                num_videos = 0

                with self.lock:
                    if not self.processed_videos: # Should not happen if started, but check
                        logger.warning(f"[{self.name}] ⚠️ Liste de vidéos vide après fin de lecture.")
                        return # Cannot proceed

                    num_videos = len(self.processed_videos)
                    old_index = self.current_video_index

                    if num_videos > 1:
                        # Pick a new random index, different from the old one
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
                         return

                    self.current_video_index = next_video_index

                # Schedule the start of the next video slightly delayed
                # Use the updated index for logging
                logger.info(f"[{self.name}] ⏱️ Planification du démarrage du prochain fichier ({self.current_video_index + 1}/{num_videos}) dans 1 seconde...")
                threading.Timer(1.0, self.start_stream).start()
                return # Don't proceed to error handling
            # --- End Successful Completion Handling ---
            
            # --- Existing Error Handling (for crashes/non-zero exit codes) ---
            logger.warning(f"[{self.name}] ⚠️ Processus FFmpeg terminé anormalement (code: {exit_code}).")
            
            # Analyser l'erreur pour détecter le type
            error_type = "unknown_error"
            diagnosis = ""
            
            if exit_code < 0:
                # Signal négatif, indique une terminaison par signal
                error_type = f"signal_{abs(exit_code)}"
                logger.warning(f"[{self.name}] Processus terminé par signal {abs(exit_code)}")
                
            elif stderr and "error" in stderr.lower():
                # Extraction du type d'erreur à partir du message
                if "no such file" in stderr.lower():
                    error_type = "missing_file"
                elif "invalid data" in stderr.lower():
                    error_type = "invalid_data"
                elif "dts" in stderr.lower():
                    error_type = "dts_error"
                elif "timeout" in stderr.lower():
                    error_type = "timeout"
                
                # Enregistrer le message d'erreur complet pour plus de contexte
                logger.warning(f"[{self.name}] 📝 Message d'erreur FFmpeg: {stderr[:200]}...")
            
            # Traiter les erreurs de diagnostic enrichi (format dictionnaire)
            if stderr and stderr.startswith("{'type':"):
                try:
                    # Tenter de récupérer le diagnostic structuré
                    import ast
                    error_info = ast.literal_eval(stderr)
                    
                    if isinstance(error_info, dict) and 'diagnosis' in error_info:
                        diagnosis = error_info['diagnosis']
                        error_type = "health_check_detailed"
                        
                        # Logs détaillés du diagnostic
                        elapsed = error_info.get('elapsed', 0)
                        cpu_usage = error_info.get('cpu_usage', 0)
                        segments_count = error_info.get('segments_count', 0)
                        avg_segment_size = error_info.get('average_segment_size', 0)
                        
                        logger.warning(f"[{self.name}] 📊 DIAGNOSTIC DÉTAILLÉ: {diagnosis}")
                        logger.warning(f"[{self.name}] ⏱️ {elapsed:.1f}s sans activité | CPU: {cpu_usage:.1f}% | Segments: {segments_count} | Taille moy: {avg_segment_size/1024:.1f}KB")
                except:
                    # Si échec du parsing, utiliser le message standard
                    error_type = "health_check_failed"
                    logger.warning(f"[{self.name}] Diagnostic non structuré: {stderr[:100]}...")
            
            # Approche spécifique pour les problèmes de santé
            if exit_code == -2:  # Code spécial pour problème de santé
                # On incrémente progressivement un compteur d'avertissements
                # au lieu de redémarrer immédiatement
                if not hasattr(self, "_health_warnings"):
                    self._health_warnings = 0
                    self._health_check_details = []
                
                # Collecter des détails sur le problème de santé
                current_time = time.time()
                elapsed_since_last_segment = current_time - getattr(self.process_manager, "last_segment_time", current_time)
                duration_threshold = getattr(self, "current_file_duration", 0) or 300  # Durée par défaut de 5 minutes
                
                health_details = {
                    "timestamp": current_time,
                    "elapsed_since_segment": elapsed_since_last_segment,
                    "file_duration": duration_threshold,
                    "diagnosis": diagnosis or "Problème de santé non spécifié",
                    "stderr": stderr[:100] if stderr else "Aucune erreur spécifique"
                }
                
                self._health_warnings += 1
                self._health_check_details.append(health_details)
                
                # Log détaillé du problème de santé
                logger.warning(f"[{self.name}] ⚠️ Problème de santé détecté - Avertissement {min(self._health_warnings, 3)}/3")
                
                if diagnosis:
                    logger.warning(f"[{self.name}] 🔍 Cause probable: {diagnosis}")
                else:
                    logger.warning(f"[{self.name}] ⏱️ Temps écoulé depuis dernier segment: {elapsed_since_last_segment:.1f}s")
                
                # On redémarre seulement après plusieurs avertissements
                if self._health_warnings >= 3:
                    logger.warning(f"[{self.name}] ❗ Redémarrage après {self._health_warnings} avertissements de santé")
                    details_log = "\n".join([f"  - {i+1}: {details['elapsed_since_segment']:.1f}s sans segment, diagnostic: {details['diagnosis']}" 
                                            for i, details in enumerate(self._health_check_details)])
                    logger.warning(f"[{self.name}] 📊 Historique des problèmes de santé:\n{details_log}")
                    
                    self._health_warnings = 0
                    self._health_check_details = []
                    self._restart_stream(diagnostic=diagnosis)
                else:
                    logger.info(f"[{self.name}] 🔍 Avertissement de santé {min(self._health_warnings, 3)}/3, surveillance continue")
            else:
                # Utiliser l'error handler seulement si ce n'est pas une fin normale (exit_code != 0)
                if self.error_handler.add_error(error_type):
                    logger.warning(f"[{self.name}] ❗ Redémarrage nécessaire après erreur: {error_type}")
                    # Log des erreurs accumulées
                    error_counts = [f"{err_type}: {data['count']}" for err_type, data in self.error_handler.errors.items() if data['count'] > 0]
                    logger.warning(f"[{self.name}] 📊 Erreurs accumulées: {', '.join(error_counts)}")
                    
                    # On ajoute un petit délai aléatoire pour éviter les redémarrages simultanés
                    # IMPORTANT: _restart_stream will call start_stream, which will use the *current* (failed) video index
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

    def _restart_stream(self, diagnostic=None) -> bool:
        """Redémarre le stream en choisissant un NOUVEAU fichier VIDÉO aléatoire en cas de problème"""
        try:
            restart_reason = diagnostic or "Raison inconnue"
            logger.info(f"[{self.name}] 🔄 Tentative de redémarrage du stream - Raison: {restart_reason}")

            # Vérifier l'utilisation CPU du système
            try:
                cpu_system = psutil.cpu_percent(interval=0.5)
                mem_percent = psutil.virtual_memory().percent
                logger.info(f"[{self.name}] 🖥️ Ressources système: CPU {cpu_system}%, Mémoire {mem_percent}%")
                
                # Avertir si ressources critiques
                if cpu_system > 85:
                    logger.warning(f"[{self.name}] ⚠️ Attention: CPU système élevé ({cpu_system}%) pendant le redémarrage")
                    time.sleep(5)  # Attendre un peu pour laisser le système se calmer
            except Exception as e:
                logger.debug(f"[{self.name}] Impossible de vérifier les ressources système: {e}")

            # Arrêter proprement les processus FFmpeg
            logger.info(f"[{self.name}] 🛑 Arrêt du processus FFmpeg en cours...")
            self.process_manager.stop_process()

            # Nettoyer le dossier HLS
            logger.info(f"[{self.name}] 🧹 Nettoyage des segments HLS...")
            hls_dir = Path(f"/app/hls/{self.name}")
            segments_before = len(list(hls_dir.glob("*.ts"))) if hls_dir.exists() else 0
            self.hls_cleaner.cleanup_channel(self.name)
            segments_after = len(list(hls_dir.glob("*.ts"))) if hls_dir.exists() else 0
            logger.info(f"[{self.name}] 🧹 Nettoyage des segments: {segments_before} → {segments_after}")

            # Attendre un peu avant de redémarrer
            time.sleep(2)
            
            # *** SELECT RANDOM NEXT VIDEO ON ERROR RESTART ***
            with self.lock:
                if not self.processed_videos:
                    logger.warning(f"[{self.name}] ⚠️ Liste de vidéos vide, impossible de choisir un fichier pour le redémarrage.")
                    return False # Can't restart if no videos

                num_videos = len(self.processed_videos)
                old_index = self.current_video_index # Index of the video that failed
                next_video_index = 0

                logger.info(f"[{self.name}] ⏭️ Sélection d'un nouveau fichier aléatoire après erreur sur l'index {old_index}")

                if num_videos > 1:
                    # Pick a new random index, different from the one that failed
                    next_video_index = random.randrange(num_videos)
                    while next_video_index == old_index:
                        logger.debug(f"[{self.name}] 🔀 Vidéo suivante aléatoire identique à celle échouée ({next_video_index}), re-tirage...")
                        next_video_index = random.randrange(num_videos)
                    logger.info(f"[{self.name}] 🔀 Sélection aléatoire pour le redémarrage: Index {next_video_index}")
                elif num_videos == 1:
                     next_video_index = 0
                     logger.info(f"[{self.name}] ℹ️ Une seule vidéo disponible, tentative de relance sur celle-ci.")
                else: # Should be caught above
                    logger.error(f"[{self.name}] ❌ Incohérence lors de la sélection aléatoire pour redémarrage.")
                    return False

                self.current_video_index = next_video_index


            # Redémarrer le stream (start_stream will now use the new random index)
            success = self.start_stream()
            if success:
                # Reset error handler counts maybe? Or only for the specific error type?
                # self.error_handler.reset() # Consider implications
                # Use the updated index for logging
                logger.info(f"[{self.name}] ✅ Stream redémarré avec succès sur un nouveau fichier ({self.current_video_index+1}/{len(self.processed_videos)}) - Ancien problème: {diagnostic}")
            else:
                logger.error(f"[{self.name}] ❌ Échec du redémarrage sur un nouveau fichier après problème: {diagnostic}")
                # Maybe try another random video? Or stop? For now, just return False.
                return False

            return success
        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur lors du redémarrage: {e}")
            import traceback
            logger.error(traceback.format_exc())
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
                         
                # Sélectionner le fichier vidéo actuel
                video_file = self.processed_videos[self.current_video_index]
                logger.info(f"[{self.name}] 🎥 Processing file ({self.current_video_index + 1}/{len(self.processed_videos)}): {video_file.name}")
                
                # Check if file still exists and is accessible
                if not video_file.exists() or not os.access(video_file, os.R_OK):
                    logger.error(f"[{self.name}] ❌ Fichier vidéo inaccessible: {video_file}. Tentative de rescan...")
                    self._scan_videos() # Try to refresh the list
                    # Check index validity again after rescan
                    if not (0 <= self.current_video_index < len(self.processed_videos)):
                         self.current_video_index = 0 # Reset if still bad
                    if not self.processed_videos: 
                        logger.error(f"[{self.name}] ❌ Aucune vidéo valide trouvée après rescan.")
                        return False
                    # Try to get the file again
                    video_file = self.processed_videos[self.current_video_index]
                    if not video_file.exists() or not os.access(video_file, os.R_OK):
                        logger.error(f"[{self.name}] ❌ Fichier toujours inaccessible après rescan: {video_file}. Abandon.")
                        return False # Give up if still inaccessible
                    logger.info(f"[{self.name}] 🎥 Reprise avec fichier ({self.current_video_index + 1}/{len(self.processed_videos)}): {video_file.name}")


                # Créer le dossier HLS
                hls_dir = Path(f"/app/hls/{self.name}")
                hls_dir.mkdir(parents=True, exist_ok=True)

                # Nettoyer les anciens segments AVANT de démarrer un nouveau fichier
                self.hls_cleaner.cleanup_channel(self.name)

                # *** Select the single video file for this run ***
                video_file = self.processed_videos[self.current_video_index]
                logger.info(f"[{self.name}] 🎥 Processing file ({self.current_video_index + 1}/{len(self.processed_videos)}): {video_file.name}")
                
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
                    return False

                logger.debug(f"[{self.name}] ⚙️ Commande FFmpeg: {' '.join(command)}")

                # Démarrer le processus FFmpeg
                success = self.process_manager.start_process(command, str(hls_dir))

                if success:
                    logger.info(f"[{self.name}] ✅ Processus FFmpeg démarré avec succès pour {video_file.name}")
                    self.error_handler.reset() # Reset errors on successful start
                else:
                    logger.error(f"[{self.name}] ❌ Échec du démarrage du processus FFmpeg pour {video_file.name}")

            return success # Return success status outside the lock

        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur start_stream: {e}")
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

                # Scanner le dossier ready_to_stream (removed sorted())
                video_files = list(ready_to_stream_dir.glob("*.mp4"))

                if not video_files:
                    logger.warning(f"[{self.name}] ⚠️ Aucun fichier MP4 dans {ready_to_stream_dir}")
                    self.processed_videos = []
                    return False
                    
                logger.info(f"[{self.name}] 🔍 {len(video_files)} fichiers trouvés dans ready_to_stream")

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

                # *** Shuffle the valid files ***
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

    def check_stream_health(self):
        """
        Vérifie la santé du stream avec une approche plus tolérante
        Logge les problèmes mais ne force pas le redémarrage immédiatement
        """
        try:
            # Initialiser le compteur d'avertissements si nécessaire
            if not hasattr(self, "_health_check_warnings"):
                self._health_check_warnings = 0
                self._last_health_check_time = time.time()
            
            # Réinitialiser périodiquement les avertissements (toutes les 30 minutes)
            if time.time() - getattr(self, "_last_health_check_time", 0) > 1800:
                self._health_check_warnings = 0
                self._last_health_check_time = time.time()
                logger.info(f"[{self.name}] Réinitialisation périodique des avertissements de santé")
            
            # Vérifier l'état de base: processus en cours?
            if not self.process_manager.is_running():
                logger.warning(f"[{self.name}] ⚠️ Processus FFmpeg inactif")
                self._health_check_warnings += 1
                
                # Seuil de tolérance plus élevé
                if self._health_check_warnings >= 3 and getattr(self, "watchers_count", 0) > 0:
                    logger.warning(f"[{self.name}] ⚠️ {self._health_check_warnings} avertissements accumulés, tentative de redémarrage")
                    self._health_check_warnings = 0
                    return self._restart_stream()
                else:
                    logger.info(f"[{self.name}] Avertissement {self._health_check_warnings}/3 (attente avant action)")
                    return False
            
            # Processus en cours, santé OK
            if self._health_check_warnings > 0:
                self._health_check_warnings -= 1  # Réduction progressive des avertissements
                logger.info(f"[{self.name}] ✅ Santé améliorée, avertissements: {self._health_check_warnings}")
            
            return True
            
        except Exception as e:
            logger.error(f"[{self.name}] Erreur lors du check de santé du stream: {e}")
            return False
            
    # Méthode supprimée car la logique de timeout des watchers est maintenant gérée par IPTVManager
    # en se basant sur les informations du ChannelStatusManager (alimenté par ClientMonitor).
    # def check_watchers_timeout(self):
    #    ...


    # Méthode supprimée car la logique de timeout des watchers est maintenant gérée par IPTVManager
    # en se basant sur les informations du ChannelStatusManager (alimenté par ClientMonitor).
    # def check_watchers_timeout(self):
    #    ...


    # Méthode supprimée car la logique de timeout des watchers est maintenant gérée par IPTVManager
    # en se basant sur les informations du ChannelStatusManager (alimenté par ClientMonitor).
    # def check_watchers_timeout(self):
    #    ...


    # Méthode supprimée car la logique de timeout des watchers est maintenant gérée par IPTVManager
    # en se basant sur les informations du ChannelStatusManager (alimenté par ClientMonitor).
    # def check_watchers_timeout(self):
    #    ...


    # Méthode supprimée car la logique de timeout des watchers est maintenant gérée par IPTVManager
    # en se basant sur les informations du ChannelStatusManager (alimenté par ClientMonitor).
    # def check_watchers_timeout(self):
    #    ...


    # Méthode supprimée car la logique de timeout des watchers est maintenant gérée par IPTVManager
    # en se basant sur les informations du ChannelStatusManager (alimenté par ClientMonitor).
    # def check_watchers_timeout(self):
    #    ...


    # Méthode supprimée car la logique de timeout des watchers est maintenant gérée par IPTVManager
    # en se basant sur les informations du ChannelStatusManager (alimenté par ClientMonitor).
    # def check_watchers_timeout(self):
    #    ...


    # Méthode supprimée car la logique de timeout des watchers est maintenant gérée par IPTVManager
    # en se basant sur les informations du ChannelStatusManager (alimenté par ClientMonitor).
    # def check_watchers_timeout(self):
    #    ...


    # Méthode supprimée car la logique de timeout des watchers est maintenant gérée par IPTVManager
    # en se basant sur les informations du ChannelStatusManager (alimenté par ClientMonitor).
    # def check_watchers_timeout(self):
    #    ...


    # Méthode supprimée car la logique de timeout des watchers est maintenant gérée par IPTVManager
    # en se basant sur les informations du ChannelStatusManager (alimenté par ClientMonitor).
    # def check_watchers_timeout(self):
    #    ...


    # Méthode supprimée car la logique de timeout des watchers est maintenant gérée par IPTVManager
    # en se basant sur les informations du ChannelStatusManager (alimenté par ClientMonitor).
    # def check_watchers_timeout(self):
    #    ...


    # Méthode supprimée car la logique de timeout des watchers est maintenant gérée par IPTVManager
    # en se basant sur les informations du ChannelStatusManager (alimenté par ClientMonitor).
    # def check_watchers_timeout(self):
    #    ...


    # Méthode supprimée car la logique de timeout des watchers est maintenant gérée par IPTVManager
    # en se basant sur les informations du ChannelStatusManager (alimenté par ClientMonitor).
    # def check_watchers_timeout(self):
    #    ...


    # Méthode supprimée car la logique de timeout des watchers est maintenant gérée par IPTVManager
    # en se basant sur les informations du ChannelStatusManager (alimenté par ClientMonitor).
    # def check_watchers_timeout(self):
    #    ...


    # Méthode supprimée car la logique de timeout des watchers est maintenant gérée par IPTVManager
    # en se basant sur les informations du ChannelStatusManager (alimenté par ClientMonitor).
    # def check_watchers_timeout(self):
    #    ...


    # Méthode supprimée car la logique de timeout des watchers est maintenant gérée par IPTVManager
    # en se basant sur les informations du ChannelStatusManager (alimenté par ClientMonitor).
    # def check_watchers_timeout(self):
    #    ...


    # Méthode supprimée car la logique de timeout des watchers est maintenant gérée par IPTVManager
    # en se basant sur les informations du ChannelStatusManager (alimenté par ClientMonitor).
    # def check_watchers_timeout(self):
    #    ...


    # Méthode supprimée car la logique de timeout des watchers est maintenant gérée par IPTVManager
    # en se basant sur les informations du ChannelStatusManager (alimenté par ClientMonitor).
    # def check_watchers_timeout(self):
    #    ...


    # Méthode supprimée car la logique de timeout des watchers est maintenant gérée par IPTVManager
    # en se basant sur les informations du ChannelStatusManager (alimenté par ClientMonitor).
    # def check_watchers_timeout(self):
    #    ...


    # Méthode supprimée car la logique de timeout des watchers est maintenant gérée par IPTVManager
    # en se basant sur les informations du ChannelStatusManager (alimenté par ClientMonitor).
    # def check_watchers_timeout(self):
    #    ...


    # Méthode supprimée car la logique de timeout des watchers est maintenant gérée par IPTVManager
    # en se basant sur les informations du ChannelStatusManager (alimenté par ClientMonitor).
    # def check_watchers_timeout(self):
    #    ...


    # Méthode supprimée car la logique de timeout des watchers est maintenant gérée par IPTVManager
    # en se basant sur les informations du ChannelStatusManager (alimenté par ClientMonitor).
    # def check_watchers_timeout(self):
    #    ...


    # Méthode supprimée car la logique de timeout des watchers est maintenant gérée par IPTVManager
    # en se basant sur les informations du ChannelStatusManager (alimenté par ClientMonitor).
    # def check_watchers_timeout(self):
    #    ...


    # Méthode supprimée car la logique de timeout des watchers est maintenant gérée par IPTVManager
    # en se basant sur les informations du ChannelStatusManager (alimenté par ClientMonitor).
    # def check_watchers_timeout(self):
    #    ...


    # Méthode supprimée car la logique de timeout des watchers est maintenant gérée par IPTVManager
    # en se basant sur les informations du ChannelStatusManager (alimenté par ClientMonitor).
    # def check_watchers_timeout(self):
    #    ...


    # Méthode supprimée car la logique de timeout des watchers est maintenant gérée par IPTVManager
    # en se basant sur les informations du ChannelStatusManager (alimenté par ClientMonitor).
    # def check_watchers_timeout(self):
    #    ...


    # Méthode supprimée car la logique de timeout des watchers est maintenant gérée par IPTVManager
    # en se basant sur les informations du ChannelStatusManager (alimenté par ClientMonitor).
    # def check_watchers_timeout(self):
    #    ...


    # Méthode supprimée car la logique de timeout des watchers est maintenant gérée par IPTVManager
    # en se basant sur les informations du ChannelStatusManager (alimenté par ClientMonitor).
    # def check_watchers_timeout(self):
    #    ...


    # Méthode supprimée car la logique de timeout des watchers est maintenant gérée par IPTVManager
    # en se basant sur les informations du ChannelStatusManager (alimenté par ClientMonitor).
    # def check_watchers_timeout(self):
    #    ...


    # Méthode supprimée car la logique de timeout des watchers est maintenant gérée par IPTVManager
    # en se basant sur les informations du ChannelStatusManager (alimenté par ClientMonitor).
    # def check_watchers_timeout(self):
    #    ...


    # Méthode supprimée car la logique de timeout des watchers est maintenant gérée par IPTVManager
    # en se basant sur les informations du ChannelStatusManager (alimenté par ClientMonitor).
    # def check_watchers_timeout(self):
    #    ...


    # Méthode supprimée car la logique de timeout des watchers est maintenant gérée par IPTVManager
    # en se basant sur les informations du ChannelStatusManager (alimenté par ClientMonitor).
    # def check_watchers_timeout(self):
    #    ...


    # Méthode supprimée car la logique de timeout des watchers est maintenant gérée par IPTVManager
    # en se basant sur les informations du ChannelStatusManager (alimenté par ClientMonitor).
    # def check_watchers_timeout(self):
    #    ...


    # Méthode supprimée car la logique de timeout des watchers est maintenant gérée par IPTVManager
    # en se basant sur les informations du ChannelStatusManager (alimenté par ClientMonitor).
    # def check_watchers_timeout(self):
    #    ...


    # Méthode supprimée car la logique de timeout des watchers est maintenant gérée par IPTVManager
    # en se basant sur les informations du ChannelStatusManager (alimenté par ClientMonitor).
    # def check_watchers_timeout(self):
    #    ...


    # Méthode supprimée car la logique de timeout des watchers est maintenant gérée par IPTVManager
    # en se basant sur les informations du ChannelStatusManager (alimenté par ClientMonitor).
    # def check_watchers_timeout(self):
    #    ...


    # Méthode supprimée car la logique de timeout des watchers est maintenant gérée par IPTVManager
    # en se basant sur les informations du ChannelStatusManager (alimenté par ClientMonitor).
    # def check_watchers_timeout(self):
    #    ...


    # Méthode supprimée car la logique de timeout des watchers est maintenant gérée par IPTVManager
    # en se basant sur les informations du ChannelStatusManager (alimenté par ClientMonitor).
    # def check_watchers_timeout(self):
    #    ...


    # Méthode supprimée car la logique de timeout des watchers est maintenant gérée par IPTVManager
    # en se basant sur les informations du ChannelStatusManager (alimenté par ClientMonitor).
    def check_watchers_timeout(self):
        """Vérifie si le stream doit être arrêté en raison d'une absence de watchers"""
        # On ne vérifie pas le timeout s'il n'y a pas de watchers_count
        if not hasattr(self, "watchers_count"):
            return False
            
        # On ne vérifie pas le timeout s'il n'est pas actif
        if not self.process_manager.is_running():
            return False
            
        # On ne vérifie pas le timeout s'il y a des watchers actifs
        if self.watchers_count > 0:
            return False
            
        # On ne vérifie pas le timeout s'il y a des erreurs critiques
        if self.error_handler.has_critical_errors():
            return False
            
        # Le stream continue de tourner même sans watchers
        return False 
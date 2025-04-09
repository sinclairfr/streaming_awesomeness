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


class IPTVChannel:
    """Gère une chaîne IPTV, son streaming et sa surveillance"""
    
    # Initialisation des variables de classe (statiques)
    _playlist_creation_timestamps = {}

    def __init__(
        self,
        name: str,
        video_dir: str,
        hls_cleaner: HLSCleaner,
        use_gpu: bool = False,
        stats_collector=None,
    ):
        self.name = name
        self.channel_name = name
        self.video_dir = video_dir
        self.use_gpu = use_gpu
        self.hls_cleaner = hls_cleaner
        self.stats_collector = stats_collector

        # Initialisation de l'error handler
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

        # Initialiser les composants dans le bon ordre
        self.position_manager = PlaybackPositionManager(name)
        self.logger = FFmpegLogger(name)
        self.command_builder = FFmpegCommandBuilder(name, use_gpu=use_gpu)
        self.process_manager = FFmpegProcessManager(name, self.logger)

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
        self.processed_videos = []
        self.watchers_count = 0
        self.last_watcher_time = time.time()
        self.last_segment_time = time.time()

        # État du scan initial
        self.initial_scan_complete = False
        self.scan_lock = threading.Lock()

        # Chargement des vidéos
        logger.info(f"[{self.name}] 🔄 Préparation initiale de la chaîne")
        self._scan_videos()
        self._create_concat_file()

        # Calcul de la durée totale
        total_duration = self._calculate_total_duration()
        self.position_manager.set_total_duration(total_duration)
        self.process_manager.set_total_duration(total_duration)

        self.initial_scan_complete = True
        self.ready_for_streaming = len(self.processed_videos) > 0

        logger.info(
            f"[{self.name}] ✅ Initialisation complète. Chaîne prête: {self.ready_for_streaming}"
        )

    def _create_concat_file(self) -> Optional[Path]:
        """Crée le fichier de concaténation avec les bons chemins et sans doublons"""
        # Vérifier si on a créé une playlist récemment pour éviter les doublons
        current_time = time.time()
        
        last_creation = IPTVChannel._playlist_creation_timestamps.get(self.name, 0)
        concat_file = Path(self.video_dir) / "_playlist.txt"

        # Si on a créé une playlist dans les 10 dernières secondes, ne pas recréer
        if current_time - last_creation < 10:
            logger.debug(
                f"[{self.name}] ℹ️ Création de playlist ignorée (dernière: il y a {current_time - last_creation:.1f}s)"
            )
            return concat_file if concat_file.exists() else None

        # Mettre à jour le timestamp AVANT de créer le fichier
        IPTVChannel._playlist_creation_timestamps[self.name] = current_time
        
        try:
            # Utiliser ready_to_stream
            ready_to_stream_dir = Path(self.video_dir) / "ready_to_stream"
            if not ready_to_stream_dir.exists():
                logger.error(f"[{self.name}] ❌ Dossier ready_to_stream introuvable")
                return None

            # Scanner le dossier ready_to_stream
            ready_files = list(ready_to_stream_dir.glob("*.mp4"))
            
            if not ready_files:
                logger.error(f"[{self.name}] ❌ Aucun fichier MP4 dans {ready_to_stream_dir}")
                return None
                
            logger.info(f"[{self.name}] 🔍 {len(ready_files)} fichiers trouvés dans ready_to_stream")

            # Mélanger les fichiers pour plus de variété
            random.shuffle(ready_files)

            # Vérifier que tous les fichiers sont valides
            valid_files = []
            for video in ready_files:
                if video.exists() and os.access(video, os.R_OK):
                    try:
                        duration = get_accurate_duration(video)
                        if duration and duration > 0:
                            valid_files.append(video)
                        else:
                            logger.warning(f"[{self.name}] ⚠️ Fichier ignoré: {video.name} (durée invalide)")
                    except Exception as e:
                        logger.warning(f"[{self.name}] ⚠️ Fichier ignoré: {video.name} (erreur validation: {e})")
                else:
                    logger.warning(f"[{self.name}] ⚠️ Fichier ignoré: {video.name} (non accessible)")

            if not valid_files:
                logger.error(f"[{self.name}] ❌ Aucun fichier valide pour la playlist")
                return None

            logger.info(f"[{self.name}] 🛠️ Créer le fichier de concaténation avec {len(valid_files)} fichiers uniques")

            # Créer le fichier de concaténation
            with open(concat_file, "w", encoding="utf-8") as f:
                for video in valid_files:
                    escaped_path = str(video.absolute()).replace("'", "'\\''")
                    f.write(f"file '{escaped_path}'\n")
                    logger.debug(f"[{self.name}] ✅ Ajout de {video.name}")

            # Vérifier que le fichier a été créé correctement
            if not concat_file.exists() or concat_file.stat().st_size == 0:
                logger.error(f"[{self.name}] ❌ Erreur: playlist vide ou non créée")
                return None

            # Mettre à jour self.processed_videos
            self.processed_videos = valid_files.copy()

            logger.info(f"[{self.name}] 🎥 Playlist créée avec {len(valid_files)} fichiers uniques en mode aléatoire")
            return concat_file

        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur _playlist.txt: {e}")
            return None

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

                # Mise à jour de la durée totale
                total_duration = self._calculate_total_duration()
                self.position_manager.set_total_duration(total_duration)
                self.process_manager.set_total_duration(total_duration)

                # Mise à jour de la playlist
                self._create_concat_file()

                logger.info(f"[{self.name}] ✅ Scan de mise à jour terminé. Chaîne prête: {self.ready_for_streaming}")
                self._last_scan_time = current_time

        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur scan de mise à jour: {e}")
        finally:
            self._scan_in_progress = False

    def _calculate_total_duration(self) -> float:
        """Calcule la durée totale de la playlist en utilisant le PlaybackPositionManager"""
        try:
            # Vérification que la liste processed_videos n'est pas vide
            if not self.processed_videos:
                logger.warning(f"[{self.name}] ⚠️ Aucune vidéo à analyser pour le calcul de durée")
                # On conserve la durée existante si possible, sinon valeur par défaut
                if hasattr(self, "total_duration") and self.total_duration > 0:
                    return self.total_duration
                return 3600.0

            # Déléguer le calcul au PlaybackPositionManager
            total_duration = self.position_manager.calculate_durations(self.processed_videos)

            if total_duration <= 0:
                logger.warning(f"[{self.name}] ⚠️ Durée totale invalide, fallback à la valeur existante ou 3600s")
                if hasattr(self, "total_duration") and self.total_duration > 0:
                    return self.total_duration
                return 3600.0

            # Mettre à jour notre durée totale
            self.total_duration = total_duration

            logger.info(f"[{self.name}] ✅ Durée totale calculée: {total_duration:.2f}s ({len(self.processed_videos)} vidéos)")
            return total_duration

        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur calcul durée: {e}")
            # Fallback à la valeur existante ou valeur par défaut
            if hasattr(self, "total_duration") and self.total_duration > 0:
                return self.total_duration
            return 3600.0

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

    def _handle_process_died(self, exit_code: int, stderr: str = ""):
        """Gère la mort du processus FFmpeg"""
        try:
            # Analyser l'erreur pour détecter le type
            error_type = "unknown_error"
            if exit_code < 0:
                error_type = f"signal_{abs(exit_code)}"
            elif stderr and "error" in stderr.lower():
                # Extraction simple du type d'erreur à partir du message
                if "no such file" in stderr.lower():
                    error_type = "missing_file"
                elif "invalid data" in stderr.lower():
                    error_type = "invalid_data"
                elif "dts" in stderr.lower():
                    error_type = "dts_error"
                elif "timeout" in stderr.lower():
                    error_type = "timeout"
            
            # Utiliser l'error handler pour gérer l'erreur
            if self.error_handler.add_error(error_type):
                logger.warning(f"[{self.name}] Redémarrage nécessaire après erreur: {error_type}")
                self._restart_stream()
            elif self.error_handler.has_critical_errors():
                logger.error(f"[{self.name}] Erreurs critiques détectées, arrêt du stream")
                self.stop_stream_if_needed()
                
        except Exception as e:
            logger.error(f"[{self.name}] Erreur lors de la gestion de la mort du processus: {e}")

    def _restart_stream(self) -> bool:
        """Redémarre le stream en cas de problème"""
        try:
            logger.info(f"🔄 Redémarrage du stream {self.name}")

            # Ajouter un délai aléatoire pour éviter les redémarrages en cascade
            jitter = random.uniform(0.5, 2.0)
            time.sleep(jitter)

            # Utiliser l'error handler pour vérifier le cooldown
            if not self.error_handler.should_restart():
                logger.info(f"⏳ Attente du cooldown de redémarrage")
                return False

            # Arrêter proprement les processus FFmpeg via le ProcessManager
            self.process_manager.stop_process()

            # Nettoyer le dossier HLS avec le HLSCleaner
            self.hls_cleaner.cleanup_channel(self.name)

            # Vérifier que nous avons des fichiers valides
            ready_dir = Path(self.video_dir) / "ready_to_stream"
            if not ready_dir.exists():
                logger.error(f"[{self.name}] ❌ Dossier ready_to_stream introuvable")
                return False

            video_files = list(ready_dir.glob("*.mp4"))
            if not video_files:
                logger.error(f"[{self.name}] ❌ Aucun fichier MP4 dans ready_to_stream")
                return False

            # Vérifier que les fichiers sont valides
            valid_files = []
            for video_file in video_files:
                if verify_file_ready(video_file):
                    valid_files.append(video_file)
                else:
                    logger.warning(f"[{self.name}] ⚠️ Fichier {video_file.name} ignoré car non valide")

            if not valid_files:
                logger.error(f"[{self.name}] ❌ Aucun fichier MP4 valide trouvé")
                return False

            # Lancer un nouveau stream
            result = self.start_stream()
            if result:
                # Réinitialiser les compteurs d'erreurs après un redémarrage réussi
                self.error_handler.reset()
                logger.info(f"[{self.name}] ✅ Stream redémarré avec succès")
            return result

        except Exception as e:
            logger.error(f"Erreur lors du redémarrage de {self.name}: {e}")
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
        """
        Démarre le flux HLS pour cette chaîne en traitant un fichier à la fois.
        """
        try:
            # Initialiser le temps de démarrage
            self._startup_time = time.time()
            
            # 1) Vérifier qu'on a des vidéos prêtes
            if not self.ready_for_streaming:
                logger.warning(f"[{self.name}] ⚠️ Chaîne non prête (pas de vidéos). Annulation du démarrage.")
                return False

            # 2) Préparer le dossier HLS
            hls_dir = Path(f"/app/hls/{self.name}")
            if not hls_dir.exists():
                # Créer le dossier HLS s'il n'existe pas
                try:
                    hls_dir.mkdir(parents=True, exist_ok=True)
                    # S'assurer que le dossier est accessible en écriture
                    os.chmod(hls_dir, 0o777)
                    logger.info(f"[{self.name}] 📁 Création du dossier HLS: {hls_dir}")
                except Exception as e:
                    logger.error(f"[{self.name}] ❌ Erreur création dossier HLS: {e}")
                    return False
            else:
                # Nettoyer le dossier existant
                self.hls_cleaner.cleanup_channel(self.name)

            # 3) Sélectionner un fichier aléatoire parmi les fichiers valides
            if not self.processed_videos:
                logger.error(f"[{self.name}] ❌ Aucun fichier vidéo disponible")
                return False
                
            video_file = random.choice(self.processed_videos)
            if not video_file.exists():
                logger.error(f"[{self.name}] ❌ Fichier sélectionné n'existe pas: {video_file}")
                return False
            
            # Suivre le fichier actuellement utilisé
            self.current_video_file = video_file
            
            # Obtenir la durée du fichier pour la surveillance
            try:
                duration = get_accurate_duration(video_file)
                if duration and duration > 0:
                    self.current_file_duration = duration
                    logger.info(f"[{self.name}] ℹ️ Durée du fichier actuel: {duration:.2f}s")
                else:
                    logger.warning(f"[{self.name}] ⚠️ Impossible de déterminer la durée du fichier")
                    self.current_file_duration = 300  # Valeur par défaut
            except Exception as e:
                logger.warning(f"[{self.name}] ⚠️ Erreur calcul durée: {e}")
                self.current_file_duration = 300  # Valeur par défaut
            
            logger.info(f"[{self.name}] 🎥 Démarrage avec le fichier: {video_file.name}")

            # 4) Construire la commande FFmpeg pour un fichier individuel
            progress_file = f"/app/logs/ffmpeg/{self.name}_progress.log"
            
            command = [
                "ffmpeg",
                "-hide_banner",
                "-loglevel", "info",
                "-y",
                "-thread_queue_size", "4096",
                "-analyzeduration", "5M",
                "-probesize", "5M",
                "-re",
                "-fflags", "+genpts+igndts+discardcorrupt",
                "-threads", "2",
                "-avoid_negative_ts", "make_zero"
            ]
            
            # Ajouter le fichier de progression
            if progress_file:
                command.extend(["-progress", progress_file])
            
            # Ajouter l'entrée (fichier individuel)
            command.extend(["-i", str(video_file)])
            
            # Paramètres de sortie
            command.extend([
                "-c:v", "copy",
                "-c:a", "copy",
                "-sn", "-dn",
                "-map", "0:v:0",
                "-map", "0:a:0?",
                "-max_muxing_queue_size", "2048",
                "-fps_mode", "passthrough",
                "-f", "hls",
                "-hls_time", "2",
                "-hls_list_size", "6",
                "-hls_delete_threshold", "1",
                "-hls_flags", "delete_segments+append_list+independent_segments",
                "-hls_allow_cache", "1",
                "-start_number", "0",
                "-hls_segment_type", "mpegts",
                "-max_delay", "1000000",
                "-hls_init_time", "1",
                "-hls_segment_filename", f"{str(hls_dir)}/segment_%d.ts",
                f"{str(hls_dir)}/playlist.m3u8"
            ])
            
            # Log de la commande complète
            logger.info("=" * 80)
            logger.info(f"[{self.name}] 🚀 Lancement FFmpeg: {' '.join(command)}")
            logger.info("=" * 80)
            
            # 5) Démarrer le processus FFmpeg via le ProcessManager
            success = self.process_manager.start_process(command, str(hls_dir))
            if not success:
                logger.error(f"[{self.name}] ❌ Échec du démarrage du processus FFmpeg")
                return False

            # 6) Démarrer la boucle de surveillance des segments en arrière-plan
            if not hasattr(self, "_segment_monitor_thread") or not self._segment_monitor_thread.is_alive():
                self._segment_monitor_thread = threading.Thread(
                    target=self._segment_monitor_loop,
                    daemon=True
                )
                self._segment_monitor_thread.start()
                logger.info(f"[{self.name}] 🔍 Boucle de surveillance des segments démarrée")
                
            # 7) Démarrer le gestionnaire de fichiers en arrière-plan 
            if not hasattr(self, "_file_manager_thread") or not self._file_manager_thread.is_alive():
                self._file_manager_thread = threading.Thread(
                    target=self._manage_video_files,
                    daemon=True
                )
                self._file_manager_thread.start()
                logger.info(f"[{self.name}] 🔄 Gestionnaire de fichiers démarré")

            logger.info(f"[{self.name}] ✅ Stream démarré avec succès")
            return True

        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur start_stream: {e}")
            return False
            
    def _manage_video_files(self):
        """
        Gère la rotation des fichiers vidéo (arrêt/démarrage de FFmpeg entre chaque fichier).
        Surveille la fin de la lecture du fichier actuel et passe au suivant.
        """
        try:
            # Attendre que le stream soit bien démarré
            time.sleep(10)
            
            while True:
                # Vérifier si le processus est en cours
                if not self.process_manager.is_running():
                    logger.info(f"[{self.name}] ℹ️ Processus FFmpeg arrêté, sélection du prochain fichier")
                    
                    # Prendre un nouveau fichier au hasard
                    if not self.processed_videos:
                        logger.error(f"[{self.name}] ❌ Plus de fichiers disponibles")
                        break
                    
                    # Vérifier que la liste des fichiers est à jour
                    self._scan_videos_async()
                    
                    # Sélectionner un fichier aléatoire
                    video_file = random.choice(self.processed_videos)
                    if not video_file.exists():
                        logger.warning(f"[{self.name}] ⚠️ Fichier sélectionné n'existe pas: {video_file}")
                        continue
                    
                    # Mettre à jour le fichier actuel
                    self.current_video_file = video_file
                    
                    # Obtenir la durée du fichier
                    try:
                        duration = get_accurate_duration(video_file)
                        if duration and duration > 0:
                            self.current_file_duration = duration
                            logger.info(f"[{self.name}] ℹ️ Durée du fichier: {duration:.2f}s")
                        else:
                            logger.warning(f"[{self.name}] ⚠️ Impossible de déterminer la durée")
                            self.current_file_duration = 300  # Valeur par défaut
                    except Exception as e:
                        logger.warning(f"[{self.name}] ⚠️ Erreur calcul durée: {e}")
                        self.current_file_duration = 300  # Valeur par défaut
                    
                    # Réinitialiser la position
                    self.last_logged_position = 0
                    
                    logger.info(f"[{self.name}] 🎥 Changement de fichier: {video_file.name}")
                    
                    # Démarrer un nouveau processus FFmpeg avec ce fichier
                    hls_dir = Path(f"/app/hls/{self.name}")
                    progress_file = f"/app/logs/ffmpeg/{self.name}_progress.log"
                    
                    command = [
                        "ffmpeg",
                        "-hide_banner",
                        "-loglevel", "info",
                        "-y",
                        "-thread_queue_size", "4096",
                        "-analyzeduration", "5M",
                        "-probesize", "5M",
                        "-re",
                        "-fflags", "+genpts+igndts+discardcorrupt",
                        "-threads", "2",
                        "-avoid_negative_ts", "make_zero",
                        "-progress", progress_file,
                        "-i", str(video_file),
                        "-c:v", "copy",
                        "-c:a", "copy",
                        "-sn", "-dn",
                        "-map", "0:v:0",
                        "-map", "0:a:0?",
                        "-max_muxing_queue_size", "2048",
                        "-fps_mode", "passthrough",
                        "-f", "hls",
                        "-hls_time", "2",
                        "-hls_list_size", "6",
                        "-hls_delete_threshold", "1",
                        "-hls_flags", "delete_segments+append_list+independent_segments",
                        "-hls_allow_cache", "1",
                        "-start_number", "0",
                        "-hls_segment_type", "mpegts",
                        "-max_delay", "1000000",
                        "-hls_init_time", "1",
                        "-hls_segment_filename", f"{str(hls_dir)}/segment_%d.ts",
                        f"{str(hls_dir)}/playlist.m3u8"
                    ]
                    
                    success = self.process_manager.start_process(command, str(hls_dir))
                    if not success:
                        logger.error(f"[{self.name}] ❌ Échec du changement de fichier")
                        time.sleep(5)  # Attendre avant de réessayer
                
                # Vérifier si on approche de la fin du fichier actuel
                elif hasattr(self, "last_logged_position") and hasattr(self, "current_file_duration"):
                    if self.last_logged_position > self.current_file_duration - 10:
                        logger.info(f"[{self.name}] ⏱️ Fin de fichier imminente, préparation du changement")
                        # Arrêter le processus pour passer au fichier suivant
                        self.process_manager.stop_process()
                        time.sleep(2)  # Petite pause avant de démarrer le suivant
                
                # Pause avant la prochaine vérification
                time.sleep(5)
                
        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur dans le gestionnaire de fichiers: {e}")

    def _scan_videos(self) -> bool:
        """Scanne les fichiers vidéos et met à jour processed_videos"""
        try:
            source_dir = Path(self.video_dir)
            ready_to_stream_dir = source_dir / "ready_to_stream"

            # Création du dossier s'il n'existe pas
            ready_to_stream_dir.mkdir(exist_ok=True)

            # On scanne les vidéos dans ready_to_stream
            mp4_files = list(ready_to_stream_dir.glob("*.mp4"))

            if not mp4_files:
                logger.warning(f"[{self.name}] ⚠️ Aucun fichier MP4 dans {ready_to_stream_dir}")
                self.ready_for_streaming = False
                return False

            # Log des fichiers trouvés
            logger.info(f"[{self.name}] 🔍 {len(mp4_files)} fichiers MP4 trouvés dans ready_to_stream")

            # Vérification que les fichiers sont valides
            valid_files = []
            for video_file in mp4_files:
                if verify_file_ready(video_file):
                    valid_files.append(video_file)
                else:
                    logger.warning(f"[{self.name}] ⚠️ Fichier {video_file.name} ignoré car non valide")

            if valid_files:
                self.processed_videos = valid_files
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
            import traceback
            logger.error(f"[{self.name}] {traceback.format_exc()}")
            return False

    def is_running(self) -> bool:
        """Vérifie si la chaîne est actuellement en streaming"""
        return self.process_manager.is_running()

    def check_stream_health(self):
        """Vérifie la santé du stream et redémarre si nécessaire"""
        try:
            # Utiliser le ProcessManager pour vérifier la santé du stream
            if not self.process_manager.check_stream_health():
                # Si le stream n'est pas sain et qu'il y a des viewers, on redémarre
                if hasattr(self, "watchers_count") and self.watchers_count > 0:
                    logger.warning(f"[{self.name}] ⚠️ Problème de santé détecté, redémarrage")
                    return self._restart_stream()
                else:
                    logger.info(f"[{self.name}] ⚠️ Problème de santé détecté mais aucun viewer actif")
            
            return True
            
        except Exception as e:
            logger.error(f"[{self.name}] ❌ Error in health check: {e}")
            return False
            
    def check_watchers_timeout(self):
        """Vérifie si le stream doit être arrêté en raison d'une absence de watchers"""
        # On ne vérifie pas le timeout s'il n'y a pas de watchers_count
        if not hasattr(self, "watchers_count"):
            return False
            
        # On ne vérifie pas le timeout si le stream n'est pas actif
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
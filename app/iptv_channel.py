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
from video_processor import verify_file_ready, get_accurate_duration

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
        self.ready_for_streaming = False
        self.total_duration = 0

        # IMPORTANT: Initialiser d'abord le PlaybackPositionManager pour charger les offsets
        self.position_manager = PlaybackPositionManager(name)
        
        # Ensuite initialiser les autres composants
        self.logger = FFmpegLogger(name)
        self.command_builder = FFmpegCommandBuilder(name, use_gpu=use_gpu) 
        self.process_manager = FFmpegProcessManager(name, self.logger)
        
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
        
        # Calcul de la durée totale et utilisation de l'offset récupéré du fichier JSON
        total_duration = self._calculate_total_duration()
        self.position_manager.set_total_duration(total_duration)
        self.process_manager.set_total_duration(total_duration)
        
        self.initial_scan_complete = True
        self.ready_for_streaming = len(self.processed_videos) > 0
        
        logger.debug(f"[{self.name}] ✅ Initialisation complète. Chaîne prête: {self.ready_for_streaming}, Offset: {self.position_manager.last_known_position:.2f}s")
        
        # Scan asynchrone en arrière-plan
        threading.Thread(target=self._scan_videos_async, daemon=True).start() 

        self._verify_playlist()
        
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
                if verify_file_ready(video_file):
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
        try:
            # Vérification que la liste processed_videos n'est pas vide
            if not self.processed_videos:
                logger.warning(f"[{self.name}] ⚠️ Aucune vidéo à analyser pour le calcul de durée")
                self.total_duration = 120.0  # Valeur par défaut
                return 120.0
                    
            total_duration = self.position_manager.calculate_durations(self.processed_videos)
            if total_duration <= 0:
                logger.warning(f"[{self.name}] ⚠️ Durée totale invalide, fallback à 120s")
                self.total_duration = 120.0
                return 120.0
                    
            self.total_duration = total_duration  # Ajouter cette ligne
            return total_duration
        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur calcul durée: {e}")
            self.total_duration = 120.0  # Ajouter cette ligne
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
            # Utiliser ready_to_stream au lieu de processed
            ready_to_stream_dir = Path(self.video_dir) / "ready_to_stream"
            if not ready_to_stream_dir.exists():
                logger.error(f"[{self.name}] ❌ Dossier ready_to_stream introuvable")
                return None
                    
            ready_files = sorted(ready_to_stream_dir.glob("*.mp4"))
            if not ready_files:
                logger.error(f"[{self.name}] ❌ Aucune vidéo dans {ready_to_stream_dir}")
                return None

            logger.info(f"[{self.name}] 🛠️ Création de _playlist.txt")
            concat_file = Path(self.video_dir) / "_playlist.txt"

            logger.debug(f"[{self.name}] 📝 Écriture de _playlist.txt")

            with open(concat_file, "w", encoding="utf-8") as f:
                for video in ready_files:
                    escaped_path = str(video.absolute()).replace("'", "'\\''")
                    f.write(f"file '{escaped_path}'\n")
                    logger.debug(f"[{self.name}] ✅ Ajout de {video.name}")

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
        """Démarre le stream avec FFmpeg en utilisant les nouvelles classes, avec fallback CPU"""
        try:
            # Vérification rapide que la chaîne est prête
            if not self.ready_for_streaming:
                logger.warning(f"[{self.name}] ⚠️ Chaîne non prête pour le streaming (pas de vidéos)")
                return False

            logger.info(f"[{self.name}] 🚀 Démarrage du stream...")

            hls_dir = Path(f"/app/hls/{self.name}")
            hls_dir.mkdir(parents=True, exist_ok=True)
            
            # Utilisation du fichier playlist déjà créé
            concat_file = Path(self.video_dir) / "_playlist.txt"
            if not concat_file.exists():
                concat_file = self._create_concat_file()
                
            if not concat_file or not concat_file.exists():
                logger.error(f"[{self.name}] ❌ _playlist.txt introuvable")
                return False
            else:
                logger.info(f"[{self.name}] ✅ _playlist.txt trouvé")

            # Récupération de l'offset de démarrage
            start_offset = self.position_manager.get_start_offset()
            if self.total_duration > 0 and start_offset > self.total_duration:
                start_offset = start_offset % self.total_duration
                logger.info(f"[{self.name}] ⏱️ Offset corrigé: {start_offset:.2f}s")

            # Synchronisation des managers
            self.position_manager.set_position(start_offset)
            self.position_manager.set_playing(True)
            self.process_manager.set_playback_offset(start_offset)
            self.process_manager.set_total_duration(self.position_manager.total_duration)

            # PREMIÈRE TENTATIVE AVEC HARDWARE ACCÉLÉRATION
            self.command_builder.optimize_for_hardware()
            logger.info(f"[{self.name}] Vérification mkv...")
            has_mkv = self.command_builder.detect_mkv_in_playlist(concat_file)
            
            command = self.command_builder.build_command(
                input_file=concat_file,
                output_dir=hls_dir,
                playback_offset=start_offset,
                progress_file=self.logger.get_progress_file(),
                has_mkv=has_mkv
            )
            
            # Lancement du process
            if not self.process_manager.start_process(command, hls_dir):
                logger.error(f"[{self.name}] ❌ Échec démarrage FFmpeg")
                
                # SECONDE TENTATIVE EN MODE CPU
                logger.warning(f"[{self.name}] 🔄 Nouvelle tentative en mode CPU")
                self.command_builder.use_gpu = False
                
                # Reconstruction de la commande en mode CPU
                cpu_command = self.command_builder.build_command(
                    input_file=concat_file,
                    output_dir=hls_dir,
                    playback_offset=start_offset,
                    progress_file=self.logger.get_progress_file(),
                    has_mkv=has_mkv
                )
                
                # Lancement du process en mode CPU
                if not self.process_manager.start_process(cpu_command, hls_dir):
                    logger.error(f"[{self.name}] ❌ Échec démarrage FFmpeg en mode CPU")
                    return False
                
            # Configuration du position_manager
            self.position_manager.set_playing(True)
            self.process_manager.set_playback_offset(start_offset)
            if hasattr(self.position_manager, 'start_periodic_save'):
                self.position_manager.start_periodic_save()
                
            logger.info(f"[{self.name}] ✅ Stream démarré avec succès à la position {start_offset:.2f}s")
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
       
    def stop_stream_if_needed(self):
        """Arrête proprement le stream en utilisant les managers"""
        try:
            if not self.process_manager.is_running():
                return
                
            logger.info(f"[{self.name}] 🛑 Arrêt du stream (dernier watcher: {time.time() - self.last_watcher_time:.1f}s)")
            
            self.position_manager.set_playing(False)
            if hasattr(self.position_manager, 'stop_save_thread'):
                self.position_manager.stop_save_thread.set()
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
                
            logger.debug(f"[{self.name}] ✅ VideoProcessor correctement initialisé")
            return True
            
        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur vérification VideoProcessor: {e}")
            return False

    def _handle_process_died(self, return_code):
        """Gère la mort du processus FFmpeg"""
        logger.error(f"[{self.name}] ❌ Processus FFmpeg terminé avec code: {return_code}")
        
        # Éviter les redémarrages en cascade avec une vérification des délais d'inactivité
        # Si le processus a été tué intentionnellement (code -9 = SIGKILL)
        if return_code == -9:
            # Calculer le temps d'inactivité
            inactivity_duration = time.time() - getattr(self, 'last_watcher_time', 0)
            
            # Si l'inactivité est supérieure au seuil, considérer que c'était un arrêt intentionnel
            if inactivity_duration > TIMEOUT_NO_VIEWERS:
                logger.info(f"[{self.name}] ✅ Processus FFmpeg tué proprement après {inactivity_duration:.1f}s d'inactivité")
                return False  # Ne pas redémarrer
        
        # Vérifier aussi le cooldown global avant d'ajouter une erreur
        elapsed = time.time() - getattr(self, "last_restart_time", 0)
        if elapsed < getattr(self.error_handler, "restart_cooldown", 60):
            logger.info(f"[{self.name}] ⏱️ Cooldown actif, pas de redémarrage immédiat ({elapsed:.1f}s < {getattr(self.error_handler, 'restart_cooldown', 60)}s)")
            return False
        
        # Sinon, comportement normal
        self.error_handler.add_error("PROCESS_DIED")
        return self._restart_stream()
    
    def _handle_position_update(self, position):
        """Reçoit les mises à jour de position du ProcessManager"""
        self.position_manager.update_from_progress(self.logger.get_progress_file())
        
    def _handle_segment_created(self, segment_path, size):  
        """Notifié quand un nouveau segment est créé"""
        self.last_segment_time = time.time()
        if self.logger:
            self.logger.log_segment(segment_path, size)
            
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


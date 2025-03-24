# iptv_channel.py
import os
import time
import threading
import random
from pathlib import Path
from typing import Optional, List
import shutil
import json
from video_processor import VideoProcessor
from hls_cleaner import HLSCleaner
from ffmpeg_logger import FFmpegLogger
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
    CRASH_THRESHOLD,
)
from video_processor import verify_file_ready, get_accurate_duration
import datetime
from error_handler import ErrorHandler


class IPTVChannel:
    """Gère une chaîne IPTV, son streaming et sa surveillance"""

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

        # Gestion des erreurs intégrée
        self.error_count = 0
        self.restart_count = 0
        self.max_restarts = 5
        self.restart_cooldown = 60
        self.last_restart_time = 0
        self.error_types = set()
        
        # Initialisation de l'error handler
        self.error_handler = ErrorHandler(
            channel_name=self.name,
            max_restarts=self.max_restarts,
            restart_cooldown=self.restart_cooldown
        )
        
        # Chemin du fichier de log unique pour ce canal
        self.crash_log_path = Path(f"/app/logs/crashes_{self.name}.log")
        self.crash_log_path.parent.mkdir(exist_ok=True)

        self.lock = threading.Lock()
        self.ready_for_streaming = False
        self.total_duration = 0

        # IMPORTANT: Initialiser d'abord le PlaybackPositionManager pour charger les offsets
        self.position_manager = PlaybackPositionManager(name)

        # Ensuite initialiser les autres composants
        self.logger = FFmpegLogger(name)
        self.command_builder = FFmpegCommandBuilder(name, use_gpu=use_gpu)
        self.process_manager = FFmpegProcessManager(name, self.logger)

        # Ajouter cette chaîne au registre global
        if hasattr(FFmpegProcessManager, "all_channels"):
            FFmpegProcessManager.all_channels[name] = self

        # Configuration des callbacks
        self.process_manager.on_process_died = self._handle_process_died

        # On ne passe pas directement la méthode mais on crée un wrapper
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

        logger.info(
            f"[{self.name}] ✅ Initialisation complète. Chaîne prête: {self.ready_for_streaming}"
        )

        _playlist_creation_timestamps = {}  # Pour suivre les créations récentes par chaîne

    def _create_concat_file(self) -> Optional[Path]:
        """Crée le fichier de concaténation avec les bons chemins et sans doublons"""
        # Vérifier si on a créé une playlist récemment pour éviter les doublons
        current_time = time.time()
        if not hasattr(IPTVChannel, "_playlist_creation_timestamps"):
            IPTVChannel._playlist_creation_timestamps = {}

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
            # Utiliser ready_to_stream au lieu de processed
            ready_to_stream_dir = Path(self.video_dir) / "ready_to_stream"
            if not ready_to_stream_dir.exists():
                logger.error(f"[{self.name}] ❌ Dossier ready_to_stream introuvable")
                return None

            # MODIFIÉ: On rescanne le dossier ready_to_stream pour être sûr d'avoir tous les fichiers
            # au lieu de se baser uniquement sur self.processed_videos qui pourrait ne pas être à jour
            ready_files = list(ready_to_stream_dir.glob("*.mp4"))
            
            if not ready_files:
                logger.error(f"[{self.name}] ❌ Aucun fichier MP4 dans {ready_to_stream_dir}")
                return None
                
            logger.info(f"[{self.name}] 🔍 {len(ready_files)} fichiers trouvés dans ready_to_stream")

            # On mélange les fichiers pour plus de variété
            import random
            random.shuffle(ready_files)

            # Vérifier que tous les fichiers existent et sont accessibles
            valid_files = []
            for video in ready_files:
                if video.exists() and os.access(video, os.R_OK):
                    # Vérifier que le fichier est un MP4 valide
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

            logger.info(
                f"[{self.name}] 🛠️ Créer le fichier de concaténation avec {len(valid_files)} fichiers uniques"
            )

            # Créer le fichier de concaténation avec une syntaxe simplifiée
            with open(concat_file, "w", encoding="utf-8") as f:
                for video in valid_files:
                    escaped_path = str(video.absolute()).replace("'", "'\\''")
                    # Utiliser une syntaxe simple pour la concaténation
                    f.write(f"file '{escaped_path}'\n")
                    logger.debug(f"[{self.name}] ✅ Ajout de {video.name}")

            # Vérifier que le fichier a été créé correctement
            if not concat_file.exists() or concat_file.stat().st_size == 0:
                logger.error(f"[{self.name}] ❌ Erreur: playlist vide ou non créée")
                return None

            # NOUVEAU: Mettre à jour self.processed_videos pour synchroniser
            self.processed_videos = valid_files.copy()

            logger.info(
                f"[{self.name}] 🎥 Playlist créée avec {len(valid_files)} fichiers uniques en mode aléatoire"
            )
            return concat_file

        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur _playlist.txt: {e}")
            return None

    def _handle_position_update(self, position):
        """Reçoit les mises à jour de position du ProcessManager"""
        try:
            # On se contente de loguer les sauts de position sans redémarrer
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

            self.last_logged_position = position
            
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
        try:
            # Vérification que la liste processed_videos n'est pas vide
            if not self.processed_videos:
                logger.warning(
                    f"[{self.name}] ⚠️ Aucune vidéo à analyser pour le calcul de durée"
                )
                # On conserve la durée existante si possible, sinon valeur par défaut
                if hasattr(self, "total_duration") and self.total_duration > 0:
                    return self.total_duration
                return 3600.0

            # Si la durée a déjà été calculée et qu'on a le même nombre de fichiers
            # qu'avant, on peut conserver la durée existante pour éviter les sauts
            existing_duration = getattr(self, "total_duration", 0)
            cached_num_videos = getattr(self, "_num_processed_videos", 0)

            if existing_duration > 0 and cached_num_videos == len(
                self.processed_videos
            ):
                # On vérifie si les fichiers sont les mêmes en comparant les noms
                current_filenames = sorted([p.name for p in self.processed_videos])
                cached_filenames = getattr(self, "_cached_filenames", [])

                if current_filenames == cached_filenames:
                    logger.info(
                        f"[{self.name}] 🔄 Conservation de la durée calculée précédemment: {existing_duration:.2f}s"
                    )
                    return existing_duration

            # Calcul de la durée via le position manager avec cache
            total_duration = self.position_manager.calculate_durations(
                self.processed_videos
            )

            if total_duration <= 0:
                logger.warning(
                    f"[{self.name}] ⚠️ Durée totale invalide, fallback à la valeur existante ou 120s"
                )
                if existing_duration > 0:
                    return existing_duration
                return 3600.0

            # On stocke les métadonnées pour les futures comparaisons
            self.total_duration = total_duration
            self._num_processed_videos = len(self.processed_videos)
            self._cached_filenames = sorted([p.name for p in self.processed_videos])

            logger.info(
                f"[{self.name}] ✅ Durée totale calculée: {total_duration:.2f}s ({len(self.processed_videos)} vidéos)"
            )
            return total_duration

        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur calcul durée: {e}")
            # Fallback à la valeur existante ou valeur par défaut
            if hasattr(self, "total_duration") and self.total_duration > 0:
                return self.total_duration
            return 3600.0

    def _check_segments(self, hls_dir: str) -> dict:
        """
        Vérifie la génération des segments HLS et retourne des données structurées

        Args:
            hls_dir: Chemin du dossier HLS

        Returns:
            dict: Informations sur les segments (count, liste, taille totale)
        """
        try:
            segment_log_path = Path(f"/app/logs/segments/{self.name}_segments.log")
            segment_log_path.parent.mkdir(parents=True, exist_ok=True)

            hls_path = Path(hls_dir)
            playlist = hls_path / "playlist.m3u8"

            if not playlist.exists():
                logger.error(f"[{self.name}] ❌ playlist.m3u8 introuvable")
                return {"success": False, "error": "playlist_not_found", "segments": []}

            # Lecture de la playlist
            with open(playlist) as f:
                segments = [line.strip() for line in f if line.strip().endswith(".ts")]

            if not segments:
                logger.warning(f"[{self.name}] ⚠️ Aucun segment dans la playlist")
                return {"success": False, "error": "no_segments", "segments": []}

            # Analyse des segments
            segment_data = []
            total_size = 0
            
            # Variable pour suivre la modification la plus récente
            most_recent_mtime = 0
            
            for segment in segments:
                segment_path = hls_path / segment
                if segment_path.exists():
                    size = segment_path.stat().st_size
                    mtime = segment_path.stat().st_mtime
                    segment_id = (
                        int(segment.split("_")[-1].split(".")[0])
                        if "_" in segment
                        else 0
                    )

                    # Mise à jour du temps de modification le plus récent
                    if mtime > most_recent_mtime:
                        most_recent_mtime = mtime

                    segment_info = {
                        "name": segment,
                        "size": size,
                        "mtime": mtime,
                        "id": segment_id,
                    }
                    segment_data.append(segment_info)
                    total_size += size
                else:
                    segment_data.append(
                        {
                            "name": segment,
                            "missing": True,
                            "id": (
                                int(segment.split("_")[-1].split(".")[0])
                                if "_" in segment
                                else 0
                            ),
                        }
                    )

            # Tri des segments par ID
            segment_data.sort(key=lambda x: x.get("id", 0))

            # Mettre à jour last_segment_time si un segment récent a été détecté
            if most_recent_mtime > 0:
                current_time = time.time()
                segment_age = current_time - most_recent_mtime
                
                # Si un segment récent a été détecté, mise à jour du timestamp 
                if segment_age < CRASH_THRESHOLD:
                    logger.debug(f"[{self.name}] ✅ Segment récent trouvé (âge: {segment_age:.1f}s), mise à jour du last_segment_time")
                    self.last_segment_time = current_time

            # Détection des sauts de segments
            jumps = []
            for i in range(1, len(segment_data)):
                current_id = segment_data[i].get("id", 0)
                prev_id = segment_data[i - 1].get("id", 0)
                if current_id - prev_id > 5:  # Saut de plus de 5 segments
                    jumps.append(
                        {"from": prev_id, "to": current_id, "gap": current_id - prev_id}
                    )
                    logger.warning(
                        f"[{self.name}] 🚨 Saut de segment détecté: {prev_id} → {current_id} (saut de {current_id - prev_id})"
                    )

            # Log des informations
            current_time = time.strftime("%Y-%m-%d %H:%M:%S")
            log_entry = f"{current_time} - Segments actifs: {len(segments)}, Taille totale: {total_size/1024:.1f}KB\n"

            for seg in segment_data:
                if "missing" in seg:
                    log_entry += f"  - {seg['name']} (MANQUANT)\n"
                else:
                    mtime_str = time.strftime("%H:%M:%S", time.localtime(seg["mtime"]))
                    log_entry += f"  - {seg['name']} (ID: {seg['id']}, Taille: {seg['size']/1024:.1f}KB, Modifié: {mtime_str})\n"

            if jumps:
                log_entry += f"  - SAUTS DÉTECTÉS: {len(jumps)}\n"
                for jump in jumps:
                    log_entry += f"    * Saut de {jump['from']} à {jump['to']} (gap: {jump['gap']})\n"

            with open(segment_log_path, "a") as f:
                f.write(log_entry)
                f.write("-" * 80 + "\n")

            return {
                "success": True,
                "count": len(segments),
                "segments": segment_data,
                "total_size": total_size,
                "jumps": jumps,
            }

        except Exception as e:
            logger.error(f"[{self.name}] Erreur analyse segments: {e}")
            return {"success": False, "error": str(e), "segments": []}

    def _handle_timeouts(self, current_time=None, crash_threshold=None):
        """
        Gère les timeouts et redémarre le stream si nécessaire

        Args:
            current_time: Temps actuel (calculé automatiquement si None)
            crash_threshold: Seuil en secondes pour considérer un crash (utilise CRASH_THRESHOLD si None)

        Returns:
            bool: True si action entreprise, False sinon
        """
        # Si pas de temps fourni, on prend le temps actuel
        if current_time is None:
            current_time = time.time()

        # Utiliser le seuil fourni ou celui de l'environnement
        if crash_threshold is None:
            crash_threshold = CRASH_THRESHOLD

        # On vérifie que last_segment_time existe
        if not hasattr(self, "last_segment_time"):
            self.last_segment_time = current_time
            return False

        # On vérifie si on a un timeout de segments
        time_since_last_segment = current_time - self.last_segment_time

        if time_since_last_segment > crash_threshold:
            logger.error(
                f"[{self.name}] 🔥 Pas de nouveau segment depuis {time_since_last_segment:.1f}s "
                f"(seuil: {crash_threshold}s)"
            )

            # Vérifier l'état du processus FFmpeg
            if not self.process_manager.is_running():
                logger.warning(f"[{self.name}] ⚠️ Processus FFmpeg déjà arrêté")
                # On recrée le stream seulement s'il y a des viewers
                if hasattr(self, "watchers_count") and self.watchers_count > 0:
                    logger.info(
                        f"[{self.name}] 🔄 Redémarrage du stream (viewers actifs: {self.watchers_count})"
                    )
                    if self._restart_stream():
                        self.error_handler.reset()
                        return True
                return False

            # Analyse des segments actuels pour diagnostiquer
            hls_dir = f"/app/hls/{self.name}"
            segments_info = self._check_segments(hls_dir)

            # Détection des causes possibles
            if segments_info.get("success"):
                jumps = segments_info.get("jumps", [])
                if jumps and len(jumps) > 0:
                    logger.error(
                        f"[{self.name}] 🚨 Problème possible: sauts de segments détectés ({len(jumps)})"
                    )
                    # On signale les sauts pour le monitoring
                    self.report_segment_jump(jumps[0]["from"], jumps[0]["to"])

            # On utilise l'error handler pour gérer les redémarrages
            if self.error_handler.add_error("segment_timeout"):
                logger.info(f"[{self.name}] 🔄 Redémarrage après timeout de segment")
                if self._restart_stream():
                    self.error_handler.reset()
                    return True

            return True

        return False

    def report_segment_jump(self, prev_segment: int, curr_segment: int):
        """
        Gère les sauts détectés dans les segments HLS avec historique et prise de décision

        Args:
            prev_segment: Le segment précédent
            curr_segment: Le segment actuel (avec un saut)
        """
        try:
            jump_size = curr_segment - prev_segment

            # On ne s'inquiète que des sauts importants
            if jump_size <= 3:  # Tolérance pour quelques segments perdus
                return

            logger.warning(
                f"[{self.name}] 🚨 Saut de segment détecté: {prev_segment} → {curr_segment} (delta: {jump_size})"
            )

            # On stocke l'historique des sauts
            if not hasattr(self, "jump_history"):
                self.jump_history = []

            # Ajout du saut à l'historique avec timestamp
            current_time = time.time()
            self.jump_history.append(
                {
                    "time": current_time,
                    "from": prev_segment,
                    "to": curr_segment,
                    "gap": jump_size,
                }
            )

            # On ne garde que les 10 derniers sauts
            if len(self.jump_history) > 10:
                self.jump_history = self.jump_history[-10:]

            # Analyse des sauts récents (5 dernières minutes)
            recent_jumps = [
                j for j in self.jump_history if current_time - j["time"] < 300
            ]

            # Détection des schémas de sauts
            if len(recent_jumps) >= 3:
                # Sauts de taille similaire (± 20%)
                similar_sized_jumps = []
                for jump in recent_jumps:
                    similar = [
                        j
                        for j in recent_jumps
                        if abs(j["gap"] - jump["gap"]) / max(1, jump["gap"]) < 0.2
                    ]
                    if len(similar) >= 3:
                        similar_sized_jumps = similar
                        break

                # Si on a des sauts similaires, c'est probablement un problème systémique
                if similar_sized_jumps:
                    avg_gap = sum(j["gap"] for j in similar_sized_jumps) / len(
                        similar_sized_jumps
                    )
                    logger.error(
                        f"[{self.name}] 🔥 Problème systémique détecté: {len(similar_sized_jumps)} sauts "
                        f"avec gap moyen de {avg_gap:.1f} segments"
                    )

                    # On redémarre seulement s'il y a des viewers actifs
                    if hasattr(self, "watchers_count") and self.watchers_count > 0:
                        if self.error_handler.add_error(
                            f"segment_jumps_{int(avg_gap)}"
                        ):
                            logger.warning(
                                f"[{self.name}] 🔄 Redémarrage programmé après détection de sauts systémiques"
                            )
                            return self._restart_stream()
                    else:
                        logger.info(
                            f"[{self.name}] ℹ️ Pas de redémarrage après sauts: aucun viewer actif"
                        )

            return False

        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur gestion saut de segment: {e}")
            return False

    def _check_viewer_inactivity(self, current_time, timeout):
        """Vérifie l'inactivité des viewers et gère l'arrêt du stream"""
        if not self.process_manager.is_running():
            return False

        inactivity_duration = current_time - self.last_watcher_time

        if inactivity_duration > timeout + 60:
            logger.info(
                f"[{self.name}] ⚠️ Inactivité détectée: {inactivity_duration:.1f}s"
            )
            return True

        return False

    def _clean_processes(self):
        """Nettoie les processus en utilisant le ProcessManager"""
        try:
            self.process_manager.stop_process()
            logger.info(f"[{self.name}] 🧹 Nettoyage des processus terminé")
        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur nettoyage processus: {e}")

    def _check_and_move_invalid_files(self):
        """Vérifie les fichiers dans ready_to_stream et déplace les invalides vers ignored"""
        try:
            ready_dir = Path(self.video_dir) / "ready_to_stream"
            ignored_dir = Path(self.video_dir) / "ignored"
            ignored_dir.mkdir(exist_ok=True)

            for video in ready_dir.glob("*.mp4"):
                # On tente de valider le fichier avec ffprobe
                is_valid = verify_file_ready(video)

                if not is_valid:
                    # On déplace vers le dossier ignored
                    dest = ignored_dir / video.name
                    reason_file = ignored_dir / f"{video.stem}_reason.txt"

                    # Si le fichier de destination existe déjà, on le supprime
                    if dest.exists():
                        dest.unlink()

                    # On déplace le fichier
                    shutil.move(str(video), str(dest))

                    # On écrit la raison
                    with open(reason_file, "w") as f:
                        f.write(
                            f"Fichier ignoré le {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
                        )
                        f.write(
                            "Raison: Fichier invalide pour le streaming (vérification échouée)\n"
                        )

                    logger.warning(
                        f"[{self.name}] 🚫 Fichier {video.name} invalide déplacé vers ignored"
                    )

            return True
        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur vérification fichiers: {e}")
            return False

    def _verify_playlist(self):
        """Vérifie que le fichier playlist est valide"""
        try:
            playlist_path = Path(self.video_dir) / "_playlist.txt"
            if not playlist_path.exists():
                logger.error(f"[{self.channel_name}] ❌ _playlist.txt n'existe pas")
                return False

            with open(playlist_path, "r") as f:
                lines = f.readlines()

            if not lines:
                logger.error(f"[{self.name}] ❌ _playlist.txt est vide")
                return False

            valid_count = 0
            for i, line in enumerate(lines, 1):
                line = line.strip()
                if not line:
                    continue

                if not line.startswith("file"):
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

    def _monitor_playlist_transition(self):
        """Surveille la transition entre les fichiers de la playlist"""
        try:
            if not self.process_manager.is_running():
                return

            # Vérifier la playlist actuelle
            playlist_path = Path(self.video_dir) / "_playlist.txt"
            if not playlist_path.exists():
                logger.error(f"[{self.name}] ❌ Playlist introuvable")
                return

            # Lire la playlist
            with open(playlist_path, "r") as f:
                lines = f.readlines()
                playlist_files = [line.strip().split("'")[1] for line in lines if line.strip().startswith("file")]

            if not playlist_files:
                logger.error(f"[{self.name}] ❌ Playlist vide")
                return

            # Vérifier que tous les fichiers existent
            missing_files = []
            for file_path in playlist_files:
                if not Path(file_path).exists():
                    missing_files.append(file_path)

            if missing_files:
                logger.error(f"[{self.name}] ❌ Fichiers manquants dans la playlist: {missing_files}")
                # Recréer la playlist sans les fichiers manquants
                self._create_concat_file()
                return

            # Vérifier la position actuelle de lecture
            if hasattr(self, "last_logged_position"):
                current_position = self.last_logged_position
            else:
                return

            # Vérifier les erreurs segment dans les logs nginx
            hls_dir = Path(f"/app/hls/{self.name}")
            playlist_file = hls_dir / "playlist.m3u8"
            
            # Vérifier si on est proche de la fin d'un fichier
            for file_path in playlist_files:
                duration = get_accurate_duration(Path(file_path))
                if duration and current_position >= duration - 30:  # Augmenté à 30 secondes pour plus de marge
                    logger.info(f"[{self.name}] 🔄 Transition imminente vers le fichier suivant: {Path(file_path).name}")
                    
                    # Vérifier l'intégrité des segments récents
                    segments_info = self._check_segments(str(hls_dir))
                    has_segment_issues = False
                    
                    if segments_info.get("success"):
                        # Vérifier les sauts de segments
                        if segments_info.get("jumps"):
                            has_segment_issues = True
                            logger.warning(f"[{self.name}] 🚨 Sauts de segments détectés pendant la transition")
                        
                        # Vérifier s'il y a des segments manquants
                        missing_segments = [s for s in segments_info.get("segments", []) if s.get("missing", False)]
                        if missing_segments:
                            has_segment_issues = True
                            logger.warning(f"[{self.name}] 🚨 Segments manquants pendant la transition: {len(missing_segments)}")
                    
                    # Vérifier si on a des erreurs de DTS
                    if hasattr(self, "last_dts_error_time"):
                        if time.time() - self.last_dts_error_time < 60:  # Si on a eu une erreur DTS récente
                            has_segment_issues = True
                            logger.warning(f"[{self.name}] ⚠️ Erreur DTS récente détectée pendant la transition")
                    
                    # Si on a détecté des problèmes ou on est à moins de 5 secondes de la fin,
                    # on force un redémarrage propre pour éviter les saccades
                    if has_segment_issues or current_position >= duration - 5:
                        logger.warning(f"[{self.name}] 🔄 Redémarrage préventif du stream pour assurer une transition fluide")
                        if self.process_manager.is_running():
                            # On sauvegarde la playlist actuelle pour la restaurer après redémarrage
                            if playlist_file.exists():
                                try:
                                    playlist_backup = str(playlist_file) + ".backup"
                                    with open(playlist_file, "r") as src, open(playlist_backup, "w") as dest:
                                        dest.write(src.read())
                                except Exception as e:
                                    logger.error(f"[{self.name}] ❌ Erreur sauvegarde playlist: {e}")
                            
                            # Redémarrage du processus FFmpeg
                            self.process_manager.restart_process()
                            
                            # Ajout d'un délai pour s'assurer que FFmpeg a le temps de démarrer
                            time.sleep(3)
                            
                            # Forcer une mise à jour de la playlist
                            self._create_concat_file()
                            return
                    
                    # Forcer une mise à jour de la playlist
                    self._create_concat_file()
                    # Ajouter un délai pour s'assurer que FFmpeg a le temps de gérer la transition
                    time.sleep(3)  # Augmenté à 3 secondes
                    break

        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur surveillance transition playlist: {e}")

    def _segment_monitor_loop(self):
        """Boucle de surveillance des segments"""
        try:
            # La boucle s'exécute tant que le processus est en cours
            while self.process_manager.is_running():
                current_time = time.time()

                # Vérification des timeouts toutes les 10 secondes
                self._handle_timeouts(current_time)

                # Health check toutes les 30 secondes
                if not hasattr(self, "last_health_check") or current_time - self.last_health_check >= 30:
                    self.channel_health_check()
                    self.last_health_check = current_time

                # Vérification directe des segments dans le système de fichiers
                self._check_filesystem_segments()

                # Analyse des segments toutes les 30 secondes
                if (
                    not hasattr(self, "last_segment_check")
                    or current_time - self.last_segment_check >= 30
                ):
                    hls_dir = f"/app/hls/{self.name}"
                    segments_info = self._check_segments(hls_dir)
                    self.last_segment_check = current_time

                    # Si on a détecté des sauts, on les reporte
                    if segments_info.get("success") and segments_info.get("jumps"):
                        for jump in segments_info.get("jumps", []):
                            self.report_segment_jump(jump["from"], jump["to"])

                # Vérification de la transition playlist toutes les 5 secondes
                if (
                    not hasattr(self, "last_playlist_check")
                    or current_time - self.last_playlist_check >= 5
                ):
                    self._monitor_playlist_transition()
                    self.last_playlist_check = current_time

                # On attend un peu avant la prochaine vérification
                time.sleep(1)

        except Exception as e:
            logger.error(
                f"[{self.name}] ❌ Erreur dans la boucle de surveillance des segments: {e}"
            )

    def _check_filesystem_segments(self):
        """
        Vérifie directement les fichiers segment sur le disque pour détecter les créations récentes
        indépendamment de la playlist. Cela résout les problèmes de détection de timeouts.
        """
        try:
            current_time = time.time()
            
            # Ne vérifier que toutes les 5 secondes pour limiter les accès disque
            if hasattr(self, "last_fs_check") and current_time - self.last_fs_check < 5:
                return
                
            self.last_fs_check = current_time
            
            # Chemin du dossier HLS
            hls_path = Path(f"/app/hls/{self.name}")
            if not hls_path.exists():
                return
                
            # Trouver tous les segments .ts
            segments = list(hls_path.glob("segment_*.ts"))
            if not segments:
                return
                
            # Trouver le segment le plus récent
            most_recent = max(segments, key=lambda s: s.stat().st_mtime)
            if not most_recent:
                return
                
            # Vérifier l'âge du segment le plus récent
            mtime = most_recent.stat().st_mtime
            segment_age = current_time - mtime
            
            # Plus permissif: considérer un segment comme récent si son âge est < CRASH_THRESHOLD * 3
            # au lieu de CRASH_THRESHOLD
            relaxed_threshold = CRASH_THRESHOLD * 3
            
            # Si le segment est récent, mettre à jour le timestamp
            if segment_age < relaxed_threshold:
                # Utiliser debug_fs=True pour éviter de spammer les logs
                debug_fs = getattr(self, 'debug_fs', False)
                if debug_fs or len(segments) % 10 == 0:  # Uniquement log tous les 10 segments ou en mode debug
                    logger.debug(
                        f"[{self.name}] 🆕 Segment récent trouvé: {most_recent.name} "
                        f"(âge: {segment_age:.1f}s, seuil: {relaxed_threshold}s, total segments: {len(segments)})"
                    )
                
                # Toujours mettre à jour le timestamp de segment pour éviter les faux timeouts
                self.last_segment_time = current_time
                
        except Exception as e:
            logger.debug(f"[{self.name}] Erreur vérification FS segments: {e}")
            
    def _restart_stream(self) -> bool:
        """Redémarre le stream en cas de problème avec une meilleure gestion de la continuité"""
        try:
            logger.info(f"🔄 Redémarrage du stream {self.name}")

            # Ajoute un délai aléatoire pour éviter les redémarrages en cascade
            jitter = random.uniform(0.5, 2.0)
            time.sleep(jitter)

            elapsed = time.time() - getattr(self, "last_restart_time", 0)
            if elapsed < self.error_handler.restart_cooldown:
                logger.info(
                    f"⏳ Attente du cooldown ({self.error_handler.restart_cooldown - elapsed:.1f}s)"
                )
                time.sleep(self.error_handler.restart_cooldown - elapsed)

            self.last_restart_time = time.time()

            # 1. Arrêter proprement le streaming séquentiel actuel
            if hasattr(self, "_streaming_active") and self._streaming_active:
                logger.info(f"[{self.name}] 🛑 Arrêt du streaming séquentiel actuel")
                self._streaming_active = False
                
                # Attendre que le thread de streaming se termine
                if hasattr(self, "_streaming_thread") and self._streaming_thread.is_alive():
                    self._streaming_thread.join(timeout=5)
                    logger.info(f"[{self.name}] ✅ Thread de streaming arrêté")

            # 2. Arrêter le processus FFmpeg en cours si présent
            if hasattr(self, "_current_process") and self._current_process:
                try:
                    self._current_process.terminate()
                    self._current_process.wait(timeout=5)
                    logger.info(f"[{self.name}] ✅ Processus FFmpeg arrêté")
                except Exception as e:
                    logger.warning(f"[{self.name}] ⚠️ Erreur arrêt processus FFmpeg: {e}")
                    try:
                        self._current_process.kill()
                    except:
                        pass

            # 3. Nettoyer le dossier HLS
            hls_dir = Path(f"/app/hls/{self.name}")
            if hls_dir.exists():
                for old_file in hls_dir.glob("*.ts"):
                    try:
                        old_file.unlink()
                    except Exception as e:
                        logger.debug(
                            f"[{self.name}] Impossible de supprimer {old_file}: {e}"
                        )

                # Supprimer aussi l'ancienne playlist
                old_playlist = hls_dir / "playlist.m3u8"
                if old_playlist.exists():
                    try:
                        old_playlist.unlink()
                    except Exception:
                        pass

            # 4. Vérifier que nous avons des fichiers valides
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

            # 5. Lancer un nouveau stream frais
            return self.start_stream()

        except Exception as e:
            logger.error(f"Erreur lors du redémarrage de {self.name}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def stop_stream_if_needed(self):
        """Arrête le stream si nécessaire"""
        try:
            # Arrêter le streaming séquentiel si actif
            if hasattr(self, "_streaming_active") and self._streaming_active:
                logger.info(f"[{self.name}] 🛑 Arrêt du streaming séquentiel")
                self._streaming_active = False
                
                # Attendre que le thread de streaming se termine
                if hasattr(self, "_streaming_thread") and self._streaming_thread.is_alive():
                    self._streaming_thread.join(timeout=5)
                    logger.info(f"[{self.name}] ✅ Thread de streaming arrêté")

            # Arrêter le processus FFmpeg en cours si présent
            if hasattr(self, "_current_process") and self._current_process:
                try:
                    self._current_process.terminate()
                    self._current_process.wait(timeout=5)
                    logger.info(f"[{self.name}] ✅ Processus FFmpeg arrêté")
                except Exception as e:
                    logger.warning(f"[{self.name}] ⚠️ Erreur arrêt processus FFmpeg: {e}")
                    try:
                        self._current_process.kill()
                    except:
                        pass

            # Nettoyer le dossier HLS
            hls_dir = Path(f"/app/hls/{self.name}")
            if hls_dir.exists():
                for seg in hls_dir.glob("*.ts"):
                    try:
                        seg.unlink()
                    except Exception as e:
                        logger.warning(f"[{self.name}] ⚠️ Erreur suppression segment {seg.name}: {e}")

                # Supprimer la playlist
                playlist = hls_dir / "playlist.m3u8"
                if playlist.exists():
                    try:
                        playlist.unlink()
                    except Exception as e:
                        logger.warning(f"[{self.name}] ⚠️ Erreur suppression playlist: {e}")

            logger.info(f"[{self.name}] ✅ Stream arrêté avec succès")
            return True

        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur arrêt stream: {e}")
            return False

    def start_stream_if_needed(self):
        """Démarre le stream uniquement s'il n'est pas déjà en cours"""
        try:
            # Vérifier si le streaming séquentiel est déjà actif
            if hasattr(self, "_streaming_active") and self._streaming_active:
                logger.info(f"[{self.name}] ✓ Streaming séquentiel déjà actif, aucune action requise")
                return True

            # Vérifier si la chaîne est prête avant de démarrer
            if not self.ready_for_streaming:
                # Vérifier si des vidéos sont disponibles même si ready_for_streaming est False
                ready_dir = Path(self.video_dir) / "ready_to_stream"
                has_videos = False

                if ready_dir.exists():
                    video_files = list(ready_dir.glob("*.mp4"))
                    has_videos = len(video_files) > 0

                    if has_videos:
                        logger.info(
                            f"[{self.name}] 🔍 Vidéos détectées dans ready_to_stream mais ready_for_streaming=False, tentative d'activation"
                        )
                        self.ready_for_streaming = True
                        # Recalcul de la durée totale en arrière-plan
                        threading.Thread(
                            target=self._calculate_total_duration, daemon=True
                        ).start()
                    else:
                        logger.warning(
                            f"[{self.name}] ⚠️ Aucune vidéo disponible, démarrage impossible"
                        )
                        return False
                else:
                    logger.warning(
                        f"[{self.name}] ⚠️ Dossier ready_to_stream inexistant, démarrage impossible"
                    )
                    return False

            # Vérifier si nous avons des fichiers MP4 dans ready_to_stream
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

            # Tout est bon, on démarre le stream
            logger.info(f"[{self.name}] 🚀 Démarrage automatique du streaming séquentiel")
            return self.start_stream()

        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur start_stream_if_needed: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def channel_health_check(self):
        """Checks if the channel is healthy and running, restarts if needed"""
        try:
            # Increment health check counter for logging
            if not hasattr(self, "_health_check_count"):
                self._health_check_count = 0
            self._health_check_count += 1
            
            # Add startup grace period tracking
            if not hasattr(self, "_startup_time"):
                self._startup_time = time.time()
            
            current_time = time.time()
            startup_duration = current_time - self._startup_time
            
            logger.info(f"[{self.name}] 🩺 Health check #{self._health_check_count} - Stream running: {self.process_manager.is_running()}, Watchers: {getattr(self, 'watchers_count', 0)}, Startup duration: {startup_duration:.1f}s")
            
            # 1. Check if the process is running
            if not self.process_manager.is_running():
                logger.warning(f"[{self.name}] ⚠️ Stream process not running but should be (watchers: {getattr(self, 'watchers_count', 0)})")
                
                # If we have active watchers, restart the stream
                if hasattr(self, "watchers_count") and self.watchers_count > 0:
                    logger.info(f"[{self.name}] 🔄 Auto-restarting stream due to active watchers ({self.watchers_count})")
                    return self.start_stream()
                return False
            
            # 2. Check if HLS directory has segments and they're recent
            hls_dir = Path(f"/app/hls/{self.name}")
            segments = list(hls_dir.glob("*.ts")) if hls_dir.exists() else []
            
            # Check playlist.m3u8 with startup grace period
            playlist_path = hls_dir / "playlist.m3u8"
            if not playlist_path.exists():
                # During startup grace period, be more lenient with playlist generation
                if startup_duration < 5:  # Réduit de 15s à 5s
                    logger.info(f"[{self.name}] ℹ️ Still in startup grace period ({startup_duration:.1f}s), waiting for playlist.m3u8...")
                    return True
                else:
                    logger.error(f"[{self.name}] ❌ playlist.m3u8 introuvable après {startup_duration:.1f}s")
            
            if not segments:
                logger.warning(f"[{self.name}] ⚠️ No segments found even though process is running")
                
                # Check if playlist.m3u8 exists and is valid
                if playlist_path.exists():
                    try:
                        with open(playlist_path, 'r') as f:
                            playlist_content = f.read()
                            if "#EXTM3U" in playlist_content:
                                logger.info(f"[{self.name}] ℹ️ playlist.m3u8 exists and is valid, waiting for segments...")
                                return True
                    except Exception as e:
                        logger.error(f"[{self.name}] ❌ Error reading playlist.m3u8: {e}")
                
                # During startup grace period, be more lenient
                if startup_duration < 5:  # Réduit de 15s à 5s
                    logger.info(f"[{self.name}] ℹ️ Still in startup grace period ({startup_duration:.1f}s), waiting for segments...")
                    return True
                
                # If we have active watchers and outside grace period, restart the stream
                if hasattr(self, "watchers_count") and self.watchers_count > 0:
                    logger.info(f"[{self.name}] 🔄 Auto-restarting stream due to missing segments")
                    return self.start_stream()
                return False

            # 3. Check for segment continuity and missing segments
            if playlist_path.exists():
                try:
                    # Analyser le contenu de la playlist pour détecter les segments référencés
                    with open(playlist_path, "r") as f:
                        playlist_content = f.readlines()
                    
                    referenced_segments = []
                    for line in playlist_content:
                        if line.strip().endswith(".ts"):
                            segment_name = line.strip()
                            referenced_segments.append(segment_name)
                    
                    # Vérifier que chaque segment référencé existe bien
                    missing_segments = []
                    for segment_name in referenced_segments:
                        segment_path = hls_dir / segment_name
                        if not segment_path.exists():
                            missing_segments.append(segment_name)
                    
                    # Si des segments référencés sont manquants, signaler l'anomalie
                    if missing_segments and len(missing_segments) > 0:
                        # Si plus de 30% des segments sont manquants, c'est un problème critique
                        missing_percent = (len(missing_segments) / len(referenced_segments)) * 100
                        logger.warning(f"[{self.name}] 🚨 Segments manquants: {len(missing_segments)}/{len(referenced_segments)} ({missing_percent:.1f}%)")
                        
                        if missing_percent > 30 and hasattr(self, "watchers_count") and self.watchers_count > 0:
                            logger.error(f"[{self.name}] ❌ Trop de segments manquants ({missing_percent:.1f}%), redémarrage du stream")
                            self.stop_stream_if_needed()
                            time.sleep(2)
                            return self.start_stream()
                except Exception as e:
                    logger.error(f"[{self.name}] ❌ Erreur analyse playlist: {e}")
            
            # Check segment age with different thresholds based on startup phase
            if segments:
                # Extract segment numbers and sort them
                segment_numbers = []
                for seg in segments:
                    try:
                        # Extract number from filename (e.g., "segment_123.ts" -> 123)
                        num = int(seg.stem.split('_')[1])
                        segment_numbers.append((num, seg))
                    except (ValueError, IndexError):
                        continue
                
                if segment_numbers:
                    # Sort by segment number
                    segment_numbers.sort(key=lambda x: x[0])
                    
                    # Get the latest segment number and file
                    latest_num, latest_segment = segment_numbers[-1]
                    
                    # Vérifier les sauts de numérotation des segments
                    segment_jumps = []
                    for i in range(1, len(segment_numbers)):
                        current_num = segment_numbers[i][0]
                        prev_num = segment_numbers[i-1][0]
                        gap = current_num - prev_num
                        if gap > 2:  # Considérer un saut si l'écart est > 2
                            segment_jumps.append((prev_num, current_num, gap))
                    
                    # Si on détecte des sauts importants dans la numérotation des segments récents
                    if segment_jumps:
                        jump_str = ", ".join([f"{prev}->{curr} (gap:{gap})" for prev, curr, gap in segment_jumps])
                        logger.warning(f"[{self.name}] 🚨 Sauts de segments détectés: {jump_str}")
                        
                        # Si on a des watchers et des sauts importants, redémarrer
                        if hasattr(self, "watchers_count") and self.watchers_count > 0:
                            # Vérifier si les sauts sont récents (parmi les derniers segments)
                            recent_jumps = [j for j in segment_jumps if j[1] >= latest_num - 10]
                            if recent_jumps:
                                logger.error(f"[{self.name}] ❌ Sauts récents détectés, redémarrage du stream")
                                self.stop_stream_if_needed()
                                time.sleep(2)
                                return self.start_stream()
                    
                    # Check if we have a reasonable number of segments
                    if len(segment_numbers) >= 2:
                        # Get the second latest segment
                        second_latest_num, second_latest_segment = segment_numbers[-2]
                        
                        # Calculate the gap between segments
                        gap = latest_num - second_latest_num
                        
                        # If gap is reasonable (1 or 2), use the latest segment
                        if gap <= 2:
                            segment_age = current_time - latest_segment.stat().st_mtime
                        else:
                            # If gap is too large, use the second latest segment
                            segment_age = current_time - second_latest_segment.stat().st_mtime
                    else:
                        # If we only have one segment, use it
                        segment_age = current_time - latest_segment.stat().st_mtime
                    
                    # More lenient threshold during startup
                    threshold = 60 if startup_duration < 60 else 120
                    
                    if segment_age > threshold:
                        logger.warning(f"[{self.name}] ⚠️ Segments too old (last: {segment_age:.1f}s ago, threshold: {threshold}s)")
                        
                        # During startup grace period, be more lenient
                        if startup_duration < 120:  # Extended from 60s to 120s
                            logger.info(f"[{self.name}] ℹ️ Still in startup grace period ({startup_duration:.1f}s), waiting for segments...")
                            return True
                        
                        # Restart if there are watchers and outside grace period
                        # Ajout d'une vérification supplémentaire: uniquement redémarrer si les segments sont vraiment vieux
                        if hasattr(self, "watchers_count") and self.watchers_count > 0 and segment_age > threshold * 1.5:  # 50% plus vieux que le seuil
                            logger.info(f"[{self.name}] 🔄 Auto-restarting stream due to stale segments")
                            self.stop_stream_if_needed()
                            time.sleep(2)
                            return self.start_stream()
            
            return True
            
        except Exception as e:
            logger.error(f"[{self.name}] ❌ Error in health check: {e}")
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
                logger.error(
                    f"[{self.name}] ❌ Permissions insuffisantes sur {video_dir}"
                )
                return False

            ready_to_stream_dir = video_dir / "ready_to_stream"
            if not ready_to_stream_dir.exists():
                try:
                    ready_to_stream_dir.mkdir(parents=True, exist_ok=True)
                except Exception as e:
                    logger.error(
                        f"[{self.name}] ❌ Impossible de créer {ready_to_stream_dir}: {e}"
                    )
                    return False

            logger.debug(f"[{self.name}] ✅ VideoProcessor correctement initialisé")
            return True

        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur vérification VideoProcessor: {e}")
            return False

    def _handle_process_died(self, exit_code: int, stderr: str):
        """Gère la mort du processus FFmpeg"""
        try:
            # Analyser l'erreur
            error_type = self._analyze_error(stderr)
            
            # Utiliser la nouvelle gestion d'erreurs intégrée
            if self.add_error(error_type):
                logger.warning(f"[{self.name}] Redémarrage nécessaire après erreur: {error_type}")
                self._restart_stream()
            elif self.has_critical_errors():
                logger.error(f"[{self.name}] Erreurs critiques détectées, arrêt du stream")
                self.stop_stream()
                
        except Exception as e:
            logger.error(f"[{self.name}] Erreur lors de la gestion de la mort du processus: {e}")

    def _check_stream_health(self) -> bool:
        """Vérifie la santé du stream"""
        try:
            if not self.process_manager.is_running():
                if self.add_error("process_not_running"):
                    logger.warning(f"[{self.name}] Processus FFmpeg arrêté, redémarrage nécessaire")
                    self._restart_stream()
                return False

            # Vérifier les erreurs critiques
            if self.has_critical_errors():
                logger.error(f"[{self.name}] Erreurs critiques détectées")
                self.stop_stream()
                return False

            # Vérifier la continuité du stream
            if not self._check_stream_continuity():
                if self.add_error("stream_discontinuity"):
                    logger.warning(f"[{self.name}] Discontinuité détectée, redémarrage nécessaire")
                    self._restart_stream()
                return False

            # Si tout est OK, reset les erreurs
            self.reset_errors()
            return True

        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur check santé: {e}")
            return False

    def _handle_segment_created(self, segment_path, size):
        """Gère la création d'un nouveau segment HLS"""
        if self.logger:
            self.logger.log_segment(segment_path, size)

        # MAJ des stats de segments
        if hasattr(self, "stats_collector"):
            # Extraction de l'ID du segment depuis le nom
            segment_id = Path(segment_path).stem.split("_")[-1]
            self.stats_collector.update_segment_stats(self.name, segment_id, size)
        
        # Mise à jour du timestamp du dernier segment
        self.last_segment_time = time.time()
        logger.debug(f"[{self.name}] ⏱️ Timestamp de segment mis à jour suite à création de {Path(segment_path).name}")

    def _scan_videos(self) -> bool:
        """Scanne les fichiers vidéos et met à jour processed_videos"""
        try:
            source_dir = Path(self.video_dir)
            ready_to_stream_dir = source_dir / "ready_to_stream"

            # Vérifier d'abord le processeur
            if not self._verify_processor():
                logger.error(f"[{self.name}] ❌ Échec de la vérification du processeur")
                self.ready_for_streaming = False
                return False

            # Création du dossier s'il n'existe pas
            ready_to_stream_dir.mkdir(exist_ok=True)

            # Vérifier et déplacer les fichiers invalides dans "ready_to_stream"
            self._check_and_move_invalid_files()

            # On réinitialise la liste des vidéos traitées
            old_processed = self.processed_videos
            self.processed_videos = []

            # On scanne d'abord les vidéos dans ready_to_stream
            mp4_files = list(ready_to_stream_dir.glob("*.mp4"))

            if not mp4_files:
                logger.warning(
                    f"[{self.name}] ⚠️ Aucun fichier MP4 dans {ready_to_stream_dir}"
                )

                # On vérifie s'il y a des fichiers à traiter
                video_extensions = (".mp4", ".avi", ".mkv", ".mov", ".m4v")
                source_files = []
                for ext in video_extensions:
                    source_files.extend(source_dir.glob(f"*{ext}"))

                if not source_files:
                    logger.warning(
                        f"[{self.name}] ⚠️ Aucun fichier vidéo dans {self.video_dir}"
                    )
                    self.ready_for_streaming = False
                    return False

                logger.info(
                    f"[{self.name}] 🔄 {len(source_files)} fichiers sources à traiter"
                )
                self.ready_for_streaming = False
                return False

            # Log explicite des fichiers trouvés
            logger.info(
                f"[{self.name}] 🔍 {len(mp4_files)} fichiers MP4 trouvés dans ready_to_stream: {[f.name for f in mp4_files]}"
            )

            # Vérification que les fichiers sont valides
            valid_files = []
            for video_file in mp4_files:
                if verify_file_ready(video_file):
                    valid_files.append(video_file)
                else:
                    logger.warning(
                        f"[{self.name}] ⚠️ Fichier {video_file.name} ignoré car non valide"
                    )

            if valid_files:
                self.processed_videos.extend(valid_files)
                logger.info(
                    f"[{self.name}] ✅ {len(valid_files)} vidéos valides trouvées dans ready_to_stream"
                )

                # Vérifie si la liste a changé
                old_names = {f.name for f in old_processed}
                new_names = {f.name for f in valid_files}

                if old_names != new_names:
                    logger.info(f"[{self.name}] 🔄 Liste des vidéos modifiée:")
                    if old_names - new_names:
                        logger.info(f"   - Supprimées: {old_names - new_names}")
                    if new_names - old_names:
                        logger.info(f"   + Ajoutées: {new_names - old_names}")

                    # Mise à jour de la playlist
                    threading.Thread(
                        target=self._create_concat_file, daemon=True
                    ).start()

                # La chaîne est prête si on a des vidéos valides
                self.ready_for_streaming = True

                # Notifier le manager que cette chaîne est prête
                self._notify_manager_ready()

                return True
            else:
                logger.warning(
                    f"[{self.name}] ⚠️ Aucun fichier MP4 valide trouvé dans ready_to_stream"
                )
                self.ready_for_streaming = False
                return False

        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur scan des vidéos: {str(e)}")
            import traceback

            logger.error(f"[{self.name}] {traceback.format_exc()}")
            return False

    def _notify_manager_ready(self):
        """Notifie le manager que cette chaîne est prête pour le streaming"""
        try:
            # On cherche le manager dans le registre global des chaînes
            if hasattr(FFmpegProcessManager, "all_channels"):
                for name, channel in FFmpegProcessManager.all_channels.items():
                    if name == self.name:
                        # On met à jour le statut dans le manager
                        if hasattr(channel, "manager") and channel.manager:
                            with channel.manager.scan_lock:
                                channel.manager.channel_ready_status[self.name] = True
                            logger.info(f"[{self.name}] ✅ Statut 'prêt' mis à jour dans le manager")
                        break
        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur notification manager: {e}")

    def start_stream(self) -> bool:
        """
        Démarre le flux HLS pour cette chaîne via FFmpeg en mode séquentiel.
        """
        try:
            # 1) Vérifier qu'on a des vidéos prêtes
            if not self.ready_for_streaming:
                logger.warning(
                    f"[{self.name}] ⚠️ Chaîne non prête (pas de vidéos). Annulation du démarrage."
                )
                return False

            # 2) Nettoyer le dossier HLS (playlist/segments) avant de lancer FFmpeg
            hls_dir = Path(f"/app/hls/{self.name}")
            if hls_dir.exists():
                # Supprime d'abord les segments existants
                for seg in hls_dir.glob("*.ts"):
                    try:
                        seg.unlink()
                    except Exception as e:
                        logger.warning(
                            f"[{self.name}] Erreur suppression segment {seg.name}: {e}"
                        )

                # Supprime l'ancienne playlist
                old_playlist = hls_dir / "playlist.m3u8"
                if old_playlist.exists():
                    try:
                        old_playlist.unlink()
                    except Exception as e:
                        logger.warning(
                            f"[{self.name}] Erreur suppression playlist: {e}"
                        )
            else:
                hls_dir.mkdir(parents=True, exist_ok=True)

            # 3) Démarrer le thread de streaming séquentiel
            if not hasattr(self, "_streaming_thread") or not self._streaming_thread.is_alive():
                self._streaming_thread = threading.Thread(
                    target=self._stream_files_sequentially,
                    daemon=True
                )
                self._streaming_thread.start()
                logger.info(f"[{self.name}] 🔄 Thread de streaming séquentiel démarré")

            # 4) Démarrer la boucle de surveillance des segments en arrière-plan
            if not hasattr(self, "_segment_monitor_thread") or not self._segment_monitor_thread.is_alive():
                self._segment_monitor_thread = threading.Thread(
                    target=self._segment_monitor_loop,
                    daemon=True
                )
                self._segment_monitor_thread.start()
                logger.info(f"[{self.name}] 🔍 Boucle de surveillance des segments démarrée")

            logger.info(f"[{self.name}] ✅ Stream démarré avec succès.")
            return True

        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur start_stream: {e}")
            return False

    def _stream_files_sequentially(self):
        """
        Stream les fichiers MP4 un par un séquentiellement depuis le dossier ready_to_stream.
        Cette méthode s'exécute dans un thread dédié.
        """
        try:
            self._streaming_active = True
            error_count = 0
            max_errors = 3  # Nombre maximum d'erreurs consécutives
            
            while self._streaming_active:
                try:
                    # Vérifier le dossier ready_to_stream
                    ready_dir = Path(self.video_dir) / "ready_to_stream"
                    if not ready_dir.exists():
                        logger.error(f"[{self.name}] ❌ Dossier ready_to_stream introuvable")
                        time.sleep(1)
                        continue
                    
                    # Récupérer et vérifier les fichiers MP4
                    video_files = list(ready_dir.glob("*.mp4"))
                    if not video_files:
                        logger.error(f"[{self.name}] ❌ Aucun fichier MP4 dans ready_to_stream")
                        time.sleep(1)
                        continue
                    
                    # Vérifier que les fichiers sont valides
                    valid_files = []
                    for video_file in video_files:
                        if verify_file_ready(video_file):
                            valid_files.append(video_file)
                        else:
                            logger.warning(f"[{self.name}] ⚠️ Fichier {video_file.name} ignoré car non valide")
                    
                    if not valid_files:
                        logger.error(f"[{self.name}] ❌ Aucun fichier MP4 valide trouvé")
                        time.sleep(1)
                        continue
                    
                    # Mélanger les fichiers pour plus de variété
                    import random
                    random.shuffle(valid_files)
                    
                    logger.info(f"[{self.name}] 🎬 Début du streaming séquentiel avec {len(valid_files)} fichiers")
                    
                    # Streamer chaque fichier séquentiellement
                    for video_file in valid_files:
                        if not self._streaming_active:
                            logger.info(f"[{self.name}] ⏹️ Arrêt du streaming séquentiel demandé")
                            return
                            
                        retry_count = 0
                        max_retries = 2
                        while retry_count < max_retries:
                            logger.info(f"[{self.name}] 🎬 Début du streaming de {video_file.name} (tentative {retry_count + 1}/{max_retries})")
                            success = self._stream_single_file(video_file)
                            
                            if success:
                                error_count = 0  # Réinitialiser le compteur d'erreurs en cas de succès
                                logger.info(f"[{self.name}] ✅ Streaming de {video_file.name} terminé avec succès")
                                break
                            
                            retry_count += 1
                            if retry_count < max_retries:
                                logger.warning(f"[{self.name}] ⚠️ Échec du streaming, nouvelle tentative dans 1s...")
                                time.sleep(1)
                        
                        if not success and self._streaming_active:
                            logger.error(f"[{self.name}] ❌ Échec du streaming de {video_file.name} après {max_retries} tentatives")
                            error_count += 1
                            if error_count >= max_errors:
                                logger.warning(f"[{self.name}] ⚠️ Trop d'erreurs ({error_count}), redémarrage du streaming")
                                return self._restart_stream()
                    
                    # Si on arrive ici, on a fini la liste des fichiers, on recommence
                    logger.info(f"[{self.name}] 🔄 Fin de la liste des fichiers, redémarrage")
                    
                except Exception as e:
                    logger.error(f"[{self.name}] ❌ Erreur dans la boucle de streaming: {e}")
                    error_count += 1
                    if error_count >= max_errors:
                        logger.warning(f"[{self.name}] ⚠️ Trop d'erreurs ({error_count}), redémarrage du streaming")
                        return self._restart_stream()
                    time.sleep(1)
                    
        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur fatale dans le streaming séquentiel: {e}")
        finally:
            self._streaming_active = False
            logger.info(f"[{self.name}] ⏹️ Fin du streaming séquentiel")
    
    def _stream_single_file(self, file_path):
        """Stream un fichier unique avec ffmpeg"""
        try:
            hls_dir = Path(f"/app/hls/{self.name}")
            
            # Obtenir les fichiers de logs
            ffmpeg_log = Path(f"/app/logs/ffmpeg/{self.name}_ffmpeg.log")
            progress_log = Path(f"/app/logs/ffmpeg/{self.name}_progress.log")
            
            # S'assurer que les dossiers de logs existent
            ffmpeg_log.parent.mkdir(parents=True, exist_ok=True)
            progress_log.parent.mkdir(parents=True, exist_ok=True)

            # Vérifier la durée du fichier avant de commencer
            duration = get_accurate_duration(file_path)
            if not duration or duration <= 0:
                logger.error(f"[{self.name}] ❌ Durée invalide pour {file_path}")
                return False

            # Utiliser le FFmpegCommandBuilder pour construire la commande
            command = self.command_builder.build_command(
                input_file=file_path,
                output_dir=str(hls_dir),
                progress_file=str(progress_log)
            )
            
            # Log de la commande complète
            logger.info("=" * 80)
            logger.info(f"[{self.name}] 🚀 Démarrage streaming: {os.path.basename(file_path)}")
            logger.info(f"$ {' '.join(command)}")
            logger.info("=" * 80)
            
            # Variables pour le monitoring
            start_time = time.time()
            last_progress_time = start_time
            last_segment_count = 0
            stall_detected = False
            stall_start_time = None
            
            # Démarrer le processus avec redirection des logs
            with open(ffmpeg_log, "a") as log_file:
                self._current_process = subprocess.Popen(
                    command,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True,
                    bufsize=1
                )
                
                # Boucle de lecture des logs en temps réel avec monitoring
                while self._current_process.poll() is None:
                    current_time = time.time()
                    
                    # Vérifier le timeout global (5 minutes)
                    if current_time - start_time > 300:
                        logger.error(f"[{self.name}] ❌ Timeout global atteint (5 minutes)")
                        self._current_process.terminate()
                        return False
                    
                    # Lire stderr pour les logs FFmpeg
                    stderr_line = self._current_process.stderr.readline()
                    if stderr_line:
                        log_file.write(stderr_line)
                        log_file.flush()
                        
                        # Log les erreurs importantes
                        if "error" in stderr_line.lower():
                            logger.error(f"[{self.name}] FFmpeg: {stderr_line.strip()}")
                        elif "warning" in stderr_line.lower():
                            logger.warning(f"[{self.name}] FFmpeg: {stderr_line.strip()}")
                    
                    # Vérifier la progression toutes les 5 secondes
                    if current_time - last_progress_time >= 5:
                        # Compter les segments actuels
                        current_segments = len(list(hls_dir.glob("segment_*.ts")))
                        
                        # Détecter un stall si pas de nouveaux segments
                        if current_segments == last_segment_count:
                            # Ne considérer comme stall que si on a déjà généré un nombre raisonnable de segments
                            # et éviter les faux positifs au début du streaming
                            if current_segments >= 3:
                                if not stall_detected:
                                    stall_detected = True
                                    stall_start_time = current_time
                                    # Utiliser debug au lieu de warning pour réduire le bruit dans les logs
                                    logger.debug(f"[{self.name}] ℹ️ Stall potentiel dans la génération des segments")
                                
                                # Augmenter le délai de stall de 30s à 60s pour être moins strict
                                if current_time - stall_start_time > 60:
                                    logger.error(f"[{self.name}] ❌ Stall prolongé détecté, redémarrage")
                                    self._current_process.terminate()
                                    return False
                            else:
                                # Pour les streams qui démarrent, être plus patient
                                if not stall_detected:
                                    stall_detected = True
                                    stall_start_time = current_time
                                # Utiliser un délai encore plus long pour les débuts de stream (120s)
                                elif current_time - stall_start_time > 120:
                                    logger.warning(f"[{self.name}] ⚠️ Démarrage lent, segments insuffisants après 2 minutes")
                                    self._current_process.terminate()
                                    return False
                        else:
                            stall_detected = False
                            stall_start_time = None
                        
                        last_segment_count = current_segments
                        last_progress_time = current_time
                        
                        # Log de progression
                        logger.debug(f"[{self.name}] 📊 Progression: {current_segments} segments générés")
                
                # Vérifier le code de retour
                return_code = self._current_process.poll() or 0
                if return_code != 0:
                    logger.error(f"[{self.name}] ❌ Erreur FFmpeg ({return_code})")
                    return False
                
                # Vérifier que des segments ont été générés
                final_segments = len(list(hls_dir.glob("segment_*.ts")))
                if final_segments == 0:
                    logger.error(f"[{self.name}] ❌ Aucun segment généré")
                    return False
                
                logger.info(f"[{self.name}] ✅ Streaming terminé avec succès ({final_segments} segments)")
                return True

        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur streaming {file_path}: {e}")
            return False

    def _should_stop_stream(self):
        """Vérifie si le stream doit être arrêté"""
        # Ne pas arrêter le stream s'il n'y a pas d'erreurs, même sans watchers
        if not self.error_handler.has_critical_errors():
            return False
        
        # Arrêter uniquement en cas d'erreurs critiques
        return True

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

    def has_critical_errors(self) -> bool:
        """Vérifie si des erreurs critiques sont présentes"""
        # On considère qu'il y a une erreur critique si :
        # - On a eu trop de redémarrages
        if self.restart_count >= self.max_restarts:
            return True
            
        # - On a eu trop d'erreurs du même type
        if self.error_count >= 3 and len(self.error_types) == 1:
            return True
            
        # - On a eu plusieurs types d'erreurs différentes
        if len(self.error_types) >= 3:
            return True
            
        return False

    def add_error(self, error_type: str) -> bool:
        """Ajoute une erreur et retourne True si un restart est nécessaire"""
        self.error_count += 1
        self.error_types.add(error_type)
        
        logger.warning(f"[{self.name}] Erreur détectée: {error_type}, total: {self.error_count}")
        
        # Log le crash
        self._log_crash(error_type)

        # On regroupe les erreurs similaires
        if self.error_count >= 3 and len(self.error_types) >= 2:
            return self.should_restart()
        return False

    def should_restart(self) -> bool:
        """Vérifie si on peut/doit redémarrer"""
        if self.restart_count >= self.max_restarts:
            logger.error(f"[{self.name}] Trop de redémarrages")
            return False
            
        current_time = time.time()
        if current_time - self.last_restart_time < self.restart_cooldown:
            return False
            
        self.restart_count += 1
        self.last_restart_time = current_time
        
        # Log le redémarrage
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            with open(self.crash_log_path, "a") as f:
                f.write(f"{timestamp} - [{self.name}] Redémarrage #{self.restart_count}\n")
                f.write("-" * 80 + "\n")
        except Exception as e:
            logger.error(f"Erreur lors de l'écriture du log de redémarrage: {e}")
        
        self.error_count = 0
        self.error_types.clear()
        return True

    def reset_errors(self):
        """Reset après un stream stable"""
        if self.error_count > 0 or self.restart_count > 0:
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            try:
                with open(self.crash_log_path, "a") as f:
                    f.write(f"{timestamp} - [{self.name}] Reset des erreurs\n")
                    f.write("-" * 80 + "\n")
            except Exception as e:
                logger.error(f"Erreur lors de l'écriture du log de reset: {e}")
                
        self.error_count = 0
        self.restart_count = 0
        self.error_types.clear()
        return True

    def _log_crash(self, error_type: str):
        """Log des erreurs dans un fichier de crash dédié"""
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            with open(self.crash_log_path, "a") as f:
                f.write(f"{timestamp} - Erreur détectée: {error_type}\n")
                f.write(f"Compteur d'erreurs: {self.error_count}, Types d'erreurs: {', '.join(self.error_types)}\n")
                f.write("-" * 80 + "\n")
        except Exception as e:
            logger.error(f"Erreur écriture log crash pour {self.name}: {e}")

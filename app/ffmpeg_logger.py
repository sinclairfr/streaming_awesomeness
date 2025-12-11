# ffmpeg_logger.py
import logging
from pathlib import Path
import os
import datetime
import time
import re
import gc
from config import logger, handle_ffmpeg_error


class FFmpegLogger:
    """
    # On centralise la gestion des logs FFmpeg avec rotation automatique
    """

    def __init__(self, channel_name: str):
        self.channel_name = channel_name
        self.base_dir = Path("/app/logs/ffmpeg")
        self.base_dir.mkdir(parents=True, exist_ok=True)

        # Fichiers de logs
        self.main_log = self.base_dir / f"{channel_name}_ffmpeg.log"
        self.progress_log = self.base_dir / f"{channel_name}_progress.log"

        # Taille max des logs (5MB)
        self.max_log_size = 5 * 1024 * 1024

        # On vérifie/nettoie les logs existants
        self._init_logs()

    def _init_logs(self):
        """Initialise les logs et vérifie leur taille"""
        for log_file in [self.main_log, self.progress_log]:
            # Crée le fichier s'il n'existe pas
            if not log_file.exists():
                log_file.touch()
            else:
                # Vérifie la taille et applique rotation si nécessaire
                self._check_and_rotate_log(log_file)

    # MÉTHODE SUPPRIMÉE: _track_segment_size()
    # Cette méthode était redondante avec le tracking fait par StatsCollector
    # via l'analyse des logs Nginx. De plus, elle utilisait gc.get_objects()
    # ce qui est extrêmement inefficace.

    def log_segment(self, segment_path: str, size: int):
        """
        Log des infos sur les segments générés directement dans le log principal.

        Note: Le tracking des stats est maintenant géré par StatsCollector via
        l'analyse des logs Nginx, ce qui évite la duplication et l'utilisation
        coûteuse de gc.get_objects().
        """
        segment_info = (
            f"{datetime.datetime.now()} - Segment {segment_path}: {size} bytes"
        )

        # On utilise le logger principal plutôt qu'un fichier séparé
        logger.debug(f"[{self.channel_name}] {segment_info}")

        # SUPPRIMÉ: Le tracking via gc.get_objects() est inefficace et redondant
        # Les stats de segments sont maintenant calculées par StatsCollector
        # en analysant les logs Nginx, ce qui est plus fiable et performant

    def _check_and_rotate_log(self, log_file: Path):
        """Vérifie la taille d'un fichier log et fait une rotation si nécessaire"""
        try:
            if not log_file.exists():
                log_file.touch()
                logger.info(f"✅ Created log file: {log_file.name}")
            elif log_file.stat().st_size > self.max_log_size:
                # Format du timestamp
                timestamp = time.strftime("%Y%m%d_%H%M%S")

                # Nouveau nom avec timestamp
                backup_name = f"{log_file.stem}_{timestamp}{log_file.suffix}"
                backup_path = log_file.parent / backup_name

                # Copie le contenu actuel vers le backup
                import shutil
                shutil.copy2(log_file, backup_path)
                
                # Vide le fichier de log actuel
                log_file.write_text("")

                logger.info(
                    f"🔄 Rotation du log {log_file.name} -> {backup_name} (taille > {self.max_log_size/1024/1024:.1f}MB)"
                )

                # Limite le nombre d'archives (garde les 5 plus récentes)
                self._cleanup_old_logs(log_file.stem, log_file.suffix)

        except Exception as e:
            logger.error(f"❌ Erreur rotation log {log_file}: {e}")

    def _cleanup_old_logs(self, base_name: str, suffix: str):
        """Garde seulement les 5 fichiers de log les plus récents pour un type donné"""
        try:
            # Liste tous les fichiers de log archivés pour ce type
            pattern = f"{base_name}_*{suffix}"
            archived_logs = list(self.base_dir.glob(pattern))

            # Trie par date de modification (du plus récent au plus ancien)
            archived_logs.sort(key=lambda p: p.stat().st_mtime, reverse=True)

            # Supprime les logs les plus anciens (au-delà des 5 premiers)
            if len(archived_logs) > 5:
                for old_log in archived_logs[5:]:
                    try:
                        old_log.unlink()
                        logger.info(f"🗑️ Suppression de l'ancien log: {old_log.name}")
                    except Exception as e:
                        logger.error(f"❌ Erreur suppression {old_log.name}: {e}")

        except Exception as e:
            logger.error(f"❌ Erreur nettoyage des anciens logs: {e}")

    def get_progress_file(self) -> Path:
        """Renvoie le chemin du fichier de progression"""
        # Vérifie/effectue une rotation si nécessaire
        self._check_and_rotate_log(self.progress_log)
        return self.progress_log

    def get_main_log_file(self) -> Path:
        """Renvoie le chemin du log principal après vérification de taille"""
        # Vérifie/effectue une rotation si nécessaire
        self._check_and_rotate_log(self.main_log)
        return self.main_log
        
    def process_error_logs(self):
        """
        Traite les logs FFmpeg pour détecter et gérer les erreurs
        """
        try:
            # Vérifier que le fichier de log existe
            if not self.main_log.exists():
                return
                
            # Lire les 20 dernières lignes du log pour rechercher des erreurs récentes
            with open(self.main_log, 'r', encoding='utf-8', errors='ignore') as f:
                # Lire tout le fichier
                lines = f.readlines()
                # Prendre les 20 dernières lignes
                last_lines = lines[-20:] if len(lines) >= 20 else lines
                
            # Rechercher des patterns d'erreur dans les dernières lignes
            error_patterns = [
                r"Invalid data found when processing input",
                r"Could not find file",
                r"Error while decoding stream",
                r"corrupt.*?frame",
                r"Fichier d'entrée introuvable"
            ]

            # Patterns à ignorer (erreurs bénignes normales dans HLS)
            ignore_patterns = [
                r"failed to delete old segment.*No such file or directory",  # Normal: cleaner a déjà supprimé
                r"hls muxer.*failed to delete",  # Même chose, format variant
            ]

            for line in last_lines:
                # Ignorer les erreurs bénignes
                should_ignore = False
                for ignore_pattern in ignore_patterns:
                    if re.search(ignore_pattern, line, re.IGNORECASE):
                        should_ignore = True
                        break

                if should_ignore:
                    continue

                # Vérifier les vraies erreurs
                for pattern in error_patterns:
                    if re.search(pattern, line, re.IGNORECASE):
                        logger.warning(f"[{self.channel_name}] 🔍 Erreur FFmpeg détectée: {line.strip()}")
                        # Appeler le gestionnaire d'erreurs
                        handle_ffmpeg_error(self.channel_name, line)
                        return  # Une seule erreur à la fois suffit
                        
        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur traitement logs FFmpeg: {e}")
            
    def check_for_errors(self):
        """
        Vérifie les logs pour des erreurs et les traite si nécessaire.
        À appeler périodiquement.
        """
        # Vérifier la taille des logs avant traitement
        self._check_and_rotate_log(self.main_log)
        # Traiter les logs pour détecter les erreurs
        self.process_error_logs()

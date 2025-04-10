# ffmpeg_logger.py
import logging
from pathlib import Path
import os
import datetime
import time
from config import logger


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

    # Ajouter cette méthode à la classe FFmpegLogger dans ffmpeg_logger.py

    def _track_segment_size(self, segment_path: str, stats_collector=None):
        """Suit la taille des segments pour les statistiques"""
        try:
            # Vérifier que le fichier existe
            if not os.path.exists(segment_path):
                return

            # Obtenir la taille du segment
            segment_size = os.path.getsize(segment_path)

            # Loguer la taille dans le fichier de log
            self.log_segment(segment_path, segment_size)

            # Si on a un StatsCollector, mettre à jour les stats
            if stats_collector:
                # Extraire le nom de la chaîne et l'ID du segment
                segment_path_obj = Path(segment_path)
                channel_name = segment_path_obj.parent.name
                segment_id_match = re.search(
                    r"segment_(\d+)\.ts", segment_path_obj.name
                )

                if segment_id_match and channel_name:
                    segment_id = segment_id_match.group(1)
                    stats_collector.update_segment_stats(
                        channel_name, segment_id, segment_size
                    )
        except Exception as e:
            logger.error(f"❌ Erreur tracking segment {segment_path}: {e}")

    def log_segment(self, segment_path: str, size: int):
        """Log des infos sur les segments générés directement dans le log principal"""
        segment_info = (
            f"{datetime.datetime.now()} - Segment {segment_path}: {size} bytes"
        )

        # On utilise le logger principal plutôt qu'un fichier séparé
        logger.debug(f"[{self.channel_name}] {segment_info}")

        # Si on a accès au StatsCollector, mettre à jour les stats
        from iptv_manager import IPTVManager

        for manager_instance in [
            obj for obj in gc.get_objects() if isinstance(obj, IPTVManager)
        ]:
            if hasattr(manager_instance, "stats_collector"):
                self._track_segment_size(segment_path, manager_instance.stats_collector)

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

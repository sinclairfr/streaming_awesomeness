# config.py
import os
import logging
import time
import json
from pathlib import Path
import shutil

# Configuration des chemins
LOG_DIR = os.getenv("LOG_DIR", "/app/logs")
CONTENT_DIR = Path(
    os.getenv("CONTENT_DIR", "/mnt/videos/streaming_awesomeness/content")
)
CHANNELS_STATUS_FILE = os.getenv("CHANNELS_STATUS_FILE", "/app/stats/channels_status.json")
NGINX_ACCESS_LOG = "/app/logs/nginx/access.log"
SERVER_URL = os.getenv("SERVER_URL", "192.168.10.183")

# Configuration des timeouts
RESOURCES_CHECK_INTERVAL = int(os.getenv("RESOURCES_CHECK_INTERVAL", "60"))
CPU_CHECK_INTERVAL = float(os.getenv("CPU_CHECK_INTERVAL", "1"))
CPU_THRESHOLD = int(os.getenv("CPU_THRESHOLD", "95"))
FFMPEG_LOG_LEVEL = os.getenv("FFMPEG_LOG_LEVEL", "info")
FFMPEG_LOGS_DIR = os.getenv("FFMPEG_LOGS_DIR", "/app/logs/ffmpeg")
USE_GPU = os.getenv("USE_GPU", "false")
VIDEO_EXTENSIONS = os.getenv("VIDEO_EXTENSIONS", ".mp4,.avi,.mkv,.mov, .m4v").split(",")
SEGMENT_AGE_THRESHOLD = int(os.getenv("SEGMENT_AGE_THRESHOLD", "120"))
WATCHERS_LOG_CYCLE = int(os.getenv("WATCHERS_LOG_CYCLE", "300"))  # 5 minutes par défaut
SUMMARY_CYCLE = int(os.getenv("SUMMARY_CYCLE", "300"))  # 5 minutes par défaut
CRASH_THRESHOLD = int(os.getenv("CRASH_THRESHOLD", "30"))  # Seuil en secondes pour considérer un crash de stream (augmenté à 30s)

# Durée du segment HLS en secondes - utilisée à la fois par FFmpegCommandBuilder et TimeTracker
HLS_SEGMENT_DURATION = float(os.getenv("HLS_SEGMENT_DURATION", "5.0"))

# Timeout pour les viewers inactifs (basé sur la durée du segment HLS)
VIEWER_INACTIVITY_TIMEOUT = float(os.getenv("VIEWER_INACTIVITY_TIMEOUT", str(HLS_SEGMENT_DURATION * 1.2)))

def get_log_level(level_str: str) -> int:
    """Convertit un niveau de log en string vers sa valeur numérique"""
    valid_levels = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }
    # On retourne le niveau demandé ou INFO par défaut
    return valid_levels.get(level_str.upper(), logging.INFO)


logging.basicConfig(
    level=get_log_level(os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s - %(name)s - [%(levelname)s] - %(message)s",
    handlers=[
        logging.StreamHandler(),  # Pour afficher dans la console
        logging.FileHandler("/app/logs/app.log"),  # Pour sauvegarder dans un fichier
    ],
)
# On s'assure que tous les loggers utilisent le même niveau
logging.getLogger().setLevel(get_log_level(os.getenv("LOG_LEVEL", "INFO")))

# On configure le logger pour les crashs
logger = logging.getLogger(__name__)

# On désactive les logs de watchdog, quoi qu'il arrive
logging.getLogger("watchdog").setLevel(logging.WARNING)

# Nouveau mode de streaming: true = ancien système (concaténation), false = fichier par fichier
# Version simplifiée pour éviter les problèmes de type

def setup_log_rotation(log_dir="/app/logs", max_size_mb=5, max_backups=5):
    """Configure une rotation basique des logs"""
    try:
        log_dir_path = Path(log_dir)
        log_dir_path.mkdir(parents=True, exist_ok=True)

        # Vérifie et nettoie les logs existants
        app_log = log_dir_path / "app.log"
        if app_log.exists() and app_log.stat().st_size > max_size_mb * 1024 * 1024:
            # Format du timestamp
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            
            # Nouveau nom avec timestamp pour l'ancien log
            backup_name = f"app_{timestamp}.log"
            backup_path = log_dir_path / backup_name

            # Copie le contenu actuel vers le backup
            shutil.copy2(app_log, backup_path)
            
            # Vide le fichier app.log actuel
            app_log.write_text("")

            logger.info(f"🔄 Rotation du log app.log -> {backup_name}")

            # Nettoie les anciennes archives
            _cleanup_old_logs(log_dir_path, "app", ".log", max_backups)

        logger.info(f"✅ Configuration de rotation des logs terminée")
        return True
    except Exception as e:
        logger.error(f"Erreur setup_log_rotation: {e}")
        return False


def _cleanup_old_logs(dir_path, base_name, suffix, max_backups=5):
    """Nettoie les anciens fichiers de logs"""
    try:
        pattern = f"{base_name}_*{suffix}"
        archived_logs = list(dir_path.glob(pattern))

        # Trie par date de modification (du plus récent au plus ancien)
        archived_logs.sort(key=lambda p: p.stat().st_mtime, reverse=True)

        # Supprime les logs les plus anciens (au-delà de max_backups)
        if len(archived_logs) > max_backups:
            for old_log in archived_logs[max_backups:]:
                try:
                    old_log.unlink()
                    logger.info(f"🗑️ Suppression de l'ancien log: {old_log.name}")
                except Exception as e:
                    logger.error(f"Erreur suppression {old_log.name}: {e}")
    except Exception as e:
        logger.error(f"Erreur nettoyage des anciens logs: {e}")

# config.py
import os
import logging
from pathlib import Path

# Configuration des chemins
LOG_DIR = os.getenv("LOG_DIR", "/logs")
CONTENT_DIR = Path(os.getenv('CONTENT_DIR', '/mnt/videos/streaming_awesomeness/content'))
NGINX_ACCESS_LOG = os.getenv('NGINX_ACCESS_LOG', '/var/log/nginx/access.log')
SERVER_URL = os.getenv('SERVER_URL', '192.168.10.183')

# Configuration des timeouts
TIMEOUT_NO_VIEWERS = int(os.getenv('TIMEOUT_NO_VIEWERS', '3600'))
RESOURCES_CHECK_INTERVAL = int(os.getenv('RESOURCES_CHECK_INTERVAL', '60'))
CPU_CHECK_INTERVAL = float(os.getenv('CPU_CHECK_INTERVAL', '1'))
CPU_THRESHOLD = int(os.getenv('CPU_THRESHOLD', '95'))

FFMPEG_LOG_LEVEL = os.getenv('FFMPEG_LOG_LEVEL', 'info')
FFMPEG_LOGS_DIR = os.getenv('FFMPEG_LOGS_DIR', '/app/logs/ffmpeg')

USE_GPU = os.getenv('USE_GPU', 'false')

VIDEO_EXTENSIONS = os.getenv('VIDEO_EXTENSIONS', '.mp4,.avi,.mkv,.mov, .m4v').split(',')
SEGMENT_AGE_THRESHOLD = int(os.getenv('SEGMENT_AGE_THRESHOLD', '120'))

def get_log_level(level_str: str) -> int:
    """Convertit un niveau de log en string vers sa valeur numérique"""
    valid_levels = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }
    # On retourne le niveau demandé ou INFO par défaut
    return valid_levels.get(level_str.upper(), logging.INFO)

logging.basicConfig(
    level=get_log_level(os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s - %(name)s - [%(levelname)s] - %(message)s",
    handlers=[
        logging.StreamHandler(),  # Pour afficher dans la console
        logging.FileHandler("/app/logs/app.log")  # Pour sauvegarder dans un fichier
    ]
)
# On s'assure que tous les loggers utilisent le même niveau
logging.getLogger().setLevel(get_log_level(os.getenv("LOG_LEVEL", "INFO")))

# On configure le logger pour les crashs
logger = logging.getLogger(__name__)


def setup_log_rotation(log_dir="/app/logs", max_size_mb=5, max_backups=5):
    """Configure une rotation basique des logs"""
    try:
        log_dir_path = Path(log_dir)
        log_dir_path.mkdir(parents=True, exist_ok=True)
        
        # Vérifie et nettoie les logs existants
        for log_file in log_dir_path.glob("*.log"):
            try:
                if log_file.stat().st_size > max_size_mb * 1024 * 1024:
                    # Format du timestamp
                    timestamp = time.strftime("%Y%m%d_%H%M%S")
                    
                    # Nouveau nom avec timestamp
                    backup_name = f"{log_file.stem}_{timestamp}{log_file.suffix}"
                    backup_path = log_file.parent / backup_name
                    
                    # Rotation
                    log_file.rename(backup_path)
                    log_file.touch()
                    
                    logger.info(f"🔄 Rotation du log {log_file.name} -> {backup_name}")
                    
                    # Nettoie les anciennes archives
                    _cleanup_old_logs(log_file.parent, log_file.stem, log_file.suffix, max_backups)
            except Exception as e:
                # Continue même si erreur sur un fichier
                print(f"Erreur rotation log {log_file}: {e}")
                continue
        
        logger.info(f"✅ Configuration de rotation des logs terminée")
        return True
    except Exception as e:
        print(f"Erreur setup_log_rotation: {e}")
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
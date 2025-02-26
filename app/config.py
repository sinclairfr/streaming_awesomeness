# config.py
import os
import logging
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()


# Configuration des chemins
LOG_DIR = os.getenv("LOG_DIR", "/logs")
CONTENT_DIR = Path(os.getenv('CONTENT_DIR', '/mnt/videos/streaming_awesomeness/content'))
NGINX_ACCESS_LOG = os.getenv('NGINX_ACCESS_LOG', '/var/log/nginx/access.log')
SERVER_URL = os.getenv('SERVER_URL', '192.168.10.183')

# Configuration des timeouts
TIMEOUT_NO_VIEWERS = int(os.getenv('TIMEOUT_NO_VIEWERS', '120'))
RESOURCES_CHECK_INTERVAL = int(os.getenv('RESOURCES_CHECK_INTERVAL', '60'))
CPU_CHECK_INTERVAL = float(os.getenv('CPU_CHECK_INTERVAL', '1'))
CPU_THRESHOLD = int(os.getenv('CPU_THRESHOLD', '95'))

FFMPEG_LOG_LEVEL = os.getenv('FFMPEG_LOG_LEVEL', 'info')
FFMPEG_LOGS_DIR = os.getenv('FFMPEG_LOGS_DIR', '/app/logs/ffmpeg')

USE_GPU = os.getenv('USE_GPU', 'false')

VIDEO_EXTENSIONS = os.getenv('VIDEO_EXTENSIONS', '.mp4,.avi,.mkv,.mov').split(',')
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

crash_logger = logging.getLogger("CrashTimer")
if not crash_logger.handlers:
    crash_handler = logging.FileHandler("/app/logs/crash_timer.log")
    crash_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(message)s")
    )
    crash_logger.addHandler(crash_handler)
    crash_logger.setLevel(logging.INFO)
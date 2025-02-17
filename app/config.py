# config.py

# On configure le logging et définit les constantes globales
import os
import logging

# On définit les constantes
SERVER_URL = os.getenv("SERVER_URL", "192.168.10.183")
NGINX_ACCESS_LOG = os.getenv("NGINX_ACCESS_LOG", "/var/log/nginx/access.log")

# On définit les paramètres de normalisation attendus
NORMALIZATION_PARAMS = {
    "codec_name": "h264",
    "pix_fmt": "yuv420p",
    "r_frame_rate": "25/1",
    "profile": "baseline",
    "level": "3.0",
}
def get_log_level(level_str: str) -> str:
    """Valide et retourne un niveau de log valide"""
    valid_levels = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }
    
    # On nettoie la valeur d'entrée
    level_str = level_str.strip().upper()
    
    # On retourne le niveau valide ou INFO par défaut
    return valid_levels.get(level_str, logging.INFO)

# On configure le niveau de logs
logging.basicConfig(
    level=get_log_level(os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s - %(name)s - [%(levelname)s] - %(message)s",
)


logger = logging.getLogger(__name__)

# On configure un logger pour le minuteur de crash
crash_logger = logging.getLogger("CrashTimer")
if not crash_logger.handlers:
    crash_handler = logging.FileHandler("/app/logs/crash_timer.log")
    crash_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(message)s")
    )
    crash_logger.addHandler(crash_handler)
    crash_logger.setLevel(logging.INFO)

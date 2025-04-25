# config.py
import os
import logging
import time
import json
import re
import socket
from pathlib import Path
import shutil

# Configure logging first
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("config")

# Configuration des chemins
LOG_DIR = os.getenv("LOG_DIR", "/app/logs")
CONTENT_DIR = Path(
    os.getenv("CONTENT_DIR", "/mnt/frigate_data/streaming_awesomeness/content")
)
CHANNELS_STATUS_FILE = os.getenv("CHANNELS_STATUS_FILE", "/app/stats/channels_status.json")
NGINX_ACCESS_LOG = "/app/logs/nginx/access.log"

# Determine server URL - automatically detect IP if set to "auto"
def get_server_ip():
    """Get the server's IP address by connecting to Google DNS server."""
    try:
        # This doesn't actually establish a connection but gives us the IP we'd use to connect
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception as e:
        print(f"Failed to auto-detect IP address: {e}")
        return "192.168.10.183"  # Fallback IP

server_url_env = os.getenv("SERVER_URL", "auto")
if server_url_env.lower() == "auto":
    SERVER_URL = get_server_ip()
    print(f"Auto-detected SERVER_URL: {SERVER_URL}")
else:
    SERVER_URL = server_url_env

# Configuration des timeouts
HLS_SEGMENT_DURATION = float(os.getenv("HLS_SEGMENT_DURATION", "2.0"))
HLS_LIST_SIZE = int(os.getenv("HLS_LIST_SIZE", "10"))

# Calcul des intervalles basés sur HLS_SEGMENT_DURATION
RESOURCES_CHECK_INTERVAL = int(os.getenv("RESOURCES_CHECK_INTERVAL", str(int(HLS_SEGMENT_DURATION * 15))))  # 15 segments
CPU_CHECK_INTERVAL = float(os.getenv("CPU_CHECK_INTERVAL", str(HLS_SEGMENT_DURATION)))  # 1 segment

# Configuration des timeouts
CPU_THRESHOLD = int(os.getenv("CPU_THRESHOLD", "95"))
FFMPEG_LOG_LEVEL = os.getenv("FFMPEG_LOG_LEVEL", "info")
FFMPEG_LOGS_DIR = os.getenv("FFMPEG_LOGS_DIR", "/app/logs/ffmpeg")
USE_GPU = os.getenv("USE_GPU", "false")
VIDEO_EXTENSIONS = os.getenv("VIDEO_EXTENSIONS", ".mp4,.avi,.mkv,.mov, .m4v").split(",")
SEGMENT_AGE_THRESHOLD = int(os.getenv("SEGMENT_AGE_THRESHOLD", "120"))

# Multiplicateur pour le timeout basé sur la durée du segment
SEGMENT_TIMEOUT_MULTIPLIER = float(os.getenv("SEGMENT_TIMEOUT_MULTIPLIER", "1.2"))

# Timeout par défaut pour les playlists ou en cas de fallback
DEFAULT_ACTIVITY_TIMEOUT = float(os.getenv("DEFAULT_ACTIVITY_TIMEOUT", str(HLS_SEGMENT_DURATION * 5)))  # 5 segments

# Timeout pour considérer un utilisateur comme actif (en secondes)
ACTIVE_VIEWER_TIMEOUT = int(os.getenv("ACTIVE_VIEWER_TIMEOUT", str(int(HLS_SEGMENT_DURATION * 5))))  # 5 segments

# Configuration des intervalles de mise à jour
MONITORING_INTERVAL = int(os.getenv("MONITORING_INTERVAL", str(int(HLS_SEGMENT_DURATION * 7.5))))  # 7.5 segments
PLAYLIST_CHECK_INTERVAL = int(os.getenv("PLAYLIST_CHECK_INTERVAL", str(int(HLS_SEGMENT_DURATION * 15))))  # 15 segments

# Configuration des cycles de logging
WATCHERS_LOG_CYCLE = int(os.getenv("WATCHERS_LOG_CYCLE", str(int(HLS_SEGMENT_DURATION * 2.5))))  # 2.5 segments
SUMMARY_CYCLE = int(os.getenv("SUMMARY_CYCLE", str(int(HLS_SEGMENT_DURATION * 30))))  # 30 segments

# Configuration des timeouts pour le monitoring des clients
CLIENT_MONITOR_SEGMENT_TIMEOUT = int(os.getenv("CLIENT_MONITOR_SEGMENT_TIMEOUT", str(int(HLS_SEGMENT_DURATION * 5))))  # 5 segments
CLIENT_MONITOR_PLAYLIST_TIMEOUT = int(os.getenv("CLIENT_MONITOR_PLAYLIST_TIMEOUT", str(int(HLS_SEGMENT_DURATION * 10))))  # 10 segments
CLIENT_MONITOR_UNKNOWN_TIMEOUT = int(os.getenv("CLIENT_MONITOR_UNKNOWN_TIMEOUT", str(int(HLS_SEGMENT_DURATION * 12.5))))  # 12.5 segments

# Configuration des timeouts de stream
STREAM_SEGMENT_TIMEOUT = int(os.getenv('STREAM_SEGMENT_TIMEOUT', str(int(HLS_SEGMENT_DURATION * 22.5))))  # 22.5 segments
STREAM_ERROR_THRESHOLD = int(os.getenv('STREAM_ERROR_THRESHOLD', '15'))
STREAM_RESTART_DELAY = int(os.getenv('STREAM_RESTART_DELAY', str(int(HLS_SEGMENT_DURATION * 15))))  # 15 segments
STREAM_INITIAL_DELAY = int(os.getenv('STREAM_INITIAL_DELAY', str(int(HLS_SEGMENT_DURATION * 15))))  # 15 segments
STREAM_START_TIMEOUT = int(os.getenv('STREAM_START_TIMEOUT', str(int(HLS_SEGMENT_DURATION * 7.5))))  # 7.5 segments
CRASH_THRESHOLD = int(os.getenv('CRASH_THRESHOLD', '120'))  # Seuil en secondes pour considérer un crash de stream

# Configuration des timeouts de nettoyage
HLS_CLEANUP_TIMEOUT = int(os.getenv("HLS_CLEANUP_TIMEOUT", str(int(HLS_SEGMENT_DURATION * 2.5))))  # 2.5 segments
CLIENT_MONITOR_CLEANUP_SEGMENT_TIMEOUT = int(os.getenv("CLIENT_MONITOR_CLEANUP_SEGMENT_TIMEOUT", str(int(HLS_SEGMENT_DURATION * 150))))  # 150 segments
CLIENT_MONITOR_CLEANUP_PLAYLIST_TIMEOUT = int(os.getenv("CLIENT_MONITOR_CLEANUP_PLAYLIST_TIMEOUT", str(int(HLS_SEGMENT_DURATION * 90))))  # 90 segments
CLIENT_MONITOR_CLEANUP_UNKNOWN_TIMEOUT = int(os.getenv("CLIENT_MONITOR_CLEANUP_UNKNOWN_TIMEOUT", str(int(HLS_SEGMENT_DURATION * 120))))  # 120 segments

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


# Essayons d'ajouter le FileHandler avec gestion d'erreur
try:
    # Vérifier que le dossier existe
    log_dir = Path("/app/logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Vérifier que le fichier existe
    log_file = log_dir / "app.log"
    if not log_file.exists():
        try:
            log_file.touch(mode=0o666)
        except:
            pass  # Ignorer l'erreur si on ne peut pas créer le fichier
    
    # Essayer d'ajouter le handler si le fichier existe et est accessible
    if log_file.exists() and os.access(log_file, os.W_OK):
        handler = logging.FileHandler(str(log_file))
        handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - [%(levelname)s] - %(message)s"))
        logging.getLogger().addHandler(handler)
except Exception as e:
    print(f"Impossible d'initialiser le FileHandler: {e}")
    
# On s'assure que tous les loggers utilisent le même niveau
logging.getLogger().setLevel(get_log_level(os.getenv("LOG_LEVEL", "INFO")))

# On configure le logger pour les crashs
logger = logging.getLogger(__name__)

# On désactive les logs de watchdog, quoi qu'il arrive
logging.getLogger("watchdog").setLevel(logging.WARNING)

def handle_ffmpeg_error(channel_name: str, error_message: str):
    """
    Détecte et gère les erreurs FFmpeg spécifiques aux fichiers.
    
    Args:
        channel_name: Nom de la chaîne
        error_message: Message d'erreur de FFmpeg
    """
    try:
        # Import ici pour éviter l'import circulaire
        from error_handler import ErrorHandler
        
        # Patterns d'erreurs liées aux fichiers
        file_error_patterns = [
            # Fichier introuvable
            r"No such file or directory: '(.*?)'",
            r"Invalid data found when processing input: '(.*?)'",
            r"Could not find file: (.*?)$",
            # Corruption de fichier
            r"Invalid data found when processing input at (.*?)",
            r"Error while decoding stream.*?: (.*?)",
            r"corrupt.*?frame in (.*?)"
        ]
        
        # Chercher un pattern qui correspond
        file_path = None
        for pattern in file_error_patterns:
            match = re.search(pattern, error_message, re.IGNORECASE)
            if match and match.group(1):
                file_path = match.group(1)
                break
                
        # Si on a trouvé un fichier problématique
        if file_path:
            logger.warning(f"[{channel_name}] 🔍 Détection d'un fichier problématique: {file_path}")
            
            # Initialiser le gestionnaire d'erreurs et traiter le problème
            error_handler = ErrorHandler(channel_name)
            if error_handler.handle_ffmpeg_file_error(channel_name, file_path):
                logger.info(f"[{channel_name}] ✅ Récupération après problème de fichier réussie")
            else:
                logger.error(f"[{channel_name}] ❌ Échec de récupération après problème de fichier")
        
    except Exception as e:
        logger.error(f"[{channel_name}] ❌ Erreur dans handle_ffmpeg_error: {e}")


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

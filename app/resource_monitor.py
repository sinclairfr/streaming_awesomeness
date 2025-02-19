import time
import threading
import subprocess
import psutil
from config import logger
import os
from dotenv import load_dotenv

# On charge les variables d'environnement
load_dotenv()

def safe_int_conversion(value, default):
    """Convertit une valeur en int de manière sécurisée"""
    try:
        if isinstance(value, str):
            # Nettoie la chaîne de caractères des valeurs non numériques
            cleaned_value = ''.join(filter(str.isdigit, value))
            return int(cleaned_value) if cleaned_value else default
        return int(value)
    except (ValueError, TypeError):
        return default

class ResourceMonitor(threading.Thread):
    def __init__(self):
        try:
            RESOURCES_CHECK_INTERVAL = float(os.getenv("RESOURCES_CHECK_INTERVAL", "60"))
            CPU_THRESHOLD = float(os.getenv("CPU_THRESHOLD", "95"))
        except ValueError as e:
            logger.error(f"Erreur de conversion des variables d'environnement: {e}")
            RESOURCES_CHECK_INTERVAL = 60
            CPU_THRESHOLD = 95

        super().__init__()
        self.interval = safe_int_conversion(RESOURCES_CHECK_INTERVAL, 60)
        self.cpu_threshold = safe_int_conversion(CPU_THRESHOLD, 95)
        self.running = True
        self.flush_logs()

    def flush_logs(self):
        """Nettoie tous les logs au démarrage"""
        ffmpeg_logs_dir = os.getenv("FFMPEG_LOGS_DIR", "/app/logs")
        logs_dir = os.getenv("LOGS_DIR", "/app/logs")

        logger.info("✨ Nettoyage des logs en cours...")
        try:
            # Crée les répertoires s'ils n'existent pas
            os.makedirs(logs_dir, exist_ok=True)
            os.makedirs(ffmpeg_logs_dir, exist_ok=True)

            # Nettoie le dossier principal des logs
            for filename in os.listdir(logs_dir):
                file_path = os.path.join(logs_dir, filename)
                if os.path.isfile(file_path) and filename.endswith('.log'):
                    try:
                        open(file_path, 'w').close()
                        logger.info(f"🧹 Log nettoyé: {filename}")
                    except Exception as e:
                        logger.error(f"Erreur lors du nettoyage de {filename}: {e}")

            # Nettoie le dossier ffmpeg
            for filename in os.listdir(ffmpeg_logs_dir):
                file_path = os.path.join(ffmpeg_logs_dir, filename)
                if os.path.isfile(file_path) and filename.endswith('.log'):
                    try:
                        open(file_path, 'w').close()
                        logger.info(f"🧹 Log ffmpeg nettoyé: {filename}")
                    except Exception as e:
                        logger.error(f"Erreur lors du nettoyage de {filename}: {e}")

            logger.info("✨ Nettoyage des logs terminé")

        except Exception as e:
            logger.error(f"Erreur lors du nettoyage des logs: {e}")

    def run(self):
        try:
            CPU_CHECK_INTERVAL = float(os.getenv("CPU_CHECK_INTERVAL", "1"))
        except ValueError:
            CPU_CHECK_INTERVAL = 1
            logger.error("Erreur de conversion de CPU_CHECK_INTERVAL, utilisation de la valeur par défaut: 1")

        while self.running:
            try:
                # Monitoring CPU
                cpu_percent = psutil.cpu_percent(interval=CPU_CHECK_INTERVAL)

                # Monitoring RAM
                ram = psutil.virtual_memory()
                ram_used_gb = ram.used / (1024 * 1024 * 1024)
                ram_total_gb = ram.total / (1024 * 1024 * 1024)

                # Monitoring GPU
                gpu_info = ""
                try:
                    result = subprocess.run(
                        ["nvidia-smi", "--query-gpu=utilization.gpu,memory.used", "--format=csv,noheader,nounits"],
                        capture_output=True,
                        text=True
                    )
                    if result.returncode == 0:
                        gpu_util, gpu_mem = result.stdout.strip().split(",")
                        gpu_info = f", GPU: {gpu_util}%, MEM GPU: {gpu_mem}MB"
                except FileNotFoundError:
                    pass

                # Logging des ressources
                logger.info(
                    f"💻 Ressources - CPU: {cpu_percent}%, RAM: {ram_used_gb:.1f}/{ram_total_gb:.1f}GB ({ram.percent}%)"
                    f"{gpu_info}"
                )
                
                # Monitoring des streams actifs
                if hasattr(self, 'get_active_streams_status'):
                    streams_status = self.get_active_streams_status()
                    if streams_status:
                        status_details = []
                        for channel, info in streams_status.items():
                            if 'error' not in info:
                                status_details.append(
                                    f"{channel}(PID:{info['pid']}, "
                                    f"CPU:{info['cpu_percent']:.1f}%, "
                                    f"⌚:{info['running_time']:.0f}s, "
                                    f"👥:{info['watchers']})"
                                )
                        if status_details:
                            logger.info(f"🎥 Streams actifs: {' | '.join(status_details)}")

            except Exception as e:
                logger.error(f"Erreur monitoring ressources: {e}")

            time.sleep(self.interval)

    def stop(self):
        self.running = False

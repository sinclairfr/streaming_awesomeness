import time
import threading
import subprocess
import psutil
from config import logger
import os
from pathlib import Path
import shutil
from config import (
    RESOURCES_CHECK_INTERVAL,
    CPU_CHECK_INTERVAL,
    CPU_THRESHOLD,
    logger,
    FFMPEG_LOGS_DIR,
)


def safe_int_conversion(value, default):
    """Convertit une valeur en int de manière sécurisée"""
    try:
        if isinstance(value, str):
            # Nettoie la chaîne de caractères des valeurs non numériques
            cleaned_value = "".join(filter(str.isdigit, value))
            return int(cleaned_value) if cleaned_value else default
        return int(value)
    except (ValueError, TypeError):
        return default


class ResourceMonitor(threading.Thread):
    def __init__(self):
        try:
            CPU_THRESHOLD = float(os.getenv("CPU_THRESHOLD", "95"))
        except ValueError as e:
            logger.error(f"Erreur de conversion des variables d'environnement: {e}")
            # RESOURCES_CHECK_INTERVAL = 60
            CPU_THRESHOLD = 95

        super().__init__()
        self.interval = safe_int_conversion(RESOURCES_CHECK_INTERVAL, 60)
        self.cpu_threshold = safe_int_conversion(CPU_THRESHOLD, 95)
        self.running = True
        self.flush_logs()

    def run(self):
        """Méthode principale du thread"""
        logger.info("🚀 Démarrage du ResourceMonitor")
        self.run_resource_monitor()

    def flush_logs(self):
        """Nettoie tous les logs au démarrage"""
        ffmpeg_logs_dir = FFMPEG_LOGS_DIR
        logs_dir = os.getenv("LOGS_DIR", "/app/logs")

        logger.info("✨ Nettoyage des logs en cours...")
        try:
            # Crée les répertoires s'ils n'existent pas
            os.makedirs(logs_dir, exist_ok=True)
            os.makedirs(ffmpeg_logs_dir, exist_ok=True)

            # Nettoie le dossier principal des logs
            for filename in os.listdir(logs_dir):
                file_path = os.path.join(logs_dir, filename)
                if os.path.isfile(file_path) and (
                    filename.endswith(".log")
                    or "_ffmpeg.log" in filename  # On ajoute cette condition
                ):
                    try:
                        open(file_path, "w").close()
                        logger.info(f"🧹 Log nettoyé: {filename}")
                    except Exception as e:
                        logger.error(f"Erreur lors du nettoyage de {filename}: {e}")

            # Nettoie le dossier ffmpeg
            for filename in os.listdir(ffmpeg_logs_dir):
                file_path = os.path.join(ffmpeg_logs_dir, filename)
                if os.path.isfile(file_path) and (
                    filename.endswith(".log")
                    or "_ffmpeg.log" in filename  # On ajoute cette condition
                ):
                    try:
                        open(file_path, "w").close()
                        logger.info(f"🧹 Log ffmpeg nettoyé: {filename}")
                    except Exception as e:
                        logger.error(f"Erreur lors du nettoyage de {filename}: {e}")

            logger.info("✨ Nettoyage des logs terminé")

        except Exception as e:
            logger.error(f"Erreur lors du nettoyage des logs: {e}")

    def stop_monitor(self):
        self.running = False

    def run_resource_monitor(self):
        try:
            CPU_CHECK_INTERVAL = float(os.getenv("CPU_CHECK_INTERVAL", "1"))
        except ValueError:
            CPU_CHECK_INTERVAL = 1
            logger.error(
                "Erreur de conversion de CPU_CHECK_INTERVAL, utilisation de la valeur par défaut: 1"
            )

        while self.running:
            try:
                # Monitoring CPU
                cpu_percent = psutil.cpu_percent(interval=CPU_CHECK_INTERVAL)

                # Monitoring RAM
                ram = psutil.virtual_memory()
                ram_used_gb = ram.used / (1024 * 1024 * 1024)
                ram_total_gb = ram.total / (1024 * 1024 * 1024)

                # IMPROVED: Only log when significant changes occur
                if (
                    not hasattr(self, "last_cpu")
                    or abs(cpu_percent - getattr(self, "last_cpu", 0)) > 5
                ):
                    self.last_cpu = cpu_percent
                    # Log with more informative message about FFmpeg processes
                    ffmpeg_procs = []
                    for proc in psutil.process_iter(["pid", "name", "cpu_percent"]):
                        try:
                            if "ffmpeg" in proc.info["name"].lower():
                                ffmpeg_procs.append(proc)
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            continue

                    ffmpeg_cpu = (
                        sum(p.info["cpu_percent"] for p in ffmpeg_procs)
                        if ffmpeg_procs
                        else 0
                    )

                    logger.info(
                        f"💻 Ressources - CPU: {cpu_percent}% (FFmpeg: ~{ffmpeg_cpu}%), RAM: {ram_used_gb:.1f}/{ram_total_gb:.1f}GB ({ram.percent}%)"
                    )

                # Monitoring GPU info remains unchanged...

                # IMPROVED: Add monitoring of disk space for HLS directory
                hls_path = Path("/app/hls")
                if hls_path.exists():
                    try:
                        usage = shutil.disk_usage(hls_path)
                        free_gb = usage.free / (1024 * 1024 * 1024)
                        total_gb = usage.total / (1024 * 1024 * 1024)
                        used_percent = (usage.used / usage.total) * 100

                        if used_percent > 85:  # Only log when disk usage is high
                            logger.warning(
                                f"⚠️ Disk space warning - HLS directory: {used_percent:.1f}% used, {free_gb:.1f}GB free"
                            )
                    except Exception as e:
                        logger.error(f"Error checking disk space: {e}")

                time.sleep(self.interval)

            except Exception as e:
                logger.error(f"Erreur monitoring ressources: {e}")
                time.sleep(60)  # Longer wait on error

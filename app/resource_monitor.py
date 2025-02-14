# resource_monitor.py

import time
import threading
import subprocess
import psutil
from config import logger

class ResourceMonitor(threading.Thread):
    """# On surveille les ressources système"""

    def __init__(self):
        super().__init__(daemon=True)
        self.interval = 20  # On fixe un intervalle de 20 secondes

    def run(self):
        while True:
            try:
                cpu_percent = psutil.cpu_percent(interval=1)
                ram = psutil.virtual_memory()
                ram_used_gb = ram.used / (1024 * 1024 * 1024)
                ram_total_gb = ram.total / (1024 * 1024 * 1024)

                # On tente de récupérer les infos GPU si nvidia-smi est dispo
                gpu_info = ""
                try:
                    result = subprocess.run(
                        [
                            "nvidia-smi",
                            "--query-gpu=utilization.gpu,memory.used",
                            "--format=csv,noheader,nounits",
                        ],
                        capture_output=True,
                        text=True
                    )
                    if result.returncode == 0:
                        gpu_util, gpu_mem = result.stdout.strip().split(",")
                        gpu_info = f", GPU: {gpu_util}%, MEM GPU: {gpu_mem}MB"
                except FileNotFoundError:
                    # Pas de GPU NVIDIA disponible
                    pass

                logger.info(
                    f"💻 Ressources - CPU: {cpu_percent}%, RAM: {ram_used_gb:.1f}/{ram_total_gb:.1f}GB ({ram.percent}%)"
                    f"{gpu_info}"
                )
            except Exception as e:
                logger.error(f"Erreur monitoring ressources: {e}")

            time.sleep(self.interval)

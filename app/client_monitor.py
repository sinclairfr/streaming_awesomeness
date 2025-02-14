# client_monitor.py
import os
import time
import threading
from config import logger

class ClientMonitor(threading.Thread):
    """
    # On surveille le fichier de log Nginx pour détecter les adresses IP des clients
    """

    def __init__(self, log_file: str):
        super().__init__(daemon=True)
        self.log_file = log_file

    def run(self):
        if not os.path.exists(self.log_file):
            logger.warning(
                "Fichier log introuvable pour le monitoring des clients : %s",
                self.log_file
            )
            return

        # On ouvre le fichier de log et on se place à la fin
        with open(self.log_file, "r") as f:
            f.seek(0, 2)
            while True:
                line = f.readline()
                if not line:
                    time.sleep(0.5)
                    continue
                # On suppose que l'IP est le premier champ de la ligne
                ip = line.split()[0]
                logger.info(f"Client connecté : {ip}")

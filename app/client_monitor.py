# client_monitor.py
import os
import time
import threading
from pathlib import Path
from config import logger
from typing import Dict, Tuple
import re
import threading

class ClientMonitor(threading.Thread):
    def __init__(self, log_path, update_watchers_callback, manager):
        super().__init__(daemon=True)
        self.log_path = log_path
        self.update_watchers = update_watchers_callback
        self.manager = manager
        self.watchers = {}
        self.lock = threading.Lock()
        
        # 🔹 Thread pour vérifier et nettoyer périodiquement les watchers
        self.cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self.cleanup_thread.start()

    def _cleanup_loop(self):
        """# Vérification toutes les 60s même si personne ne regarde"""
        while True:
            time.sleep(60)
            self._cleanup_inactive()
    
    def _cleanup_inactive(self):
        """# Nettoie les watchers inactifs et met à jour les chaînes non consultées"""
        now = time.time()
        to_remove = []

        with self.lock:
            # 🔹 Identifie les watchers inactifs
            for (channel, ip), last_seen in self.watchers.items():
                if now - last_seen > 60:  # Plus de 60s sans requête
                    to_remove.append((channel, ip))

            # 🔹 Supprime les watchers inactifs
            for key in to_remove:
                del self.watchers[key]
                logger.info(f"🗑️ Watcher supprimé: {key[1]} -> {key[0]}")

            # 🔹 Vérifie les chaînes qui n'ont plus de watchers et met leur compteur à zéro
            channels = set(ch for ch, _ in to_remove)
            for channel in channels:
                count = len([1 for (ch, _), _ in self.watchers.items() if ch == channel])
                logger.warning(f"⚠️ Mise à jour {channel} : {count} watchers restants")  # 🔹 Log en WARNING
                self.manager.update_watchers(channel, count, "/hls/")

    def run(self):
        """On surveille les requêtes clients"""
        logger.debug("👀 Démarrage de la surveillance des requêtes...")
        
        try:
            if not os.path.exists(self.log_path):
                logger.error(f"❌ Log file introuvable: {self.log_path}")
                return
                
            logger.debug(f"📖 Ouverture du log: {self.log_path}")
            with open(self.log_path, "r") as f:
                f.seek(0, 2)  # On va à la fin
                while True:
                    line = f.readline().strip()
                    if not line:
                        time.sleep(0.5)
                        continue

                    logger.debug(f"📝 Ligne lue: {line}")
                        
                    # On ne s'intéresse qu'aux requêtes HLS
                    if "GET /hls/" not in line:
                        continue
                            
                    parts = line.split()
                    if len(parts) < 7:
                        continue
                            
                    ip = parts[0]
                    request = parts[6].strip('"')  # Retire les guillemets
                    
                    # On extrait le channel
                    match = re.search(r'/hls/([^/]+)/', request)
                    if not match:
                        continue
                            
                    channel = match.group(1)
                    logger.debug(f"🔍 Requête détectée: {ip} -> {channel} ({request})")

                    # Update des watchers avec logs explicites
                    with self.lock:
                        old_count = len([1 for (ch, _), ts in self.watchers.items() if ch == channel])
                        self.watchers[(channel, ip)] = time.time()
                        new_count = len([1 for (ch, _), ts in self.watchers.items() if ch == channel])
                        
                        logger.debug(f"🔄 Update watchers {channel}: {old_count} -> {new_count}")
                        self.update_watchers(channel, new_count, request)

        except Exception as e:
            logger.error(f"❌ Erreur fatale dans client_monitor: {e}")
            logger.error(traceback.format_exc())
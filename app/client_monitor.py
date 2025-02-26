# client_monitor.py
import os
import time
import threading
from pathlib import Path
from config import logger
from typing import Dict, Tuple
import re

class ClientMonitor(threading.Thread):
    def __init__(self, log_path, update_watchers_callback, manager):
        super().__init__(daemon=True)
        self.log_path = log_path
        self.update_watchers = update_watchers_callback
        self.manager = manager
        self.watchers = {}
        self.lock = threading.Lock()

        self.inactivity_threshold = 120

        self.cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self.cleanup_thread.start()

    def _cleanup_loop(self):
        while True:
            time.sleep(60)
            self._cleanup_inactive()

    def _cleanup_inactive(self):
        """Nettoie les watchers inactifs"""
        now = time.time()
        to_remove = []

        with self.lock:
            # Check for inactive watchers (>60s without request)
            for (channel, ip), last_seen in self.watchers.items():
                if now - last_seen > self.inactivity_threshold:
                    to_remove.append((channel, ip))

            affected_channels = set()

            for key in to_remove:
                channel, ip = key
                del self.watchers[key]
                affected_channels.add(channel)
                logger.info(f"🗑️ Watcher supprimé: {ip} -> {channel}")

            for channel in affected_channels:
                count = len([1 for (ch, _), _ in self.watchers.items() if ch == channel])
                logger.warning(f"⚠️ Mise à jour {channel} : {count} watchers restants")
                self.update_watchers(channel, count, "/hls/")

    def run(self):
        """On surveille les requêtes clients"""
        logger.debug("👀 Démarrage de la surveillance des requêtes...")

        try:
            if not os.path.exists(self.log_path):
                logger.error(f"❌ Log file introuvable: {self.log_path}")
                return

            logger.debug(f"📖 Ouverture du log: {self.log_path}")
            with open(self.log_path, "r") as f:
                f.seek(0, 2)
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
                    request = parts[6].strip('"')

                    # On extrait le channel
                    match = re.search(r'/hls/([^/]+)/', request)
                    if not match:
                        continue

                    channel = match.group(1)
                    logger.debug(f"🔍 Requête détectée: {ip} -> {channel} ({request})")

                    with self.lock:
                        old_count = len([1 for (ch, _), ts in self.watchers.items() if ch == channel])
                        self.watchers[(channel, ip)] = time.time()
                        new_count = len([1 for (ch, _), ts in self.watchers.items() if ch == channel])

                        logger.info(f"🔄 Update watchers {channel}: {old_count} -> {new_count}")
                        self.update_watchers(channel, new_count, request)

        except Exception as e:
            logger.error(f"❌ Erreur fatale dans client_monitor: {e}")
            logger.error(traceback.format_exc())
            logger.error(traceback.format_exc())
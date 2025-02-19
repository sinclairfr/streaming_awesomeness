import os
import time
import threading
from pathlib import Path
from config import logger
from typing import Dict, Tuple
import re
from session_manager import SessionManager

class ClientMonitor(threading.Thread):
    def __init__(self, log_path, update_watchers_callback, manager):
        super().__init__(daemon=True)
        self.log_path = log_path
        self.update_watchers = update_watchers_callback
        self.manager = manager
        self.session_manager = SessionManager()
        self.lock = threading.Lock()

    def run(self):
        """Surveillance des requêtes clients avec gestion des sessions"""
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

                    self._process_log_line(line)

        except Exception as e:
            logger.error(f"❌ Erreur fatale dans client_monitor: {e}")
            logger.error(traceback.format_exc())

    def _process_log_line(self, line: str):
        """Traite une ligne de log et appelle le callback update_watchers"""
        try:
            if "GET /hls/" not in line:
                return
                    
            parts = line.split()
            if len(parts) < 7:
                return
                    
            ip = parts[0]
            request = parts[6].strip('"')
            
            # On extrait le channel
            match = re.search(r'/hls/([^/]+)/', request)
            if not match:
                return
                    
            channel = match.group(1)
            
            # Utilisation du session_manager pour le tracking
            session = self.session_manager.register_activity(ip, channel, request)
            stats = self.session_manager.get_channel_stats(channel)
            
            # Appel du callback de l'IPTVManager
            self.update_watchers(
                channel, 
                stats["active_viewers"],
                request
            )

        except Exception as e:
            logger.error(f"❌ Erreur traitement ligne log: {e}")

    def get_channel_stats(self, channel: str) -> dict:
        return self.session_manager.get_channel_stats(channel)

    def is_channel_active(self, channel: str) -> bool:
        """Vérifie si un canal a des viewers actifs"""
    def get_active_viewers(self, channel: str) -> int:
        return len(self.session_manager.get_channel_sessions(channel))

    def is_channel_active(self, channel: str) -> bool:
        return self.get_active_viewers(channel) > 0

        """Met à jour les watchers en fonction des requêtes m3u8 et ts"""
        try:
            # Correction ici : on accède directement aux channels du manager
            if channel_name not in self.manager.channels:
                logger.error(f"❌ Chaîne inconnue: {channel_name}")
                return

            channel = self.manager.channels[channel_name]
            
            # Mise à jour SYSTÉMATIQUE du last_watcher_time à chaque requête
            channel.last_watcher_time = time.time()
            
            # Si c'est une requête de segment, on met aussi à jour last_segment_time
            if ".ts" in request_path:
                channel.last_segment_time = time.time()
                
            old_count = channel.watchers_count
            channel.watchers_count = count

            if old_count != count:
                logger.info(f"📊 Mise à jour {channel_name}: {count} watchers")
                
                if old_count == 0 and count > 0:
                    logger.info(f"[{channel_name}] 🔥 Premier watcher, démarrage du stream")
                    if not channel.start_stream_if_needed():
                        logger.error(f"[{channel_name}] ❌ Échec démarrage stream")
                elif old_count > 0 and count == 0:
                    # On ne coupe PAS immédiatement, on laisse le monitoring gérer ça
                    logger.info(f"[{channel_name}] ⚠️ Plus de watchers recensés")

        except Exception as e:
            logger.error(f"❌ Erreur update_watchers: {e}")
            logger.error(traceback.format_exc())  # Ajout du traceback pour plus de détails
            
    def get_active_streams_status(self) -> dict:
        """Retourne l'état des streams actifs"""
        status = {}
        for channel_name, channel in self.channels.items():
            if channel.ffmpeg_process:
                try:
                    proc = psutil.Process(channel.ffmpeg_process.pid)
                    status[channel_name] = {
                        'pid': proc.pid,
                        'cpu_percent': proc.cpu_percent(),
                        'memory_percent': proc.memory_percent(),
                        'running_time': time.time() - proc.create_time(),
                        'watchers': channel.watcher_count
                    }
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    status[channel_name] = {
                        'pid': channel.ffmpeg_process.pid,
                        'error': 'Process not accessible'
                    }
        return status
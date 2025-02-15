# client_monitor.py
import os
import time
import threading
from pathlib import Path
from config import logger
from typing import Dict, Tuple
import re

class ClientMonitor(threading.Thread):
    def __init__(self, log_file: str, manager):
        super().__init__(daemon=True)
        self.log_file = log_file
        self.manager = manager
        self.watchers: Dict[Tuple[str, str], float] = {}
        self.lock = threading.Lock()
        
        # Debug
        self.last_line = None
        logger.info(f"📊 Moniteur clients initialisé sur {log_file}")

    def run(self):
        try:
            # On s'assure que le fichier et le dossier existent
            log_path = Path(self.log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            if not log_path.exists():
                log_path.touch()

            logger.info("🔄 Démarrage monitoring clients")
            
            while True:
                try:
                    # On ouvre en mode lecture et on va à la fin
                    with open(self.log_file, "r") as f:
                        f.seek(0, 2)
                        logger.info("👀 Monitor actif, en attente de requêtes...")
                        
                        while True:
                            line = f.readline()
                            if not line:
                                time.sleep(0.1)
                                continue
                                
                            self.last_line = line.strip()
                            self._process_log_line(line)
                except Exception as e:
                    logger.error(f"❌ Erreur lecture log: {e}")
                    time.sleep(1)
                    continue

        except Exception as e:
            logger.error(f"❌ Erreur fatale monitor: {e}")
            raise

    def _process_log_line(self, line: str):
        """Traite une ligne de log nginx"""
        try:
            # On ne s'intéresse qu'aux requêtes HLS
            if "GET /hls/" not in line:
                return
                
            # Format: IP - - [date] "GET /hls/CHANNEL/segment_X.ts HTTP/1.1" 200 ...
            parts = line.split()
            if len(parts) < 7:
                return
                
            ip = parts[0]
            request = parts[6].strip('"')  # Retire les guillemets
            
            # On extrait le channel
            match = re.search(r'/hls/([^/]+)/', request)
            if not match:
                return
                
            channel = match.group(1)
            logger.info(f"🔍 Requête détectée: {ip} -> {channel} ({request})")
            
            # Mise à jour des watchers
            with self.lock:
                # On compte les watchers actuels
                old_count = len([1 for (ch, _), ts in self.watchers.items() 
                               if ch == channel])
                
                # On met à jour le timestamp
                self.watchers[(channel, ip)] = time.time()
                
                # On recompte
                new_count = len([1 for (ch, _), ts in self.watchers.items() 
                               if ch == channel])
                
                if old_count != new_count:
                    logger.info(f"👥 Changement watchers {channel}: {old_count} -> {new_count}")
                    self.manager.update_watchers(channel, new_count)
                    
        except Exception as e:
            logger.error(f"❌ Erreur traitement ligne: {e}")
            logger.error(f"Ligne: {line}")

    def _cleanup_inactive(self):
        """Nettoie les watchers inactifs"""
        now = time.time()
        to_remove = []
        
        with self.lock:
            # On identifie les watchers inactifs
            for (channel, ip), last_seen in self.watchers.items():
                if now - last_seen > 15:  # 15s sans activité
                    to_remove.append((channel, ip))
            
            # On les supprime
            for key in to_remove:
                del self.watchers[key]
                logger.info(f"🗑️ Watcher supprimé: {key[1]} -> {key[0]}")
                
            # On met à jour les compteurs
            channels = set(ch for ch, _ in to_remove)
            for channel in channels:
                count = len([1 for (ch, _), _ in self.watchers.items() 
                           if ch == channel])
                self.manager.update_watchers(channel, count)
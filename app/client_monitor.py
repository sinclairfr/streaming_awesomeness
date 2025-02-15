# client_monitor.py
import os
import time
import threading
from pathlib import Path
from config import logger
from typing import Dict, Tuple
import re

class ClientMonitor(threading.Thread):
    def __init__(self, log_path, update_watchers_callback):
        super().__init__(daemon=True)
        self.log_path = log_path
        self.update_watchers = update_watchers_callback  # 🔥 On stocke la fonction


    def run(self):
        logger.info("👀 Surveillance des requêtes clients en cours...")
        with open(self.log_path, "r") as f:
            f.seek(0, 2)  # Aller à la fin du fichier pour lire les nouvelles lignes
            while True:
                line = f.readline().strip()
                if not line:
                    time.sleep(0.5)
                    continue

                # Analyse de la ligne (format access.log de Nginx)
                parts = line.split(" ")
                if len(parts) > 6:
                    ip_address = parts[0]
                    request_path = parts[6]  # Chemin de la requête HTTP

                    # 🔥 Extraire le nom de la chaîne depuis `request_path`
                    match = re.search(r'/hls/([^/]+)/', request_path)
                    if match:
                        channel_name = match.group(1)
                        logger.info(f"🔍 Requête détectée: {ip_address} -> {channel_name} ({request_path})")

                        # ✅ Correction : Fournir les 3 arguments à `update_watchers`
                        self.update_watchers(channel_name, 1, request_path)
                    else:
                        logger.warning(f"⚠️ Impossible d'extraire le channel depuis la requête : {request_path}")

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
                    self.update_watchers(channel, new_count, request_path)
                    
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
                self.manager.update_watchers(channel, count, request_path)
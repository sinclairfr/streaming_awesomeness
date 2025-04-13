"""
StatsCollector simplifié à l'extrême - Se contente de suivre les incréments et sauvegarder
"""
import os
import json
import time
import threading
import re  # Added for regex parsing
from pathlib import Path
from typing import Dict, Set, Optional, Tuple # Added Tuple
from config import logger, HLS_SEGMENT_DURATION, NGINX_ACCESS_LOG, ACTIVE_VIEWER_TIMEOUT

# Regex to parse Nginx access log lines for SUCCESSFUL HLS Segment requests
# Captures: 1: IP Address, 2: Channel Name, 3: Bytes Sent, 4: User Agent
LOG_SEGMENT_REGEX = re.compile(
    r'^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s+-\s+-\s+'  # 1: IP Address
    r'\[.*?\]\s+'                                      # Timestamp
    r'"GET /hls/([^/]+)/segment_\d+\.ts\s+HTTP/1\.[01]"\s+' # 2: Channel Name
    r'(?:200|206)\s+'                                  # Status Code (200 OK or 206 Partial Content)
    r'(\d+)\s+'                                        # 3: Bytes Sent
    r'".*?"\s+'                                        # Referrer
    r'"(.*?)"'                                         # 4: User Agent
)
# Regex for Playlist requests (to update last_seen, first_seen etc.)
LOG_PLAYLIST_REGEX = re.compile(
    r'^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s+-\s+-\s+' # 1: IP Address
    r'\[.*?\]\s+'                                      # Timestamp
    r'"GET /hls/([^/]+)/playlist\.m3u8\s+HTTP/1\.[01]"\s+'# 2: Channel Name
    r'\d{3}\s+'                                        # Status Code (any)
    r'\d+\s+'                                         # Bytes Sent
    r'".*?"\s+'                                        # Referrer
    r'"(.*?)"'                                         # 3: User Agent
)

class StatsCollector:
    """Gère les statistiques basées sur les logs Nginx (bytes transférés)."""

    def __init__(self, stats_dir="/app/stats"):
        """Initialise le collecteur de statistiques."""
        self.stats_dir = Path(stats_dir)
        self.stats_dir.mkdir(parents=True, exist_ok=True)

        self.channel_stats_file = self.stats_dir / "channel_stats_bytes.json"
        self.user_stats_file = self.stats_dir / "user_stats_bytes.json"

        self.lock = threading.Lock()
        
        # Initialiser les structures de base
        self.channel_stats = {"global": {"total_watch_time": 0, "total_bytes_transferred": 0, "unique_viewers": set(), "last_update": 0}}
        self.user_stats = {
            "users": {},
            "last_updated": int(time.time())
        }
        
        # Dictionnaire pour suivre le canal actif de chaque IP
        self.active_channel_by_ip = {}

        # Calculer le timeout basé sur HLS_SEGMENT_DURATION et HLS_LIST_SIZE
        segment_duration = float(os.getenv("HLS_SEGMENT_DURATION", "2.0"))
        hls_list_size = int(os.getenv("HLS_LIST_SIZE", "10"))
        # Le timeout est la durée totale de la playlist HLS plus une marge de sécurité
        # Marge de sécurité = 2 segments pour tenir compte des délais réseau et de traitement
        self.viewer_timeout = segment_duration * (hls_list_size + 2)
        logger.info(f"📊 Timeout des spectateurs calculé: {self.viewer_timeout:.1f}s (basé sur {segment_duration}s x {hls_list_size} segments + marge)")

        self._load_stats()

        # --- Background Threads ---
        self.stop_event = threading.Event()

        # Save Thread
        self.save_interval = 15
        self.save_thread = threading.Thread(target=self._save_loop, daemon=True)
        self.save_thread.start()

        # Nginx Log Monitor Thread
        self.log_monitor_thread = threading.Thread(target=self._log_monitor_loop, daemon=True)
        self.log_monitor_thread.start()

        # Cleanup Thread - Nouveau thread pour le nettoyage périodique
        self.cleanup_interval = HLS_SEGMENT_DURATION * 7
        self.cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self.cleanup_thread.start()

        logger.info(f"📊 StatsCollector initialisé (Mode: Nginx Logs / Bytes Transferred) - Timeout des spectateurs actifs: {ACTIVE_VIEWER_TIMEOUT}s")

    def _load_stats(self):
        """Charge les stats depuis les fichiers (mode bytes)."""
        with self.lock:
            try:
                # --- Channel Stats ---
                channel_file_existed = self.channel_stats_file.exists()
                if not channel_file_existed:
                    logger.info(f"📊 Fichier channel_stats introuvable ({self.channel_stats_file}). Création d'un fichier vide.")
                    try:
                        with open(self.channel_stats_file, 'w') as f:
                            f.write("{}")
                        os.chmod(self.channel_stats_file, 0o666) # Set permissions
                    except Exception as e_create:
                        logger.error(f"❌ Impossible de créer le fichier channel_stats: {e_create}")
                        # Proceed with default in-memory stats anyway

                # Try to load channel stats (will load the empty one if just created)
                if self.channel_stats_file.exists(): # Check again in case creation failed
                    with open(self.channel_stats_file, 'r') as f:
                        loaded_channel_stats = json.load(f)
                        # Check if loaded data is a dictionary (basic validation)
                        if isinstance(loaded_channel_stats, dict):
                            self.channel_stats = loaded_channel_stats
                            # Convert lists to sets for unique_viewers
                            if "global" in self.channel_stats:
                                self.channel_stats["global"]["unique_viewers"] = set(self.channel_stats["global"].get("unique_viewers", []))
                            for channel in self.channel_stats:
                                if channel != "global" and "unique_viewers" in self.channel_stats[channel]:
                                    self.channel_stats[channel]["unique_viewers"] = set(self.channel_stats[channel]["unique_viewers"])
                            # Ensure global exists if file was empty or missing keys
                            if "global" not in self.channel_stats:
                                self.channel_stats["global"] = {"total_watch_time": 0, "total_bytes_transferred": 0, "unique_viewers": set(), "last_update": 0}
                        else:
                            logger.warning(f"⚠️ Fichier channel_stats ({self.channel_stats_file}) ne contient pas un JSON valide (objet). Utilisation des valeurs par défaut.")
                            self.channel_stats = {"global": {"total_watch_time": 0, "total_bytes_transferred": 0, "unique_viewers": set(), "last_update": 0}}
                else: # File still doesn't exist (creation failed)
                    self.channel_stats = {"global": {"total_watch_time": 0, "total_bytes_transferred": 0, "unique_viewers": set(), "last_update": 0}}
                    if not channel_file_existed: # Log only if we tried and failed to create
                         logger.warning("📊 Utilisation des channel_stats par défaut en mémoire.")
                    # No need to log if it existed but couldn't be read (json.load would raise error)


                # --- User Stats ---
                user_file_existed = self.user_stats_file.exists()
                if not user_file_existed:
                    logger.info(f"📊 Fichier user_stats introuvable ({self.user_stats_file}). Création d'un fichier vide.")
                    try:
                        with open(self.user_stats_file, 'w') as f:
                            f.write("{}")
                        os.chmod(self.user_stats_file, 0o666) # Set permissions
                    except Exception as e_create:
                        logger.error(f"❌ Impossible de créer le fichier user_stats: {e_create}")
                        # Proceed with default in-memory stats anyway

                # Try to load user stats (will load the empty one if just created)
                if self.user_stats_file.exists(): # Check again in case creation failed
                    with open(self.user_stats_file, 'r') as f:
                         loaded_user_stats = json.load(f)
                         # Basic validation
                         if isinstance(loaded_user_stats, dict) and "users" in loaded_user_stats:
                             self.user_stats = loaded_user_stats
                             # Ensure last_updated exists
                             if "last_updated" not in self.user_stats:
                                 self.user_stats["last_updated"] = int(time.time())
                         else:
                             logger.warning(f"⚠️ Fichier user_stats ({self.user_stats_file}) ne contient pas un JSON valide attendu. Utilisation des valeurs par défaut.")
                             self.user_stats = {"users": {}, "last_updated": int(time.time())}

                else: # File still doesn't exist (creation failed)
                    self.user_stats = {
                        "users": {},
                        "last_updated": int(time.time())
                    }
                    if not user_file_existed: # Log only if we tried and failed to create
                        logger.warning("📊 Utilisation des user_stats par défaut en mémoire.")
                    # No need to log if it existed but couldn't be read (json.load would raise error)


            except json.JSONDecodeError as e_json:
                logger.error(f"❌ Erreur de décodage JSON lors du chargement des stats: {e_json}. Fichier corrompu ? Utilisation des valeurs par défaut.")
                # Fallback to empty stats
                self.channel_stats = {"global": {"total_watch_time": 0, "total_bytes_transferred": 0, "unique_viewers": set(), "last_update": 0}}
                self.user_stats = {"users": {}, "last_updated": int(time.time())}
            except Exception as e:
                logger.error(f"❌ Erreur générale lors du chargement des stats: {e}", exc_info=True)
                # Fallback to empty stats
                self.channel_stats = {"global": {"total_watch_time": 0, "total_bytes_transferred": 0, "unique_viewers": set(), "last_update": 0}}
                self.user_stats = {
                    "users": {},
                    "last_updated": int(time.time())
                }

    def _save_loop(self):
        """Boucle de sauvegarde périodique."""
        while not self.stop_event.is_set():
            # Use event wait for interruptible sleep
            if self.stop_event.wait(self.save_interval):
                break # Exit if stop event is set
            try:
                self._save_stats()
            except Exception as e:
                logger.error(f"❌ Erreur dans la boucle de sauvegarde: {e}", exc_info=True)

    def _save_stats(self):
        """Sauvegarde les stats (mode bytes) dans les fichiers."""
        with self.lock:
            try:
                # Save channel stats
                channel_stats_to_save = self.channel_stats.copy()
                # Convert sets to lists for JSON serialization
                if "global" in channel_stats_to_save:
                    channel_stats_to_save["global"]["unique_viewers"] = list(channel_stats_to_save["global"]["unique_viewers"])
                for channel in channel_stats_to_save:
                    if channel != "global" and "unique_viewers" in channel_stats_to_save[channel]:
                        channel_stats_to_save[channel]["unique_viewers"] = list(channel_stats_to_save[channel]["unique_viewers"])
                
                with open(self.channel_stats_file, 'w') as f:
                    json.dump(channel_stats_to_save, f, indent=2)

                # Save user stats
                with open(self.user_stats_file, 'w') as f:
                    json.dump(self.user_stats, f, indent=2)

                logger.info(f"💾 Stats sauvegardées: {len(self.channel_stats.get('global', {}).get('unique_viewers', []))} utilisateurs uniques, {len(self.channel_stats) -1} chaînes actives.")
            except Exception as e:
                logger.error(f"❌ Erreur lors de la sauvegarde des stats: {e}")

    # --- Log Monitoring ---
    def _log_monitor_loop(self):
        """Surveille en continu le fichier access.log de Nginx."""
        logger.info(f"👁️ Démarrage surveillance Nginx Log: {NGINX_ACCESS_LOG}")
        last_inode = None
        try:
            while not self.stop_event.is_set():
                try:
                    current_inode = os.stat(NGINX_ACCESS_LOG).st_ino
                    if last_inode is None:
                        last_inode = current_inode

                    # Handle log rotation: reopen file if inode changes
                    if current_inode != last_inode:
                        logger.info(f"🔄 Détection rotation du fichier log Nginx. Réouverture...")
                        if 'f' in locals() and not f.closed:
                             f.close()
                        # Wait a moment for the file system
                        time.sleep(0.5)
                        f = open(NGINX_ACCESS_LOG, 'r')
                        last_inode = current_inode
                        logger.info("✅ Fichier log Nginx ré-ouvert.")
                    elif 'f' not in locals() or f.closed:
                         f = open(NGINX_ACCESS_LOG, 'r')
                         # Go to end only if it's the initial open or after rotation
                         if last_inode == current_inode:
                             f.seek(0, os.SEEK_END)
                             logger.info("⏩ Positionné à la fin du fichier log actuel.")

                    # Read new lines
                    line = f.readline()
                    while line:
                        self._parse_and_update_from_log(line.strip())
                        line = f.readline() # Read next line immediately if available

                    # If no lines were read, wait before checking again
                    if not line:
                         # Use interruptible wait
                        if self.stop_event.wait(0.2): # Check every 200ms
                            break

                except FileNotFoundError:
                    logger.warning(f"⏳ Fichier log Nginx introuvable: {NGINX_ACCESS_LOG}. Réessai dans 5 secondes...")
                    if 'f' in locals() and not f.closed:
                        f.close()
                    last_inode = None # Reset inode tracking
                    if self.stop_event.wait(5): # Wait 5 seconds before retrying
                         break
                except Exception as e:
                    logger.error(f"❌ Erreur dans la boucle de surveillance Nginx: {e}", exc_info=True)
                    # Avoid busy-looping on persistent errors
                    if self.stop_event.wait(5):
                        break # Exit if stop event is set during error wait

        finally:
            if 'f' in locals() and not f.closed:
                f.close()
            logger.info(f"🛑 Arrêt surveillance Nginx Log: {NGINX_ACCESS_LOG}")


    def _parse_and_update_from_log(self, line: str):
        """Parse une ligne de log et met à jour les stats si pertinent."""
        segment_match = LOG_SEGMENT_REGEX.match(line)
        if segment_match:
            ip_address, channel_name, bytes_str, user_agent = segment_match.groups()
            try:
                bytes_transferred = int(bytes_str)
                if bytes_transferred > 0:
                    # logger.debug(f"Log Segment: IP={ip_address}, Chan={channel_name}, Bytes={bytes_transferred}, UA={user_agent}")
                    self._record_log_activity(ip_address, channel_name, user_agent, bytes_transferred)
                # else: ignore 0 byte transfers for segments

            except ValueError:
                 logger.warning(f"Impossible de parser les bytes '{bytes_str}' depuis la ligne: {line}")
            return # Processed as segment

        playlist_match = LOG_PLAYLIST_REGEX.match(line)
        if playlist_match:
            ip_address, channel_name, user_agent = playlist_match.groups()
            # logger.debug(f"Log Playlist: IP={ip_address}, Chan={channel_name}, UA={user_agent}")
            # Record activity for playlists, but with 0 bytes transferred
            self._record_log_activity(ip_address, channel_name, user_agent, 0)
            return # Processed as playlist

        # Optional: Log other lines if needed for debugging
        else:
             logger.debug(f"Ligne ignorée (non HLS/format inconnu): {line}")


    def _record_log_activity(self, ip, channel, user_agent, bytes_transferred):
        """
        Enregistre l'activité d'un utilisateur basée sur les logs.
        Cette méthode est appelée par ClientMonitor ou d'autres composants qui analysent les logs.
        
        Args:
            ip: Adresse IP de l'utilisateur
            channel: Nom de la chaîne
            user_agent: User-agent du client
            bytes_transferred: Taille du transfert en octets (0 pour les segments HLS)
        """
        try:
            current_time = time.time()
            
            # Vérifier si l'utilisateur a changé de chaîne
            if not hasattr(self, '_current_channels'):
                self._current_channels = {}
            
            old_channel = self._current_channels.get(ip)
            if old_channel and old_channel != channel:
                # L'utilisateur a changé de chaîne, on le retire de l'ancienne
                if ip in self.user_stats["users"] and isinstance(self.user_stats["users"][ip], dict):
                    channels = self.user_stats["users"][ip].get('channels', {})
                    if old_channel in channels:
                        # Forcer l'inactivité sur l'ancienne chaîne
                        self.user_stats["users"][ip]['channels'][old_channel]['last_seen'] = 0
                        logger.info(f"🔄 {ip} a changé de chaîne: {old_channel} → {channel}")
                        
                        # Forcer une mise à jour des spectateurs de l'ancienne chaîne
                        active_ips_old = []
                        for viewer_ip in self.user_stats["users"]:
                            if viewer_ip == ip:
                                continue
                            viewer_data = self.user_stats["users"][viewer_ip]
                            if not isinstance(viewer_data, dict):
                                continue
                            channels = viewer_data.get('channels', {})
                            if not isinstance(channels, dict):
                                continue
                            if old_channel in channels:
                                channel_data = channels[old_channel]
                                if isinstance(channel_data, dict):
                                    last_seen = channel_data.get('last_seen', 0)
                                    if current_time - last_seen < self.viewer_timeout:
                                        active_ips_old.append(viewer_ip)
                        
                        if hasattr(self, 'update_watchers_callback'):
                            self.update_watchers_callback(old_channel, len(active_ips_old), active_ips_old, "/hls/", source="channel_change")
                            logger.info(f"[{old_channel}] 👥 Mise à jour après départ de {ip}: {len(active_ips_old)} spectateurs restants")
            
            # Mettre à jour la chaîne actuelle de l'utilisateur
            self._current_channels[ip] = channel
            
            # Vérifier si on a déjà fait une mise à jour récente pour cette IP/chaîne
            update_key = f"{ip}_{channel}"
            if hasattr(self, '_last_updates'):
                last_update = self._last_updates.get(update_key, 0)
                if current_time - last_update < HLS_SEGMENT_DURATION:
                    # Mise à jour trop récente, on accumule juste les bytes
                    if bytes_transferred > 0:
                        if hasattr(self, '_pending_bytes'):
                            self._pending_bytes[update_key] = self._pending_bytes.get(update_key, 0) + bytes_transferred
                    return
            else:
                self._last_updates = {}
                self._pending_bytes = {}
            
            # Mettre à jour le timestamp de dernière mise à jour
            self._last_updates[update_key] = current_time
            
            # Obtenir les bytes accumulés et réinitialiser
            accumulated_bytes = self._pending_bytes.get(update_key, 0) + bytes_transferred
            if update_key in self._pending_bytes:
                del self._pending_bytes[update_key]
            
            # Obtenir l'utilisateur ou en créer un nouveau
            if ip not in self.user_stats["users"]:
                self.user_stats["users"][ip] = {
                    "first_seen": current_time,
                    "last_seen": current_time,
                    "user_agent": user_agent,
                    "total_watch_time": 0,
                    "total_bytes": 0,
                    "channels": {}
                }
            user = self.user_stats["users"][ip]

            # Mettre à jour last_seen
            user["last_seen"] = current_time

            # Mettre à jour user_agent si disponible
            if user_agent and not user.get("user_agent"):
                user["user_agent"] = user_agent
            
            # Initialiser stats pour cette chaîne si nécessaire
            if channel not in user["channels"]:
                user["channels"][channel] = {
                    "first_seen": current_time,
                    "last_seen": current_time,
                    "total_watch_time": 0,
                    "total_bytes": 0
                }
            
            # Mettre à jour last_seen pour cette chaîne
            user["channels"][channel]["last_seen"] = current_time
            
            # Mettre à jour les bytes transférés si > 0
            if accumulated_bytes > 0:
                user["channels"][channel]["total_bytes"] = user["channels"][channel].get("total_bytes", 0) + accumulated_bytes
                user["total_bytes"] = user.get("total_bytes", 0) + accumulated_bytes

            # Update watch time for this specific channel for this user
            if bytes_transferred == 0:  # Indique que c'est un segment HLS
                # Calculer le temps écoulé depuis la dernière mise à jour
                last_update = user["channels"][channel].get("last_update_time", current_time)
                time_since_last_update = current_time - last_update
                
                # Si le temps écoulé est significatif, ajuster la durée
                if time_since_last_update > HLS_SEGMENT_DURATION:
                    # Ajuster la durée pour compenser les segments potentiellement manqués
                    adjusted_duration = HLS_SEGMENT_DURATION * (time_since_last_update / HLS_SEGMENT_DURATION)
                    # Limiter la compensation à 3x la durée normale
                    max_compensation = HLS_SEGMENT_DURATION * 3
                    segment_duration = min(adjusted_duration, max_compensation)
                else:
                    segment_duration = HLS_SEGMENT_DURATION
                
                # Mise à jour du temps de visionnage pour l'utilisateur
                old_time = user["channels"][channel].get("total_watch_time", 0)
                user["channels"][channel]["total_watch_time"] = old_time + segment_duration
                user["total_watch_time"] = user.get("total_watch_time", 0) + segment_duration
                
                # Mettre à jour le timestamp de la dernière mise à jour
                user["channels"][channel]["last_update_time"] = current_time
                
                logger.debug(f"⏱️ {ip} sur {channel}: +{segment_duration:.1f}s (total: {user['channels'][channel]['total_watch_time']:.1f}s)")

            # Vérifier si la chaîne existe dans les stats globales
            if channel not in self.channel_stats:
                self.channel_stats[channel] = {
                    "first_seen": current_time,
                    "last_seen": current_time,
                    "total_watch_time": 0,
                    "total_bytes": 0,
                    "unique_viewers": [ip]
                }
            else:
                # Mettre à jour last_seen
                self.channel_stats[channel]["last_seen"] = current_time
                
                # Ajouter l'IP à la liste des viewers uniques si elle n'y est pas déjà
                if ip not in self.channel_stats[channel].get("unique_viewers", []):
                    if "unique_viewers" not in self.channel_stats[channel]:
                        self.channel_stats[channel]["unique_viewers"] = []
                    self.channel_stats[channel]["unique_viewers"].append(ip)
            
            # CORRECTION: Ajouter l'IP aux viewers uniques globaux
            if "unique_viewers" not in self.channel_stats["global"]:
                if isinstance(self.channel_stats["global"], dict):
                    self.channel_stats["global"]["unique_viewers"] = set()
                else:
                    self.channel_stats["global"] = {"total_watch_time": 0, "total_bytes_transferred": 0, "unique_viewers": set(), "last_update": 0}
            
            # Si c'est un set, ajouter directement
            if isinstance(self.channel_stats["global"]["unique_viewers"], set):
                self.channel_stats["global"]["unique_viewers"].add(ip)
            # Si c'est une liste, vérifier si l'IP est déjà présente avant d'ajouter
            elif isinstance(self.channel_stats["global"]["unique_viewers"], list):
                if ip not in self.channel_stats["global"]["unique_viewers"]:
                    self.channel_stats["global"]["unique_viewers"].append(ip)
            
            # Update watch time and byte counts
            if bytes_transferred == 0:  # Segment HLS
                old_time = self.channel_stats[channel].get("total_watch_time", 0)
                self.channel_stats[channel]["total_watch_time"] = old_time + segment_duration
                self.channel_stats["global"]["total_watch_time"] = self.channel_stats["global"].get("total_watch_time", 0) + segment_duration
                logger.debug(f"⏱️ Global {channel}: +{segment_duration}s (total: {self.channel_stats[channel]['total_watch_time']}s)")
            else:
                self.channel_stats[channel]["total_bytes"] = self.channel_stats[channel].get("total_bytes", 0) + accumulated_bytes
                self.channel_stats["global"]["total_bytes"] = self.channel_stats["global"].get("total_bytes", 0) + accumulated_bytes
                logger.debug(f"📦 {ip} sur {channel}: +{accumulated_bytes} bytes (total: {self.channel_stats[channel]['total_bytes']} bytes)")
                logger.debug(f"📦 Global {channel}: +{accumulated_bytes} bytes (total: {self.channel_stats[channel]['total_bytes']} bytes)")
            
            # CRUCIAL: Notifier le gestionnaire IPTV ici directement des spectateurs actifs
            if hasattr(self, 'update_watchers_callback') and self.update_watchers_callback:
                # Construire la liste des IPs actives pour cette chaîne
                active_ips = []
                for viewer_ip in self.user_stats["users"]:
                    # Un spectateur n'est considéré actif que sur sa chaîne actuelle
                    if self._current_channels.get(viewer_ip) == channel:
                        viewer_data = self.user_stats["users"][viewer_ip]
                        if isinstance(viewer_data, dict):
                            channels = viewer_data.get('channels', {})
                            if isinstance(channels, dict) and channel in channels:
                                channel_data = channels[channel]
                                if isinstance(channel_data, dict):
                                    last_seen = channel_data.get('last_seen', 0)
                                    # Vérifier l'inactivité basée sur HLS_SEGMENT_DURATION * 3
                                    if current_time - last_seen < HLS_SEGMENT_DURATION * 3:
                                        active_ips.append(viewer_ip)
                
                # Forcer une mise à jour immédiate pour chaque segment
                if bytes_transferred == 0:  # C'est un segment HLS
                    try:
                        self.update_watchers_callback(channel, len(active_ips), active_ips, "/hls/", source="stats_collector_direct")
                        logger.info(f"[{channel}] 👁️ Mise à jour directe via StatsCollector: {len(active_ips)} spectateurs actifs")
                    except Exception as e:
                        logger.error(f"[{channel}] ❌ Erreur lors de la mise à jour directe des spectateurs: {str(e)}")
                
                logger.debug(f"✅ Mise à jour terminée pour {ip} sur {channel}")
            
        except Exception as e:
            logger.error(f"❌ Erreur d'enregistrement d'activité: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def handle_channel_change(self, ip, previous_channel, new_channel):
        """
        Gère le changement de chaîne pour un utilisateur spécifique.
        Cette méthode est appelée par ClientMonitor quand un changement de chaîne est détecté.
        
        Args:
            ip: Adresse IP de l'utilisateur qui change de chaîne
            previous_channel: Nom de la chaîne précédente
            new_channel: Nom de la nouvelle chaîne
        """
        logger.info(f"🔄 StatsCollector: Traitement du changement de chaîne {ip}: {previous_channel} → {new_channel}")
        try:
            current_time = time.time()
            
            # Retirer l'IP de l'ancienne chaîne dans le dictionnaire de suivi
            if hasattr(self, 'active_channel_by_ip'):
                if ip in self.active_channel_by_ip and self.active_channel_by_ip[ip] == previous_channel:
                    del self.active_channel_by_ip[ip]
            
            # Mettre à jour le dictionnaire de suivi pour la nouvelle chaîne
            self.active_channel_by_ip[ip] = new_channel
            
            # Mettre à jour les statistiques pour la nouvelle chaîne
            if new_channel not in self.channel_stats:
                self.channel_stats[new_channel] = {
                    "first_seen": current_time,
                    "last_seen": current_time,
                    "total_watch_time": 0,
                    "total_bytes": 0,
                    "unique_viewers": [ip]
                }
            else:
                # Mettre à jour last_seen
                self.channel_stats[new_channel]["last_seen"] = current_time
                
                # Ajouter l'IP à la liste des viewers uniques si elle n'y est pas déjà
                if "unique_viewers" not in self.channel_stats[new_channel]:
                    self.channel_stats[new_channel]["unique_viewers"] = []
                
                if ip not in self.channel_stats[new_channel]["unique_viewers"]:
                    self.channel_stats[new_channel]["unique_viewers"].append(ip)
            
            # CORRECTION: Ajouter l'IP aux viewers uniques globaux
            if "unique_viewers" not in self.channel_stats["global"]:
                if isinstance(self.channel_stats["global"], dict):
                    self.channel_stats["global"]["unique_viewers"] = set()
                else:
                    self.channel_stats["global"] = {"total_watch_time": 0, "total_bytes_transferred": 0, "unique_viewers": set(), "last_update": 0}
            
            # Si c'est un set, ajouter directement
            if isinstance(self.channel_stats["global"]["unique_viewers"], set):
                self.channel_stats["global"]["unique_viewers"].add(ip)
            # Si c'est une liste, vérifier si l'IP est déjà présente avant d'ajouter
            elif isinstance(self.channel_stats["global"]["unique_viewers"], list):
                if ip not in self.channel_stats["global"]["unique_viewers"]:
                    self.channel_stats["global"]["unique_viewers"].append(ip)
            
            # Mettre à jour les statistiques utilisateur
            if ip not in self.user_stats["users"]:
                self.user_stats["users"][ip] = {
                    "first_seen": current_time,
                    "last_seen": current_time,
                    "total_watch_time": 0,
                    "total_bytes": 0,
                    "channels": {}
                }
            
            # Mettre à jour le timestamp de dernière activité pour la nouvelle chaîne
            if new_channel not in self.user_stats["users"][ip].get("channels", {}):
                self.user_stats["users"][ip]["channels"][new_channel] = {
                    "first_seen": current_time,
                    "last_seen": current_time,
                    "total_watch_time": 0,
                    "total_bytes": 0
                }
            else:
                self.user_stats["users"][ip]["channels"][new_channel]["last_seen"] = current_time
            
            # Marquer explicitement l'IP comme inactive sur l'ancienne chaîne
            if previous_channel and previous_channel in self.user_stats["users"][ip].get("channels", {}):
                # Mettre à jour le timestamp de dernière activité pour forcer l'inactivité
                self.user_stats["users"][ip]["channels"][previous_channel]["last_seen"] = 0
            
            # Calculer les spectateurs actifs pour les deux chaînes
            active_ips_new_channel = []
            active_ips_old_channel = []
            
            # Parcourir les utilisateurs pour trouver les spectateurs actifs
            for viewer_ip, viewer_data in self.user_stats["users"].items():
                if isinstance(viewer_ip, str) and isinstance(viewer_data, dict) and "channels" in viewer_data:
                    # Pour la nouvelle chaîne
                    if new_channel in viewer_data["channels"]:
                        if isinstance(viewer_data["channels"][new_channel], dict) and "last_seen" in viewer_data["channels"][new_channel]:
                            last_seen = viewer_data["channels"][new_channel]["last_seen"]
                            if isinstance(last_seen, (int, float)) and current_time - last_seen < self.viewer_timeout:
                                active_ips_new_channel.append(viewer_ip)
                    
                    # Pour l'ancienne chaîne (exclure l'IP qui change)
                    if viewer_ip != ip and previous_channel in viewer_data["channels"]:
                        if isinstance(viewer_data["channels"][previous_channel], dict) and "last_seen" in viewer_data["channels"][previous_channel]:
                            last_seen = viewer_data["channels"][previous_channel]["last_seen"]
                            if isinstance(last_seen, (int, float)) and current_time - last_seen < self.viewer_timeout:
                                active_ips_old_channel.append(viewer_ip)
            
            # S'assurer que l'IP actuelle est incluse dans la liste des spectateurs de la nouvelle chaîne
            if ip not in active_ips_new_channel:
                active_ips_new_channel.append(ip)
            
            # Mise à jour des spectateurs pour les deux chaînes via le callback
            if hasattr(self, 'update_watchers_callback') and self.update_watchers_callback:
                # Mettre à jour l'ancienne chaîne
                try:
                    logger.info(f"[{previous_channel}] 🔄 Mise à jour après changement: {len(active_ips_old_channel)} spectateurs actifs restants")
                    self.update_watchers_callback(previous_channel, len(active_ips_old_channel), active_ips_old_channel, "/hls/", source="channel_change")
                except Exception as e:
                    logger.error(f"[{previous_channel}] ❌ Erreur lors de la mise à jour après changement: {str(e)}")
                
                # Mettre à jour la nouvelle chaîne
                try:
                    logger.info(f"[{new_channel}] 🔄 Mise à jour après changement: {len(active_ips_new_channel)} spectateurs actifs")
                    self.update_watchers_callback(new_channel, len(active_ips_new_channel), active_ips_new_channel, "/hls/", source="channel_change")
                except Exception as e:
                    logger.error(f"[{new_channel}] ❌ Erreur lors de la mise à jour après changement: {str(e)}")
                
                logger.info(f"✅ Changement de chaîne {ip}: {previous_channel} → {new_channel} traité avec succès")
            else:
                logger.warning("⚠️ update_watchers_callback non disponible, impossible de mettre à jour les spectateurs")
                
        except Exception as e:
            logger.error(f"❌ Erreur lors du traitement du changement de chaîne: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

    def _cleanup_loop(self):
        """Boucle de nettoyage périodique des viewers inactifs."""
        while not self.stop_event.is_set():
            try:
                self._cleanup_inactive_viewers()
            except Exception as e:
                logger.error(f"❌ Erreur dans la boucle de nettoyage: {e}", exc_info=True)
            
            # Attendre avant la prochaine vérification
            if self.stop_event.wait(self.cleanup_interval):
                break

    def _cleanup_inactive_viewers(self):
        """Nettoie les viewers inactifs sur toutes les chaînes."""
        current_time = time.time()
        with self.lock:
            # Parcourir toutes les chaînes
            for channel in list(self.channel_stats.keys()):
                if channel == "global":
                    continue
                
                # Trouver les viewers actifs pour cette chaîne
                active_ips = []
                for ip, user_data in self.user_stats["users"].items():
                    if not isinstance(user_data, dict):
                        continue
                    
                    channels = user_data.get("channels", {})
                    if not isinstance(channels, dict):
                        continue
                    
                    if channel in channels:
                        channel_data = channels[channel]
                        if isinstance(channel_data, dict):
                            last_seen = channel_data.get("last_seen", 0)
                            if current_time - last_seen < self.viewer_timeout:
                                active_ips.append(ip)
                
                # Mettre à jour les stats de la chaîne
                if "unique_viewers" in self.channel_stats[channel]:
                    self.channel_stats[channel]["unique_viewers"] = active_ips
                
                # Notifier le changement via le callback
                if hasattr(self, 'update_watchers_callback') and self.update_watchers_callback:
                    try:
                        self.update_watchers_callback(channel, len(active_ips), active_ips, "/hls/", source="periodic_cleanup")
                        logger.debug(f"[{channel}] 👥 Mise à jour périodique: {len(active_ips)} spectateurs actifs")
                    except Exception as e:
                        logger.error(f"[{channel}] ❌ Erreur lors de la mise à jour périodique: {str(e)}")

    def stop(self):
        """Arrête tous les threads."""
        self.stop_event.set()
        if hasattr(self, 'save_thread'):
            self.save_thread.join(timeout=5)
        if hasattr(self, 'log_monitor_thread'):
            self.log_monitor_thread.join(timeout=5)
        if hasattr(self, 'cleanup_thread'):
            self.cleanup_thread.join(timeout=5)
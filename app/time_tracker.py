import time
import threading
from typing import Dict, Optional, Set
from config import logger

class TimeTracker:
    """Classe centralisée pour gérer le suivi du temps de visionnage"""
    
    # Timeouts standardisés (en secondes)
    # REMOVED: SEGMENT_TIMEOUT = 300  # Augmenté à 5 minutes (300 secondes) - Seems unused now?
    # REMOVED: PLAYLIST_TIMEOUT = 300  # Augmenté à 5 minutes (300 secondes) - Seems unused now?
    WATCHER_INACTIVITY_TIMEOUT = 5  # Changed from 120s to 5s for near-instant updates
    DEBOUNCE_INTERVAL = 1.0  # Réduit à 1 seconde pour être plus réactif
    SEGMENT_DURATION = 4.0  # Durée standard d'un segment (Used by StatsCollector?)
    # Timeout pour la période tampon avant suppression réelle
    _removal_buffer_timeout = 2 # Changed from 60s to 2s for near-instant updates
    
    def __init__(self, stats_collector):
        self.stats_collector = stats_collector
        self._lock = threading.Lock()
        # MODIFIED: Simplified watcher structure
        self._watchers: Dict[str, Dict] = {}  # {ip: {"channel": str, "last_update": float, "expiry_time": float}}
        self._active_segments: Dict[str, Set[str]] = {}  # {channel: {ip}}
        self._last_cleanup_time = time.time()
        self._watcher_removal_buffer = {}  # {ip: {"time": float, "channel": str}} - Tampons pour éviter les suppressions prématurées
        
        logger.info(f"⏱️ TimeTracker initialisé avec les timeouts : Default Inactivity={self.WATCHER_INACTIVITY_TIMEOUT}s, Buffer={self._removal_buffer_timeout}s")
        
    # MODIFIED: Accept expiry_duration
    def record_activity(self, ip: str, channel: str, expiry_duration: Optional[float] = None):
        """Enregistre une activité pour un IP sur une chaîne, calculant son nouvel expiry_time."""
        with self._lock:
            current_time = time.time()
            
            # Calculate the absolute expiry time for this activity
            if expiry_duration and expiry_duration > 0:
                absolute_expiry_time = current_time + expiry_duration
                log_reason = f"specific duration {expiry_duration:.1f}s" 
            else:
                absolute_expiry_time = current_time + self.WATCHER_INACTIVITY_TIMEOUT
                log_reason = f"default timeout {self.WATCHER_INACTIVITY_TIMEOUT}s"

            # Mettre à jour ou ajouter l'entrée dans _watchers
            if ip in self._watchers:
                self._watchers[ip]["last_update"] = current_time
                self._watchers[ip]["channel"] = channel # Update channel in case it changed
                self._watchers[ip]["expiry_time"] = absolute_expiry_time # Update expiry time
            else:
                # Si le watcher n'est pas dans _watchers, créer une nouvelle entrée
                self._watchers[ip] = {
                    "channel": channel,
                    "last_update": current_time,
                    "expiry_time": absolute_expiry_time # Store calculated expiry time
                }
                logger.debug(f"[{channel}] 🆕 Nouveau watcher {ip} ajouté à TimeTracker.")

            # Ajouter/Maintenir l'IP dans l'ensemble actif pour la chaîne
            # (Cette partie est importante car get_active_watchers l'utilise)
            if channel not in self._active_segments:
                self._active_segments[channel] = set()
            self._active_segments[channel].add(ip)
            
            # Annuler toute suppression en attente pour cette IP
            if ip in self._watcher_removal_buffer:
                del self._watcher_removal_buffer[ip]
                logger.debug(f"[{channel}] ⏳ Suppression annulée pour {ip} suite à nouvelle activité.")
                
            logger.debug(f"[{channel}] ✅ Activité pour {ip}: Expiry calculé à {absolute_expiry_time:.1f} (raison: {log_reason})")
        
    # NOTE: These handlers might be redundant if ClientMonitor.record_activity is the only entry point
    # Keeping them for now in case StatsCollector or other components use them.
    # They will use the default timeout logic.
    def handle_segment_request(self, channel: str, ip: str) -> None:
        """Gère une requête de segment (probablement via StatsCollector)"""
        # Call record_activity with no specific duration -> uses default timeout
        self.record_activity(ip, channel)
        # Potentially add stats collection logic back here if needed and not redundant
        # current_time = time.time()
        # watcher = self._watchers.get(ip) # Get updated watcher data
        # if watcher and current_time - watcher.get("last_processed", 0) >= self.DEBOUNCE_INTERVAL:
        #     watcher["last_processed"] = current_time
        #     if self.stats_collector:
        #         self.stats_collector.add_watch_time(channel, ip, self.SEGMENT_DURATION)
        #     logger.debug(f"⏱️ Segment traité (stats) pour {ip} sur {channel}")
    
    def handle_playlist_request(self, channel: str, ip: str) -> None:
        """Gère une requête de playlist (probablement via StatsCollector)"""
        # Call record_activity with no specific duration -> uses default timeout
        self.record_activity(ip, channel)
        # Potentially add stats collection logic back here if needed and not redundant
        # ... (similar logic as in handle_segment_request for stats if required)
    
    def cleanup_inactive_watchers(self) -> None:
        """Nettoie les watchers inactifs en se basant sur leur expiry_time calculé."""
        with self._lock:
            current_time = time.time()
            
            # Changed from 60 seconds to 2 seconds to run cleanup much more frequently
            if current_time - self._last_cleanup_time < 2:
                return
                
            self._last_cleanup_time = current_time
            buffer_ips = []
            
            total_watchers_before = len(self._watchers)
            buffer_count_before = len(self._watcher_removal_buffer)
            logger.debug(f"🔍 Nettoyage TimeTracker: {total_watchers_before} watchers directs, {buffer_count_before} en buffer.")
            
            # 1. Traiter le buffer de suppression existant
            expired_buffer_ips = []
            for ip, data in self._watcher_removal_buffer.items():
                if current_time - data["time"] > self._removal_buffer_timeout:
                    expired_buffer_ips.append(ip)
                    logger.info(f"🧹 IP {ip} (sur {data.get('channel', 'inconnue')}) supprimée DÉFINITIVEMENT après buffer de {self._removal_buffer_timeout}s")
            
            # Supprimer les IPs expirées du buffer et de _active_segments
            for ip in expired_buffer_ips:
                channel_to_remove_from = self._watcher_removal_buffer.pop(ip, {}).get("channel")
                if channel_to_remove_from and channel_to_remove_from in self._active_segments:
                    self._active_segments[channel_to_remove_from].discard(ip)
                    if not self._active_segments[channel_to_remove_from]: # Nettoyer clé vide
                        del self._active_segments[channel_to_remove_from]
                    logger.debug(f"🧹 IP {ip} retirée de l'ensemble actif {channel_to_remove_from} après expiration buffer")
            
            # 2. Vérifier les watchers actifs (_watchers) pour inactivité
            inactive_candidates = [] # Utiliser une liste temporaire
            for ip, watcher in self._watchers.items(): # Iterate directly, decide later
                expiry_time = watcher.get("expiry_time")
                
                is_inactive = False
                inactivity_reason = ""
                
                if expiry_time:
                    if current_time > expiry_time:
                        is_inactive = True
                        inactivity_reason = f"expiry_time {expiry_time:.1f} dépassé"
                    else:
                        # Still active based on specific expiry
                        logger.debug(f"✅ Watcher {ip} actif (expiry dans {expiry_time - current_time:.1f}s)")
                else:
                    # Fallback (shouldn't happen often with new logic)
                    last_update = watcher.get("last_update", 0)
                    if current_time - last_update > self.WATCHER_INACTIVITY_TIMEOUT:
                        is_inactive = True
                        inactivity_reason = f"défaut timeout {self.WATCHER_INACTIVITY_TIMEOUT}s (last_update: {last_update:.1f})"
                    else:
                         logger.debug(f"✅ Watcher {ip} actif (fallback timeout, last_update il y a {current_time-last_update:.1f}s)")
                
                if is_inactive:
                    # Only consider putting in buffer if not already there
                    if ip not in self._watcher_removal_buffer:
                         inactive_candidates.append((ip, watcher, inactivity_reason))
                    # else: # Already in buffer, do nothing here
                    #     logger.debug(f"[{watcher.get('channel')}] Watcher {ip} déjà dans le buffer, ignoré.")

            # 3. Mettre les candidats inactifs dans le buffer et les retirer de _watchers
            for ip, watcher, reason in inactive_candidates:
                channel = watcher["channel"]
                buffer_ips.append(ip)
                self._watcher_removal_buffer[ip] = {
                    "time": current_time,
                    "channel": channel
                }
                # Remove from the main watcher list *only when adding to buffer*
                del self._watchers[ip]
                logger.info(f"⏱️ Watcher {ip} mis en buffer sur {channel} (raison: {reason})")
                
            # Log final du nettoyage
            total_watchers_after = len(self._watchers)
            buffer_count_after = len(self._watcher_removal_buffer)
            moved_to_buffer_count = len(buffer_ips)
            removed_definitively_count = len(expired_buffer_ips)

            log_summary = f"🧹 Nettoyage terminé: {removed_definitively_count} supprimés définitivement, {moved_to_buffer_count} déplacés vers buffer. État final: {total_watchers_after} directs, {buffer_count_after} en buffer."
            if removed_definitively_count > 0 or moved_to_buffer_count > 0:
                 logger.info(log_summary)
            else:
                 logger.debug(log_summary) # Log as debug if no changes
                
            # Nettoyage des entrées vides dans _active_segments (peut arriver si une chaîne n'a plus de watchers)
            empty_channels = [ch for ch, ips in self._active_segments.items() if not ips]
            for ch in empty_channels:
                del self._active_segments[ch]
                logger.debug(f"🧹 Canal vide '{ch}' retiré de _active_segments.")

    def get_watcher_channel(self, ip: str) -> Optional[str]:
        """Retourne la chaîne actuelle d'un watcher (y compris s'il est dans le buffer)."""
        with self._lock:
            watcher = self._watchers.get(ip)
            return watcher["channel"] if watcher else None
    
    def is_being_removed(self, ip: str) -> bool:
        """Vérifie si un watcher est en cours de suppression (dans le buffer)"""
        return ip in self._watcher_removal_buffer 

    def get_active_watchers(self, channel: str = None, include_buffer: bool = False) -> Set[str]:
        """Retourne l'ensemble des IPs des watchers actifs, optionnellement pour un canal spécifique.
           Si include_buffer est True, inclut aussi les IPs dans le buffer de suppression.
        """
        with self._lock:
            if channel:
                active_set = self._active_segments.get(channel, set())
            else:
                active_set = set().union(*self._active_segments.values())
            
            if include_buffer:
                active_set.update(self._watcher_removal_buffer.keys())
            
            return active_set
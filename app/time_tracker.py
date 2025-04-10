import time
import threading
from typing import Dict, Optional, Set
from config import logger, HLS_SEGMENT_DURATION, VIEWER_INACTIVITY_TIMEOUT

class TimeTracker:
    """Classe centralisée pour gérer le suivi du temps de visionnage"""
    
    # Timeouts standardisés (en secondes)
    # REMOVED: SEGMENT_TIMEOUT = 300  # Augmenté à 5 minutes (300 secondes) - Seems unused now?
    # REMOVED: PLAYLIST_TIMEOUT = 300  # Augmenté à 5 minutes (300 secondes) - Seems unused now?
    WATCHER_INACTIVITY_TIMEOUT = VIEWER_INACTIVITY_TIMEOUT  # Timeout basé sur la variable d'environnement
    DEBOUNCE_INTERVAL = 1.0  # Réduit à 1 seconde pour être plus réactif
    SEGMENT_DURATION = HLS_SEGMENT_DURATION  # Utilise la variable globale de config.py
    # Timeout pour la période tampon avant suppression réelle
    _removal_buffer_timeout = 2  # Changed from 60s to 2s for near-instant updates
    
    def __init__(self, stats_collector):
        self.stats_collector = stats_collector
        self._lock = threading.Lock()
        # MODIFIED: Simplified watcher structure
        self._watchers: Dict[str, Dict] = {}  # {ip: {"channel": str, "last_update": float, "expiry_time": float}}
        self._active_segments: Dict[str, Set[str]] = {}  # {channel: {ip}}
        self._last_cleanup_time = time.time()
        self._watcher_removal_buffer = {}  # {ip: {"time": float, "channel": str}} - Tampons pour éviter les suppressions prématurées
        
        logger.info(f"⏱️ TimeTracker initialisé avec HLS_SEGMENT_DURATION={self.SEGMENT_DURATION}s, timeout={self.WATCHER_INACTIVITY_TIMEOUT}s")
        
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

            # Check if this IP was on a different channel before
            previous_channel = None
            channel_changed = False
            if ip in self._watchers:
                previous_channel = self._watchers[ip].get("channel")
                if previous_channel and previous_channel != channel:
                    channel_changed = True
                    # The user changed channels - immediately clean up the previous channel
                    logger.debug(f"🔄 User {ip} changed channel: {previous_channel} -> {channel}. Removing from previous.")
                    # Remove from active set of previous channel
                    if previous_channel in self._active_segments and ip in self._active_segments[previous_channel]:
                        self._active_segments[previous_channel].remove(ip)
                        # If the set is now empty, remove the channel
                        if not self._active_segments[previous_channel]:
                            del self._active_segments[previous_channel]
                    
                    # Explicitly notify stats collector about the channel change if available
                    if hasattr(self, 'stats_collector') and self.stats_collector:
                        # Notify stats collector about channel change
                        self.stats_collector.handle_channel_change(ip, previous_channel, channel)

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
        
        # Force a cleanup if the channel changed to immediately update counts
        if channel_changed and previous_channel:
            # Signal that the previous channel needs immediate update due to channel change
            self._last_cleanup_time = 0  # Forcer un nettoyage immédiat
            self.cleanup_inactive_watchers()
            # Forcer la mise à jour explicite de l'ancien canal pour s'assurer qu'il est nettoyé
            self.force_channel_status_update(previous_channel)
    
    def handle_segment_request(self, channel: str, ip: str) -> None:
        """
        DÉPRÉCIÉ - Méthode de compatibilité avec l'ancien système.
        
        Utilisez plutôt `record_activity` directement avec les paramètres appropriés.
        Cette méthode est conservée uniquement pour la compatibilité avec le StatsCollector.
        """
        # Simple délégation à record_activity
        self.record_activity(ip, channel)
    
    def handle_playlist_request(self, channel: str, ip: str) -> None:
        """
        DÉPRÉCIÉ - Méthode de compatibilité avec l'ancien système.
        
        Utilisez plutôt `record_activity` directement avec les paramètres appropriés.
        Cette méthode est conservée uniquement pour la compatibilité avec le StatsCollector.
        """
        # Simple délégation à record_activity
        self.record_activity(ip, channel)
    
    def cleanup_inactive_watchers(self) -> None:
        """Nettoie les watchers inactifs en se basant sur leur expiry_time calculé."""
        with self._lock:
            current_time = time.time()
            
            # Executez le nettoyage au maximum toutes les secondes pour éviter trop de charges
            if current_time - self._last_cleanup_time < 1.0:
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
                    # Log explicite pour la suppression définitive du viewer
                    logger.info(f"🗑️ Viewer {ip} supprimé DÉFINITIVEMENT de la chaîne {channel_to_remove_from}")
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
                logger.debug(f"⏱️ Watcher {ip} mis en buffer sur {channel} (raison: {reason})")
                
                # S'assurer que l'IP est retirée immédiatement des segments actifs
                if channel in self._active_segments and ip in self._active_segments[channel]:
                    self._active_segments[channel].remove(ip)
                    # Log explicite pour indiquer la suppression du viewer de la chaîne
                    logger.debug(f"🚫 Viewer {ip} supprimé de la chaîne {channel} pour cause d'inactivité")
                    if not self._active_segments[channel]:
                        del self._active_segments[channel]
                
            # Log final du nettoyage
            total_watchers_after = len(self._watchers)
            buffer_count_after = len(self._watcher_removal_buffer)
            moved_to_buffer_count = len(buffer_ips)
            removed_definitively_count = len(expired_buffer_ips)

            if removed_definitively_count > 0 or moved_to_buffer_count > 0:
                 log_summary = f"🧹 Nettoyage terminé: {removed_definitively_count} supprimés définitivement, {moved_to_buffer_count} déplacés vers buffer. État final: {total_watchers_after} directs, {buffer_count_after} en buffer."
                 logger.debug(log_summary)
                
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
            # Si on demande les watchers pour un canal spécifique
            if channel:
                # On récupère uniquement les watchers actifs (non en buffer) pour ce canal
                active_set = self._active_segments.get(channel, set()).copy()
                
                # Si on veut inclure le buffer, on ajoute les IPs du buffer QUI SONT POUR CE CANAL SPECIFIQUE
                if include_buffer:
                    buffer_ips_for_channel = {
                        ip for ip, data in self._watcher_removal_buffer.items() 
                        if data.get("channel") == channel
                    }
                    active_set.update(buffer_ips_for_channel)
            else:
                # Pour tous les canaux, on récupère tous les watchers actifs
                active_set = set().union(*self._active_segments.values()) if self._active_segments else set()
                
                # Si on veut inclure le buffer, on ajoute toutes les IPs du buffer
                if include_buffer:
                    active_set.update(self._watcher_removal_buffer.keys())
            
            return active_set
    
    def force_flush_inactive(self):
        """Force un nettoyage immédiat de tous les viewers inactifs"""
        logger.info("🧹 Forçage du nettoyage des viewers inactifs")
        
        # Remettre à zéro le timestamp du dernier nettoyage
        self._last_cleanup_time = 0
        
        # Lancer un nettoyage immédiat
        self.cleanup_inactive_watchers()
        
        # Nettoyer définitivement tous les viewers en buffer
        current_time = time.time()
        with self._lock:
            # Récupérer tous les viewers en buffer
            buffered_viewers = list(self._watcher_removal_buffer.items())
            
            for ip, data in buffered_viewers:
                channel = data.get("channel", "unknown")
                logger.info(f"🗑️ Flush forcé: Viewer {ip} supprimé DÉFINITIVEMENT de la chaîne {channel}")
                
                # Supprimer du buffer
                if ip in self._watcher_removal_buffer:
                    del self._watcher_removal_buffer[ip]
                
                # Supprimer également des segments actifs (par précaution)
                if channel in self._active_segments and ip in self._active_segments[channel]:
                    self._active_segments[channel].remove(ip)
                    if not self._active_segments[channel]:
                        del self._active_segments[channel]
            
            if buffered_viewers:
                logger.info(f"🧹 Flush terminé: {len(buffered_viewers)} viewers supprimés définitivement")
            else:
                logger.info("✓ Aucun viewer à nettoyer dans le buffer")

    def force_channel_status_update(self, channel: str):
        """Force la mise à jour explicite de l'état d'un canal"""
        with self._lock:
            logger.info(f"🔄 Forçage de la mise à jour du statut pour le canal: {channel}")
            
            # Vérifier si le canal existe dans nos données
            active_watchers = self._active_segments.get(channel, set()).copy()
            
            if not active_watchers:
                logger.info(f"ℹ️ Canal {channel} n'a aucun watcher actif, notification aux gestionnaires")
                # Notifier le gestionnaire de stats que ce canal n'a plus de watchers
                if hasattr(self, 'stats_collector') and self.stats_collector:
                    self.stats_collector.update_channel_watchers(channel, 0)
            else:
                logger.info(f"ℹ️ Canal {channel} a {len(active_watchers)} watchers actifs: {list(active_watchers)}")
                # Notifier le gestionnaire de stats de l'état actuel
                if hasattr(self, 'stats_collector') and self.stats_collector:
                    self.stats_collector.update_channel_watchers(channel, len(active_watchers))
                
                # Forcer la mise à jour des temps d'activité pour tous les watchers de ce canal
                for ip in active_watchers:
                    current_channel = self.get_watcher_channel(ip)
                    if current_channel == channel:
                        logger.debug(f"🔄 Mise à jour du timestamp pour watcher {ip} sur {channel}")
                    elif current_channel and current_channel != channel:
                        logger.warning(f"⚠️ Watcher {ip} apparaît dans {channel} mais est actif sur {current_channel}")
                        # Le retirer de ce canal puisqu'il est actif ailleurs
                        if channel in self._active_segments and ip in self._active_segments[channel]:
                            self._active_segments[channel].remove(ip)
                            if not self._active_segments[channel]:
                                del self._active_segments[channel]
                            logger.info(f"🗑️ Watcher {ip} retiré de {channel} car actif sur {current_channel}")
                            # Notifier de la mise à jour
                            if hasattr(self, 'stats_collector') and self.stats_collector:
                                self.stats_collector.update_channel_watchers(channel, len(self._active_segments.get(channel, set())))
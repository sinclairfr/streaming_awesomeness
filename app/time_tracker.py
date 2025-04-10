import time
import threading
from typing import Dict, Optional, Set
from config import logger

class TimeTracker:
    """Classe centralisée pour gérer le suivi du temps de visionnage"""
    
    # Timeouts standardisés (en secondes)
    # SEGMENT_TIMEOUT = 300  # Augmenté à 5 minutes (300 secondes) - Seems unused now?
    # PLAYLIST_TIMEOUT = 300  # Augmenté à 5 minutes (300 secondes) - Seems unused now?
    WATCHER_INACTIVITY_TIMEOUT = 120  # 2 minutes d'inactivité (réduit de 900)
    DEBOUNCE_INTERVAL = 1.0  # Réduit à 1 seconde pour être plus réactif
    SEGMENT_DURATION = 4.0  # Durée standard d'un segment
    # Timeout pour la période tampon avant suppression réelle
    _removal_buffer_timeout = 60 # 1 minute (réduit de 300)
    
    def __init__(self, stats_collector):
        self.stats_collector = stats_collector
        self._lock = threading.Lock()
        self._watchers: Dict[str, Dict] = {}  # {ip: {"channel": str, "last_update": float, "last_segment": float, "last_playlist": float}}
        self._active_segments: Dict[str, Set[str]] = {}  # {channel: {ip}}
        self._last_cleanup_time = time.time()
        self._watcher_removal_buffer = {}  # {ip: {"time": float, "channel": str}} - Tampons pour éviter les suppressions prématurées
        # self._removal_buffer_timeout = 300  # Déplacé vers le haut
        
        logger.info(f"⏱️ TimeTracker initialisé avec les timeouts : Watcher={self.WATCHER_INACTIVITY_TIMEOUT}s, Segment={self.SEGMENT_DURATION}s")
        
    def record_activity(self, ip: str, channel: str):
        """Enregistre une activité pour un IP sur une chaîne, réinitialisant son timer d'inactivité."""
        with self._lock:
            current_time = time.time()
            
            # Mettre à jour ou ajouter l'entrée dans _active_segments
            if channel not in self._active_segments:
                self._active_segments[channel] = set() # Use set directly
            
            # Mettre à jour la dernière activité DANS _watchers (si présent)
            if ip in self._watchers:
                self._watchers[ip]["last_update"] = current_time
                self._watchers[ip]["channel"] = channel # Update channel in case it changed
                # Maybe update type? Depends on source of activity call
            else:
                # Si le watcher n'est pas dans _watchers, il est peut-être dans le buffer
                # ou c'est un tout nouveau. Créons/MàJ son entrée de base.
                self._watchers[ip] = {
                    "channel": channel,
                    "last_update": current_time,
                    "last_segment": current_time, # Assume segment if type unknown
                    "last_playlist": 0,
                    "type": "unknown" 
                }

            # Ajouter l'IP à l'ensemble actif pour la chaîne
            self._active_segments[channel].add(ip)
            
            # Annuler toute suppression en attente pour cette IP (très important)
            if ip in self._watcher_removal_buffer:
                del self._watcher_removal_buffer[ip]
                logger.debug(f"[{channel}] ⏳ Suppression annulée pour {ip} suite à nouvelle activité.")
                
            logger.debug(f"[{channel}] ✅ Activité enregistrée pour {ip} à {current_time:.1f}")
        
    def handle_segment_request(self, channel: str, ip: str) -> None:
        """Gère une requête de segment"""
        with self._lock:
            current_time = time.time()
            
            # Retirer l'IP du buffer de suppression si elle existe
            self._watcher_removal_buffer.pop(ip, None)
            
            # Initialiser les structures si nécessaire
            if ip not in self._watchers:
                self._watchers[ip] = {
                    "channel": channel,
                    "last_update": current_time,
                    "last_segment": current_time,
                    "last_playlist": 0,
                    "type": "segment"  # Traquer le type de requête
                }
                logger.info(f"🆕 TimeTracker: Nouveau watcher {ip} sur {channel}")
            else:
                old_channel = self._watchers[ip]["channel"]
                if old_channel != channel:
                    # Si le watcher change de chaîne, le supprimer de l'ancienne chaîne
                    if old_channel in self._active_segments:
                        self._active_segments[old_channel].discard(ip)
                        logger.info(f"🔄 TimeTracker: Watcher {ip} change de chaîne: {old_channel} -> {channel}")
            
            watcher = self._watchers[ip]
            
            # Mettre à jour les timestamps (toujours, même si c'est trop proche)
            watcher["last_segment"] = current_time
            watcher["last_update"] = current_time
            watcher["channel"] = channel
            watcher["type"] = "segment"  # Mettre à jour le type
            
            # Mettre à jour les segments actifs (toujours)
            self._active_segments.setdefault(channel, set()).add(ip)
            
            # Vérifier si c'est un nouveau segment pour les statistiques
            if current_time - watcher.get("last_processed", 0) >= self.DEBOUNCE_INTERVAL:
                watcher["last_processed"] = current_time
                # Mettre à jour le temps de visionnage
                if self.stats_collector:
                    self.stats_collector.add_watch_time(channel, ip, self.SEGMENT_DURATION)
                
                logger.debug(f"⏱️ Segment traité pour {ip} sur {channel}")
    
    def handle_playlist_request(self, channel: str, ip: str) -> None:
        """Gère une requête de playlist"""
        with self._lock:
            current_time = time.time()
            
            # Retirer l'IP du buffer de suppression si elle existe
            self._watcher_removal_buffer.pop(ip, None)
            
            # Initialiser les structures si nécessaire
            if ip not in self._watchers:
                self._watchers[ip] = {
                    "channel": channel,
                    "last_update": current_time,
                    "last_segment": 0,
                    "last_playlist": current_time,
                    "type": "playlist"  # Traquer le type de requête
                }
                logger.info(f"🆕 TimeTracker: Nouveau watcher {ip} sur {channel} (playlist)")
            else:
                old_channel = self._watchers[ip]["channel"]
                if old_channel != channel:
                    # Si le watcher change de chaîne, le supprimer de l'ancienne chaîne
                    if old_channel in self._active_segments:
                        self._active_segments[old_channel].discard(ip)
                        logger.info(f"🔄 TimeTracker: Watcher {ip} change de chaîne: {old_channel} -> {channel}")
            
            watcher = self._watchers[ip]
            
            # Mettre à jour les timestamps (toujours, même si c'est trop proche)
            watcher["last_playlist"] = current_time
            watcher["last_update"] = current_time
            watcher["channel"] = channel
            watcher["type"] = "playlist"  # Mettre à jour le type
            
            # Mettre à jour les segments actifs (toujours pour une playlist)
            self._active_segments.setdefault(channel, set()).add(ip)
            
            # Vérifier si c'est une nouvelle playlist pour les statistiques
            if current_time - watcher.get("last_processed", 0) >= self.DEBOUNCE_INTERVAL:
                watcher["last_processed"] = current_time
                
                # Calculer le temps écoulé depuis la dernière mise à jour
                elapsed = min(current_time - watcher["last_update"], self.PLAYLIST_TIMEOUT)
                
                # Mettre à jour le temps de visionnage
                if self.stats_collector and elapsed > 0:
                    self.stats_collector.add_watch_time(channel, ip, elapsed)
                
                logger.debug(f"⏱️ Playlist traitée pour {ip} sur {channel} (temps: {elapsed:.1f}s)")
    
    def cleanup_inactive_watchers(self) -> None:
        """Nettoie les watchers inactifs"""
        with self._lock:
            current_time = time.time()
            
            # Limiter la fréquence des nettoyages complets (une fois par minute max)
            if current_time - self._last_cleanup_time < 60:
                return
                
            self._last_cleanup_time = current_time
            inactive_ips = []
            buffer_ips = []
            
            # AJOUT DE LOG: nombre total de watchers avant nettoyage
            total_watchers = len(self._watchers)
            logger.debug(f"🔍 Vérification de {total_watchers} watchers pour inactivité")
            
            # Traiter d'abord le buffer de suppression
            expired_buffer = []
            truly_removed_ips = set()
            for ip, data in self._watcher_removal_buffer.items():
                if current_time - data["time"] > self._removal_buffer_timeout:
                    expired_buffer.append(ip)
                    truly_removed_ips.add(ip) # Keep track of IPs whose buffer expired
                    logger.info(f"🧹 IP {ip} supprimée DÉFINITIVEMENT après période tampon de {self._removal_buffer_timeout}s")
            
            # Supprimer les IPs expirées du buffer
            for ip in expired_buffer:
                channel_to_remove_from = self._watcher_removal_buffer.get(ip, {}).get("channel")
                self._watcher_removal_buffer.pop(ip, None)
                if channel_to_remove_from and channel_to_remove_from in self._active_segments:
                    self._active_segments[channel_to_remove_from].discard(ip)
                    logger.debug(f"🧹 IP {ip} retirée de l'ensemble actif pour {channel_to_remove_from} après expiration buffer")
            
            # Now check current watchers for inactivity
            for ip, watcher in list(self._watchers.items()): # Iterate over a copy
                # Vérifier l'inactivité basée sur le type de requête le plus récent
                last_activity = max(watcher.get("last_segment", 0), watcher.get("last_playlist", 0))
                inactivity_time = current_time - last_activity
                
                # Utiliser le timeout très long configuré
                if inactivity_time > self.WATCHER_INACTIVITY_TIMEOUT:
                    # Check if already in buffer (shouldn't happen if iterating self._watchers)
                    if ip not in self._watcher_removal_buffer:
                        channel = watcher["channel"]
                        
                        # Mettre dans le buffer
                        buffer_ips.append(ip)
                        self._watcher_removal_buffer[ip] = {
                            "time": current_time,
                            "channel": channel,
                            "type": watcher.get("type", "unknown")
                        }
                        
                        # AJOUT DE LOG: raison de mise en buffer
                        logger.info(f"⏱️ Watcher {ip} mis en buffer sur {channel} (inactif depuis {inactivity_time:.1f}s > {self.WATCHER_INACTIVITY_TIMEOUT}s)")
                            
                        # Supprimer de la liste principale _watchers seulement quand on met en buffer
                        del self._watchers[ip]
                        
                else:
                    # AJOUT DE LOG: watcher encore actif
                    logger.debug(f"✅ Watcher {ip} encore actif (dernière activité il y a {inactivity_time:.1f}s)")
                
            # AJOUT DE LOG: résumé de l'opération
            if buffer_ips: # Changed from inactive_ips to buffer_ips for clarity
                logger.info(f"🧹 {len(buffer_ips)}/{total_watchers} watchers mis en buffer pour inactivité (seront retirés après timeout)")
            else:
                logger.debug(f"✅ Aucun nouveau watcher inactif à mettre en buffer")
                
            # Log des watchers actifs par chaîne
            # This log might be slightly misleading now as it only shows _watchers (non-buffered)
            # Consider logging active_segments instead for a truer picture of active connections
                channel_counts = {}
            for channel, ips in self._active_segments.items():
                if ips: # Only log channels with active IPs in the segment tracker
                   channel_counts[channel] = len(ips)
            
            if channel_counts:
                for channel, count in channel_counts.items():
                    logger.debug(f"👥 État TimeTracker - {channel}: {count} watchers actifs (selon _active_segments)")
    
    def get_watcher_channel(self, ip: str) -> Optional[str]:
        """Retourne la chaîne actuelle d'un watcher"""
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
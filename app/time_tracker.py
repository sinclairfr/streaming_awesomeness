import time
import threading
from typing import Dict, Optional, Set
from config import logger

class TimeTracker:
    """Classe centralisée pour gérer le suivi du temps de visionnage"""
    
    # Timeouts standardisés (en secondes)
    SEGMENT_TIMEOUT = 30
    PLAYLIST_TIMEOUT = 20
    DEBOUNCE_INTERVAL = 3.0  # Intervalle minimum entre deux mises à jour
    SEGMENT_DURATION = 4.0  # Durée standard d'un segment
    
    def __init__(self, stats_collector):
        self.stats_collector = stats_collector
        self._lock = threading.Lock()
        self._watchers: Dict[str, Dict] = {}  # {ip: {"channel": str, "last_update": float, "last_segment": float, "last_playlist": float}}
        self._active_segments: Dict[str, Set[str]] = {}  # {channel: {ip}}
        
    def handle_segment_request(self, channel: str, ip: str) -> None:
        """Gère une requête de segment"""
        with self._lock:
            current_time = time.time()
            
            # Initialiser les structures si nécessaire
            if ip not in self._watchers:
                self._watchers[ip] = {
                    "channel": channel,
                    "last_update": 0,
                    "last_segment": 0,
                    "last_playlist": 0
                }
            
            watcher = self._watchers[ip]
            
            # Vérifier si c'est un nouveau segment
            if current_time - watcher["last_segment"] >= self.DEBOUNCE_INTERVAL:
                # Mettre à jour le temps de visionnage
                if self.stats_collector:
                    self.stats_collector.add_watch_time(channel, ip, self.SEGMENT_DURATION)
                
                # Mettre à jour les timestamps
                watcher["last_segment"] = current_time
                watcher["last_update"] = current_time
                watcher["channel"] = channel
                
                # Mettre à jour les segments actifs
                self._active_segments.setdefault(channel, set()).add(ip)
                
                logger.debug(f"⏱️ Segment traité pour {ip} sur {channel}")
    
    def handle_playlist_request(self, channel: str, ip: str) -> None:
        """Gère une requête de playlist"""
        with self._lock:
            current_time = time.time()
            
            # Initialiser les structures si nécessaire
            if ip not in self._watchers:
                self._watchers[ip] = {
                    "channel": channel,
                    "last_update": 0,
                    "last_segment": 0,
                    "last_playlist": 0
                }
            
            watcher = self._watchers[ip]
            
            # Vérifier si c'est une nouvelle playlist
            if current_time - watcher["last_playlist"] >= self.DEBOUNCE_INTERVAL:
                # Calculer le temps écoulé depuis la dernière mise à jour
                elapsed = min(current_time - watcher["last_update"], self.PLAYLIST_TIMEOUT)
                
                # Mettre à jour le temps de visionnage
                if self.stats_collector and elapsed > 0:
                    self.stats_collector.add_watch_time(channel, ip, elapsed)
                
                # Mettre à jour les timestamps
                watcher["last_playlist"] = current_time
                watcher["last_update"] = current_time
                watcher["channel"] = channel
                
                logger.debug(f"⏱️ Playlist traitée pour {ip} sur {channel} (temps: {elapsed:.1f}s)")
    
    def cleanup_inactive_watchers(self) -> None:
        """Nettoie les watchers inactifs"""
        with self._lock:
            current_time = time.time()
            inactive_ips = []
            
            for ip, watcher in self._watchers.items():
                # Vérifier l'inactivité basée sur le type de requête le plus récent
                last_activity = max(watcher["last_segment"], watcher["last_playlist"])
                
                if current_time - last_activity > self.SEGMENT_TIMEOUT:
                    inactive_ips.append(ip)
                    channel = watcher["channel"]
                    if channel in self._active_segments:
                        self._active_segments[channel].discard(ip)
            
            # Supprimer les watchers inactifs
            for ip in inactive_ips:
                del self._watchers[ip]
                logger.info(f"🧹 Watcher inactif supprimé: {ip}")
    
    def get_active_watchers(self, channel: str) -> Set[str]:
        """Retourne les watchers actifs pour une chaîne"""
        with self._lock:
            return self._active_segments.get(channel, set())
    
    def get_watcher_channel(self, ip: str) -> Optional[str]:
        """Retourne la chaîne actuelle d'un watcher"""
        with self._lock:
            watcher = self._watchers.get(ip)
            return watcher["channel"] if watcher else None 
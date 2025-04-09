import time
import threading
from typing import Dict, Optional, Set
from config import logger

class TimeTracker:
    """Classe centralisée pour gérer le suivi du temps de visionnage"""
    
    # Timeouts standardisés (en secondes)
    SEGMENT_TIMEOUT = 300  # Augmenté à 5 minutes (300 secondes)
    PLAYLIST_TIMEOUT = 300  # Augmenté à 5 minutes (300 secondes)
    WATCHER_INACTIVITY_TIMEOUT = 900  # 15 minutes d'inactivité (augmenté de 10 à 15 minutes)
    DEBOUNCE_INTERVAL = 1.0  # Réduit à 1 seconde pour être plus réactif
    SEGMENT_DURATION = 4.0  # Durée standard d'un segment
    
    def __init__(self, stats_collector):
        self.stats_collector = stats_collector
        self._lock = threading.Lock()
        self._watchers: Dict[str, Dict] = {}  # {ip: {"channel": str, "last_update": float, "last_segment": float, "last_playlist": float}}
        self._active_segments: Dict[str, Set[str]] = {}  # {channel: {ip}}
        self._last_cleanup_time = time.time()
        self._watcher_removal_buffer = {}  # {ip: {"time": float, "channel": str}} - Tampons pour éviter les suppressions prématurées
        self._removal_buffer_timeout = 300  # 5 minutes de période tampon
        
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
            for ip, data in self._watcher_removal_buffer.items():
                if current_time - data["time"] > self._removal_buffer_timeout:
                    expired_buffer.append(ip)
                    logger.info(f"🧹 IP {ip} supprimée du buffer après période tampon de {self._removal_buffer_timeout}s")
            
            # Supprimer les IPs expirées du buffer
            for ip in expired_buffer:
                self._watcher_removal_buffer.pop(ip, None)
            
            for ip, watcher in self._watchers.items():
                # Vérifier l'inactivité basée sur le type de requête le plus récent
                last_activity = max(watcher["last_segment"], watcher["last_playlist"])
                inactivity_time = current_time - last_activity
                
                # Utiliser le timeout très long configuré
                if inactivity_time > self.WATCHER_INACTIVITY_TIMEOUT:
                    channel = watcher["channel"]
                    
                    # Au lieu de supprimer immédiatement, placer dans le buffer
                    buffer_ips.append(ip)
                    self._watcher_removal_buffer[ip] = {
                        "time": current_time,
                        "channel": channel,
                        "type": watcher.get("type", "unknown")
                    }
                    
                    # AJOUT DE LOG: raison de mise en buffer
                    logger.info(f"⏱️ Watcher {ip} mis en buffer sur {channel} (inactif depuis {inactivity_time:.1f}s > {self.WATCHER_INACTIVITY_TIMEOUT}s)")
                    if channel in self._active_segments:
                        self._active_segments[channel].discard(ip)
                else:
                    # AJOUT DE LOG: watcher encore actif
                    logger.debug(f"✅ Watcher {ip} encore actif (dernière activité il y a {inactivity_time:.1f}s)")
            
            # Supprimer les watchers mis en buffer de la liste active
            for ip in buffer_ips:
                inactive_ips.append(ip)
                del self._watchers[ip]
                
            # AJOUT DE LOG: résumé de l'opération
            if inactive_ips:
                logger.info(f"🧹 {len(inactive_ips)}/{total_watchers} watchers mis en buffer pour inactivité")
            else:
                logger.debug(f"✅ Aucun watcher inactif à mettre en buffer")
                
            # Log des watchers actifs par chaîne
            if total_watchers > 0:
                channel_counts = {}
                for ip, watcher in self._watchers.items():
                    channel = watcher["channel"]
                    channel_counts[channel] = channel_counts.get(channel, 0) + 1
                
                for channel, count in channel_counts.items():
                    logger.debug(f"👥 {channel}: {count} watchers actifs")
    
    def get_active_watchers(self, channel: str) -> Set[str]:
        """Retourne les watchers actifs pour une chaîne"""
        with self._lock:
            active_set = self._active_segments.get(channel, set())
            if active_set:
                logger.debug(f"👥 TimeTracker: Chaîne {channel} a {len(active_set)} watchers actifs: {', '.join(active_set)}")
            return active_set
    
    def get_watcher_channel(self, ip: str) -> Optional[str]:
        """Retourne la chaîne actuelle d'un watcher"""
        with self._lock:
            watcher = self._watchers.get(ip)
            return watcher["channel"] if watcher else None
    
    def is_being_removed(self, ip: str) -> bool:
        """Vérifie si un watcher est en cours de suppression (dans le buffer)"""
        return ip in self._watcher_removal_buffer 
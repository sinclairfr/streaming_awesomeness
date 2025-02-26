import threading
import time
import uuid
from typing import Dict, List, Optional
from dataclasses import dataclass
from config import logger

@dataclass
class ViewerMetrics:
    """Stocke les métriques détaillées pour un canal"""
    total_watch_time: float = 0
    session_count: int = 0
    peak_concurrent_viewers: int = 0
    segments_delivered: int = 0
    
    def update(self, active_sessions_count: int):
        """Met à jour les métriques avec les sessions actives"""
        self.peak_concurrent_viewers = max(
            self.peak_concurrent_viewers, 
            active_sessions_count
        )

class ViewerSession:
    """Représente une session de visionnage unique"""
    def __init__(self, ip: str, channel: str):
        self.ip = ip
        self.channel = channel
        self.session_id = str(uuid.uuid4())
        self.start_time = time.time()
        self.last_activity = time.time()
        self.segment_count = 0
        self.last_segment = 0
        self.is_active = True

    def update_activity(self, segment_number: int):
        """Met à jour l'activité de la session"""
        self.last_activity = time.time()
        self.segment_count += 1
        self.last_segment = segment_number
        
    def get_duration(self) -> float:
        """Retourne la durée de la session en secondes"""
        return time.time() - self.start_time

    def is_session_stale(self, timeout: int = 30) -> bool:
        """Vérifie si la session est inactive"""
        return time.time() - self.last_activity > timeout

class SessionManager:
    """Gère toutes les sessions de visionnage"""
    def __init__(self):
        self.sessions: Dict[str, ViewerSession] = {}
        self.metrics: Dict[str, ViewerMetrics] = {}
        self.lock = threading.Lock()
        
        # Démarrage du thread de nettoyage
        self.cleanup_thread = threading.Thread(
            target=self._cleanup_loop,
            daemon=True
        )
        self.cleanup_thread.start()

    def register_activity(self, ip: str, channel: str, segment_number: int) -> ViewerSession:
        """Enregistre l'activité d'un viewer"""
        with self.lock:
            session_key = f"{ip}_{channel}"
            
            # Création ou récupération de la session
            if session_key not in self.sessions:
                self.sessions[session_key] = ViewerSession(ip, channel)
                if channel not in self.metrics:
                    self.metrics[channel] = ViewerMetrics()
                self.metrics[channel].session_count += 1
                logger.info(f"📺 Nouvelle session: {ip} -> {channel}")
            
            session = self.sessions[session_key]
            session.update_activity(segment_number)
            
            # Mise à jour des métriques
            self.metrics[channel].segments_delivered += 1
            active_count = len(self.get_channel_sessions(channel))
            self.metrics[channel].update(active_count)
            
            return session

    def get_channel_sessions(self, channel: str) -> List[ViewerSession]:
        """Retourne toutes les sessions actives pour un canal"""
        with self.lock:
            return [
                session for session in self.sessions.values()
                if session.channel == channel and session.is_active
            ]

    def get_channel_stats(self, channel: str) -> dict:
        """Retourne les statistiques détaillées pour un canal"""
        with self.lock:
            active_sessions = self.get_channel_sessions(channel)
            metrics = self.metrics.get(channel, ViewerMetrics())
            
            return {
                "active_viewers": len(active_sessions),
                "total_watch_time": metrics.total_watch_time,
                "peak_viewers": metrics.peak_concurrent_viewers,
                "segments_delivered": metrics.segments_delivered,
                "current_sessions": [
                    {
                        "ip": session.ip,
                        "duration": session.get_duration(),
                        "segments": session.segment_count
                    }
                    for session in active_sessions
                ]
            }

    def end_session(self, session_key: str):
        """Termine proprement une session"""
        with self.lock:
            if session_key in self.sessions:
                session = self.sessions[session_key]
                session.is_active = False
                duration = session.get_duration()
                
                # Mise à jour des métriques finales
                if session.channel in self.metrics:
                    self.metrics[session.channel].total_watch_time += duration
                
                logger.info(
                    f"👋 Fin de session: {session.ip} -> {session.channel} "
                    f"(durée: {duration:.1f}s, segments: {session.segment_count})"
                )
                
                del self.sessions[session_key]

    def _cleanup_loop(self):
        """Nettoie périodiquement les sessions inactives"""
        while True:
            try:
                with self.lock:
                    for session_key, session in list(self.sessions.items()):
                        if session.is_session_stale():
                            self.end_session(session_key)
                
                time.sleep(10)  # Vérification toutes les 10s
                
            except Exception as e:
                logger.error(f"Erreur nettoyage sessions: {e}")
                time.sleep(10)

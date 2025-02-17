# stream_error_handler.py

import time
from config import logger

class StreamErrorHandler:
    def __init__(self, channel_name: str):
        self.channel_name = channel_name
        self.error_count = 0
        self.restart_count = 0
        self.max_restarts = 3
        self.restart_cooldown = 60
        self.last_restart_time = 0
        self.error_types = set()  # Pour tracker les types d'erreurs

    def add_error(self, error_type: str) -> bool:
        """
        Ajoute une erreur et retourne True si un restart est nécessaire
        """
        self.error_count += 1
        self.error_types.add(error_type)
        
        # On regroupe les erreurs similaires
        if self.error_count >= 3 and len(self.error_types) >= 2:
            return self.should_restart()
        return False

    def should_restart(self) -> bool:
        """Vérifie si on peut/doit redémarrer"""
        if self.restart_count >= self.max_restarts:
            logger.error(f"[{self.channel_name}] Trop de redémarrages")
            return False
            
        current_time = time.time()
        if current_time - self.last_restart_time < self.restart_cooldown:
            return False
            
        self.restart_count += 1
        self.last_restart_time = current_time
        self.error_count = 0
        self.error_types.clear()
        return True

    def reset(self):
        """Reset après un stream stable"""
        self.error_count = 0
        self.restart_count = 0
        self.error_types.clear()
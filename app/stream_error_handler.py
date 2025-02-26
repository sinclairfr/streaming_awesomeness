# stream_error_handler.py

import time
from config import logger
import datetime

class StreamErrorHandler:
    def __init__(self, channel_name: str):
        self.channel_name = channel_name
        self.error_count = 0
        self.restart_count = 0
        self.max_restarts = 5
        self.restart_cooldown = 60
        self.last_restart_time = 0
        self.error_types = set()  # Pour tracker les types d'erreurs

    def add_error(self, error_type: str) -> bool:
        """
        Ajoute une erreur et retourne True si un restart est nécessaire
        """
        self.error_count += 1
        self.error_types.add(error_type)
        
        logger.warning(f"[{self.channel_name}] Erreur détectée: {error_type}, total: {self.error_count}")
        
        # Log le crash
        self._log_crash(error_type)

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
        
        # Log le redémarrage
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            with open(self.crash_log_path, "a") as f:
                f.write(f"{timestamp} - [{self.channel_name}] Redémarrage #{self.restart_count}\n")
                f.write("-" * 80 + "\n")
        except Exception as e:
            logger.error(f"Erreur lors de l'écriture du log de redémarrage: {e}")
        
        self.error_count = 0
        self.error_types.clear()
        return True

    def reset(self):
        """Reset après un stream stable"""
        if self.error_count > 0 or self.restart_count > 0:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            try:
                with open(self.crash_log_path, "a") as f:
                    f.write(f"{timestamp} - [{self.channel_name}] Reset des erreurs\n")
                    f.write("-" * 80 + "\n")
            except Exception as e:
                logger.error(f"Erreur lors de l'écriture du log de reset: {e}")
                
        self.error_count = 0
        self.restart_count = 0
        self.error_types.clear()
        return True

    def _log_crash(self, error_type: str):
        """Log des erreurs dans un fichier de crash dédié."""
        crash_log_path = Path(f"/app/logs/crashes_{self.channel_name}.log")
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        try:
            with open(crash_log_path, "a") as f:
                f.write(f"{timestamp} - Erreur détectée: {error_type}\n")
                f.write("-" * 80 + "\n")
        except Exception as e:
            logger.error(f"Erreur écriture log crash pour {self.channel_name}: {e}")

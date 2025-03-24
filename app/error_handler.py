import time
import threading
import datetime
from pathlib import Path
from config import logger
from typing import Set

class ErrorHandler:
    """Gère les erreurs et les redémarrages pour une chaîne IPTV"""
    
    def __init__(self, channel_name: str, max_restarts: int = 5, restart_cooldown: int = 60):
        self.channel_name = channel_name
        self.error_count = 0
        self.restart_count = 0
        self.max_restarts = max_restarts
        self.restart_cooldown = restart_cooldown
        self.last_restart_time = 0
        self.error_types: Set[str] = set()
        self.lock = threading.Lock()
        
        # Chemin du fichier de log unique pour ce canal
        self.crash_log_path = Path(f"/app/logs/crashes_{self.channel_name}.log")
        self.crash_log_path.parent.mkdir(exist_ok=True)

    def has_critical_errors(self) -> bool:
        """Vérifie si des erreurs critiques sont présentes"""
        with self.lock:
            # On considère qu'il y a une erreur critique si :
            # - On a eu trop de redémarrages
            if self.restart_count >= self.max_restarts:
                return True
                
            # - On a eu trop d'erreurs du même type
            if self.error_count >= 3 and len(self.error_types) == 1:
                return True
                
            # - On a eu plusieurs types d'erreurs différentes
            if len(self.error_types) >= 3:
                return True
                
            return False

    def add_error(self, error_type: str) -> bool:
        """Ajoute une erreur et retourne True si un restart est nécessaire"""
        with self.lock:
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
        with self.lock:
            if self.restart_count >= self.max_restarts:
                logger.error(f"[{self.channel_name}] Trop de redémarrages")
                return False
                
            current_time = time.time()
            if current_time - self.last_restart_time < self.restart_cooldown:
                return False
                
            self.restart_count += 1
            self.last_restart_time = current_time
            
            # Log le redémarrage
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
        with self.lock:
            if self.error_count > 0 or self.restart_count > 0:
                timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
        """Log des erreurs dans un fichier de crash dédié"""
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            with open(self.crash_log_path, "a") as f:
                f.write(f"{timestamp} - Erreur détectée: {error_type}\n")
                f.write(f"Compteur d'erreurs: {self.error_count}, Types d'erreurs: {', '.join(self.error_types)}\n")
                f.write("-" * 80 + "\n")
        except Exception as e:
            logger.error(f"Erreur écriture log crash pour {self.channel_name}: {e}")

    def get_errors(self) -> dict:
        """Retourne l'état actuel des erreurs"""
        with self.lock:
            return {
                "count": self.error_count,
                "types": list(self.error_types),
                "restarts": self.restart_count,
                "has_critical": self.has_critical_errors()
            } 
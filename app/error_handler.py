import time
import threading
import datetime
from pathlib import Path
from config import logger
from typing import Set

class ErrorHandler:
    """Gère les erreurs et les stratégies de redémarrage"""
    
    def __init__(self, channel_name: str, max_restarts: int = 5, restart_cooldown: int = 60):
        self.channel_name = channel_name
        self.max_restarts = max_restarts
        self.restart_cooldown = restart_cooldown
        self.errors = {}  # {error_type: {count: N, last_time: timestamp}}
        self.restart_count = 0
        self.last_restart_time = 0
        self.critical_threshold = 10  # Augmenter le seuil critique
        self.error_types: Set[str] = set()
        self.lock = threading.Lock()
        
        # Chemin du fichier de log unique pour ce canal
        self.crash_log_path = Path(f"/app/logs/crashes_{self.channel_name}.log")
        self.crash_log_path.parent.mkdir(exist_ok=True)

    def add_error(self, error_type: str) -> bool:
        """
        Ajoute une erreur et détermine si un redémarrage est nécessaire
        
        Returns:
            bool: True si un redémarrage est nécessaire
        """
        current_time = time.time()
        
        if error_type not in self.errors:
            self.errors[error_type] = {"count": 0, "last_time": 0}
        
        # Mettre à jour le compteur d'erreurs
        self.errors[error_type]["count"] += 1
        self.errors[error_type]["last_time"] = current_time
        
        total_errors = sum(e["count"] for e in self.errors.values())
        logger.warning(f"[{self.channel_name}] Erreur détectée: {error_type}, total: {total_errors}")
        
        # Vérifier si on devrait redémarrer
        if self._should_restart(error_type):
            return True
            
        return False
        
    def _should_restart(self, error_type: str) -> bool:
        """Détermine si un redémarrage est nécessaire en fonction du type d'erreur"""
        current_time = time.time()
        error_info = self.errors.get(error_type, {"count": 0, "last_time": 0})
        
        # Signal 2 (SIGINT) - s'il ne vient pas de quelque part d'autre, 
        # c'est probablement une erreur grave
        if error_type == "signal_2" and error_info["count"] >= 2:
            logger.warning(f"[{self.channel_name}] SIGINT détecté plusieurs fois, redémarrage requis")
            return True
        
        # Pour les signaux en général, ils sont plus graves
        if error_type.startswith("signal_") and error_info["count"] >= 3:
            logger.warning(f"[{self.channel_name}] Trop de signaux détectés, redémarrage requis")
            return True
            
        # Pour les erreurs génériques, être plus tolérant
        total_errors = sum(e["count"] for e in self.errors.values())
        if total_errors >= 5:
            logger.warning(f"[{self.channel_name}] Trop d'erreurs accumulées, redémarrage requis")
            return True
            
        return False
        
    def should_restart(self) -> bool:
        """Vérifie si le cooldown est passé avant d'autoriser un redémarrage"""
        current_time = time.time()
        
        # Si on a dépassé le nombre max de redémarrages
        if self.restart_count >= self.max_restarts:
            logger.warning(f"[{self.channel_name}] Nombre maximum de redémarrages atteint: {self.restart_count}")
            return False
            
        # Si le cooldown n'est pas encore passé
        if current_time - self.last_restart_time < self.restart_cooldown:
            logger.info(f"[{self.channel_name}] Attente du cooldown: {current_time - self.last_restart_time:.1f}s/{self.restart_cooldown}s")
            return False
            
        # OK pour redémarrer
        self.restart_count += 1
        self.last_restart_time = current_time
        logger.info(f"[{self.channel_name}] Redémarrage autorisé ({self.restart_count}/{self.max_restarts})")
        return True
        
    def reset(self):
        """Réinitialise les erreurs après un redémarrage réussi"""
        # On ne réinitialise pas restart_count pour limiter le nombre total de redémarrages
        self.errors = {}
        logger.info(f"[{self.channel_name}] Compteurs d'erreurs réinitialisés")
        
    def has_critical_errors(self) -> bool:
        """Vérifie si des erreurs critiques sont présentes"""
        total_errors = sum(e["count"] for e in self.errors.values())
        return total_errors >= self.critical_threshold  # Valeur augmentée

    def _log_crash(self, error_type: str):
        """Log des erreurs dans un fichier de crash dédié"""
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            with open(self.crash_log_path, "a") as f:
                f.write(f"{timestamp} - Erreur détectée: {error_type}\n")
                f.write(f"Compteur d'erreurs: {sum(e['count'] for e in self.errors.values())}, Types d'erreurs: {', '.join(self.errors.keys())}\n")
                f.write("-" * 80 + "\n")
        except Exception as e:
            logger.error(f"Erreur écriture log crash pour {self.channel_name}: {e}")

    def get_errors(self) -> dict:
        """Retourne l'état actuel des erreurs"""
        with self.lock:
            return {
                "count": sum(e["count"] for e in self.errors.values()),
                "types": list(self.errors.keys()),
                "restarts": self.restart_count,
                "has_critical": self.has_critical_errors()
            } 
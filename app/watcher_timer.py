import time
import threading
from typing import Dict, Optional
from config import logger

class WatcherTimer:
    """Classe pour gérer le minuteur de visionnage d'un watcher"""
    
    def __init__(self, channel_name: str, ip: str, stats_collector):
        self.channel_name = channel_name
        self.ip = ip
        self.stats_collector = stats_collector
        self.start_time = time.time()
        self.last_update = time.time()
        self._running = False  # Correctement initialisé à False
        self._thread = None  # Ne pas créer le thread immédiatement
        self._lock = threading.Lock()  # Ajout d'un lock pour la synchronisation
        
        logger.info(f"⏱️ Nouveau minuteur créé pour {ip} sur {channel_name}")
    
    def start(self):
        """Démarre le minuteur s'il n'est pas déjà en cours"""
        with self._lock:
            if self._running:
                return
                
            self._running = True
            
            # Créer et démarrer le thread seulement s'il n'existe pas ou s'il est terminé
            if self._thread is None or not self._thread.is_alive():
                self._thread = threading.Thread(target=self._timer_loop, daemon=True)
                self._thread.start()
                logger.debug(f"⏱️ Minuteur démarré pour {self.ip} sur {self.channel_name}")
    
    def stop(self):
        """Arrête le minuteur et enregistre le temps final"""
        with self._lock:
            if not self._running:
                return

            self._running = False
            end_time = time.time()
            watch_time = end_time - self.last_update

            # Enregistrer le temps final
            if self.stats_collector:
                self.stats_collector.add_watch_time(self.channel_name, self.ip, watch_time)
                logger.info(f"⏱️ Temps final pour {self.ip} sur {self.channel_name}: {watch_time:.1f}s")
    
    def _timer_loop(self):
        """Boucle principale du minuteur"""
        min_update_interval = 4.0  # Intervalle minimum entre mises à jour (était 4.0 secondes)
        
        while self._running:
            current_time = time.time()
            watch_time = current_time - self.last_update
            
            # Mise à jour uniquement après un intervalle minimum
            if watch_time >= min_update_interval:
                if self.stats_collector:
                    self.stats_collector.add_watch_time(self.channel_name, self.ip, watch_time)
                self.last_update = current_time
            
            time.sleep(1)  # Vérification toutes les secondes
    
    def is_running(self) -> bool:
        """Vérifie si le minuteur est actif"""
        with self._lock:
            return self._running
    
    def get_total_time(self) -> float:
        """Retourne le temps total de visionnage"""
        return time.time() - self.start_time 
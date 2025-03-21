import asyncio
import logging
import time
from typing import Dict, Set, Optional
from datetime import datetime
import threading
from dataclasses import dataclass
from contextlib import asynccontextmanager

# Configuration du logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class WatcherStats:
    start_time: float
    last_activity: float
    bytes_transferred: int
    playlist_requests: int
    current_channel: str
    is_active: bool = True

class StreamingService:
    def __init__(self):
        self._watchers: Dict[str, WatcherStats] = {}
        self._channel_stats: Dict[str, Dict] = {}
        self._lock = threading.Lock()
        self._cleanup_interval = 300  # 5 minutes pour le nettoyage des watchers inactifs
        self._inactivity_threshold = 180  # 3 minutes d'inactivité avant de considérer un watcher comme inactif
        self._cleanup_task: Optional[asyncio.Task] = None
        self._logger = logging.getLogger(__name__)

    async def start(self):
        """Démarre le service de streaming et la tâche de nettoyage."""
        self._cleanup_task = asyncio.create_task(self._cleanup_inactive_watchers())
        self._logger.info("StreamingService démarré")

    async def stop(self):
        """Arrête le service de streaming et nettoie les ressources."""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        self._logger.info("StreamingService arrêté")

    @asynccontextmanager
    async def _watcher_lock(self, watcher_id: str):
        """Gestionnaire de contexte pour verrouiller les opérations sur un watcher spécifique."""
        async with asyncio.Lock():
            try:
                yield
            finally:
                pass

    def register_watcher(self, watcher_id: str, channel: str) -> None:
        """Enregistre un nouveau watcher avec des statistiques initiales."""
        with self._lock:
            if watcher_id in self._watchers:
                self._logger.warning(f"Watcher {watcher_id} déjà enregistré")
                return

            current_time = time.time()
            self._watchers[watcher_id] = WatcherStats(
                start_time=current_time,
                last_activity=current_time,
                bytes_transferred=0,
                playlist_requests=0,
                current_channel=channel
            )
            
            if channel not in self._channel_stats:
                self._channel_stats[channel] = {
                    'total_watchers': 0,
                    'total_bytes': 0,
                    'peak_watchers': 0,
                    'last_update': current_time
                }
            
            self._channel_stats[channel]['total_watchers'] += 1
            self._channel_stats[channel]['peak_watchers'] = max(
                self._channel_stats[channel]['peak_watchers'],
                self._channel_stats[channel]['total_watchers']
            )
            
            self._logger.info(f"Nouveau watcher {watcher_id} enregistré pour la chaîne {channel}")

    async def update_watcher_activity(self, watcher_id: str, bytes_transferred: int = 0) -> None:
        """Met à jour l'activité d'un watcher et ses statistiques."""
        async with self._watcher_lock(watcher_id):
            if watcher_id not in self._watchers:
                self._logger.warning(f"Tentative de mise à jour d'un watcher inexistant: {watcher_id}")
                return

            watcher = self._watchers[watcher_id]
            current_time = time.time()
            
            watcher.last_activity = current_time
            watcher.bytes_transferred += bytes_transferred
            
            channel = watcher.current_channel
            if channel in self._channel_stats:
                self._channel_stats[channel]['total_bytes'] += bytes_transferred
                self._channel_stats[channel]['last_update'] = current_time

    async def change_channel(self, watcher_id: str, new_channel: str) -> None:
        """Gère le changement de chaîne d'un watcher."""
        async with self._watcher_lock(watcher_id):
            if watcher_id not in self._watchers:
                self._logger.warning(f"Tentative de changement de chaîne pour un watcher inexistant: {watcher_id}")
                return

            watcher = self._watchers[watcher_id]
            old_channel = watcher.current_channel

            # Mise à jour des statistiques de l'ancienne chaîne
            if old_channel in self._channel_stats:
                self._channel_stats[old_channel]['total_watchers'] -= 1

            # Mise à jour des statistiques de la nouvelle chaîne
            if new_channel not in self._channel_stats:
                self._channel_stats[new_channel] = {
                    'total_watchers': 0,
                    'total_bytes': 0,
                    'peak_watchers': 0,
                    'last_update': time.time()
                }

            self._channel_stats[new_channel]['total_watchers'] += 1
            self._channel_stats[new_channel]['peak_watchers'] = max(
                self._channel_stats[new_channel]['peak_watchers'],
                self._channel_stats[new_channel]['total_watchers']
            )

            watcher.current_channel = new_channel
            self._logger.info(f"Watcher {watcher_id} changé de {old_channel} vers {new_channel}")

    async def _cleanup_inactive_watchers(self) -> None:
        """Nettoie périodiquement les watchers inactifs."""
        while True:
            try:
                current_time = time.time()
                inactive_watchers = []

                with self._lock:
                    for watcher_id, watcher in self._watchers.items():
                        if current_time - watcher.last_activity > self._inactivity_threshold:
                            inactive_watchers.append(watcher_id)
                            self._logger.debug(f"Watcher {watcher_id} marqué comme inactif")

                for watcher_id in inactive_watchers:
                    await self.remove_watcher(watcher_id)

                await asyncio.sleep(self._cleanup_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._logger.error(f"Erreur lors du nettoyage des watchers: {e}")
                await asyncio.sleep(5)  # Attente avant de réessayer en cas d'erreur

    async def remove_watcher(self, watcher_id: str) -> None:
        """Supprime un watcher et met à jour les statistiques."""
        async with self._watcher_lock(watcher_id):
            if watcher_id not in self._watchers:
                return

            watcher = self._watchers[watcher_id]
            channel = watcher.current_channel

            with self._lock:
                if channel in self._channel_stats:
                    self._channel_stats[channel]['total_watchers'] -= 1

                del self._watchers[watcher_id]
                self._logger.info(f"Watcher {watcher_id} supprimé de la chaîne {channel}")

    def get_channel_stats(self, channel: str) -> Dict:
        """Récupère les statistiques d'une chaîne."""
        with self._lock:
            return self._channel_stats.get(channel, {
                'total_watchers': 0,
                'total_bytes': 0,
                'peak_watchers': 0,
                'last_update': time.time()
            })

    def get_watcher_stats(self, watcher_id: str) -> Optional[WatcherStats]:
        """Récupère les statistiques d'un watcher spécifique."""
        with self._lock:
            return self._watchers.get(watcher_id)

    def get_active_watchers_count(self, channel: str) -> int:
        """Retourne le nombre de watchers actifs pour une chaîne."""
        with self._lock:
            return self._channel_stats.get(channel, {}).get('total_watchers', 0) 
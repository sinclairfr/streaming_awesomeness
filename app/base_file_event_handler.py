from abc import ABC, abstractmethod
from watchdog.events import FileSystemEventHandler
import threading
import time
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class BaseFileEventHandler(FileSystemEventHandler, ABC):
    """Classe de base abstraite pour les gestionnaires d'événements de fichiers"""
    
    def __init__(self, manager):
        self.manager = manager
        self.lock = threading.Lock()
        self.last_update_time = {}  # Pour éviter les mises à jour trop fréquentes
        self.update_cooldown = 5  # 5 secondes entre mises à jour
        super().__init__()

    def _should_process_event(self, event) -> bool:
        """Vérifie si l'événement doit être traité"""
        if event.is_directory:
            return False
            
        path = Path(event.src_path)
        if path.suffix.lower() not in [".mp4", ".mkv", ".avi", ".mov", ".m4v"]:
            return False
            
        # Ignorer les fichiers temporaires
        if "tmp_" in path.name or path.name.startswith('.') or path.name.startswith('~'):
            return False
            
        return True

    def _get_channel_name(self, path: str) -> str:
        """Extrait le nom de la chaîne à partir du chemin"""
        try:
            path_parts = Path(path).parts
            content_index = -1

            # Cherche l'indice du dossier "content"
            for i, part in enumerate(path_parts):
                if part.lower() == "content":
                    content_index = i
                    break

            # Si content trouvé et il y a un élément après
            if content_index >= 0 and content_index + 1 < len(path_parts):
                return path_parts[content_index + 1]

            # Plan B: on prend juste le dossier parent
            return Path(path).parent.name

        except Exception as e:
            logger.error(f"❌ Erreur extraction nom de chaîne: {e}")
            return ""

    def _check_cooldown(self, channel_name: str) -> bool:
        """Vérifie si on peut traiter un événement pour une chaîne donnée"""
        current_time = time.time()
        with self.lock:
            last_time = self.last_update_time.get(channel_name, 0)
            if current_time - last_time < self.update_cooldown:
                return False
            self.last_update_time[channel_name] = current_time
            return True

    @abstractmethod
    def _handle_event(self, event):
        """Méthode abstraite pour gérer les événements"""
        pass

    def on_created(self, event):
        """Gère la création d'un fichier"""
        self._handle_event(event)

    def on_modified(self, event):
        """Gère la modification d'un fichier"""
        self._handle_event(event)

    def on_deleted(self, event):
        """Gère la suppression d'un fichier"""
        self._handle_event(event)

    def on_moved(self, event):
        """Gère le déplacement d'un fichier"""
        self._handle_event(event) 
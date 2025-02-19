# event_handler.py
import time
import threading
from watchdog.events import FileSystemEventHandler
from pathlib import Path
import os
from config import logger

class ChannelEventHandler(FileSystemEventHandler):
    def __init__(self, manager):
        self.manager = manager
        self.copying_files = {}  # Dict pour tracker les fichiers en cours de copie
        self.lock = threading.Lock()
        super().__init__()

    def is_file_ready(self, file_path: str, timeout: int = 300) -> bool:
        """
        Vérifie si un fichier a fini d'être copié en surveillant sa taille.
        
        Args:
            file_path: Chemin du fichier
            timeout: Timeout en secondes
            
        Returns:
            bool: True si le fichier est prêt, False sinon
        """
        path = Path(file_path)
        if not path.exists():
            return False
            
        start_time = time.time()
        last_size = -1
        stable_count = 0
        
        while time.time() - start_time < timeout:
            try:
                # On vérifie si le fichier est accessible
                if not os.access(file_path, os.R_OK):
                    time.sleep(1)
                    continue
                    
                current_size = path.stat().st_size
                
                # Si la taille n'a pas changé
                if current_size == last_size:
                    stable_count += 1
                    # Si stable pendant 3 secondes, on considère que c'est fini
                    if stable_count >= 3:
                        return True
                else:
                    stable_count = 0
                    
                last_size = current_size
                time.sleep(1)
                
            except (OSError, PermissionError) as e:
                # Le fichier est probablement encore verrouillé
                logger.debug(f"Fichier {file_path} pas encore accessible: {e}")
                time.sleep(1)
                continue
                
        logger.warning(f"⏰ Timeout en attendant {file_path}")
        return False

    def on_created(self, event):
        if event.is_directory:
            logger.info(f"📂 Nouveau dossier détecté: {event.src_path}")
            self.manager.scan_channels()
            return

        # Pour les fichiers, on vérifie s'ils sont en cours de copie
        with self.lock:
            if event.src_path in self.copying_files:
                return

            self.copying_files[event.src_path] = time.time()
            
        # On lance un thread dédié pour surveiller la copie
        threading.Thread(
            target=self._wait_for_copy_completion,
            args=(event.src_path,),
            daemon=True
        ).start()

    def _wait_for_copy_completion(self, file_path: str):
        """Attend la fin de la copie et déclenche le scan"""
        try:
            if self.is_file_ready(file_path):
                logger.info(f"✅ Copie terminée: {file_path}")
                self.manager.scan_channels()
            else:
                logger.warning(f"❌ Échec suivi copie: {file_path}")
        finally:
            with self.lock:
                self.copying_files.pop(file_path, None)

    def on_modified(self, event):
        if not event.is_directory:
            # On ignore les modifications si le fichier est en cours de copie
            with self.lock:
                if event.src_path in self.copying_files:
                    return
            self._handle_event(event)

    def _handle_event(self, event):
        logger.debug(f"🔄 Modification détectée: {event.src_path}")
        self.manager.scan_channels()
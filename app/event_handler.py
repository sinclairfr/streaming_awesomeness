# event_handler.py
import time
import threading
from watchdog.events import FileSystemEventHandler
from config import logger
from dotenv import load_dotenv
load_dotenv()

# Classe pour gérer les événements de changement de fichiers dans le répertoire des chaînes
class ChannelEventHandler(FileSystemEventHandler):
    # Constructeur
    def __init__(self, manager):
        # On stocke le gestionnaire IPTV pour pouvoir appeler ses méthodes
        self.manager = manager
        # On initialise les variables pour gérer le délai des événements
        self.last_event_time = 0
        self.event_delay = 10  # On attend 10s pour regrouper les événements
        self.pending_events = set()
        self.event_timer = None
        super().__init__()

    # Méthode pour gérer les événements, avec un délai pour regrouper les événements
    def _handle_event(self, event):
        current_time = time.time()
        if current_time - self.last_event_time >= self.event_delay:
            self.pending_events.add(event.src_path)
            if self.event_timer:
                self.event_timer.cancel()
            self.event_timer = threading.Timer(
                self.event_delay,
                self._process_pending_events
            )
            self.event_timer.start()
            
    # Méthode pour traiter les événements groupés, après le délai
    def _process_pending_events(self):
        if self.pending_events:
            logger.debug(f"On traite {len(self.pending_events)} événements groupés")
           # On appelle la méthode scan_channels du gestionnaire IPTV pour traiter les événements
            self.manager.scan_channels()
            # On réinitialise les variables pour le délai des événements
            self.pending_events.clear()
            self.last_event_time = time.time()
            
    # Méthode pour gérer les événements de modification de fichiers, en appelant _handle_event
    def on_modified(self, event):
        # On ignore les modifications de dossiers
        if not event.is_directory:
            self._handle_event(event)

    # Méthode pour gérer les événements de création de fichiers, en appelant _handle_event
    def on_created(self, event):
        if event.is_directory:
            logger.info(f"🔍 Nouveau dossier détecté: {event.src_path}")
            # On attend un peu que les fichiers soient copiés
            # TODO : gestion plus intelligentes pour la copie des dossiers (sans timer)
            time.sleep(60)
            self._handle_event(event)

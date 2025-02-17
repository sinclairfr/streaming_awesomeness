# event_handler.py

import time
import threading
from watchdog.events import FileSystemEventHandler
from config import logger

class ChannelEventHandler(FileSystemEventHandler):
    def __init__(self, manager):
        self.manager = manager
        self.last_event_time = 0
        self.event_delay = 10  # On attend 10s pour regrouper les événements
        self.pending_events = set()
        self.event_timer = None
        super().__init__()

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

    def _process_pending_events(self):
        if self.pending_events:
            logger.debug(f"On traite {len(self.pending_events)} événements groupés")
            self.manager.scan_channels()
            self.pending_events.clear()
            self.last_event_time = time.time()

    def on_modified(self, event):
        if not event.is_directory:
            self._handle_event(event)

    def on_created(self, event):
        if event.is_directory:
            logger.info(f"🔍 Nouveau dossier détecté: {event.src_path}")
            # On attend un peu que les fichiers soient copiés
            time.sleep(60)
            self._handle_event(event)

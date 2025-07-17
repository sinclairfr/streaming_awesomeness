# base_file_event_handler.py
from watchdog.events import FileSystemEventHandler
from config import logger

class BaseFileEventHandler(FileSystemEventHandler):
    def __init__(self, manager):
        self.manager = manager

    def on_created(self, event):
        logger.debug(f"Event type: {event.event_type} path : {event.src_path}")

    def on_deleted(self, event):
        logger.debug(f"Event type: {event.event_type} path : {event.src_path}")

    def on_modified(self, event):
        logger.debug(f"Event type: {event.event_type} path : {event.src_path}")

    def on_moved(self, event):
        logger.debug(f"Event type: {event.event_type} path : {event.src_path}")
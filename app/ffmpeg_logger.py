# ffmpeg_logger.py
import logging
from pathlib import Path
from config import logger
import datetime

class FFmpegLogger:
    """
    # On centralise la gestion des logs FFmpeg ici
    """
    def __init__(self, channel_name: str):
        self.channel_name = channel_name
        self.base_dir = Path("/app/logs/ffmpeg")
        self.base_dir.mkdir(parents=True, exist_ok=True)
        
        # Fichiers de logs
        self.main_log = self.base_dir / f"{channel_name}_ffmpeg.log"
        self.progress_log = self.base_dir / f"{channel_name}_progress.log"
        self.segments_log = self.base_dir / f"{channel_name}_segments.log"
        
        # On crée/initialise tous les fichiers
        for log_file in [self.main_log, self.progress_log, self.segments_log]:
            log_file.touch(exist_ok=True)
    
    def log_segment(self, segment_number: int, size: int):
        """Log des infos sur les segments générés"""
        with open(self.segments_log, "a") as f:
            f.write(f"{datetime.datetime.now()} - Segment {segment_number}: {size} bytes\n")
    
    def get_progress_file(self) -> Path:
        """Renvoie le chemin du fichier de progression"""
        return self.progress_log
    
    def get_main_log_file(self) -> Path:
        """Renvoie le chemin du log principal"""
        return self.main_log
import os
from pathlib import Path
from typing import List, Optional
from config import CONTENT_DIR, logger
from video_processor import get_accurate_duration

class PlaylistManager:
    def __init__(self, channel_name: str):
        self.channel_name = channel_name
        self.content_dir = Path(CONTENT_DIR) / channel_name
        self.processed_dir = self.content_dir / "processed"
        self.playlist_file = self.content_dir / "playlist.txt"

    def get_playlist_content(self) -> Optional[str]:
        """Récupère le contenu de la playlist"""
        try:
            if not self.playlist_file.exists():
                logger.error(f"[{self.channel_name}] ❌ Playlist non trouvée: {self.playlist_file}")
                return None

            with open(self.playlist_file, "r", encoding="utf-8") as f:
                return f.read().strip()
        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur lecture playlist: {e}")
            return None

    def get_playlist_files(self) -> List[Path]:
        """Récupère la liste des fichiers de la playlist"""
        try:
            content = self.get_playlist_content()
            if not content:
                return []

            files = []
            for line in content.split("\n"):
                if line.startswith("file "):
                    # Extraction du chemin entre guillemets
                    file_path = line.split("'")[1]
                    full_path = self.content_dir / file_path
                    if full_path.exists():
                        files.append(full_path)
                    else:
                        logger.warning(f"[{self.channel_name}] ⚠️ Fichier non trouvé: {full_path}")

            return files
        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur liste fichiers: {e}")
            return []

    def get_playlist_duration(self) -> float:
        """Calcule la durée totale de la playlist"""
        try:
            total_duration = 0.0
            files = self.get_playlist_files()
            
            for file_path in files:
                duration = get_accurate_duration(file_path)
                if duration and duration > 0:
                    total_duration += duration
                else:
                    logger.warning(f"[{self.channel_name}] ⚠️ Durée invalide pour {file_path}")

            return total_duration
        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur calcul durée: {e}")
            return 0.0

    def get_next_file(self, current_file: Path) -> Optional[Path]:
        """Trouve le prochain fichier dans la playlist"""
        try:
            files = self.get_playlist_files()
            if not files:
                return None

            current_index = -1
            for i, file in enumerate(files):
                if file == current_file:
                    current_index = i
                    break

            if current_index == -1:
                return files[0]  # Si le fichier actuel n'est pas dans la liste, on commence par le premier

            next_index = (current_index + 1) % len(files)
            return files[next_index]
        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur recherche prochain fichier: {e}")
            return None 
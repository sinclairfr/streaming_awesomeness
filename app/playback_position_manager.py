# playback_position_manager.py
import os
import time
import json
import threading
import subprocess
from pathlib import Path
from config import logger


class PlaybackPositionManager:
    """
    Gestionnaire de durée pour les streams.
    Permet de calculer et sauvegarder la durée totale des vidéos.
    """
    def __init__(self, channel_name):
        self.channel_name = channel_name
        self.lock = threading.Lock()
        
        # Gestion de la durée
        self.total_duration = 0.0         # Durée totale de la playlist
        self.file_durations = {}          # Durées individuelles des fichiers
        
        # Création du dossier de sauvegarde si nécessaire
        self.state_dir = Path("/app/state")
        self.state_dir.mkdir(exist_ok=True, parents=True)
        
        # Chargement de l'état précédent
        self._load_state()
    
    def set_total_duration(self, duration):
        """
        Définit la durée totale de la playlist
        """
        with self.lock:
            self.total_duration = duration
            self._save_state()
    
    def calculate_durations(self, video_files):
        """
        Calcule la durée totale à partir d'une liste de fichiers vidéo
        """
        try:
            logger.info(f"[{self.channel_name}] 🕒 Calcul des durées pour {len(video_files)} fichiers...")
            
            # On réinitialise les durées
            self.file_durations = {}
            self.total_duration = 0
            
            for video in video_files:
                duration = self._get_file_duration(video)
                if duration > 0:
                    self.file_durations[str(video)] = duration
                    self.total_duration += duration
            
            logger.info(f"[{self.channel_name}] ✅ Durée totale: {self.total_duration:.2f}s")
            
            # On sauvegarde l'état
            self._save_state()
            
            return self.total_duration
            
        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur calcul durées: {e}")
            return 0
    
    def _get_file_duration(self, video_path, max_retries=2):
        """
        Obtient la durée d'un fichier vidéo avec retries
        """
        for i in range(max_retries + 1):
            try:
                cmd = [
                    "ffprobe",
                    "-v", "error",
                    "-show_entries", "format=duration",
                    "-of", "default=noprint_wrappers=1:nokey=1",
                    str(video_path)
                ]

                result = subprocess.run(cmd, capture_output=True, text=True)
                
                if result.returncode == 0:
                    try:
                        duration = float(result.stdout.strip())
                        if duration > 0:
                            return duration
                    except ValueError:
                        pass
                
                # Si on arrive ici, c'est que ça a échoué
                logger.warning(f"[{self.channel_name}] ⚠️ Tentative {i+1}/{max_retries+1} échouée pour {video_path}")
                time.sleep(0.5)  # Petite pause avant la prochaine tentative
                
            except Exception as e:
                logger.error(f"[{self.channel_name}] ❌ Erreur ffprobe: {e}")
        
        # Si on a échoué après toutes les tentatives, on renvoie une durée par défaut
        logger.error(f"[{self.channel_name}] ❌ Impossible d'obtenir la durée pour {video_path}")
        return 0
    
    def _save_state(self):
        """Sauvegarde uniquement la durée totale"""
        try:
            state_file = self.state_dir / f"{self.channel_name}_position.json"
            
            state = {
                "channel_name": self.channel_name,
                "total_duration": self.total_duration,
                "last_update": time.time()
            }
            
            with open(state_file, 'w') as f:
                json.dump(state, f)
                
        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur sauvegarde état: {e}")
    
    def _load_state(self):
        """Charge la durée totale de la playlist"""
        try:
            state_file = self.state_dir / f"{self.channel_name}_position.json"
            
            if not state_file.exists():
                return
                
            with open(state_file, 'r') as f:
                state = json.load(f)
                
            # On ne charge que si c'est le même channel
            if state.get("channel_name") != self.channel_name:
                return
                
            # On récupère uniquement la durée totale
            self.total_duration = state.get("total_duration", 0)
            
        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur lors du chargement: {e}")
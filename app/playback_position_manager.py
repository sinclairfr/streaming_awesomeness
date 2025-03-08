# playback_position_manager.py
import os
import time
import random
import json
import threading
import subprocess
from pathlib import Path
from config import logger


class PlaybackPositionManager:
    """
    # Gestionnaire de position de lecture pour les streams
    # Permet de suivre, sauvegarder et restaurer la position de lecture
    """
    def __init__(self, channel_name):
        self.channel_name = channel_name
        self.lock = threading.Lock()
        
        # Valeurs de position
        self.current_position = 0.0       # Position actuelle en secondes
        self.last_known_position = 0.0    # Dernière position connue avant arrêt
        self.start_offset = 0.0           # Offset de démarrage
        self.last_update_time = time.time()  # Dernier moment où on a mis à jour la position
        
        # Gestion de la durée
        self.total_duration = 0.0         # Durée totale de la playlist
        self.file_durations = {}          # Durées individuelles des fichiers
        
        # État
        self.is_playing = False           # Si la lecture est en cours
        self.last_progress_file = None    # Dernier fichier de progression utilisé
        
        # Création du dossier de sauvegarde si nécessaire
        self.state_dir = Path("/app/state")
        self.state_dir.mkdir(exist_ok=True, parents=True)
        
        # Chargement de l'état précédent
        self._load_state()
  
    def set_playback_offset(self, offset):
        """
        # Définit l'offset de lecture avec validation
        """
        with self.lock:
            # Vérifier que les attributs existent, sinon les initialiser
            if not hasattr(self, 'playback_offset'):
                self.playback_offset = 0.0
            if not hasattr(self, 'last_playback_time'):
                self.last_playback_time = time.time()
            
            # S'assurer que l'offset est valide et dans les limites
            if self.total_duration > 0:
                # Appliquer le modulo pour rester dans les limites
                self.playback_offset = offset % self.total_duration
                logger.debug(f"[{self.channel_name}] 🔄 Offset ajusté à {self.playback_offset:.2f}s (modulo {self.total_duration:.2f}s)")
            else:
                self.playback_offset = offset
                
            self.last_playback_time = time.time()
            self._save_state()         
 
    def set_total_duration(self, duration):
        """
        # Définit la durée totale de la playlist
        """
        with self.lock:
            self.total_duration = duration
            self._save_state()
    
    def get_start_offset(self):
        # Utiliser l'horodatage actuel pour calculer un offset qui évolue naturellement
        reference_date = time.mktime((2025, 1, 1, 0, 0, 0, 0, 0, 0))
        current_time = time.time()
        elapsed = current_time - reference_date
        if self.total_duration > 0:
            offset = elapsed % self.total_duration
            logger.info(f"[{self.channel_name}] Offset calculé: {offset:.2f}s (modulo {self.total_duration:.2f}s)")
            return offset
        else:
            # Fallback sécurisé
            logger.warning(f"[{self.channel_name}] ⚠️ Durée totale invalide, offset à 0")
            return 0.0
     
    def calculate_durations(self, video_files):
        """
        # Calcule la durée totale à partir d'une liste de fichiers vidéo
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
        # Obtient la durée d'un fichier vidéo avec retries
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
        """Sauvegarde uniquement les informations essentielles"""
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
        """Charge seulement la durée totale de la playlist"""
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
            
    def update_from_progress(self, progress_file):
        """
        # Met à jour la position à partir du fichier de progression FFmpeg
        # Avec détection des anomalies de saut au démarrage
        """
        if not progress_file or not Path(progress_file).exists():
            return False
            
        try:
            self.last_progress_file = progress_file
            
            with open(progress_file, 'r') as f:
                content = f.read()
                
                # On cherche la position dans le fichier de progression
                if 'out_time_ms=' in content:
                    position_lines = [l for l in content.split('\n') if 'out_time_ms=' in l]
                    if position_lines:
                        # On prend la dernière ligne
                        time_part = position_lines[-1].split('=')[1]
                        
                        if time_part.isdigit():
                            # Conversion en secondes
                            ms_value = int(time_part)
                            new_position = ms_value / 1_000_000
                            
                            # AJOUT: Détection des sauts anormaux (régression d'offset importante)
                            if hasattr(self, 'start_offset') and self.start_offset > 10:
                                # Si on est dans les premières secondes après le démarrage
                                now = time.time()
                                if hasattr(self, 'stream_start_time') and now - self.stream_start_time < 30:
                                    # Si le nouveau temps est proche de zéro alors qu'on avait demandé un offset important
                                    if new_position < 10 and abs(new_position - self.start_offset) > 30:
                                        logger.warning(f"[{self.channel_name}] ⚠️ Détection d'une régression anormale d'offset: {new_position}s vs {self.start_offset}s demandé, on ignore cette mise à jour")
                                        return False
                            
                            # Correction pour valeurs négatives 
                            if ms_value < 0 and self.total_duration > 0:
                                ms_value = (self.total_duration * 1_000_000) + ms_value
                                new_position = ms_value / 1_000_000
                            
                            # Mise à jour de la position
                            with self.lock:
                                # Log des sauts importants
                                if abs(self.current_position - new_position) > 30:
                                    logger.info(f"[{self.channel_name}] 📊 Saut détecté: {self.current_position:.2f}s → {new_position:.2f}s")
                                    
                                self.current_position = new_position
                                self.last_update_time = time.time()
                                
                            return True
            
            return False
                                
        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur lecture position: {e}")
            return False
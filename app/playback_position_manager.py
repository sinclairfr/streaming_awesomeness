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
    # Ajouter cette méthode à PlaybackPositionManager (playback_position_manager.py)
    def set_playback_offset(self, offset):
        """
        # Définit l'offset de lecture
        """
        with self.lock:
            self.playback_offset = offset
            self.last_playback_time = time.time()
            self._save_state()    
            
    def update_from_progress(self, progress_file):
        """
        # Met à jour la position à partir du fichier de progression FFmpeg
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
                            
                            # Correction pour valeurs négatives (parfois FFmpeg donne des valeurs négatives)
                            if ms_value < 0 and self.total_duration > 0:
                                ms_value = (self.total_duration * 1_000_000) + ms_value
                            
                            # Mise à jour de la position
                            with self.lock:
                                self.current_position = ms_value / 1_000_000
                                self.last_update_time = time.time()
                                
                            return True
            
            return False
                                
        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur lecture position: {e}")
            return False
    
    def save_position(self):
        """
        # Sauvegarde la position actuelle
        """
        with self.lock:
            # Si la lecture est en cours, on calcule la position actuelle
            if self.is_playing:
                elapsed = time.time() - self.last_update_time
                self.current_position += elapsed
                
                # On gère le bouclage automatique
                if self.total_duration > 0:
                    self.current_position %= self.total_duration
            
            # On sauvegarde la position
            self.last_known_position = self.current_position
            self.last_update_time = time.time()
            
            # On écrit l'état dans un fichier
            self._save_state()
            
            logger.info(f"[{self.channel_name}] 💾 Position sauvegardée: {self.current_position:.2f}s")
            return True
    
    def get_position(self):
        """
        # Renvoie la position actuelle estimée
        """
        with self.lock:
            # Si la lecture est en cours, on estime la position actuelle
            if self.is_playing:
                elapsed = time.time() - self.last_update_time
                position = self.current_position + elapsed
                
                # On gère le bouclage automatique
                if self.total_duration > 0:
                    position %= self.total_duration
                
                return position
            
            # Sinon, on renvoie la dernière position connue
            return self.last_known_position
    
    def set_position(self, position):
        """
        # Définit manuellement la position
        """
        with self.lock:
            self.current_position = position
            self.last_known_position = position
            self.last_update_time = time.time()
            self._save_state()
    
    def set_playing(self, is_playing):
        """
        # Met à jour l'état de lecture
        """
        with self.lock:
            # Si on passe de playing à stopped, on sauvegarde la position
            if self.is_playing and not is_playing:
                self.save_position()
            
            # Si on passe de stopped à playing, on met à jour le timestamp
            if not self.is_playing and is_playing:
                self.last_update_time = time.time()
            
            self.is_playing = is_playing
    
    def set_total_duration(self, duration):
        """
        # Définit la durée totale de la playlist
        """
        with self.lock:
            self.total_duration = duration
            self._save_state()
    
    def get_random_offset(self):
        """
        # Génère un offset de démarrage aléatoire
        """
        with self.lock:
            if self.total_duration <= 0:
                return 0
            
            # On prend un offset aléatoire entre 0 et 80% de la durée totale
            max_offset = self.total_duration * 0.8
            self.start_offset = random.uniform(0, max_offset)
            
            logger.info(f"[{self.channel_name}] 🎲 Offset aléatoire généré: {self.start_offset:.2f}s")
            
            return self.start_offset
    
    def get_start_offset(self):
        """
        # Renvoie l'offset de démarrage sécurisé
        """
        try:
            # Vérifier si on a une position connue valide
            if hasattr(self, 'last_known_position') and self.last_known_position > 0:
                logger.info(f"[{self.channel_name}] ⏱️ Reprise à la dernière position: {self.last_known_position:.2f}s")
                return self.last_known_position
                
            # Sinon on génère un offset aléatoire si possible
            if hasattr(self, 'total_duration') and self.total_duration > 0:
                max_offset = self.total_duration * 0.8
                random_offset = random.uniform(0, max_offset)
                logger.info(f"[{self.channel_name}] 🎲 Offset aléatoire généré: {random_offset:.2f}s")
                return random_offset
                
            # Fallback sécurisé
            logger.warning(f"[{self.channel_name}] ⚠️ Utilisation d'un offset de démarrage de secours (0)")
            return 0.0
            
        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur calcul offset: {e}")
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
        """
        # Sauvegarde l'état dans un fichier JSON
        """
        try:
            state_file = self.state_dir / f"{self.channel_name}_position.json"
            
            state = {
                "channel_name": self.channel_name,
                "current_position": self.current_position,
                "last_known_position": self.last_known_position,
                "start_offset": self.start_offset,
                "total_duration": self.total_duration,
                "last_update": time.time()
            }
            
            with open(state_file, 'w') as f:
                json.dump(state, f)
                
        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur sauvegarde état: {e}")
    
    def _load_state(self):
        """
        # Charge l'état depuis un fichier JSON
        """
        try:
            state_file = self.state_dir / f"{self.channel_name}_position.json"
            
            if not state_file.exists():
                return
                
            with open(state_file, 'r') as f:
                state = json.load(f)
                
            # On ne charge que si c'est le même channel
            if state.get("channel_name") != self.channel_name:
                return
                
            # On restaure l'état
            self.current_position = state.get("current_position", 0)
            self.last_known_position = state.get("last_known_position", 0)
            self.start_offset = state.get("start_offset", 0)
            self.total_duration = state.get("total_duration", 0)
            
            # On vérifie si l'état n'est pas trop vieux (max 24h)
            last_update = state.get("last_update", 0)
            if time.time() - last_update > 86400:  # 24h en secondes
                logger.warning(f"[{self.channel_name}] ⚠️ État trop ancien, on réinitialise")
                self.current_position = 0
                self.last_known_position = 0
                self.start_offset = 0
                return
                
            logger.info(f"[{self.channel_name}] ✅ État chargé, dernière position: {self.last_known_position:.2f}s")
                
        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur chargement état: {e}")
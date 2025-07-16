import time
import threading
import datetime
import os
import shutil
import subprocess
from pathlib import Path
from config import logger
from typing import Set, Dict, Any, Optional

class ErrorHandler:
    """Gère les erreurs et les stratégies de redémarrage avec une approche plus tolérante"""
    
    def __init__(self, channel_name: str, max_restarts: int = 5, restart_cooldown: int = 60):
        self.channel_name = channel_name
        self.max_restarts = max_restarts
        self.restart_cooldown = restart_cooldown
        self.errors: Dict[str, Dict[str, Any]] = {}  # {error_type: {count: N, last_time: timestamp}}
        self.restart_count = 0
        self.last_restart_time = 0
        self.critical_threshold = 15  # Augmenté pour être plus tolérant (était 10)
        self.error_types: Set[str] = set()
        self.lock = threading.Lock()
        
        # Période de réinitialisation des erreurs
        self.error_reset_period = 3600  # 1 heure
        self.last_error_reset = time.time()
        
        # Chemin du fichier de log unique pour ce canal
        self.crash_log_path = Path(f"/app/logs/crashes_{self.channel_name}.log")
        self.crash_log_path.parent.mkdir(exist_ok=True)

    def add_error(self, error_type: str) -> bool:
        """
        Ajoute une erreur et détermine si un redémarrage est nécessaire
        avec une approche plus tolérante
        
        Returns:
            bool: True si un redémarrage est nécessaire
        """
        current_time = time.time()
        
        # Réinitialiser périodiquement le compteur d'erreurs pour éviter l'accumulation
        if current_time - self.last_error_reset > self.error_reset_period:
            self._reset_error_counts()
            self.last_error_reset = current_time
            logger.info(f"[{self.channel_name}] 🔄 Réinitialisation périodique des compteurs d'erreurs")
        
        with self.lock:
            if error_type not in self.errors:
                self.errors[error_type] = {"count": 0, "last_time": 0}
            
            # Mettre à jour le compteur d'erreurs
            self.errors[error_type]["count"] += 1
            self.errors[error_type]["last_time"] = current_time
            
            # Log de l'erreur
            self._log_crash(error_type)
            
            total_errors = sum(e["count"] for e in self.errors.values())
            logger.warning(f"[{self.channel_name}] Erreur détectée: {error_type}, total: {total_errors}/{self.critical_threshold}")
            
            # Vérifier si on devrait redémarrer
            return self._should_restart(error_type)
        
    def _should_restart(self, error_type: str) -> bool:
        """
        Détermine si un redémarrage est nécessaire avec une approche plus tolérante
        """
        error_info = self.errors.get(error_type, {"count": 0, "last_time": 0})
        
        # 1. SIGINT (signal 2) - cas spécial, plus grave
        if error_type == "signal_2" and error_info["count"] >= 3:  # Augmenté de 2 à 3
            logger.warning(f"[{self.channel_name}] SIGINT détecté {error_info['count']} fois, redémarrage requis")
            return True
        
        # 2. Pour les signaux, plus tolérant
        if error_type.startswith("signal_") and error_info["count"] >= 4:  # Augmenté de 3 à 4
            logger.warning(f"[{self.channel_name}] {error_info['count']} signaux {error_type} détectés, redémarrage requis")
            return True
            
        # 3. Pour les erreurs de données, plus tolérant
        if error_type in ["invalid_data", "dts_error"] and error_info["count"] >= 5:  # Augmenté pour ces erreurs spécifiques
            logger.warning(f"[{self.channel_name}] Trop d'erreurs {error_type}, redémarrage requis")
            return True
            
        # 4. Pour les erreurs génériques, encore plus tolérant
        total_errors = sum(e["count"] for e in self.errors.values())
        if total_errors >= 8:  # Augmenté de 5 à 8
            logger.warning(f"[{self.channel_name}] {total_errors} erreurs accumulées, redémarrage requis")
            return True
            
        # Pas assez d'erreurs pour justifier un redémarrage
        return False
        
    def should_restart(self) -> bool:
        """
        Vérifie si le cooldown est passé avant d'autoriser un redémarrage
        """
        current_time = time.time()
        
        with self.lock:
            # Si on a dépassé le nombre max de redémarrages, on limite
            if self.restart_count >= self.max_restarts:
                # On calcule le temps écoulé depuis le dernier redémarrage
                elapsed = current_time - self.last_restart_time
                
                # Si ça fait plus d'une heure, on réinitialise le compteur
                if elapsed > 3600:  # 1 heure
                    logger.info(f"[{self.channel_name}] Réinitialisation du compteur de redémarrages après 1h")
                    self.restart_count = 0
                else:
                    logger.warning(f"[{self.channel_name}] Limite de {self.max_restarts} redémarrages atteinte")
                    return False
                
            # Vérifier le cooldown
            if current_time - self.last_restart_time < self.restart_cooldown:
                remaining = self.restart_cooldown - (current_time - self.last_restart_time)
                logger.info(f"[{self.channel_name}] Attente du cooldown: {remaining:.1f}s restantes")
                return False
                
            # OK pour redémarrer
            self.restart_count += 1
            self.last_restart_time = current_time
            logger.info(f"[{self.channel_name}] Redémarrage autorisé ({self.restart_count}/{self.max_restarts})")
            return True
        
    def reset(self):
        """Réinitialise les erreurs après un redémarrage réussi"""
        with self.lock:
            self._reset_error_counts()
            logger.info(f"[{self.channel_name}] Compteurs d'erreurs réinitialisés")
    
    def _reset_error_counts(self):
        """Réinitialise les compteurs d'erreurs mais conserve les types"""
        self.errors = {error_type: {"count": 0, "last_time": 0} for error_type in self.errors}
        
    def has_critical_errors(self) -> bool:
        """Vérifie si des erreurs critiques sont présentes"""
        with self.lock:
            total_errors = sum(e["count"] for e in self.errors.values())
            return total_errors >= self.critical_threshold
    
    def get_errors_summary(self) -> str:
        """Génère un résumé des erreurs pour le log"""
        with self.lock:
            if not self.errors:
                return "Aucune erreur"
                
            error_counts = [f"{err_type}: {data['count']}" for err_type, data in self.errors.items() if data['count'] > 0]
            if not error_counts:
                return "Aucune erreur active"
                
            return f"Erreurs: {', '.join(error_counts)}"

    def _log_crash(self, error_type: str):
        """Log des erreurs dans un fichier de crash dédié"""
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            with open(self.crash_log_path, "a") as f:
                f.write(f"{timestamp} - Erreur détectée: {error_type}\n")
                f.write(f"Compteur: {sum(e['count'] for e in self.errors.values())}, Types: {', '.join(self.errors.keys())}\n")
                f.write("-" * 80 + "\n")
        except Exception as e:
            logger.error(f"Erreur écriture log crash pour {self.channel_name}: {e}")

    def get_errors(self) -> dict:
        """Retourne l'état actuel des erreurs"""
        with self.lock:
            return {
                "count": sum(e["count"] for e in self.errors.values()),
                "types": list(self.errors.keys()),
                "restarts": self.restart_count,
                "has_critical": self.has_critical_errors(),
                "summary": self.get_errors_summary()
            }

    def handle_ffmpeg_file_error(self, channel_name: str, file_path: str) -> bool:
        """
        Gère l'erreur de fichier FFmpeg en:
        1) Déplaçant le fichier dans le dossier ignored
        2) Régénérant playlist.txt
        3) Relançant la chaîne
        4) Mettant à jour la playlist.m3u globale

        Args:
            channel_name: Nom de la chaîne
            file_path: Chemin du fichier problématique

        Returns:
            bool: True si la récupération a réussi, False sinon
        """
        logger.warning(f"[{channel_name}] 🛠️ Tentative de récupération après erreur de fichier FFmpeg")
        
        try:
            # 1. Vérifier et déplacer le fichier dans le dossier ignored
            file_path_obj = Path(file_path)
            if file_path_obj.exists():
                # Créer le dossier ignored s'il n'existe pas
                ignored_dir = Path(f"/mnt/docker_data/streaming_awesomeness/content/{channel_name}/ignored")
                ignored_dir.mkdir(exist_ok=True)
                
                # Déplacer le fichier
                target_file = ignored_dir / file_path_obj.name
                shutil.move(str(file_path_obj), str(target_file))
                logger.info(f"[{channel_name}] ✅ Fichier déplacé vers ignored: {file_path_obj.name}")
            else:
                logger.warning(f"[{channel_name}] ⚠️ Fichier introuvable pour déplacement: {file_path}")
            
            # 2. Régénérer la playlist.txt
            success = self._regenerate_playlist(channel_name)
            if not success:
                logger.error(f"[{channel_name}] ❌ Échec de régénération de playlist.txt")
                return False
                
            # 3. Relancer la chaîne
            success = self._restart_channel(channel_name)
            if not success:
                logger.error(f"[{channel_name}] ❌ Échec de redémarrage de la chaîne")
                return False
                
            # 4. Mettre à jour la playlist.m3u globale
            success = self._update_global_playlist()
            if not success:
                logger.warning(f"[{channel_name}] ⚠️ Échec de mise à jour de la playlist globale")
                # On continue malgré l'échec car la chaîne fonctionne
            
            logger.info(f"[{channel_name}] ✅ Récupération après erreur FFmpeg réussie")
            return True
            
        except Exception as e:
            logger.error(f"[{channel_name}] ❌ Erreur récupération après erreur FFmpeg: {e}")
            return False
    
    def _regenerate_playlist(self, channel_name: str) -> bool:
        """Régénère la playlist.txt à partir des fichiers dans ready_to_stream"""
        try:
            # Chemin du dossier ready_to_stream
            ready_dir = Path(f"/mnt/docker_data/streaming_awesomeness/content/{channel_name}/ready_to_stream")
            playlist_path = Path(f"/mnt/docker_data/streaming_awesomeness/content/{channel_name}/playlist.txt")
            
            if not ready_dir.exists():
                logger.error(f"[{channel_name}] ❌ Dossier ready_to_stream introuvable")
                return False
                
            # Filtrer uniquement les fichiers vidéo
            video_extensions = ['.mp4', '.avi', '.mkv', '.mov', '.m4v']
            videos = []
            
            for ext in video_extensions:
                videos.extend(list(ready_dir.glob(f"*{ext}")))
            
            if not videos:
                logger.warning(f"[{channel_name}] ⚠️ Aucun fichier vidéo trouvé dans ready_to_stream")
                # Créer une playlist vide quand même
                with open(playlist_path, "w", encoding="utf-8") as f:
                    f.write("# Playlist vide - aucun fichier disponible\n")
                return True
                
            # Trier les vidéos par nom
            videos.sort(key=lambda x: x.name.lower())
            
            # Écrire la playlist
            with open(playlist_path, "w", encoding="utf-8") as f:
                for video in videos:
                    # Chemin relatif par rapport au dossier de la chaîne
                    rel_path = f"ready_to_stream/{video.name}"
                    f.write(f"file '{rel_path}'\n")
            
            logger.info(f"[{channel_name}] ✅ Playlist régénérée avec {len(videos)} fichiers")
            return True
            
        except Exception as e:
            logger.error(f"[{channel_name}] ❌ Erreur régénération playlist: {e}")
            return False
    
    def _restart_channel(self, channel_name: str) -> bool:
        """Redémarre la chaîne en appelant le gestionnaire approprié"""
        try:
            # On envoie un signal via le dossier state pour forcer un redémarrage
            trigger_file = Path(f"/app/state/{channel_name}_restart")
            trigger_file.touch(exist_ok=True)
            
            logger.info(f"[{channel_name}] ✅ Signal de redémarrage envoyé")
            return True
            
        except Exception as e:
            logger.error(f"[{channel_name}] ❌ Erreur envoi signal redémarrage: {e}")
            return False
    
    def _update_global_playlist(self) -> bool:
        """Met à jour la playlist.m3u globale"""
        try:
            # Exécute le script de création de la playlist maître
            result = subprocess.run(
                ["python3", "/app/create_master_playlist.py"], 
                capture_output=True, 
                text=True, 
                timeout=30
            )
            
            if result.returncode == 0:
                logger.info("✅ Playlist.m3u globale mise à jour")
                return True
            else:
                logger.error(f"❌ Échec mise à jour playlist globale: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Erreur mise à jour playlist globale: {e}")
            return False 
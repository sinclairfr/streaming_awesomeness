#!/usr/bin/env python3
"""
Playlist Watchdog - Surveille et maintient la playlist principale
"""
import os
import sys
import time
import traceback
import logging
import signal
from pathlib import Path
import subprocess

# Configuration
PLAYLIST_PATH = "/app/hls/playlist.m3u"
CHECK_INTERVAL = 60  # Vérification toutes les 60 secondes
try:
    from config import SERVER_URL
except ImportError:
    # Fallback if config.py is not available
    SERVER_URL = os.getenv("SERVER_URL", "192.168.10.183")
MASTER_PLAYLIST_CREATOR = "/app/create_master_playlist.py"

# Configuration du logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('/app/logs/playlist_watchdog.log')
    ]
)
logger = logging.getLogger('PlaylistWatchdog')

class PlaylistWatchdog:
    """Service de surveillance et maintenance de la playlist principale"""
    
    def __init__(self):
        self.running = True
        self.last_check = 0
        self.last_repair = 0
        
        # Mettre en place les gestionnaires de signaux
        signal.signal(signal.SIGTERM, self.handle_signal)
        signal.signal(signal.SIGINT, self.handle_signal)
        
        logger.info("Service de surveillance de playlist démarré")
    
    def handle_signal(self, signum, frame):
        """Gère les signaux d'arrêt"""
        logger.info(f"Signal {signum} reçu, arrêt en cours...")
        self.running = False
    
    def check_playlist(self):
        """Vérifie que la playlist principale existe et est valide"""
        try:
            # Vérifier si le fichier existe
            playlist_exists = os.path.exists(PLAYLIST_PATH)
            
            if not playlist_exists:
                logger.warning(f"Playlist absente: {PLAYLIST_PATH}")
                return False
            
            # Vérifier la taille
            file_size = os.path.getsize(PLAYLIST_PATH)
            if file_size == 0:
                logger.warning(f"Playlist vide: {PLAYLIST_PATH}")
                return False
            
            # Vérifier le contenu minimum
            with open(PLAYLIST_PATH, "r", encoding="utf-8") as f:
                content = f.read()
                
            if not content.startswith("#EXTM3U"):
                logger.warning(f"Playlist mal formatée, ne commence pas par #EXTM3U")
                return False
            
            # Vérifier les permissions
            permissions = oct(os.stat(PLAYLIST_PATH).st_mode)[-3:]
            if permissions != "777":
                logger.warning(f"Permissions incorrectes: {permissions}, mise à jour...")
                os.chmod(PLAYLIST_PATH, 0o777)
            
            logger.info(f"Playlist vérifiée: {PLAYLIST_PATH}, taille: {file_size} octets")
            return True
            
        except Exception as e:
            logger.error(f"Erreur vérification playlist: {e}")
            return False
    
    def repair_playlist(self):
        """Répare la playlist si nécessaire"""
        try:
            # Vérifier si le script de création existe
            if os.path.exists(MASTER_PLAYLIST_CREATOR) and os.access(MASTER_PLAYLIST_CREATOR, os.X_OK):
                logger.info(f"Lancement du script de réparation: {MASTER_PLAYLIST_CREATOR}")
                result = subprocess.run(
                    [MASTER_PLAYLIST_CREATOR],
                    capture_output=True,
                    text=True
                )
                
                if result.returncode == 0:
                    logger.info("✅ Playlist réparée avec succès via le script")
                    for line in result.stdout.splitlines():
                        logger.debug(f"[create_master_playlist] {line}")
                    return True
                else:
                    logger.error(f"❌ Échec du script de réparation: {result.stderr}")
            else:
                logger.warning(f"Script de réparation non trouvé: {MASTER_PLAYLIST_CREATOR}")
            
            # Méthode de secours: créer une playlist minimale
            logger.info("Création d'une playlist de secours...")
            
            # Vérifier les dossiers HLS
            hls_dir = Path("/app/hls")
            active_channels = []
            
            # Trouver les chaînes actives
            for channel_dir in hls_dir.iterdir():
                if channel_dir.is_dir() and channel_dir.name != "stats" and (channel_dir / "playlist.m3u8").exists():
                    segments = list(channel_dir.glob("segment_*.ts"))
                    if segments:
                        active_channels.append(channel_dir.name)
            
            # Générer le contenu
            content = "#EXTM3U\n"
            if active_channels:
                for channel in sorted(active_channels):
                    content += f'#EXTINF:-1 tvg-id="{channel}" tvg-name="{channel}",{channel}\n'
                    content += f"http://{SERVER_URL}/hls/{channel}/playlist.m3u8\n"
                logger.info(f"Ajout de {len(active_channels)} chaînes actives: {', '.join(active_channels)}")
            else:
                content += "# Aucune chaîne active pour le moment (réparation)\n"
                logger.warning("Aucune chaîne active détectée pour la playlist")
            
            # Écrire le contenu
            with open(PLAYLIST_PATH, "w", encoding="utf-8") as f:
                f.write(content)
            
            # Définir les permissions
            os.chmod(PLAYLIST_PATH, 0o777)
            
            # Vérifier le résultat
            if os.path.exists(PLAYLIST_PATH) and os.path.getsize(PLAYLIST_PATH) > 0:
                logger.info(f"✅ Playlist réparée manuellement: {os.path.getsize(PLAYLIST_PATH)} octets")
                return True
            else:
                logger.error("❌ Échec de la réparation manuelle")
                return False
            
        except Exception as e:
            logger.error(f"Erreur réparation playlist: {e}")
            logger.error(traceback.format_exc())
            
            # Dernier recours - playlist minimale
            try:
                with open(PLAYLIST_PATH, "w", encoding="utf-8") as f:
                    f.write("#EXTM3U\n# Playlist minimale d'urgence\n")
                os.chmod(PLAYLIST_PATH, 0o777)
                logger.info("✅ Playlist minimale d'urgence créée")
                return True
            except Exception as e2:
                logger.critical(f"❌ Échec critique réparation playlist: {e2}")
                return False
    
    def run(self):
        """Boucle principale de surveillance"""
        logger.info("Démarrage de la surveillance des playlists")
        
        while self.running:
            try:
                current_time = time.time()
                
                # Vérifier si l'intervalle est écoulé
                if current_time - self.last_check >= CHECK_INTERVAL:
                    self.last_check = current_time
                    
                    # Vérifier la playlist
                    valid = self.check_playlist()
                    
                    # Réparer si nécessaire (avec intervalle minimal de 5 minutes entre réparations)
                    if not valid and (current_time - self.last_repair >= 300):
                        logger.warning("Playlist invalide, lancement de la réparation...")
                        repaired = self.repair_playlist()
                        if repaired:
                            logger.info("✅ Playlist réparée avec succès")
                            self.last_repair = current_time
                        else:
                            logger.error("❌ Échec de la réparation")
                
                # Pause pour économiser les ressources
                time.sleep(5)
                
            except Exception as e:
                logger.error(f"Erreur dans la boucle de surveillance: {e}")
                logger.error(traceback.format_exc())
                time.sleep(10)  # Pause plus longue en cas d'erreur
        
        logger.info("Arrêt du service de surveillance")

if __name__ == "__main__":
    # Lancer le watchdog
    watchdog = PlaylistWatchdog()
    watchdog.run() 
#!/usr/bin/env python3

import os
import sys
import signal
import time
from pathlib import Path
from iptv_manager import IPTVManager
from config import logger
import threading
import time
import psutil
from config import (
    CONTENT_DIR,
    USE_GPU,
    logger
)

class Application:
    def __init__(self):
        self.manager = None
        self.running = True
        
        # On gère SIGTERM et SIGINT
        signal.signal(signal.SIGTERM, self.handle_signal)
        signal.signal(signal.SIGINT, self.handle_signal)
        
    def handle_signal(self, signum, frame):
        """On gère les signaux système pour un arrêt propre"""
        logger.info(f"Signal {signum} reçu, nettoyage et arrêt...")
        self.running = False
        if self.manager:
            self.manager.cleanup_manager()
    
    def setup(self):
        """On initialise l'environnement"""
        try:
            # Configuration du fuseau horaire
            import time
            import os
            os.environ['TZ'] = 'Europe/Paris'
            time.tzset()  # Applique le changement
            logger.info(f"🕒 Fuseau horaire configuré: {time.tzname}")
            
            # On vérifie/crée les dossiers requis avec les bonnes permissions
            required_dirs = [
                CONTENT_DIR,
                "/app/hls",
                "/app/logs",
                "/app/logs/ffmpeg",
                "/app/logs/nginx"
            ]
            
            for d in required_dirs:
                path = Path(d)
                # Create directory with full permissions
                path.mkdir(parents=True, exist_ok=True)
                os.chmod(str(path), 0o777)
                logger.info(f"📁 Dossier {d} créé/vérifié avec permissions 777")
                
            # Create nginx log file if it doesn't exist
            nginx_log = Path("/app/logs/nginx/access.log")
            if not nginx_log.exists():
                nginx_log.touch()
                os.chmod(str(nginx_log), 0o666)
                logger.info("📝 Fichier access.log créé avec permissions 666")
            
            # On vérifie les permissions
            for d in [CONTENT_DIR, "/app/hls", "/app/logs", "/app/logs/nginx"]:
                path = Path(d)
                if not os.access(str(path), os.W_OK):
                    logger.error(f"❌ Pas de droits d'écriture sur {d}")
                    return False
            
            # Initialiser la rotation des logs
            from config import setup_log_rotation
            setup_log_rotation("/app/logs")
            setup_log_rotation("/app/logs/ffmpeg")
            setup_log_rotation("/app/logs/nginx")
            
            # Créer la playlist maître initiale
            try:
                # Utiliser notre script dédié
                logger.info("🔄 Création de la playlist maître initiale...")
                import subprocess
                result = subprocess.run(
                    ["/app/create_master_playlist.py"],
                    capture_output=True, 
                    text=True
                )
                
                if result.returncode == 0:
                    logger.info("✅ Playlist maître initiale créée avec succès")
                    # Enregistrer les logs du script pour debug
                    for line in result.stdout.splitlines():
                        logger.debug(f"📝 [create_master_playlist] {line}")
                else:
                    logger.warning("⚠️ Échec création playlist via script, création manuelle")
                    logger.warning(f"Erreur: {result.stderr}")
                    
                    # Création manuelle en fallback
                    master_playlist = Path("/app/hls/playlist.m3u")
                    with open(master_playlist, "w", encoding="utf-8") as f:
                        f.write("#EXTM3U\n# Playlist initiale (fallback)\n")
                    os.chmod(str(master_playlist), 0o777)
                    logger.info("✅ Playlist maître minimale créée manuellement")
            except Exception as e:
                logger.error(f"❌ Erreur création playlist initiale: {e}")
                # Dernière tentative
                try:
                    with open("/app/hls/playlist.m3u", "w", encoding="utf-8") as f:
                        f.write("#EXTM3U\n")
                    os.chmod("/app/hls/playlist.m3u", 0o777)
                    logger.info("✅ Playlist maître créée en dernier recours")
                except:
                    logger.error("❌ Impossible de créer la playlist maître")
            
            # Démarrer le watchdog de playlist
            self._start_playlist_watchdog()
                    
            return True
            
        except Exception as e:
            logger.error(f"Erreur setup: {e}")
            return False
            
    def _start_playlist_watchdog(self):
        """Démarre le service de surveillance des playlists en arrière-plan"""
        try:
            watchdog_path = "/app/playlist_watchdog.py"
            if os.path.exists(watchdog_path) and os.access(watchdog_path, os.X_OK):
                logger.info("🔄 Démarrage du service de surveillance des playlists...")
                
                # Démarrer le processus en arrière-plan, détaché, avec redirection stdout/stderr vers un log
                import subprocess
                log_file = open("/app/logs/playlist_watchdog.log", "a")
                
                process = subprocess.Popen(
                    [watchdog_path],
                    stdout=log_file,
                    stderr=log_file,
                    start_new_session=True
                )
                
                # Vérifier si le processus a bien démarré
                if process.poll() is None:
                    logger.info(f"✅ Service de surveillance des playlists démarré avec PID {process.pid}")
                else:
                    logger.error(f"❌ Échec du démarrage du service de surveillance")
            else:
                logger.warning(f"⚠️ Script de surveillance non trouvé ou non exécutable: {watchdog_path}")
        except Exception as e:
            logger.error(f"❌ Erreur démarrage service de surveillance: {e}")
            
    def run_main_loop(self):
        """Boucle principale avec gestion des erreurs"""
        if not self.setup():
            logger.critical("❌ Échec de l'initialisation, arrêt.")
            return 1
            
        retry_count = 0
        max_retries = 3
        
        while self.running:
            try:
                if not self.manager:
                    # On instancie le manager avec le chemin du contenu
                    use_gpu = USE_GPU.lower()
                    self.manager = IPTVManager(CONTENT_DIR, use_gpu=use_gpu)
                
                # On lance le manager
                self.manager.run_manager_loop()
                
            except Exception as e:
                logger.error(f"Erreur critique: {e}")
                if self.manager:
                    self.manager.cleanup_manager()
                    self.manager = None
                    
                retry_count += 1
                if retry_count >= max_retries:
                    logger.critical(f"❌ Trop d'erreurs ({retry_count}), arrêt.")
                    return 1
                    
                # On attend avant de réessayer
                time.sleep(5)
                continue
                
        return 0

if __name__ == "__main__":
    app = Application()
    sys.exit(app.run_main_loop())

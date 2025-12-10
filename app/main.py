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
    logger,
    HLS_DIR
)
import traceback
from channel_api import start_api_server

class Application:
    def __init__(self):
        self.manager = None
        self.running = True
        self.api_thread = None

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
                HLS_DIR,
                "/app/logs",
                "/app/logs/ffmpeg",
                "/app/logs/nginx"
            ]
            
            # Vérifier d'abord quels répertoires sont accessibles
            writeable_dirs = {}
            for d in required_dirs:
                path = Path(d)
                # Create directory if it doesn't exist
                try:
                    path.mkdir(parents=True, exist_ok=True)
                except PermissionError:
                    logger.warning(f"⚠️ Permission denied lors de la création de {d}")
                
                # Check if directory exists and is writable
                if path.exists():
                    writeable_dirs[d] = os.access(str(path), os.W_OK)
                    if writeable_dirs[d]:
                        logger.info(f"📁 Dossier {d} vérifié et accessible en écriture.")
                    else:
                        logger.warning(f"⚠️ Dossier {d} n'est pas accessible en écriture.")
                else:
                    writeable_dirs[d] = False
                    logger.warning(f"⚠️ Dossier {d} n'existe pas et ne peut pas être créé")
                
            # Create nginx log file if it doesn't exist and logs directory is writable
            if writeable_dirs.get("/app/logs/nginx", False):
                nginx_log = Path("/app/logs/nginx/access.log")
                if not nginx_log.exists():
                    try:
                        nginx_log.touch()
                        logger.info("📝 Fichier access.log créé.")
                    except PermissionError:
                        logger.warning("⚠️ Impossible de créer le fichier access.log (permission denied)")
            
            # Verify critical directories are writable
            critical_dirs = [HLS_DIR, "/app/logs"]
            for d in critical_dirs:
                if not writeable_dirs.get(d, False) and not os.access(d, os.W_OK):
                    logger.error(f"❌ Pas de droits d'écriture sur le répertoire critique {d}")
                    # We'll continue anyway and try to use what we have
            
            # Try to initialize log rotation for accessible directories
            from config import setup_log_rotation
            if writeable_dirs.get("/app/logs", False):
                setup_log_rotation("/app/logs")
            if writeable_dirs.get("/app/logs/ffmpeg", False):
                setup_log_rotation("/app/logs/ffmpeg")
            if writeable_dirs.get("/app/logs/nginx", False):
                setup_log_rotation("/app/logs/nginx")
            
            # Forcer la création de la playlist maître au démarrage comme source de vérité
            try:
                import subprocess
                playlist_script = "/app/create_master_playlist.py"
                if os.access(playlist_script, os.X_OK):
                    logger.info("🚀 Exécution du script create_master_playlist.py pour établir la source de vérité...")
                    result = subprocess.run([playlist_script], capture_output=True, text=True, check=True)
                    logger.info("✅ Script de création de playlist exécuté avec succès.")
                    logger.debug(f"Sortie du script:\n{result.stdout}")
                else:
                    logger.error(f"Le script {playlist_script} n'est pas exécutable.")
            except (subprocess.CalledProcessError, FileNotFoundError) as e:
                logger.error(f"❌ Échec de l'exécution du script de création de playlist: {e}")
                # Tenter une création manuelle minimale en cas d'échec
                with open(f"{HLS_DIR}/playlist.m3u", "w", encoding="utf-8") as f:
                    f.write("#EXTM3U\n")
                logger.warning("Création d'une playlist minimale suite à l'échec du script.")
            
            # Démarrer le watchdog de playlist si la playlist a pu être créée
            if os.path.exists(f"{HLS_DIR}/playlist.m3u"):
                self._start_playlist_watchdog()
                    
            # Continue even if there were permission issues
            return True
            
        except Exception as e:
            logger.error(f"Erreur setup: {e}")
            logger.debug(f"Détail de l'erreur: {traceback.format_exc()}")
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

                    # Démarrer l'API Flask pour les actions sur les chaînes
                    try:
                        self.api_thread = start_api_server(self.manager, host='0.0.0.0', port=5000)
                        logger.info("✅ API des chaînes démarrée sur le port 5000")
                    except Exception as e:
                        logger.error(f"❌ Erreur lors du démarrage de l'API: {e}")

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

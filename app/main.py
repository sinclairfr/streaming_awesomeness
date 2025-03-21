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
            
            # On vérifie/crée les dossiers requis
            for d in [CONTENT_DIR, "/app/hls", "/app/logs", "/app/logs/ffmpeg"]:
                Path(d).mkdir(parents=True, exist_ok=True)
                
            # On vérifie les permissions
            for d in [CONTENT_DIR, "/app/hls", "/app/logs"]:
                path = Path(d)
                if not os.access(str(path), os.W_OK):
                    logger.error(f"❌ Pas de droits d'écriture sur {d}")
                    return False
            
            # Initialiser la rotation des logs
            from config import setup_log_rotation
            setup_log_rotation("/app/logs")
            setup_log_rotation("/app/logs/ffmpeg")
                    
            return True
            
        except Exception as e:
            logger.error(f"Erreur setup: {e}")
            return False

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

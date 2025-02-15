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

def background_ffmpeg_check(app):
    """ On vérifie les processus FFmpeg toutes les 60s même si aucune chaîne n'est regardée """
    while app.manager is None:
        logger.warning("⏳ Attente de l'initialisation du manager IPTV...")
        time.sleep(5)

    counter = 0
    while True:
        counter += 1
        # On ne log qu'une seule fois par itération
        logger.warning(f"🔥 DEBUG: Vérification FFmpeg (itération {counter})")
        for channel in app.manager.channels.values():
            channel.log_ffmpeg_processes()
        time.sleep(60)  # On attend une minute complète


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
            self.manager.cleanup()
    
    def setup(self):
        """On initialise l'environnement"""
        try:
            # On vérifie/crée les dossiers requis
            for d in ["/app/content", "/app/hls", "/app/logs", "/app/logs/ffmpeg"]:
                Path(d).mkdir(parents=True, exist_ok=True)
                
            # On vérifie les permissions
            for d in ["/app/content", "/app/hls", "/app/logs"]:
                path = Path(d)
                if not os.access(str(path), os.W_OK):
                    logger.error(f"❌ Pas de droits d'écriture sur {d}")
                    return False
                    
            return True
            
        except Exception as e:
            logger.error(f"Erreur setup: {e}")
            return False
            
    def run(self):
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
                    use_gpu = os.getenv("USE_GPU", "auto").lower() == "auto"
                    self.manager = IPTVManager("/app/content", use_gpu=use_gpu)
                
                # On lance le manager
                self.manager.run()
                
            except Exception as e:
                logger.error(f"Erreur critique: {e}")
                if self.manager:
                    self.manager.cleanup()
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
    threading.Thread(target=background_ffmpeg_check, args=(app,), daemon=True).start()
    sys.exit(app.run())

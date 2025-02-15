# test_system.py

import os
import time
import subprocess
from pathlib import Path
import requests
import logging

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

def test_nginx_logs():
    """Vérifie que nginx log correctement les accès"""
    logger.info("=== Test logs nginx ===")
    
    log_file = "/var/log/nginx/access.log"
    if not Path(log_file).exists():
        logger.error("❌ Log file manquant!")
        return False
        
    # On fait une requête de test
    try:
        response = requests.get("http://localhost/hls/playlist.m3u")
        logger.info(f"Test request status: {response.status_code}")
        
        # On vérifie le log
        time.sleep(1)
        with open(log_file) as f:
            last_line = f.readlines()[-1]
            logger.info(f"Dernière ligne log: {last_line}")
            
        return True
    except Exception as e:
        logger.error(f"❌ Erreur test logs: {e}")
        return False

def test_ffmpeg():
    """Vérifie que FFmpeg fonctionne"""
    logger.info("\n=== Test FFmpeg ===")
    
    try:
        result = subprocess.run(
            ["ffmpeg", "-version"],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            logger.info(f"✅ FFmpeg OK: {result.stdout.split()[2]}")
            return True
        else:
            logger.error(f"❌ Erreur FFmpeg: {result.stderr}")
            return False
    except Exception as e:
        logger.error(f"❌ FFmpeg non trouvé: {e}")
        return False

def test_directories():
    """Vérifie les dossiers et permissions"""
    logger.info("\n=== Test dossiers ===")
    
    dirs = [
        "/app/hls",
        "/app/content",
        "/app/logs",
        "/app/logs/ffmpeg",
        "/var/log/nginx"
    ]
    
    all_ok = True
    for d in dirs:
        path = Path(d)
        try:
            if not path.exists():
                logger.error(f"❌ {d} manquant!")
                path.mkdir(parents=True)
                all_ok = False
            
            # Test permissions
            test_file = path / "_test"
            try:
                test_file.touch()
                test_file.unlink()
                logger.info(f"✅ {d} accessible en écriture")
            except Exception as e:
                logger.error(f"❌ {d} non accessible: {e}")
                all_ok = False
                
        except Exception as e:
            logger.error(f"❌ Erreur {d}: {e}")
            all_ok = False
            
    return all_ok

def main():
    """Tests principaux"""
    logger.info("=== Démarrage tests système ===")
    
    ok = True
    ok &= test_directories()
    ok &= test_ffmpeg()
    ok &= test_nginx_logs()
    
    if ok:
        logger.info("\n✅ Tous les tests OK")
    else:
        logger.error("\n❌ Certains tests ont échoué")

if __name__ == "__main__":
    main()
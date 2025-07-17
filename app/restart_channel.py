#!/usr/bin/env python3

import os
import sys
import time
import traceback
from pathlib import Path

# We need to add the app directory to the path to import other modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from iptv_channel import IPTVChannel
from hls_cleaner import HLSCleaner
from config import CONTENT_DIR, USE_GPU, logger

def restart_channel(channel_name: str):
    """
    Performs a direct, manual restart of a single channel.
    This script is intended to be called from the command line.
    """
    logger.info(f"🔄 Exécution du redémarrage manuel pour la chaîne: {channel_name}")
    
    channel_dir = Path(CONTENT_DIR) / channel_name
    if not channel_dir.is_dir():
        logger.error(f"❌ Le dossier de la chaîne '{channel_name}' est introuvable à: {channel_dir}")
        return False
        
    try:
        # 1. Instantiate the HLS cleaner.
        hls_cleaner = HLSCleaner()

        # 2. Instantiate the channel object directly.
        logger.info(f"[{channel_name}] 1/3 - Initialisation de l'objet chaîne...")
        channel = IPTVChannel(
            name=channel_name,
            video_dir=str(channel_dir),
            hls_cleaner=hls_cleaner,
            use_gpu=USE_GPU.lower() == 'true',
            stats_collector=None # No stats for manual restart
        )
        
        if not channel.is_ready_for_streaming():
            logger.error(f"[{channel_name}] ❌ La chaîne n'est pas prête pour le streaming après initialisation. Vérifiez les fichiers vidéo.")
            return False

        # 3. Call the channel's internal restart method.
        logger.info(f"[{channel_name}] 2/3 - Appel de la méthode de redémarrage interne...")
        success = channel._restart_stream(diagnostic="manual_cli_restart")
        
        if success:
            logger.info(f"[{channel_name}] 3/3 - ✅ Le processus de redémarrage pour '{channel_name}' a été lancé avec succès.")
        else:
            logger.error(f"[{channel_name}] 3/3 - ❌ Échec du lancement du processus de redémarrage pour '{channel_name}'.")

        return success

    except Exception as e:
        logger.critical(f"❌ Une erreur inattendue est survenue lors du redémarrage manuel de '{channel_name}': {e}")
        logger.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 restart_channel.py <channel_name>")
        sys.exit(1)
        
    channel_to_restart = sys.argv[1]
    if restart_channel(channel_to_restart):
        sys.exit(0)
    else:
        sys.exit(1)
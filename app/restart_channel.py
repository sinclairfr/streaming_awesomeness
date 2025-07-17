#!/usr/bin/env python3

import os
import sys
from pathlib import Path
import time

# We need to add the app directory to the path to import other modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config import logger, CONTENT_DIR

COMMAND_DIR = "/app/commands"

def request_channel_restart(channel_name: str):
    """
    Requests a graceful restart of a single channel by creating a command file.
    This script is intended to be called from the command line.
    """
    logger.info(f"🔄 Demande de redémarrage pour la chaîne: {channel_name}")

    # 1. Vérifier que le dossier de la chaîne existe
    channel_dir = Path(CONTENT_DIR) / channel_name
    if not channel_dir.is_dir():
        logger.error(f"❌ Le dossier de la chaîne '{channel_name}' est introuvable à: {channel_dir}")
        return False

    # 2. Créer le dossier de commandes s'il n'existe pas
    command_path = Path(COMMAND_DIR)
    command_path.mkdir(parents=True, exist_ok=True)

    # 3. Créer le fichier de commande
    command_file = command_path / f"{channel_name}.restart"
    try:
        with open(command_file, 'w') as f:
            f.write(str(int(time.time())))
        logger.info(f"✅ Fichier de commande créé: {command_file}")
        return True
    except Exception as e:
        logger.critical(f"❌ Impossible de créer le fichier de commande '{command_file}': {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 restart_channel.py <channel_name>")
        sys.exit(1)
        
    channel_to_restart = sys.argv[1]
    if request_channel_restart(channel_to_restart):
        sys.exit(0)
    else:
        sys.exit(1)
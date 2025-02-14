#!/usr/bin/env python3

# On importe la librairie standard
import os
import sys
import shutil
import time
import json
import glob
import random
import logging
import datetime
import subprocess
import threading
import traceback
from queue import Queue
from pathlib import Path
from typing import Optional, List

# On importe les librairies tierces
import psutil
from tqdm import tqdm  # On affiche la barre de progression CLI
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# main.py
import signal
from iptv_manager import IPTVManager
from config import logger

def handle_signal(signum, frame):
    # On gère les signaux système pour un arrêt propre
    logger.info(f"Signal {signum} reçu, nettoyage et arrêt...")
    manager.cleanup()
    exit(0)

if __name__ == "__main__":
    # On instancie le manager avec le chemin du contenu et l'option GPU
    manager = IPTVManager("./content", use_gpu=False)

    # On attache les signaux SIGTERM et SIGINT pour un arrêt propre
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    # On lance le manager dans sa boucle principale
    manager.run()
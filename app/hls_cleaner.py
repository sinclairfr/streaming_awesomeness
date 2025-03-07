# hls_cleaner.py
import shutil
import time
import threading
from pathlib import Path
import os
import logging

from config import logger

# Logger
logger = logging.getLogger("HLS_CLEANER")

class HLSCleaner:
    """Gestionnaire unique de nettoyage des segments HLS"""

    def __init__(self, hls_dir: str = "/app/hls/"):
        # On permet de personnaliser le dossier HLS
        self.hls_dir = Path(hls_dir)
        self.max_hls_age = 7200  # 2 heures au lieu de 1 heure
        self.min_free_space_gb = 5.0  # Abaissé pour conserver plus de segments
        self.cleanup_interval = 1200  # 20 minutes au lieu de 10
        
        # On crée le dossier HLS s'il n'existe pas
        self.hls_dir.mkdir(parents=True, exist_ok=True)
        
        self.stop_event = threading.Event()
        self.cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        
        logger.info(f"HLSCleaner initialisé sur {self.hls_dir}")

    def start(self):
        """Démarre le nettoyage en arrière-plan"""
        logger.info("🔄 Démarrage du monitoring HLS...")
        self.cleanup_thread.start()

    def stop(self):
        """Arrête proprement le nettoyage"""
        self.stop_event.set()
        self.cleanup_thread.join()
        logger.info("⏹️ Arrêt du monitoring HLS.")

    def initial_cleanup(self):
        """Nettoyage initial au démarrage"""
        try:
            logger.info("🧹 Nettoyage initial...")
            for item in self.hls_dir.glob("**/*"):
                if item.is_file() and item.suffix in ['.ts', '.m3u8']:
                    item.unlink()
                    logger.debug(f"Supprimé: {item}")
            logger.info("✨ Nettoyage initial terminé")
        except Exception as e:
            logger.error(f"Erreur nettoyage initial: {e}")

    def cleanup_channel(self, channel_name: str):
        """Nettoie les fichiers d'une chaîne spécifique"""
        try:
            channel_dir = self.hls_dir / channel_name
            if channel_dir.exists():
                for item in channel_dir.glob("*"):
                    if item.is_file():
                        item.unlink()
                logger.info(f"✨ Chaîne {channel_name} nettoyée")
        except Exception as e:
            logger.error(f"Erreur nettoyage chaîne {channel_name}: {e}")

    def _cleanup_loop(self):
        """Boucle de nettoyage périodique"""
        while not self.stop_event.is_set():
            try:
                self._check_disk_space()
                self._cleanup_old_segments()
                self._cleanup_orphaned_segments()
            except Exception as e:
                logger.error(f"Erreur dans la boucle de nettoyage: {e}")

            time.sleep(self.cleanup_interval)

    def _check_disk_space(self):
        """Vérifie l'espace disque"""
        try:
            disk_usage = shutil.disk_usage(self.hls_dir)
            free_space_gb = disk_usage.free / (1024**3)

            if free_space_gb < self.min_free_space_gb:
                logger.warning(f"⚠️ Espace disque faible: {free_space_gb:.2f} GB")
                self._aggressive_cleanup()

        except Exception as e:
            logger.error(f"Erreur vérification espace disque: {e}")

    def _cleanup_old_segments(self):
        """On ne supprime que les segments vraiment obsolètes"""
        try:
            now = time.time()
            deleted_count = 0
            
            # On vérifie chaque dossier de chaîne
            for channel_dir in self.hls_dir.glob("*"):
                if not channel_dir.is_dir():
                    continue
                    
                playlist = channel_dir / "playlist.m3u8"
                if not playlist.exists():
                    continue
                    
                # On lit la playlist pour identifier les segments actifs
                active_segments = set()
                try:
                    with open(playlist) as f:
                        for line in f:
                            if line.strip().endswith('.ts'):
                                active_segments.add(line.strip())
                except Exception as e:
                    logger.error(f"Erreur lecture playlist {playlist}: {e}")
                    continue
                    
                # On ne supprime que les segments inactifs ET vieux
                for segment in channel_dir.glob("*.ts"):
                    if (
                        segment.name not in active_segments 
                        and now - segment.stat().st_mtime > self.max_hls_age
                    ):
                        try:
                            segment.unlink()
                            deleted_count += 1
                            logger.debug(f"Segment supprimé: {segment}")
                        except Exception as e:
                            logger.error(f"Erreur suppression {segment}: {e}")

            if deleted_count > 0:
                logger.info(f"🧹 {deleted_count} segments obsolètes supprimés")

        except Exception as e:
            logger.error(f"Erreur nettoyage segments: {e}")
            
    def _cleanup_orphaned_segments(self):
        """Supprime les segments non référencés"""
        try:
            for channel_dir in self.hls_dir.iterdir():
                if not channel_dir.is_dir():
                    continue

                playlist = channel_dir / "playlist.m3u8"
                if not playlist.exists():
                    # Pas de playlist = on supprime tous les segments
                    for f in channel_dir.glob("*.ts"):
                        f.unlink()
                    continue

                # On lit la playlist pour trouver les segments référencés
                with open(playlist) as f:
                    referenced = set(
                        line.strip() for line in f 
                        if line.strip().endswith('.ts')
                    )

                # On supprime les segments non référencés
                for segment in channel_dir.glob("*.ts"):
                    if segment.name not in referenced:
                        segment.unlink()

        except Exception as e:
            logger.error(f"Erreur nettoyage segments orphelins: {e}")

    def _aggressive_cleanup(self):
        """Nettoyage agressif en cas de manque d'espace"""
        try:
            segments = sorted(
                self.hls_dir.glob("**/*.ts"),
                key=lambda x: x.stat().st_mtime
            )
            # On supprime la moitié des segments les plus vieux
            to_delete = len(segments) // 2
            for f in segments[:to_delete]:
                f.unlink()
            logger.info(f"🗑️ Nettoyage agressif: {to_delete} segments supprimés")
        except Exception as e:
            logger.error(f"Erreur nettoyage agressif: {e}")
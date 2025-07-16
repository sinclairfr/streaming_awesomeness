# hls_cleaner.py
import shutil
import time
import threading
import logging
import os
from pathlib import Path
from typing import Dict, List, Union, Optional

from config import logger, HLS_DIR

# On utilise le logger de config
# logger = logging.getLogger("HLS_CLEANER")


class HLSCleaner:
    """Gestionnaire unique de nettoyage des segments HLS"""

    def __init__(self, hls_dir: str = HLS_DIR):
        # On permet de personnaliser le dossier HLS
        self.hls_dir = Path(hls_dir)
        self.max_hls_age = 28800  # Augmenté à 8h (au lieu de 4h)
        self.min_free_space_gb = 1.0  # Abaissé pour conserver plus de segments
        self.cleanup_interval = 3600  # Nettoyage toutes les heures

        # On crée le dossier HLS s'il n'existe pas
        self.hls_dir.mkdir(parents=True, exist_ok=True)

        self.stop_event = threading.Event()
        self.cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)

        logger.info(f"HLSCleaner initialisé sur {self.hls_dir}")

    def _clean_old_segments(self, channel_name: str):
        """Supprime les segments obsolètes d'une chaîne spécifique"""
        try:
            channel_dir = self.hls_dir / channel_name
            if not channel_dir.exists():
                logger.debug(f"Le dossier de la chaîne {channel_name} n'existe pas.")
                return
                
                
            # Lire la playlist pour identifier les segments actifs
            playlist_path = channel_dir / "playlist.m3u8"
            active_segments = set()
            
            if playlist_path.exists():
                try:
                    with open(playlist_path, 'r') as f:
                        for line in f:
                            line = line.strip()
                            if line.endswith(".ts"):
                                active_segments.add(Path(line).name)
                except Exception as e:
                    logger.warning(f"⚠️ Erreur lecture playlist {playlist_path}: {e}")
            
            # Supprimer les segments trop vieux ou non référencés
            now = time.time()
            deleted_count = 0
            
            for segment in channel_dir.glob("*.ts"):
                # Garder les segments récents ET actifs, supprimer les autres
                if (segment.name not in active_segments or 
                    now - segment.stat().st_mtime > self.max_hls_age):
                    try:
                        segment.unlink()
                        deleted_count += 1
                    except Exception as e:
                        logger.warning(f"⚠️ Erreur suppression {segment}: {e}")
            
            if deleted_count > 0:
                logger.info(f"🧹 Nettoyage {channel_name}: {deleted_count} segments supprimés")
                
        except Exception as e:
            logger.error(f"❌ Erreur _clean_old_segments pour {channel_name}: {e}")
            
    def start(self):
        """Démarre le nettoyage en arrière-plan"""
        logger.info("🔄 Démarrage du monitoring HLS...")
        self.cleanup_thread.start()

    def stop_cleaner(self):
        """Arrête proprement le nettoyage"""
        self.stop_event.set()
        self.cleanup_thread.join()
        logger.info("⏹️ Arrêt du monitoring HLS.")

    def initial_cleanup(self):
        """Effectue un nettoyage initial de tous les répertoires HLS"""
        logger.info("🧹 HLSCleaner: Nettoyage initial des dossiers HLS")
        try:
            # S'assurer que le dossier principal existe avec les bonnes permissions
            Path(self.hls_dir).mkdir(parents=True, exist_ok=True)
            
            # Nettoyer chaque dossier de chaîne
            for channel_dir in Path(self.hls_dir).iterdir():
                if channel_dir.is_dir():
                    
                    # Nettoyage des anciens segments
                    self._clean_old_segments(channel_dir.name)
            
            logger.info("✅ HLSCleaner: Nettoyage initial terminé")
            return True
        except Exception as e:
            logger.error(f"❌ HLSCleaner: Erreur nettoyage initial: {e}")
            return False

    def cleanup_channel(self, channel_name: str):
        """Supprime le dossier HLS complet d'une chaîne."""
        try:
            channel_dir = self.hls_dir / channel_name
            if channel_dir.exists() and channel_dir.is_dir():
                shutil.rmtree(channel_dir)
                logger.info(f"🗑️ Dossier HLS de la chaîne '{channel_name}' supprimé.")
        except Exception as e:
            logger.error(f"Erreur lors de la suppression du dossier HLS pour '{channel_name}': {e}")

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

    def _cleanup_old_segments(self):
        """On ne supprime que les segments vraiment obsolètes et non référencés"""
        try:
            now = time.time()
            deleted_count = 0
            # Augmentation du délai avant suppression des segments
            extended_max_age = self.max_hls_age * 2  # Double le délai normal pour éviter les suppressions prématurées

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
                            line = line.strip()
                            if line.endswith(".ts"):
                                # On prend juste le nom du fichier sans le chemin
                                segment_name = Path(line).name
                                active_segments.add(segment_name)
                except Exception as e:
                    logger.error(f"Erreur lecture playlist {playlist}: {e}")
                    continue

                # On ne supprime QUE les segments inactifs ET vraiment vieux
                for segment in channel_dir.glob("*.ts"):
                    if (
                        segment.name not in active_segments
                        and now - segment.stat().st_mtime > extended_max_age  # Utilise le délai étendu
                    ):
                        try:
                            segment.unlink()
                            deleted_count += 1
                            logger.debug(f"Segment supprimé: {segment}")
                        except Exception as e:
                            logger.error(f"Erreur suppression {segment}: {e}")
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
                        line.strip() for line in f if line.strip().endswith(".ts")
                    )

                # On supprime les segments non référencés
                for segment in channel_dir.glob("*.ts"):
                    if segment.name not in referenced:
                        segment.unlink()

        except Exception as e:
            logger.error(f"Erreur nettoyage segments orphelins: {e}")

    def _check_disk_space(self):
        """Vérifie l'espace disque disponible et nettoie si nécessaire"""
        try:
            # Récupère les stats du filesystem
            stats = shutil.disk_usage(self.hls_dir)
            free_gb = stats.free / (1024 * 1024 * 1024)

            # Si l'espace libre est inférieur au seuil
            if free_gb < self.min_free_space_gb:
                logger.warning(
                    f"⚠️ Espace disque faible: {free_gb:.2f} GB (seuil: {self.min_free_space_gb} GB)"
                )
                # Nettoyage agressif
                self._aggressive_cleanup()

        except Exception as e:
            logger.error(f"Erreur vérification espace disque: {e}")

    def _aggressive_cleanup(self):
        """Nettoyage agressif en cas de manque d'espace"""
        try:
            segments = sorted(
                self.hls_dir.glob("**/*.ts"), key=lambda x: x.stat().st_mtime
            )
            # On supprime la moitié des segments les plus vieux
            to_delete = len(segments) // 2
            for f in segments[:to_delete]:
                f.unlink()
            logger.info(f"🗑️ Nettoyage agressif: {to_delete} segments supprimés")
        except Exception as e:
            logger.error(f"Erreur nettoyage agressif: {e}")

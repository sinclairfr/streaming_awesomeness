# hls_cleaner.py
import shutil
import time
import threading
from pathlib import Path
from typing import List
from config import logger

class HLSCleaner:
    """Gestionnaire unique de nettoyage des segments HLS"""

    def __init__(
        self,
        base_hls_dir: str,
        max_segment_age: int = 600,
        min_free_space_gb: float = 10.0,
    ):
        self.base_hls_dir = Path(base_hls_dir)
        self.max_segment_age = max_segment_age
        self.min_free_space_gb = min_free_space_gb
        self.cleanup_interval = 60
        self.stop_event = threading.Event()
        self.cleanup_thread = None
        self.lock = threading.Lock()

    def initial_cleanup(self):
        """Nettoyage initial au démarrage"""
        with self.lock:
            try:
                logger.info("🧹 Nettoyage initial HLS...")
                if self.base_hls_dir.exists():
                    shutil.rmtree(self.base_hls_dir)
                self.base_hls_dir.mkdir(parents=True, exist_ok=True)
                logger.info("✨ Dossier HLS réinitialisé")
            except Exception as e:
                logger.error(f"Erreur nettoyage initial HLS: {e}")

    def cleanup_channel(self, channel_name: str):
        """Nettoie tous les segments d'une chaîne"""
        with self.lock:
            channel_dir = self.base_hls_dir / channel_name
            if channel_dir.exists():
                try:
                    shutil.rmtree(channel_dir)
                    channel_dir.mkdir(parents=True, exist_ok=True)
                    logger.info(f"✨ Chaîne {channel_name} nettoyée")
                except Exception as e:
                    logger.error(f"Erreur nettoyage chaîne {channel_name}: {e}")

    def start(self):
        """Démarre la surveillance du nettoyage HLS"""
        if not self.cleanup_thread or not self.cleanup_thread.is_alive():
            self.stop_event.clear()
            self.cleanup_thread = threading.Thread(
                target=self._cleanup_loop,
                daemon=True
            )
            self.cleanup_thread.start()
            logger.info("🔄 Démarrage monitoring HLS")

    def stop(self):
        """Arrête la surveillance du nettoyage HLS"""
        if self.cleanup_thread:
            self.stop_event.set()
            self.cleanup_thread.join()
            self.cleanup_thread = None
            logger.info("⏹️ Arrêt monitoring HLS")

    def _cleanup_loop(self):
        """Boucle principale de nettoyage périodique"""
        while not self.stop_event.is_set():
            try:
                self._check_disk_space()
                self._cleanup_old_segments()
                self._cleanup_orphaned_segments()
                time.sleep(self.cleanup_interval)
            except Exception as e:
                logger.error(f"Erreur boucle nettoyage: {e}")

    def _check_disk_space(self):
        """Vérifie l'espace disque et effectue un nettoyage agressif si nécessaire"""
        try:
            disk_usage = shutil.disk_usage(self.base_hls_dir)
            free_space_gb = disk_usage.free / (1024**3)

            if free_space_gb < self.min_free_space_gb:
                logger.warning(f"⚠️ Espace disque critique: {free_space_gb:.2f} GB")
                self._aggressive_cleanup()

        except Exception as e:
            logger.error(f"Erreur vérification espace disque: {e}")

    def _aggressive_cleanup(self):
        """Nettoyage agressif si l'espace disque est critique"""
        try:
            segments = self._get_all_segments()
            segments_by_channel = {}

            for segment in segments:
                channel = segment.parent.name
                if channel not in segments_by_channel:
                    segments_by_channel[channel] = []
                segments_by_channel[channel].append(segment)

            for channel, segs in segments_by_channel.items():
                sorted_segs = sorted(
                    segs,
                    key=lambda x: x.stat().st_mtime,
                    reverse=True
                )
                # On garde 3 segments seulement
                for seg in sorted_segs[3:]:
                    try:
                        seg.unlink()
                        logger.debug(f"Segment supprimé (espace critique): {seg}")
                    except Exception as e:
                        logger.error(f"Erreur suppression segment {seg}: {e}")

            logger.info("Nettoyage agressif des segments terminé")

        except Exception as e:
            logger.error(f"Erreur nettoyage agressif: {e}")

    def _cleanup_old_segments(self):
        """Nettoie les segments plus vieux que max_segment_age"""
        try:
            current_time = time.time()
            old_segments = []

            for segment in self._get_all_segments():
                try:
                    if current_time - segment.stat().st_mtime > self.max_segment_age:
                        old_segments.append(segment)
                except Exception:
                    old_segments.append(segment)

            for segment in old_segments:
                try:
                    segment.unlink()
                    logger.debug(f"Segment périmé supprimé: {segment}")
                except Exception as e:
                    logger.error(f"Erreur suppression segment périmé {segment}: {e}")

            if old_segments:
                logger.info(f"🧹 {len(old_segments)} segments périmés supprimés")

        except Exception as e:
            logger.error(f"Erreur nettoyage segments périmés: {e}")

    def _cleanup_orphaned_segments(self):
        """Nettoie les segments qui ne sont plus référencés dans les playlists"""
        try:
            for channel_dir in self.base_hls_dir.iterdir():
                if not channel_dir.is_dir():
                    continue

                try:
                    playlist_path = channel_dir / "playlist.m3u8"
                    if not playlist_path.exists():
                        # On efface tous les segments si pas de playlist
                        for segment in channel_dir.glob("*.ts"):
                            try:
                                segment.unlink()
                                logger.debug(f"Segment orphelin supprimé: {segment}")
                            except Exception as e:
                                logger.error(f"Erreur suppression segment orphelin {segment}: {e}")
                        continue

                    # On lit la playlist
                    with open(playlist_path, "r") as f:
                        playlist_content = f.read()

                    # On récupère la liste des segments référencés
                    referenced_segments = {
                        line.strip()
                        for line in playlist_content.splitlines()
                        if line.strip().endswith(".ts")
                    }

                    # On efface les segments non référencés
                    for segment in channel_dir.glob("*.ts"):
                        if segment.name not in referenced_segments:
                            try:
                                segment.unlink()
                                logger.debug(f"Segment non référencé supprimé: {segment}")
                            except Exception as e:
                                logger.error(f"Erreur suppression segment non référencé {segment}: {e}")

                except Exception as e:
                    logger.error(f"Erreur nettoyage chaîne {channel_dir.name}: {e}")

        except Exception as e:
            logger.error(f"Erreur nettoyage segments orphelins: {e}")

    def _get_all_segments(self) -> List[Path]:
        """Retourne la liste de tous les segments HLS"""
        segments = []
        try:
            for channel_dir in self.base_hls_dir.iterdir():
                if channel_dir.is_dir():
                    segments.extend(channel_dir.glob("*.ts"))
        except Exception as e:
            logger.error(f"Erreur lecture segments: {e}")
        return segments

    def _clean_hls_directory(self):
        """Nettoyage d'un dossier HLS spécifique (non utilisé directement)"""
        self.hls_cleaner._clean_hls_directory(self.name)

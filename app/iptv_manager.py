# iptv_manager.py
import os
import sys
import time
import glob
import shutil
import signal
import random
import psutil
import threading
import traceback
from queue import Queue
from pathlib import Path
from watchdog.observers import Observer

from config import (
    logger,
    SERVER_URL,
    NGINX_ACCESS_LOG
)
from event_handler import ChannelEventHandler
from hls_cleaner import HLSCleaner
from client_monitor import ClientMonitor
from resource_monitor import ResourceMonitor
from iptv_channel import IPTVChannel

class IPTVManager:
    """
    # On gère toutes les chaînes, le nettoyage HLS, la playlist principale, etc.
    """

    # Constantes
    VIDEO_EXTENSIONS = (".mp4", ".avi", ".mkv", ".mov")
    CPU_THRESHOLD = 85
    SEGMENT_AGE_THRESHOLD = 30  # En secondes

    def __init__(self, content_dir: str, use_gpu: bool = False):
        self.content_dir = content_dir
        self.use_gpu = use_gpu
        self.channels = {}

        # On crée un nettoyeur HLS unique
        self.hls_cleaner = HLSCleaner("./hls")
        self.hls_cleaner.initial_cleanup()
        self.hls_cleaner.start()

        self.scan_lock = threading.Lock()
        self.failing_channels = set()
        self.channel_queue = Queue()

        logger.info("Initialisation du gestionnaire IPTV")
        self._clean_startup()

        # Thread de traitement de la file d'attente de chaînes
        self.queue_processor = threading.Thread(
            target=self._process_channel_queue,
            daemon=True
        )
        self.queue_processor.start()

        # On paramètre l'observer pour détecter les changements de contenu
        self.observer = Observer()
        event_handler = ChannelEventHandler(self)
        self.observer.schedule(event_handler, self.content_dir, recursive=True)

        # Scan initial du contenu
        logger.info(f"Scan initial dans {self.content_dir}")
        self.scan_channels(initial=True, force=True)

        # Moniteur clients
        self.client_monitor = ClientMonitor(NGINX_ACCESS_LOG)
        self.client_monitor.start()

        # Moniteur ressources
        self.resource_monitor = ResourceMonitor()
        self.resource_monitor.start()

        # Thread de mise à jour de la playlist maître
        self.master_playlist_updater = threading.Thread(
            target=self._master_playlist_loop,
            daemon=True
        )
        self.master_playlist_updater.start()

    def _clean_startup(self):
        """# On nettoie avant de démarrer"""
        try:
            logger.info("🧹 Nettoyage initial...")
            patterns_to_clean = [
                ("./hls/**/*", "Fichiers HLS"),
                ("./content/**/_playlist.txt", "Playlists"),
                ("./content/**/*.vtt", "Fichiers VTT"),
                ("./content/**/temp_*", "Fichiers temporaires"),
            ]
            for pattern, desc in patterns_to_clean:
                count = 0
                for f in glob.glob(pattern, recursive=True):
                    try:
                        if os.path.isfile(f):
                            os.remove(f)
                        elif os.path.isdir(f):
                            shutil.rmtree(f)
                        count += 1
                    except Exception as e:
                        logger.error(f"Erreur nettoyage {f}: {e}")
                logger.info(f"✨ {count} {desc} supprimés")
            os.makedirs("./hls", exist_ok=True)
        except Exception as e:
            logger.error(f"Erreur nettoyage initial: {e}")

    def _process_channel_queue(self):
        """# On traite la file d'attente (channels) en vérifiant les ressources"""
        while True:
            try:
                channel = self.channel_queue.get()
                cpu_usage = psutil.cpu_percent()
                if cpu_usage > self.CPU_THRESHOLD:
                    logger.warning(
                        f"CPU trop chargé ({cpu_usage}%), on retarde {channel.name}"
                    )
                    time.sleep(10)
                    self.channel_queue.put(channel)
                    continue

                if channel.name in self.failing_channels:
                    logger.info(f"Chaîne {channel.name} en échec, nettoyage...")
                    self._clean_channel(channel.name)

                success = self._start_channel(channel)
                if not success:
                    self.failing_channels.add(channel.name)
                else:
                    self.failing_channels.discard(channel.name)
            except Exception as e:
                logger.error(f"Erreur file d'attente: {e}")
            finally:
                self.channel_queue.task_done()

    def _start_channel(self, channel: IPTVChannel) -> bool:
        """# On tente de démarrer une chaîne"""
        try:
            if psutil.cpu_percent() > self.CPU_THRESHOLD:
                logger.warning(f"CPU trop chargé pour {channel.name}")
                return False

            if not channel._scan_videos():
                logger.error(f"Aucune vidéo valide pour {channel.name}")
                return False

            start_time = time.time()
            if not channel.start_stream():
                return False

            # On attend l'apparition des segments
            while time.time() - start_time < 10:
                if list(Path(f"./hls/{channel.name}").glob("*.ts")):
                    logger.info(f"✅ Chaîne {channel.name} démarrée avec succès")
                    return True
                time.sleep(0.5)

            logger.error(f"❌ Timeout démarrage {channel.name}")
            return False
        except Exception as e:
            logger.error(f"Erreur démarrage {channel.name}: {e}")
            return False

    def scan_channels(self, force: bool = False, initial: bool = False):
        """# On scanne les dossiers de chaînes et lance le traitement"""
        with self.scan_lock:
            try:
                content_path = Path(self.content_dir)
                if not content_path.exists():
                    logger.error(f"Le dossier {content_path} n'existe pas!")
                    return

                channel_dirs = [d for d in content_path.iterdir() if d.is_dir()]
                processing_queue = []

                for channel_dir in channel_dirs:
                    channel_name = channel_dir.name
                    processed_dir = channel_dir / "processed"
                    if processed_dir.exists() and list(processed_dir.glob("*.mp4")):
                        if channel_name not in self.channels:
                            channel = IPTVChannel(
                                channel_name,
                                str(channel_dir),
                                hls_cleaner=self.hls_cleaner,
                                use_gpu=self.use_gpu
                            )
                            self.channels[channel_name] = channel
                            threading.Thread(
                                target=self._start_ready_channel,
                                args=(channel,)
                            ).start()
                        else:
                            if force or initial or self._needs_update(channel_dir):
                                channel = self.channels[channel_name]
                                threading.Thread(
                                    target=self._update_channel_playlist,
                                    args=(channel, channel_dir)
                                ).start()
                    else:
                        processing_queue.append(channel_dir)

                if processing_queue:
                    threading.Thread(
                        target=self._process_new_channels,
                        args=(processing_queue,)
                    ).start()
            except Exception as e:
                logger.error(f"Erreur scan des chaînes: {e}")

    def _start_ready_channel(self, channel: IPTVChannel):
        """# On démarre rapidement une chaîne déjà prête"""
        try:
            logger.info(f"Démarrage rapide de la chaîne {channel.name}")
            if channel._scan_videos():
                if channel.start_stream():
                    logger.info(f"✅ {channel.name} démarrée")
                    self.generate_master_playlist()
                else:
                    logger.error(f"❌ Échec démarrage {channel.name}")
            else:
                logger.error(f"❌ Échec scan vidéos {channel.name}")
        except Exception as e:
            logger.error(f"Erreur démarrage {channel.name}: {e}")

    def _process_new_channels(self, channel_dirs):
        """# On traite de nouvelles chaînes sans vidéo traitée"""
        for channel_dir in channel_dirs:
            try:
                channel_name = channel_dir.name
                logger.info(f"Traitement de {channel_name}")

                channel = IPTVChannel(
                    channel_name,
                    str(channel_dir),
                    hls_cleaner=self.hls_cleaner,
                    use_gpu=self.use_gpu
                )
                self.channels[channel_name] = channel

                # Démarrage direct
                if channel.processed_videos and channel.start_stream():
                    logger.info(f"✅ Chaîne {channel_name} démarrée")
                    self.generate_master_playlist()
                else:
                    logger.error(f"❌ Échec démarrage {channel_name}")
            except Exception as e:
                logger.error(f"Erreur {channel_dir.name}: {e}")

    def generate_master_playlist(self):
        """# On génère la playlist principale avec les chaînes prêtes"""
        try:
            playlist_path = os.path.abspath("./hls/playlist.m3u")
            logger.info(f"On génère la master playlist: {playlist_path}")

            ready_channels = []
            for name, channel in sorted(self.channels.items()):
                if self._verify_channel(channel):
                    ready_channels.append(name)
                    logger.info(f"Chaîne {name} prête")
                else:
                    logger.debug(f"{name} pas prête")
                    if name not in self.failing_channels:
                        self.channel_queue.put(channel)

            with open(playlist_path, "w", encoding="utf-8") as f:
                f.write("#EXTM3U\n")
                for name in ready_channels:
                    f.write(f'#EXTINF:-1 tvg-id="{name}" tvg-name="{name}",{name}\n')
                    f.write(f"http://{SERVER_URL}/hls/{name}/playlist.m3u8\n")

            logger.info(f"Playlist mise à jour ({len(ready_channels)} chaînes)")
        except Exception as e:
            logger.error(f"Erreur génération master playlist: {e}")
            logger.error(traceback.format_exc())

    def _verify_channel(self, channel: IPTVChannel) -> bool:
        """# On vérifie si une chaîne produit des segments récents"""
        try:
            if not self._is_channel_ready(channel.name):
                return False
            hls_dir = Path(f"./hls/{channel.name}")
            if not hls_dir.exists():
                return False
            ts_files = list(hls_dir.glob("*.ts"))
            if not ts_files:
                return False
            newest_ts = max(ts_files, key=lambda x: x.stat().st_mtime)
            if time.time() - newest_ts.stat().st_mtime > self.SEGMENT_AGE_THRESHOLD:
                return False
            return True
        except Exception as e:
            logger.error(f"Erreur vérification {channel.name}: {e}")
            return False

    def _is_channel_ready(self, channel_name: str) -> bool:
        """# On vérifie si une chaîne a des vidéos traitées"""
        try:
            channel_dir = Path(f"./content/{channel_name}")
            processed_dir = channel_dir / "processed"
            if not processed_dir.exists():
                return False

            processed_videos = [
                f for f in processed_dir.glob("*.mp4") if not f.name.startswith("temp_")
            ]
            if not processed_videos:
                return False

            source_videos = [
                f for f in channel_dir.glob("*.*")
                if f.suffix.lower() in self.VIDEO_EXTENSIONS
                and not f.name.startswith("temp_")
                and f.parent == channel_dir
            ]
            processed_names = {v.stem for v in processed_videos}
            source_names = {v.stem for v in source_videos}
            return processed_names >= source_names and len(processed_videos) > 0
        except Exception as e:
            logger.error(f"Erreur is_channel_ready {channel_name}: {e}")
            return False

    def _update_channel_playlist(self, channel: IPTVChannel, channel_dir: Path):
        """# On met à jour la playlist d'une chaîne existante"""
        try:
            new_videos = self._scan_new_videos(channel_dir)
            if not new_videos:
                logger.debug(f"Pas de nouveau contenu pour {channel.name}")
                return

            from video_processor import VideoProcessor
            processor = VideoProcessor(str(channel_dir), use_gpu=self.use_gpu)
            processor.process_videos_async()
            new_processed = processor.wait_for_completion()
            if not new_processed:
                logger.error(f"Échec traitement nouveaux fichiers: {channel.name}")
                return

            channel.processed_videos.extend(new_processed)
            channel.processed_videos.sort()

            concat_file = channel._create_concat_file()
            if not concat_file:
                logger.error(f"Échec création concat file: {channel.name}")
                return

            logger.info(f"Playlist mise à jour pour {channel.name}")
            if not self._verify_channel(channel):
                logger.warning(f"Problème stream {channel.name}")
                if not channel.start_stream():
                    logger.error(f"Échec redémarrage {channel.name}")

        except Exception as e:
            logger.error(f"Erreur mise à jour {channel.name}: {e}")
            logger.error(traceback.format_exc())

    def _scan_new_videos(self, channel_dir: Path) -> list:
        """# On détecte les nouvelles vidéos non encore traitées"""
        try:
            processed_dir = channel_dir / "processed"
            if not processed_dir.exists():
                return []
            current_videos = {f.stem for f in processed_dir.glob("*.mp4")}
            all_videos = {f.stem for f in channel_dir.glob("*.mp4")}
            new_videos = all_videos - current_videos
            return [channel_dir / f"{video}.mp4" for video in new_videos]
        except Exception as e:
            logger.error(f"Erreur scan nouveaux fichiers: {e}")
            return []

    def _clean_channel(self, channel_name: str):
        """# Nettoyage approfondi d'une chaîne"""
        try:
            self.hls_cleaner.cleanup_channel(channel_name)
            hls_dir = Path(f"./hls/{channel_name}")
            if hls_dir.exists():
                shutil.rmtree(hls_dir)
            hls_dir.mkdir(parents=True, exist_ok=True)

            channel_dir = Path(f"./content/{channel_name}")
            if channel_dir.exists():
                for pattern in ["_playlist.txt", "*.vtt", "temp_*"]:
                    for f in channel_dir.glob(pattern):
                        try:
                            f.unlink()
                        except Exception as e:
                            logger.error(f"Erreur suppression {f}: {e}")

            logger.info(f"Nettoyage terminé pour {channel_name}")
        except Exception as e:
            logger.error(f"Erreur nettoyage {channel_name}: {e}")

    def _needs_update(self, channel_dir: Path) -> bool:
        """# On vérifie sommairement si un dossier a pu être mis à jour"""
        try:
            logger.debug(f"Vérification mises à jour: {channel_dir.name}")
            video_files = list(channel_dir.glob("*.mp4"))
            if not video_files:
                logger.debug(f"Aucun fichier dans {channel_dir.name}")
                return True
            return True
        except Exception as e:
            logger.error(f"Erreur needs_update {channel_dir}: {e}")
            return True

    def _master_playlist_loop(self):
        """# Boucle de mise à jour continue de la playlist"""
        while True:
            try:
                self.generate_master_playlist()
                time.sleep(60)
            except Exception as e:
                logger.error(f"Erreur maj master playlist: {e}")

    def cleanup(self):
        """# On arrête le nettoyeur, l'observer et on nettoie tout"""
        logger.info("Début du nettoyage...")
        if hasattr(self, "hls_cleaner"):
            logger.info("Arrêt du nettoyeur HLS...")
            self.hls_cleaner.stop()

        if hasattr(self, "observer"):
            logger.info("On arrête l'observer...")
            self.observer.stop()
            self.observer.join()

        for name, channel in self.channels.items():
            logger.info(f"Nettoyage canal {name}...")
            channel._clean_processes()

        logger.info("Nettoyage terminé")

    def _signal_handler(self, signum, frame):
        """# On gère les signaux système"""
        logger.info(f"Signal {signum} reçu, nettoyage...")
        self.cleanup()
        sys.exit(0)

    def run(self):
        """# On exécute la boucle principale"""
        try:
            self.scan_channels()
            self.generate_master_playlist()
            self.observer.start()

            cpu_thread = threading.Thread(target=self._cpu_monitor, daemon=True)
            cpu_thread.start()

            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.cleanup()
        except Exception as e:
            logger.error(f"🔥 Erreur dans le gestionnaire IPTV : {e}")
            self.cleanup()

    def _cpu_monitor(self):
        """# On surveille la CPU sur 1 minute"""
        usage_samples = []
        threshold = 90
        while True:
            try:
                usage = psutil.cpu_percent(interval=1)
                usage_samples.append(usage)
                if len(usage_samples) >= 60:
                    avg_usage = sum(usage_samples) / len(usage_samples)
                    if avg_usage > threshold:
                        logger.warning(
                            f"ALERTE CPU : moyenne {avg_usage:.1f}% sur 1 min"
                        )
                    usage_samples = []
            except Exception as e:
                logger.error(f"Erreur monitoring CPU : {e}")

    def _clean_directory(self, directory: Path):
        """# Nettoyage d'un dossier (non utilisé directement)"""
        if not directory.exists():
            return
        for item in directory.glob("**/*"):
            try:
                if item.is_file():
                    item.unlink()
                elif item.is_dir():
                    shutil.rmtree(item)
            except Exception as e:
                logger.error(f"Erreur suppression {item}: {e}")


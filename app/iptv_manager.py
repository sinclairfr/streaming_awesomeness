# iptv_manager.py
import os
import sys
import time
import glob
import shutil
import signal
import random
import psutil
import traceback
import subprocess 
from queue import Queue
from pathlib import Path
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
import threading
from event_handler import ChannelEventHandler
from hls_cleaner import HLSCleaner
from client_monitor import ClientMonitor
from resource_monitor import ResourceMonitor
from iptv_channel import IPTVChannel
import signal
from ffmpeg_monitor import FFmpegMonitor
from config import (
    CONTENT_DIR,
    NGINX_ACCESS_LOG,
    SERVER_URL,
    TIMEOUT_NO_VIEWERS,
    logger,
    VIDEO_EXTENSIONS,
    CPU_THRESHOLD,
    SEGMENT_AGE_THRESHOLD
)

class IPTVManager:
    """
    # On gère toutes les chaînes, le nettoyage HLS, la playlist principale, etc.
    """
    
    def __init__(self, content_dir: str, use_gpu: bool = False):
        self.ensure_hls_directory()  # Sans argument pour le dossier principal

        self.content_dir = content_dir
        self.use_gpu = use_gpu
        self.channels = {}

        # Moniteur FFmpeg
        self.ffmpeg_monitor = FFmpegMonitor(self.channels)
        self.ffmpeg_monitor.start()

        # On initialise le nettoyeur HLS avec le bon chemin
        self.hls_cleaner = HLSCleaner("/app/hls")
        self.hls_cleaner.initial_cleanup()
        self.hls_cleaner.start()

        self.scan_lock = threading.Lock()
        self.failing_channels = set()

        logger.info("Initialisation du gestionnaire IPTV")
        self._clean_startup()

        # Observer
        self.observer = Observer()
        event_handler = ChannelEventHandler(self)
        self.observer.schedule(event_handler, self.content_dir, recursive=True)

        # On crée tous les objets IPTVChannel mais SANS démarrer FFmpeg
        logger.info(f"Scan initial dans {self.content_dir}")
        self.scan_channels(initial=True, force=True)

        # Moniteur clients
        self.client_monitor = ClientMonitor(NGINX_ACCESS_LOG, self.update_watchers, self)
        self.client_monitor.start()

        # Moniteur ressources
        self.resource_monitor = ResourceMonitor()
        self.resource_monitor.start()

        # Thread de mise à jour de la playlist maître
        self.master_playlist_updater = threading.Thread(
            target=self._manage_master_playlist,
            daemon=True
        )
        self.master_playlist_updater.start()

        # Thread qui vérifie les watchers
        self.watchers_thread = threading.Thread(
            target=self._watchers_loop,
            daemon=True
        )
        self.watchers_thread.start()
        self.watchers_thread = threading.Thread(target=self._watchers_loop, daemon=True)
        self.running = True
    
    def _watchers_loop(self):
        """Surveille l'activité des watchers et arrête les streams inutilisés"""
        while True:
            try:
                current_time = time.time()
                channels_checked = set()

                # Pour chaque chaîne, on vérifie l'activité
                for channel_name, channel in self.channels.items():
                    if not hasattr(channel, 'last_watcher_time'):
                        continue

                    # On calcule l'inactivité
                    inactivity_duration = current_time - channel.last_watcher_time

                    # Si inactif depuis plus de TIMEOUT_NO_VIEWERS (120s par défaut)
                    if inactivity_duration > TIMEOUT_NO_VIEWERS:
                        if channel.process_manager.is_running():
                            logger.warning(
                                f"[{channel_name}] ⚠️ Stream inactif depuis {inactivity_duration:.1f}s, on arrête FFmpeg"
                            )
                            channel.stop_stream_if_needed()

                    channels_checked.add(channel_name)

                # On vérifie les processus FFmpeg orphelins
                for proc in psutil.process_iter(attrs=["pid", "name", "cmdline"]):
                    try:
                        if "ffmpeg" in proc.info["name"].lower():
                            cmd_str = " ".join(str(arg) for arg in proc.info.get("cmdline", []))
                            
                            # Pour chaque chaîne, on vérifie si le process lui appartient
                            for channel_name in self.channels:
                                if f"/hls/{channel_name}/" in cmd_str:
                                    if channel_name not in channels_checked:
                                        logger.warning(f"🔥 Process FFmpeg orphelin détecté pour {channel_name}, PID {proc.pid}")
                                        try:
                                            os.kill(proc.pid, signal.SIGKILL)
                                            logger.info(f"✅ Process orphelin {proc.pid} nettoyé")
                                        except:
                                            pass
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        continue

                time.sleep(10)  # Vérification toutes les 10s

            except Exception as e:
                logger.error(f"❌ Erreur watchers_loop: {e}")
                time.sleep(10)
                
    def update_watchers(self, channel_name: str, count: int, request_path: str):
        """Met à jour les watchers en fonction des requêtes m3u8 et ts"""
        try:
            if channel_name not in self.channels:
                logger.error(f"❌ Chaîne inconnue: {channel_name}")
                return

            channel = self.channels[channel_name]

            # Mise à jour SYSTÉMATIQUE du last_watcher_time à chaque requête
            channel.last_watcher_time = time.time()

            # Si c'est une requête de segment, on met aussi à jour last_segment_time
            if ".ts" in request_path:
                channel.last_segment_time = time.time()

            old_count = channel.watchers_count
            channel.watchers_count = count

            if old_count != count:
                logger.info(f"📊 Mise à jour {channel_name}: {count} watchers")

                if old_count == 0 and count > 0:
                    logger.info(f"[{channel_name}] 🔥 Premier watcher, démarrage du stream")
                    if not channel.start_stream():
                        logger.error(f"[{channel_name}] ❌ Échec démarrage stream")
                elif old_count > 0 and count == 0:
                    # On ne coupe PAS immédiatement, on laisse le monitoring gérer ça
                    logger.info(f"[{channel_name}] ⚠️ Plus de watchers recensés")

        except Exception as e:
            logger.error(f"❌ Erreur update_watchers: {e}")

    def _clean_startup(self):
        """# On nettoie avant de démarrer"""
        try:
            logger.info("🧹 Nettoyage initial...")
            patterns_to_clean = [
                ("/app/hls/**/*", "Fichiers HLS"),
                ("/app/content/**/_playlist.txt", "Playlists"),
                ("/app/content/**/*.vtt", "Fichiers VTT"),
                ("/app/content/**/temp_*", "Fichiers temporaires"),
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
            os.makedirs("/app/hls", exist_ok=True)
        except Exception as e:
            logger.error(f"Erreur nettoyage initial: {e}")

    def scan_channels(self, force: bool = False, initial: bool = False):
        """
        On scanne le contenu pour détecter les nouveaux dossiers (chaînes).
        """
        with self.scan_lock:
            try:
                content_path = Path(self.content_dir)
                if not content_path.exists():
                    logger.error(f"Le dossier {content_path} n'existe pas!")
                    return

                channel_dirs = [d for d in content_path.iterdir() if d.is_dir()]

                logger.info(f"📡 Scan des chaînes disponibles...")
                for channel_dir in channel_dirs:
                    channel_name = channel_dir.name

                    if channel_name in self.channels:
                        logger.info(f"🔄 Chaîne existante : {channel_name}")
                        continue

                    logger.info(f"✅ Nouvelle chaîne trouvée : {channel_name}")
                    self.channels[channel_name] = IPTVChannel(
                        channel_name,
                        str(channel_dir),
                        hls_cleaner=self.hls_cleaner,
                        use_gpu=self.use_gpu
                    )

                logger.info(f"📡 Scan terminé, {len(self.channels)} chaînes enregistrées.")

            except Exception as e:
                logger.error(f"Erreur scan des chaînes: {e}")

    def ensure_hls_directory(self, channel_name: str = None):
        """Crée et configure les dossiers HLS avec les bonnes permissions"""
        try:
            # Dossier HLS principal
            base_hls = Path("/app/hls")
            if not base_hls.exists():
                logger.info("📂 Création du dossier HLS principal...")
                base_hls.mkdir(parents=True, exist_ok=True)
                os.chmod(base_hls, 0o777)

            # Dossier spécifique à une chaîne si demandé
            if channel_name:
                channel_hls = base_hls / channel_name
                if not channel_hls.exists():
                    logger.info(f"📂 Création du dossier HLS pour {channel_name}")
                    channel_hls.mkdir(parents=True, exist_ok=True)
                    os.chmod(channel_hls, 0o777)
        except Exception as e:
            logger.error(f"❌ Erreur création dossiers HLS: {e}")

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

    def _manage_master_playlist(self):
        """
        # On gère la création et mise à jour de la playlist principale.
        # Cette méthode tourne en boucle et regénère la playlist toutes les 60s.
        """
        while True:
            try:
                playlist_path = os.path.abspath("/app/hls/playlist.m3u")
                logger.info(f"On génère la master playlist: {playlist_path}")

                with open(playlist_path, "w", encoding="utf-8") as f:
                    f.write("#EXTM3U\n")

                    # On référence TOUTES les chaînes self.channels
                    for name, channel in sorted(self.channels.items()):
                        f.write(f'#EXTINF:-1 tvg-id="{name}" tvg-name="{name}",{name}\n')
                        f.write(f"http://{SERVER_URL}/hls/{name}/playlist.m3u8\n")

                logger.info(f"Playlist mise à jour ({len(self.channels)} chaînes)")
                time.sleep(60)  # On attend 60s avant la prochaine mise à jour

            except Exception as e:
                logger.error(f"Erreur maj master playlist: {e}")
                logger.error(traceback.format_exc())
                time.sleep(60)  # On attend même en cas d'erreur

    def cleanup(self):
        logger.info("Début du nettoyage...")
        if hasattr(self, "hls_cleaner"):
            self.hls_cleaner.stop()

        if hasattr(self, "observer"):
            self.observer.stop()
            self.observer.join()

        for name, channel in self.channels.items():
            channel._clean_processes()

        logger.info("Nettoyage terminé")

    def run(self):
        try:
            # Démarrer la boucle de surveillance des watchers
            self.watchers_thread.start()
            logger.info("🔄 Boucle de surveillance des watchers démarrée")
            logger.debug("📥 Scan initial des chaînes...")
            self.scan_channels()
            logger.debug("🕵️ Démarrage de l'observer...")
            self.observer.start()

            # Debug du client_monitor
            logger.debug("🚀 Démarrage du client_monitor...")
            if not hasattr(self, 'client_monitor') or not self.client_monitor.is_alive():
                logger.error("❌ client_monitor n'est pas démarré!")

            while True:
                time.sleep(1)

        except KeyboardInterrupt:
            self.cleanup()
        except Exception as e:
            logger.error(f"🔥 Erreur manager : {e}")
            self.cleanup()

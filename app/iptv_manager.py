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
from queue import Queue
from pathlib import Path
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
import threading
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
import os
from pathlib import Path
import subprocess  # Ajouter en haut du fichier
import signal
import os
from ffmpeg_monitor import FFmpegMonitor 
from dotenv import load_dotenv
# On charge les variables d'environnement
load_dotenv()

class IPTVManager:
    """
    # On gère toutes les chaînes, le nettoyage HLS, la playlist principale, etc.
    """

    # Constantes
    VIDEO_EXTENSIONS = (".mp4", ".avi", ".mkv", ".mov")
    CPU_THRESHOLD = 85
    SEGMENT_AGE_THRESHOLD = 30  # En secondes
    

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

    def _watchers_loop(self):
        TIMEOUT_NO_VIEWERS = int(os.getenv("TIMEOUT_NO_VIEWERS", "120"))  # Par défaut 60s
        """Surveille l'activité des watchers et arrête les streams inutilisés"""
        while True:
            try:
                current_time = time.time()
                
                # On maintient un dict des processus FFmpeg par chaîne
                ffmpeg_processes = {}
                
                # On scanne tous les processus
                for proc in psutil.process_iter(attrs=["pid", "name", "cmdline"]):
                    try:
                        if "ffmpeg" in proc.info["name"].lower():
                            if not proc.info.get("cmdline"):
                                continue
                                
                            # On ne garde que le premier match pour chaque processus
                            channel_found = False
                            cmd_str = " ".join(str(arg) for arg in proc.info["cmdline"])
                            for channel_name in self.channels:
                                if f"/hls/{channel_name}/" in cmd_str and not channel_found:
                                    channel_found = True
                                    if channel_name not in ffmpeg_processes:
                                        ffmpeg_processes[channel_name] = set()
                                    ffmpeg_processes[channel_name].add(proc.info["pid"])
                                    break  # On sort dès qu'on a trouvé une chaîne
                                
                            # Si on n'a pas trouvé de chaîne valide, c'est peut-être un zombie
                            if not channel_found:
                                try:
                                    os.kill(proc.info["pid"], signal.SIGKILL)
                                    logger.info(f"🧹 Nettoyage processus zombie FFmpeg: {proc.info['pid']}")
                                except:
                                    pass
                                
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        continue

                # Pour chaque chaîne, on vérifie l'état
                for channel_name, channel in self.channels.items():
                    pids = ffmpeg_processes.get(channel_name, set())
                    if len(pids) > 1:  # Plus d'un processus FFmpeg
                        logger.warning(f"⚠️ {channel_name}: {len(pids)} processus FFmpeg actifs")
                        
                        # On garde le processus le plus récent
                        newest_pid = max(pids, key=lambda pid: psutil.Process(pid).create_time())
                        logger.info(f"[{channel_name}] Conservation du processus le plus récent: {newest_pid}")
                        
                        # On nettoie les autres
                        for pid in pids:
                            if pid != newest_pid:
                                logger.info(f"[{channel_name}] Nettoyage ancien processus: {pid}")
                                try:
                                    os.kill(pid, signal.SIGKILL)
                                except:
                                    pass

                    if channel.last_watcher_time and current_time - channel.last_watcher_time > TIMEOUT_NO_VIEWERS:
                        if channel.ffmpeg_process is not None:
                            logger.info(f"⏹️ Arrêt FFmpeg pour {channel_name} (inactif depuis {current_time - channel.last_watcher_time:.1f}s)")
                            channel._clean_processes()

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
            
            # On vérifie si l'état a vraiment changé
            if not hasattr(channel, '_last_update_state'):
                channel._last_update_state = {'count': -1, 'time': 0}
                
            current_time = time.time()
            
            # On évite les mises à jour trop rapprochées avec les mêmes valeurs
            if (count == channel._last_update_state['count'] and 
                current_time - channel._last_update_state['time'] < 1):
                return
                
            # On met à jour l'état
            channel._last_update_state['count'] = count
            channel._last_update_state['time'] = current_time
            
            old_count = channel.watchers_count
            channel.watchers_count = count

            if ".ts" in request_path:
                channel.last_segment_request = current_time

            # On ne log que si quelque chose a vraiment changé
            if old_count != count:
                logger.info(f"📊 Mise à jour {channel_name}: {count} watchers")
                
                if old_count == 0 and count > 0:
                    logger.info(f"[{channel_name}] 🔥 APPEL de start_stream() (0 -> 1 watcher)")
                    if not channel.start_stream():
                        logger.error(f"[{channel_name}] ❌ Échec démarrage stream")
                elif old_count > 0 and count == 0:
                    logger.info(f"[{channel_name}] 🛑 Plus aucun watcher, arrêt du stream...")
                    channel.stop_stream_if_needed()

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
                if list(Path(f"/app/hls/{channel.name}").glob("*.ts")):
                    logger.info(f"✅ Chaîne {channel.name} démarrée avec succès")
                    return True
                time.sleep(0.5)

            logger.error(f"❌ Timeout démarrage {channel.name}")
            return False
        except Exception as e:
            logger.error(f"Erreur démarrage {channel.name}: {e}")
            return False

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

    def _start_ready_channel(self, channel: IPTVChannel):
        """# On démarre rapidement une chaîne déjà prête"""
        try:
            logger.info(f"Démarrage rapide de la chaîne {channel.name}")
            if channel._scan_videos():
                if channel.start_stream():
                    logger.info(f"✅ {channel.name} démarrée")
                    #self.generate_master_playlist()
                else:
                    logger.error(f"❌ Échec démarrage {channel.name}")
            else:
                logger.error(f"❌ Échec scan vidéos {channel.name}")
        except Exception as e:
            logger.error(f"Erreur démarrage {channel.name}: {e}")

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
            
    def _is_channel_ready(self, channel_name: str) -> bool:
        """# On vérifie si une chaîne a des vidéos traitées"""
        try:
            channel_dir = Path(f"/app/content/{channel_name}")
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
            hls_dir = Path(f"/app/hls/{channel_name}")
            if hls_dir.exists():
                shutil.rmtree(hls_dir)
            hls_dir.mkdir(parents=True, exist_ok=True)

            channel_dir = Path(f"/app/content/{channel_name}")
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
                logger.info(f"Aucun fichier dans {channel_dir.name}")
                return True
            return True
        except Exception as e:
            logger.error(f"Erreur needs_update {channel_dir}: {e}")
            return True

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
        
    def _signal_handler(self, signum, frame):
        """# On gère les signaux système"""
        logger.info(f"Signal {signum} reçu, nettoyage...")
        self.cleanup()
        sys.exit(0)

    def run(self):
        try:
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
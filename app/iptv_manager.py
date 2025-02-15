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
import os
from pathlib import Path

def ensure_persistent_hls():
    """Crée et fixe les permissions du dossier HLS au démarrage"""
    hls_path = Path("/app/hls")
    if not hls_path.exists():
        print("📂 Création du dossier HLS...")
        hls_path.mkdir(parents=True, exist_ok=True)
    
    os.chmod(hls_path, 0o777)  # Assure que FFmpeg pourra écrire dedans

ensure_persistent_hls()

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

        # On initialise le nettoyeur HLS avec le bon chemin
        self.hls_cleaner = HLSCleaner("/app/hls")
        self.hls_cleaner.initial_cleanup()
        self.hls_cleaner.start()

        self.scan_lock = threading.Lock()
        self.failing_channels = set()
        self.channel_queue = Queue()

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
        self.client_monitor = ClientMonitor(NGINX_ACCESS_LOG, self)
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

        # Thread qui vérifie les watchers
        self.watchers_thread = threading.Thread(
            target=self._watchers_loop,
            daemon=True
        )
        self.watchers_thread.start()
    def _watchers_loop(self):
        """
        Boucle qui vérifie périodiquement si watchers_count=0 depuis plus de 60s,
        et arrête FFmpeg pour économiser des ressources.
        """
        while True:
            try:
                for channel_name, channel in self.channels.items():
                    if channel.watchers_count == 0:
                        # personne ne regarde
                        # Est-ce qu'on est à 0 depuis plus de 60s ?
                        idle_time = time.time() - channel.last_watcher_time
                        if idle_time > 60:
                            channel.stop_stream_if_needed()
            except Exception as e:
                logger.error(f"Erreur watchers_loop: {e}")

            time.sleep(10)  # on vérifie toutes les 10s

    def update_watchers(self, channel_name: str, count: int):
        """Met à jour le nombre de watchers et gère le stream"""
        try:
            logger.info(f"📊 Mise à jour {channel_name}: {count} watchers")

            if channel_name not in self.channels:
                logger.error(f"❌ Chaîne inconnue: {channel_name}")
                return

            channel = self.channels[channel_name]
            old_count = channel.watchers_count

            # Mise à jour du nombre de watchers
            channel.watchers_count = count
            if count > 0:
                channel.last_watcher_time = time.time()

            logger.info(f"[{channel_name}] Watchers: {old_count} -> {count}")

            # 🔥 LOGS POUR DEBUG 🔥
            if old_count == 0 and count > 0:
                logger.info(f"[{channel_name}] 🔥 APPEL de start_stream() (0 -> 1 watcher)")
                if not channel.start_stream():
                    logger.error(f"[{channel_name}] ❌ Échec démarrage stream")

            elif old_count > 0 and count == 0:
                logger.info(f"[{channel_name}] 🛑 Plus aucun watcher, arrêt du stream...")
                channel.stop_stream_if_needed()

            # On met à jour la master playlist
            self.generate_master_playlist()

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
                    self.generate_master_playlist()
                else:
                    logger.error(f"❌ Échec démarrage {channel.name}")
            else:
                logger.error(f"❌ Échec scan vidéos {channel.name}")
        except Exception as e:
            logger.error(f"Erreur démarrage {channel.name}: {e}")

    def ensure_hls_directory(self, channel_name: str):
        """Crée le dossier HLS si inexistant avec les bonnes permissions"""
        hls_path = Path(f"/app/hls/{channel_name}")
        if not hls_path.exists():
            logger.info(f"📂 Création du dossier HLS pour {channel_name}")
            hls_path.mkdir(parents=True, exist_ok=True)
            os.chmod(hls_path, 0o777)  # Autorise tout le monde à écrire

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
        try:
            playlist_path = os.path.abspath("/app/hls/playlist.m3u")
            logger.info(f"On génère la master playlist: {playlist_path}")

            with open(playlist_path, "w", encoding="utf-8") as f:
                f.write("#EXTM3U\n")

                # On référence TOUTES les chaînes self.channels
                for name, channel in sorted(self.channels.items()):
                    # Ajout direct dans la playlist, même si FFmpeg n’est pas lancé
                    f.write(f'#EXTINF:-1 tvg-id="{name}" tvg-name="{name}",{name}\n')
                    f.write(f"http://{SERVER_URL}/hls/{name}/playlist.m3u8\n")

            logger.info(f"Playlist mise à jour ({len(self.channels)} chaînes)")
        except Exception as e:
            logger.error(f"Erreur génération master playlist: {e}")
            logger.error(traceback.format_exc())

    def _verify_channel(self, channel: IPTVChannel) -> bool:
        """# On vérifie si une chaîne produit des segments récents"""
        try:
            if not self._is_channel_ready(channel.name):
                return False
            hls_dir = Path(f"/app/hls/{channel.name}")
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
                logger.debug(f"Aucun fichier dans {channel_dir.name}")
                return True
            return True
        except Exception as e:
            logger.error(f"Erreur needs_update {channel_dir}: {e}")
            return True

    def _master_playlist_loop(self):
        while True:
            try:
                self.generate_master_playlist()
                time.sleep(60)
            except Exception as e:
                logger.error(f"Erreur maj master playlist: {e}")

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
            self.scan_channels()
            self.generate_master_playlist()
            self.observer.start()

            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.cleanup()
        except Exception as e:
            logger.error(f"🔥 Erreur manager : {e}")
            self.cleanup()
            
    def _signal_handler(self, signum, frame):
        """# On gère les signaux système"""
        logger.info(f"Signal {signum} reçu, nettoyage...")
        self.cleanup()
        sys.exit(0)

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

    import psutil
    import threading

    def monitor_ffmpeg(self):
        while True:
            if self.ffmpeg_process:
                if not psutil.pid_exists(self.ffmpeg_process.pid):
                    logger.warning(f"[{self.name}] ⚠️ FFmpeg s'est arrêté ! Relance en cours...")
                    self.start_stream()
            time.sleep(30)  # Vérifier toutes les 30 secondes

    def start_stream(self) -> bool:
        with self.lock:
            try:
                logger.info(f"[{self.name}] 🔄 Début du start_stream()")

                # Assurer que le dossier HLS existe
                hls_dir = f"/app/hls/{self.name}"
                if not os.path.exists(hls_dir):
                    logger.info(f"[{self.name}] 📂 Création du dossier HLS : {hls_dir}")
                    os.makedirs(hls_dir, exist_ok=True)

                # Vérifier si _playlist.txt est bien généré
                concat_file = self._create_concat_file()
                if not concat_file or not concat_file.exists():
                    logger.error(f"[{self.name}] ❌ _playlist.txt manquant, arrêt du stream.")
                    return False

                # Vérifier si les fichiers vidéo existent bien
                for video in self.processed_videos:
                    if not os.path.exists(video):
                        logger.error(f"[{self.name}] ❌ Fichier vidéo manquant : {video}")
                        return False

                # Lancer FFmpeg
                cmd = self._build_ffmpeg_command(hls_dir)
                logger.info(f"[{self.name}] 📝 Commande FFmpeg : {' '.join(cmd)}")
                self.ffmpeg_process = self._start_ffmpeg_process(cmd)

                if not self.ffmpeg_process:
                    logger.error(f"[{self.name}] ❌ Échec du démarrage de FFmpeg")
                    return False

                return True
            except Exception as e:
                logger.error(f"[{self.name}] ❌ Erreur critique dans start_stream : {e}")
                self._clean_processes()
                return False

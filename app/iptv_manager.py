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
from queue import Queue, Empty
from pathlib import Path
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
import threading
from event_handler import FileEventHandler, ReadyContentHandler
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
    SEGMENT_AGE_THRESHOLD,
    SUMMARY_CYCLE,
    WATCHERS_LOG_CYCLE,
    CHANNELS_STATUS_FILE,
)
from stats_collector import StatsCollector
from channel_status_manager import ChannelStatusManager
import json
import re
from log_utils import parse_access_log


class IPTVManager:
    """Gestionnaire centralisé des chaînes IPTV"""

    def __init__(self, content_dir: str, use_gpu: bool = False):
        # Assurons-nous que la valeur de USE_GPU est bien prise de l'environnement
        use_gpu_env = os.getenv("USE_GPU", "false").lower() == "true"
        
        # Initialize ready_event_handler first
        try:
            self.ready_event_handler = ReadyContentHandler(self)
            logger.info("✅ ReadyContentHandler initialized")
        except Exception as e:
            logger.error(f"❌ Error initializing ReadyContentHandler: {e}")
            self.ready_event_handler = None
        
        # Configuration
        self.content_dir = content_dir
        self.use_gpu = use_gpu or use_gpu_env
        self.channels = {}
        self.channel_ready_status = {}
        self.log_path = NGINX_ACCESS_LOG
        
        # Initialisation de last_position pour le suivi des logs
        self.last_position = 0
        if os.path.exists(self.log_path):
            with open(self.log_path, "r") as f:
                f.seek(0, 2)
                self.last_position = f.tell()
                logger.info(f"📝 Position initiale de lecture des logs: {self.last_position} bytes")
        
        # Initialisation des verrous et structures de données
        self.lock = threading.Lock()
        self._active_watchers = {}
        self.watchers = {}
        
        # Verrou et cooldown pour les scans
        self.scan_lock = threading.Lock()
        self.last_scan_time = 0
        self.scan_cooldown = 60
        self.scan_queue = Queue()
        self.failing_channels = set()

        # Queue pour les chaînes à initialiser en parallèle
        self.channel_init_queue = Queue()
        self.max_parallel_inits = 5
        self.active_init_threads = 0
        self.init_threads_lock = threading.Lock()

        # Initialize channel status manager first
        self.channel_status = None  # Initialiser à None
        self.init_channel_status_manager()

        # Initialisation des composants
        try:
            self.stats_collector = StatsCollector()
            logger.info("📊 StatsCollector initialisé")
        except Exception as e:
            logger.error(f"❌ Erreur initialisation StatsCollector: {e}")
            self.stats_collector = None

        # Initialize client monitor
        try:
            self.client_monitor = ClientMonitor(
                log_path=self.log_path,
                update_watchers_callback=self.update_watchers,
                manager=self,
                stats_collector=self.stats_collector
            )
            self.client_monitor.start()
            logger.info("👁️ ClientMonitor initialisé et démarré")
        except Exception as e:
            logger.error(f"❌ Erreur initialisation ClientMonitor: {e}")
            self.client_monitor = None

        # Moniteur FFmpeg
        self.ffmpeg_monitor = FFmpegMonitor(self.channels)
        self.ffmpeg_monitor.start()

        # On initialise le nettoyeur HLS avec le bon chemin
        self.hls_cleaner = HLSCleaner("/app/hls")
        # Le nettoyage initial sera fait dans _clean_startup

        # Initialisation des threads
        self.stop_scan_thread = threading.Event()
        self.stop_init_thread = threading.Event()
        self.stop_watchers = threading.Event()
        self.stop_status_update = threading.Event()

        # Thread de surveillance des watchers
        self.watchers_thread = threading.Thread(
            target=self._watchers_loop,
            daemon=True
        )

        # Thread de scan unifié
        self.scan_thread = threading.Thread(
            target=self._scan_worker,
            daemon=True
        )

        # Thread d'initialisation des chaînes
        self.channel_init_thread = threading.Thread(
            target=self._process_channel_init_queue,
            daemon=True
        )

        # Thread de nettoyage des watchers inactifs
        self.cleanup_thread = threading.Thread(
            target=self._cleanup_thread_loop,
            daemon=True
        )

        # Thread de mise à jour des statuts
        self.status_update_thread = threading.Thread(
            target=self._status_update_loop,
            daemon=True
        )

        logger.info("Initialisation du gestionnaire IPTV amélioré")
        self._clean_startup()

        # Observer
        self.observer = Observer()
        event_handler = FileEventHandler(self)
        self.observer.schedule(event_handler, self.content_dir, recursive=True)
        logger.info(f"👁️ Observer configuré pour surveiller {self.content_dir} en mode récursif")

        # Démarrage des threads dans l'ordre correct
        self.scan_thread.start()
        logger.info("🔄 Thread de scan unifié démarré")

        self.channel_init_thread.start()
        logger.info("🔄 Thread d'initialisation des chaînes démarré")

        self.cleanup_thread.start()
        logger.info("🧹 Thread de nettoyage des watchers démarré")

        self.watchers_thread.start()
        logger.info("👥 Thread de surveillance des watchers démarré")

        self.status_update_thread.start()
        logger.info("📊 Thread de mise à jour des statuts démarré")

        # Force un scan initial
        logger.info("🔍 Forçage du scan initial des chaînes...")
        self._do_scan(force=True)
        logger.info("✅ Scan initial terminé")

    def request_scan(self, force: bool = False):
        """Demande un scan en le mettant dans la queue"""
        self.scan_queue.put(force)
        logger.debug("Scan demandé" + (" (forcé)" if force else ""))

    def _scan_worker(self):
        """Thread qui gère les scans de manière centralisée"""
        while not self.stop_scan_thread.is_set():
            try:
                # Attendre une demande de scan ou le délai périodique
                try:
                    force = self.scan_queue.get(timeout=300)  # 5 minutes de délai par défaut
                except Empty:  # Fixed: Using imported Empty instead of Queue.Empty
                    force = False  # Scan périodique normal

                current_time = time.time()
                
                # Vérifier le cooldown sauf si scan forcé
                if not force and (current_time - self.last_scan_time) < self.scan_cooldown:
                    logger.debug("⏭️ Scan ignoré (cooldown)")
                    continue

                with self.scan_lock:
                    logger.info("🔍 Démarrage du scan" + (" forcé" if force else ""))
                    self._do_scan(force)
                    self.last_scan_time = time.time()

            except Exception as e:
                logger.error(f"❌ Erreur dans le thread de scan: {e}")
                time.sleep(5)

    def _do_scan(self, force: bool = False):
        """Effectue le scan réel des chaînes"""
        try:
            content_path = Path(self.content_dir)
            if not content_path.exists():
                logger.error(f"Le dossier {content_path} n'existe pas!")
                return

            # Scan des dossiers de chaînes
            channel_dirs = [d for d in content_path.iterdir() if d.is_dir()]
            logger.info(f"📂 {len(channel_dirs)} dossiers de chaînes trouvés")

            for channel_dir in channel_dirs:
                channel_name = channel_dir.name
                
                if channel_name in self.channels:
                    # Mise à jour des chaînes existantes
                    if force:
                        logger.info(f"🔄 Rafraîchissement de la chaîne {channel_name}")
                        channel = self.channels[channel_name]
                        if hasattr(channel, "_scan_videos"):
                            channel._scan_videos()
                    continue

                # Nouvelle chaîne détectée
                logger.info(f"✅ Nouvelle chaîne trouvée: {channel_name}")
                
                # Si c'est un scan forcé, on initialise immédiatement
                if force:
                    self._init_channel_async({
                        "name": channel_name,
                        "dir": channel_dir,
                        "from_queue": False  # Indique que ce n'est pas de la queue
                    })
                else:
                    # Sinon on met dans la queue pour initialisation différée
                    self.channel_init_queue.put({
                        "name": channel_name,
                        "dir": channel_dir,
                        "from_queue": True  # Indique que c'est de la queue
                    })

            # Mise à jour de la playlist maître
            self._update_master_playlist()

        except Exception as e:
            logger.error(f"Erreur scan des chaînes: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def stop(self):
        """Arrête proprement le gestionnaire"""
        logger.info("🛑 Arrêt du gestionnaire IPTV...")
        self.stop_scan_thread.set()
        self.stop_init_thread.set()
        
        if hasattr(self, 'scan_thread'):
            self.scan_thread.join(timeout=5)
            
        if hasattr(self, 'channel_init_thread'):
            self.channel_init_thread.join(timeout=5)

        # Arrêt des autres composants...

    def _get_active_watcher_ips(self, channel_name):
        """Récupère la liste des IPs actives pour une chaîne"""
        with self.lock:
            # Si la chaîne est dans notre dictionnaire, renvoyer les IPs actifs
            if channel_name in self._active_watchers:
                return self._active_watchers[channel_name]
            
            # Sinon, tenter de récupérer depuis client_monitor
            if hasattr(self, 'client_monitor') and hasattr(self.client_monitor, 'watchers'):
                active_ips = set()
                for ip, data in self.client_monitor.watchers.items():
                    if data.get("current_channel") == channel_name:
                        active_ips.add(ip)
                return active_ips
                
            # Aucune information trouvée
            return set()

    def update_watchers(self, channel_name: str, watcher_count: int, path: str = "/hls/"):
        """Met à jour le nombre de watchers pour une chaîne"""
        try:
            # Ignorer les mises à jour si le channel_name ressemble à une IP
            if channel_name and any(c.isdigit() for c in channel_name.split('.')):
                logger.debug(f"⏭️ Ignoré mise à jour watchers pour IP: {channel_name}")
                return

            if channel_name not in self.channels:
                logger.warning(f"⚠️ Tentative de mise à jour des watchers pour une chaîne inexistante: {channel_name}")
                return

            # Mise à jour du compteur de watchers
            channel = self.channels[channel_name]
            old_count = getattr(channel, 'watchers_count', 0)
            
            # Toujours mettre à jour le compteur et le timestamp
            channel.watchers_count = watcher_count
            channel.last_watcher_time = time.time()
            
            # Log seulement si le nombre a changé
            if old_count != watcher_count:
                logger.info(f"[{channel_name}] 👥 Changement watchers: {old_count} → {watcher_count}")

            # Forcer une mise à jour immédiate du statut
            if hasattr(self, "channel_status") and self.channel_status is not None:
                is_active = bool(getattr(channel, "ready_for_streaming", False))
                is_streaming = bool(channel.process_manager.is_running() if hasattr(channel, "process_manager") else False)
                
                # Récupérer la liste des watchers actifs
                active_watchers = self._active_watchers.get(channel_name, set())
                
                # Mise à jour du statut
                self.channel_status.update_channel(
                    channel_name, 
                    is_active=is_active,
                    viewers=watcher_count,
                    streaming=is_streaming,
                    watchers=list(active_watchers)
                )

            # Vérifier si la chaîne est arrêtée mais devrait être active
            if watcher_count > 0 and not channel.process_manager.is_running():
                logger.warning(f"[{channel_name}] ⚠️ Chaîne arrêtée avec {watcher_count} watchers actifs")
                
                if channel.ready_for_streaming:
                    logger.info(f"[{channel_name}] 🔄 Redémarrage automatique de la chaîne")
                    if channel.start_stream():
                        logger.info(f"[{channel_name}] ✅ Chaîne redémarrée avec succès")
                    else:
                        logger.error(f"[{channel_name}] ❌ Échec du redémarrage de la chaîne")
                else:
                    logger.warning(f"[{channel_name}] ⚠️ Chaîne non prête pour le streaming")

            # Mise à jour des statistiques
            if hasattr(self, 'stats_collector') and self.stats_collector and watcher_count > 0:
                # Mise à jour du temps de visionnage pour chaque IP active
                if active_watchers:
                    for ip in active_watchers:
                        self.stats_collector.add_watch_time(channel_name, ip, 5.0)
                    self.stats_collector.save_stats()
                    self.stats_collector.save_user_stats()

        except Exception as e:
            logger.error(f"❌ Erreur mise à jour watchers pour {channel_name}: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def _reset_channel_statuses(self):
        """Reset all channel statuses to inactive with zero viewers at startup"""
        if hasattr(self, "channel_status"):
            for name in self.channels:
                self.channel_status.update_channel(
                    name,
                    is_active=False,
                    viewers=0,
                    streaming=False
                )
            logger.info("🔄 All channel statuses reset at startup")

    def _log_channels_summary(self):
        """Génère et affiche un récapitulatif de l'état des chaînes"""
        try:
            if not self.channels:
                logger.info("📊 Aucune chaîne disponible pour le récapitulatif")
                return

            # Organiser les chaînes par état
            active_with_viewers = []  # Chaînes actives avec viewers
            active_without_viewers = []  # Chaînes actives sans viewers
            stopped_channels = []  # Chaînes arrêtées

            for name, channel in sorted(self.channels.items()):
                watchers_count = getattr(channel, "watchers_count", 0)
                is_streaming = (
                    channel.process_manager.is_running()
                    if hasattr(channel, "process_manager")
                    else False
                )

                channel_info = {
                    "name": name,
                    "watchers": watchers_count,
                    "streaming": is_streaming,
                    "last_activity": getattr(channel, "last_watcher_time", 0),
                }

                if is_streaming:  # Si la chaîne est en streaming
                    if watchers_count > 0:  # Et qu'elle a des viewers
                        active_with_viewers.append(channel_info)
                    else:  # En streaming mais sans viewers
                        active_without_viewers.append(channel_info)
                else:  # Chaîne arrêtée
                    stopped_channels.append(channel_info)

            # Construire le récapitulatif
            summary_lines = ["📊 RÉCAPITULATIF DES CHAÎNES:"]

            # Afficher les chaînes actives avec viewers
            if active_with_viewers:
                active_parts = []
                for ch in active_with_viewers:
                    active_parts.append(f"🟢 {ch['name']}: {ch['watchers']} viewers")

                summary_lines.append(
                    "CHAÎNES AVEC VIEWERS: " + " | ".join(active_parts)
                )
            else:
                summary_lines.append("CHAÎNES AVEC VIEWERS: Aucune")

            # Afficher les chaînes actives sans viewers
            if active_without_viewers:
                inactive_parts = []
                for ch in active_without_viewers[
                    :5
                ]:  # Limiter à 5 pour éviter des logs trop longs
                    inactive_parts.append(f"{ch['name']}")

                remaining = len(active_without_viewers) - 5
                if remaining > 0:
                    inactive_parts.append(f"et {remaining} autres")

                summary_lines.append(
                    f"CHAÎNES ACTIVES SANS VIEWERS: {len(active_without_viewers)} ({', '.join(inactive_parts)})"
                )

            # Nombre total de chaînes arrêtées
            if stopped_channels:
                stopped_names = [ch['name'] for ch in stopped_channels]
                summary_lines.append(f"CHAÎNES ARRÊTÉES: {len(stopped_channels)} ({', '.join(stopped_names)})")
            else:
                summary_lines.append("CHAÎNES ARRÊTÉES: Aucune")

            # Stats globales
            total_viewers = sum(ch["watchers"] for ch in active_with_viewers)
            total_streams = len(active_with_viewers) + len(active_without_viewers)
            summary_lines.append(
                f"TOTAL: {total_viewers} viewers sur {total_streams} streams actifs ({len(self.channels)} chaînes)"
            )

            # Afficher le récapitulatif
            logger.info("\n".join(summary_lines))

        except Exception as e:
            logger.error(f"❌ Erreur génération récapitulatif: {e}")

    def _clean_startup(self):
        """Nettoyage initial optimisé"""
        try:
            logger.info("🧹 Nettoyage initial...")
            
            # Nettoyage des dossiers HLS
            self.hls_cleaner.initial_cleanup()
            self.hls_cleaner.start()
            logger.info("✅ Nettoyage initial terminé")
            
        except Exception as e:
            logger.error(f"❌ Erreur lors du nettoyage initial: {e}")

    def scan_channels(self, force: bool = False, initial: bool = False):
        """
        Scanne le contenu pour détecter les nouveaux dossiers (chaînes).
        Version améliorée avec limitation de fréquence et debug pour isolation du problème
        """
        # Limiter la fréquence des scans
        current_time = time.time()
        scan_cooldown = 30  # 30s entre scans complets (sauf si force=True)

        if (
            not force
            and not initial
            and hasattr(self, "last_scan_time")
            and current_time - self.last_scan_time < scan_cooldown
        ):
            logger.debug(
                f"Scan ignoré: dernier scan il y a {current_time - self.last_scan_time:.1f}s"
            )
            return

        setattr(self, "last_scan_time", current_time)

        with self.scan_lock:
            try:
                content_path = Path(self.content_dir)
                if not content_path.exists():
                    logger.error(f"Le dossier {content_path} n'existe pas!")
                    return

                # Debugging: log the directory and its existence
                logger.info(f"🔍 Scanning content directory: {content_path} (exists: {content_path.exists()})")
                
                # Get all subdirectories 
                channel_dirs = [d for d in content_path.iterdir() if d.is_dir()]
                
                # Debug the found directories
                logger.info(f"📂 Found {len(channel_dirs)} potential channel directories: {[d.name for d in channel_dirs]}")

                logger.info(f"📡 Scan des chaînes disponibles...")
                for channel_dir in channel_dirs:
                    channel_name = channel_dir.name
                    
                    # Debug: check if this directory should be a channel
                    logger.debug(f"Examining directory: {channel_name} ({channel_dir})")

                    if channel_name in self.channels:
                        # Si la chaîne existe déjà, on vérifie son état
                        if force:
                            logger.info(
                                f"🔄 Rafraîchissement de la chaîne {channel_name}"
                            )
                            channel = self.channels[channel_name]
                            if hasattr(channel, "refresh_videos"):
                                channel.refresh_videos()
                        else:
                            logger.info(f"✅ Chaîne existante: {channel_name}")
                        continue

                    logger.info(f"✅ Nouvelle chaîne trouvée: {channel_name}")

                    # Ajoute la chaîne à la queue d'initialisation
                    self.channel_init_queue.put(
                        {"name": channel_name, "dir": channel_dir}
                    )

                logger.info(f"📡 Scan terminé, {len(channel_dirs)} chaînes identifiées")

            except Exception as e:
                logger.error(f"Erreur scan des chaînes: {e}")
                import traceback
                logger.error(traceback.format_exc())

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

    def _manage_master_playlist(self):
        """
        Gère la création et mise à jour de la playlist principale.
        Cette méthode tourne en boucle et regénère la playlist toutes les 60s,
        ou peut être appelée explicitement après un changement.
        """
        # Si c'est un appel direct (après mise à jour d'une chaîne), faire une mise à jour unique
        if threading.current_thread() != self.master_playlist_updater:
            try:
                self._update_master_playlist()
                return
            except Exception as e:
                logger.error(f"Erreur mise à jour ponctuelle de la playlist: {e}")
                return

        # Sinon, c'est la boucle normale
        while True:
            try:
                self._update_master_playlist()
                time.sleep(60)  # On attend 60s avant la prochaine mise à jour
            except Exception as e:
                logger.error(f"Erreur maj master playlist: {e}")
                logger.error(traceback.format_exc())
                time.sleep(60)  # On attend même en cas d'erreur

    def _update_master_playlist(self):
        """Effectue la mise à jour de la playlist principale"""
        playlist_path = os.path.abspath("/app/hls/playlist.m3u")
        logger.info(f"🔄 Master playlist maj.: {playlist_path}")

        with open(playlist_path, "w", encoding="utf-8") as f:
            f.write("#EXTM3U\n")

            # Re-vérifie chaque chaîne pour confirmer qu'elle est prête
            with self.scan_lock:
                for name, channel in self.channels.items():
                    # Vérification directe des fichiers
                    ready_dir = Path(channel.video_dir) / "ready_to_stream"
                    has_videos = (
                        list(ready_dir.glob("*.mp4")) if ready_dir.exists() else []
                    )

                    # Mise à jour du statut si nécessaire
                    if has_videos and not self.channel_ready_status.get(name, False):
                        logger.info(
                            f"[{name}] 🔄 Mise à jour auto du statut: chaîne prête (vidéos trouvées)"
                        )
                        self.channel_ready_status[name] = True
                        channel.ready_for_streaming = True
                    elif not has_videos and self.channel_ready_status.get(name, False):
                        logger.info(
                            f"[{name}] ⚠️ Mise à jour auto du statut: chaîne non prête (aucune vidéo)"
                        )
                        self.channel_ready_status[name] = False
                        channel.ready_for_streaming = False

            # Ne référence que les chaînes prêtes
            ready_channels = []
            for name, channel in sorted(self.channels.items()):
                if (
                    name in self.channel_ready_status
                    and self.channel_ready_status[name]
                ):
                    ready_channels.append((name, channel))

            # Écriture des chaînes prêtes
            for name, channel in ready_channels:
                f.write(f'#EXTINF:-1 tvg-id="{name}" tvg-name="{name}",{name}\n')
                f.write(f"http://{SERVER_URL}/hls/{name}/playlist.m3u8\n")

        logger.info(
            f"Playlist mise à jour ({len(ready_channels)} chaînes prêtes sur {len(self.channels)} totales)"
        )

    def cleanup_manager(self):
        """Cleanup everything before shutdown"""
        logger.info("Début du nettoyage...")
        
        # Stop all threads first
        self.stop_scan_thread.set()
        self.stop_init_thread.set()
        self.stop_watchers.set()
        self.stop_status_update.set()

        # Stop components that might be None
        try:
            if self.channel_status is not None:
                self.channel_status.stop()
                logger.info("✅ Channel status manager stopped")
        except:
            pass

        try:
            if self.stats_collector is not None:
                self.stats_collector.stop()
                logger.info("📊 StatsCollector arrêté")
        except:
            pass

        try:
            if self.hls_cleaner is not None:
                self.hls_cleaner.stop_cleaner()
                logger.info("🧹 HLS Cleaner arrêté")
        except:
            pass

        # Join threads with timeout
        threads_to_join = [
            (self.channel_init_thread, "Channel init thread"),
            (self.scan_thread, "Scan thread"),
            (self.watchers_thread, "Watchers thread"),
            (self.cleanup_thread, "Cleanup thread"),
            (self.status_update_thread, "Status update thread")
        ]

        for thread, name in threads_to_join:
            try:
                if thread and thread.is_alive():
                    thread.join(timeout=5)
                    logger.info(f"✅ {name} stopped")
            except:
                pass

        # Stop observers
        try:
            if hasattr(self, "observer"):
                self.observer.stop()
                self.observer.join(timeout=5)
                logger.info("✅ Main observer stopped")
        except:
            pass

        try:
            if hasattr(self, "ready_observer"):
                self.ready_observer.stop()
                self.ready_observer.join(timeout=5)
                logger.info("✅ Ready observer stopped")
        except:
            pass

        # Clean up channels
        for name, channel in self.channels.items():
            try:
                channel._clean_processes()
                logger.info(f"✅ Channel {name} cleaned up")
            except:
                pass

        logger.info("✅ Nettoyage terminé")

    def _setup_ready_observer(self):
        """Configure l'observateur pour les dossiers ready_to_stream de chaque chaîne"""
        try:
            # D'abord, arrêter et recréer l'observateur si nécessaire pour éviter les doublons
            if hasattr(self, "ready_observer") and self.ready_observer.is_alive():
                self.ready_observer.stop()
                self.ready_observer.join(timeout=5)

            self.ready_observer = Observer()

            # Pour chaque chaîne existante
            paths_scheduled = set()  # Pour éviter les doublons

            for name, channel in self.channels.items():
                ready_dir = Path(channel.video_dir) / "ready_to_stream"
                ready_dir.mkdir(
                    parents=True, exist_ok=True
                )  # S'assurer que le dossier existe

                if ready_dir.exists() and str(ready_dir) not in paths_scheduled:
                    self.ready_observer.schedule(
                        self.ready_event_handler, str(ready_dir), recursive=False
                    )
                    paths_scheduled.add(str(ready_dir))
                    logger.debug(
                        f"👁️ Surveillance ready_to_stream configurée pour {name}: {ready_dir}"
                    )

            # Démarrage de l'observateur
            self.ready_observer.start()
            logger.info(
                f"🚀 Observateur ready_to_stream démarré pour {len(paths_scheduled)} chemins"
            )

        except Exception as e:
            logger.error(f"❌ Erreur configuration surveillance ready_to_stream: {e}")
            import traceback

            logger.error(traceback.format_exc())

    def _parse_access_log(self, line: str) -> tuple:
        """Parse une ligne de log nginx en utilisant la fonction utilitaire 
        
        Retourne: (ip, channel, request_type, is_valid, path)
        """
        return parse_access_log(line)

    def process_iptv_log_lines(self):
        logger.debug(f"[IPTV_MANAGER] 🔄 Début process_iptv_log_lines")

        try:
            # Vérifier si on doit logger (au moins 2 secondes entre les logs)
            current_time = time.time()
            if hasattr(self, "last_log_time") and current_time - self.last_log_time < 2.0:
                return True

            # Lecture des nouvelles lignes
            with open(self.log_path, "r") as f:
                f.seek(self.last_position)
                new_lines = f.readlines()

                # Mise à jour de la position
                self.last_position = f.tell()

                # Traitement des nouvelles lignes
                channel_updates = {}  # {channel_name: count}
                for line in new_lines:
                    if not line.strip():
                        continue

                    # Traiter la ligne
                    ip, channel, request_type, is_valid, _ = self._parse_access_log(
                        line
                    )

                    if is_valid and channel:
                        # On met à jour le timestamp pour ce watcher
                        current_time = time.time()
                        self.watchers[(channel, ip)] = current_time

                        # Regrouper par chaîne pour ne faire qu'une mise à jour
                        if channel not in channel_updates:
                            channel_updates[channel] = set()
                        channel_updates[channel].add(ip)

                # Mise à jour groupée par chaîne
                for channel, ips in channel_updates.items():
                    count = len(ips)
                    # Log uniquement si le nombre a changé
                    if not hasattr(self, "last_watcher_counts"):
                        self.last_watcher_counts = {}
                    
                    if channel not in self.last_watcher_counts or self.last_watcher_counts[channel] != count:
                        logger.info(
                            f"[{channel}] 👁️ MAJ watchers: {count} actifs - {list(ips)}"
                        )
                        self.last_watcher_counts[channel] = count
                        self.update_watchers(channel, count, "/hls/")

                self.last_log_time = current_time
                return True

        except Exception as e:
            logger.error(f"❌ Erreur traitement nouvelles lignes: {e}")
            import traceback

            logger.error(traceback.format_exc())
            return False

    def _monitor_nginx_logs(self):
        """Surveillance des logs nginx en temps réel avec watchdog"""
        try:
            from watchdog.observers import Observer
            from watchdog.events import FileSystemEventHandler

            class LogHandler(FileSystemEventHandler):
                def __init__(self, manager):
                    self.manager = manager
                    self.last_position = 0
                    self._init_position()

                def _init_position(self):
                    """Initialise la position de lecture"""
                    if os.path.exists(self.manager.log_path):
                        with open(self.manager.log_path, "r") as f:
                            f.seek(0, 2)  # Positionnement à la fin
                            self.last_position = f.tell()

                def on_modified(self, event):
                    if event.src_path == self.manager.log_path:
                        self.manager.process_iptv_log_lines()

            # Initialiser l'observer
            observer = Observer()
            observer.schedule(LogHandler(self), os.path.dirname(self.log_path), recursive=False)
            observer.start()
            logger.info(f"🔍 Surveillance des logs nginx démarrée: {self.log_path}")

            # Boucle de surveillance
            while True:
                time.sleep(1)

        except Exception as e:
            logger.error(f"❌ Erreur surveillance logs: {e}")
            # En cas d'erreur, on attend un peu avant de réessayer
            time.sleep(5)

    def _cleanup_inactive_watchers(self):
        """Nettoie les watchers inactifs du IPTVManager"""
        current_time = time.time()
        inactive_watchers = []

        # Identifier les watchers inactifs
        with self.lock:
            for (channel, ip), last_seen_time in self.watchers.items():
                # Si pas d'activité depuis plus de 60 secondes
                if current_time - last_seen_time > 60:  # 1 minute d'inactivité
                    inactive_watchers.append((channel, ip))
                    logger.debug(f"⏱️ Manager: Watcher {ip} inactif depuis {current_time - last_seen_time:.1f}s sur {channel}")

            # Supprimer les watchers inactifs et mettre à jour les chaînes affectées
            channels_to_update = set()
            for (channel, ip) in inactive_watchers:
                # Supprimer des watchers
                if (channel, ip) in self.watchers:
                    del self.watchers[(channel, ip)]
                
                # Supprimer de _active_watchers
                if channel in self._active_watchers and ip in self._active_watchers[channel]:
                    self._active_watchers[channel].remove(ip)
                    channels_to_update.add(channel)
                    logger.info(f"🧹 Manager: Suppression du watcher inactif: {ip} sur {channel}")

            # Mettre à jour les compteurs de watchers pour les chaînes affectées
            for channel in channels_to_update:
                if channel in self.channels:
                    watcher_count = len(self._active_watchers.get(channel, set()))
                    self.channels[channel].watchers_count = watcher_count
                    self.channels[channel].last_watcher_time = current_time
                    logger.info(f"[{channel}] 👁️ Manager: Mise à jour après nettoyage: {watcher_count} watchers actifs")

    def _cleanup_thread_loop(self):
        """Thread de nettoyage périodique"""
        logger.info("🧹 Démarrage de la boucle de nettoyage")
        
        while not self.stop_watchers.is_set():
            try:
                # Nettoyage des watchers inactifs
                self._cleanup_inactive_watchers()
                
                # Vérification des timeouts des chaînes
                self._check_channels_timeout()
                
                # Pauser entre les nettoyages
                time.sleep(30)
                
            except Exception as e:
                logger.error(f"❌ Erreur dans la boucle de nettoyage: {e}")
                import traceback
                logger.error(traceback.format_exc())
                time.sleep(10)  # Pause en cas d'erreur

    def _update_channel_status(self):
        """Update channel status for dashboard"""
        try:
            # Vérifier que le channel_status est bien initialisé
            if self.channel_status is None:
                logger.error("❌ Channel status manager non initialisé, impossible de mettre à jour les statuts")
                return False

            # Get current watchers
            current_watchers = self._get_current_watchers()
            
            # Préparer les données de statut
            status_data = {
                "channels": {},
                "last_updated": int(time.time()),
                "active_viewers": sum(len(watchers) for watchers in current_watchers.values())
            }
            
            # Update each channel status
            for name, channel in self.channels.items():
                watchers = current_watchers.get(name, set())
                viewer_count = len(watchers)
                
                status_data["channels"][name] = {
                    "active": bool(getattr(channel, "ready_for_streaming", False)),
                    "streaming": bool(channel.process_manager.is_running() if hasattr(channel, "process_manager") else False),
                    "viewers": viewer_count,
                    "watchers": list(watchers),
                    "last_update": int(time.time())
                }
            
            # Mise à jour via le channel_status manager
            success = self.channel_status.update_all_channels(status_data["channels"])
            if success:
                logger.debug("✅ Mise à jour des statuts réussie")
            else:
                logger.error("❌ Échec de la mise à jour des statuts")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ Error updating channel status: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def _status_update_loop(self):
        """Background thread to periodically update channel status"""
        update_interval = 1  # Update every second
        logger.info("🔄 Démarrage de la boucle de mise à jour des statuts")
        
        while not self.stop_status_update.is_set():
            try:
                # Vérifier que le channel_status est bien initialisé
                if self.channel_status is None:
                    logger.warning("⚠️ Channel status manager non initialisé, tentative de réinitialisation")
                    self.init_channel_status_manager()
                    time.sleep(1)
                    continue

                # Update channel status
                success = self._update_channel_status()
                if not success:
                    logger.warning("⚠️ Échec de la mise à jour des statuts, nouvelle tentative dans 1s")
                
                # Sleep until next update
                time.sleep(update_interval)
            except Exception as e:
                logger.error(f"❌ Error in status update loop: {e}")
                import traceback
                logger.error(traceback.format_exc())
                time.sleep(1)  # Reduced from 10s to 1s on error

    def init_channel_status_manager(self):
        """Initialize the channel status manager for dashboard"""
        try:
            # Si déjà initialisé, ne pas réinitialiser
            if self.channel_status is not None:
                logger.info("ℹ️ Channel status manager déjà initialisé, skip")
                return

            logger.info("🚀 Initialisation du gestionnaire de statuts des chaînes")
            # Créer le dossier stats s'il n'existe pas
            stats_dir = Path(os.path.dirname(CHANNELS_STATUS_FILE))
            stats_dir.mkdir(parents=True, exist_ok=True)
            os.chmod(stats_dir, 0o777)
            
            # Initialiser le gestionnaire de statuts
            from channel_status_manager import ChannelStatusManager
            self.channel_status = ChannelStatusManager(
                status_file=CHANNELS_STATUS_FILE
            )
            logger.info("✅ Channel status manager initialized for dashboard")
            
            # Faire une mise à jour initiale
            self._update_channel_status()
            
        except Exception as e:
            logger.error(f"❌ Error initializing channel status manager: {e}")
            import traceback
            logger.error(traceback.format_exc())
            self.channel_status = None

    def _check_channels_timeout(self):
        """Vérifie périodiquement les timeouts des chaînes sans watchers"""
        try:
            for channel_name, channel in self.channels.items():
                channel.check_watchers_timeout()
        except Exception as e:
            logger.error(f"❌ Erreur vérification timeouts: {e}")

    def run_manager_loop(self):
        try:
            # Initialize channel status manager
            self.init_channel_status_manager()
            
            # Démarrer la boucle de surveillance des watchers
            if not self.watchers_thread.is_alive():
                self.watchers_thread.start()
                logger.info("🔄 Boucle de surveillance des watchers démarrée")

            # Démarrer la surveillance des logs nginx
            nginx_monitor_thread = threading.Thread(target=self._monitor_nginx_logs, daemon=True)
            nginx_monitor_thread.start()
            logger.info("🔍 Surveillance des logs nginx démarrée")

            # Configurer l'observateur pour ready_to_stream
            self._setup_ready_observer()

            # Démarrer l'observer principal s'il n'est pas déjà en cours
            if not self.observer.is_alive():
                self.observer.start()

            # Attente dynamique pour l'initialisation des chaînes
            max_wait_time = 10  # Réduit de 15 à 10 secondes
            check_interval = 0.5  # Réduit de 1 à 0.5 secondes pour des vérifications plus fréquentes
            start_time = time.time()
            
            logger.info(f"⏳ Attente de l'initialisation des chaînes (max {max_wait_time}s)...")
            
            while time.time() - start_time < max_wait_time:
                # Compter les chaînes prêtes
                ready_channels = sum(1 for name, is_ready in self.channel_ready_status.items() 
                                  if is_ready and name in self.channels 
                                  and self.channels[name].ready_for_streaming)
                
                total_channels = len(self.channels)
                
                if total_channels > 0:
                    ready_percentage = (ready_channels / total_channels) * 100
                    logger.info(f"📊 Progression: {ready_channels}/{total_channels} chaînes prêtes ({ready_percentage:.1f}%)")
                    
                    # Réduire le seuil à 50% pour démarrer plus tôt
                    if ready_percentage >= 50:
                        logger.info("✅ Seuil de chaînes prêtes atteint, continuation...")
                        break
                    
                    # Si on a au moins une chaîne prête et qu'on attend depuis plus de 5 secondes,
                    # on continue quand même
                    elif ready_channels > 0 and (time.time() - start_time) > 5:
                        logger.info(f"✅ Continuation avec {ready_channels} chaînes prêtes après 5s d'attente")
                        break
                
                time.sleep(check_interval)
            
            # Démarrage automatique des chaînes prêtes
            self.auto_start_ready_channels()

            # Boucle principale avec vérification des timeouts
            while True:
                self._check_channels_timeout()
                time.sleep(60)  # Vérification toutes les minutes

        except KeyboardInterrupt:
            self.cleanup_manager()
        except Exception as e:
            logger.error(f"🔥 Erreur manager : {e}")
            self.cleanup_manager()

    def _periodic_scan(self):
        """Effectue un scan périodique des chaînes pour détecter les changements"""
        while not self.stop_periodic_scan.is_set():
            try:
                logger.debug(f"🔄 Scan périodique des chaînes en cours...")
                self.scan_channels(force=True)
                
                # NOUVEAU: Resynchronisation périodique des playlists
                self._resync_all_playlists()
                
                # Attente jusqu'au prochain scan
                self.stop_periodic_scan.wait(self.periodic_scan_interval)
            except Exception as e:
                logger.error(f"❌ Erreur scan périodique: {e}")
                self.stop_periodic_scan.wait(60)  # En cas d'erreur, on attend 1 minute

    # NOUVELLE MÉTHODE: resynchronisation des playlists
    def _resync_all_playlists(self):
        """Force la resynchronisation des playlists pour toutes les chaînes"""
        try:
            resync_count = 0
            for channel_name, channel in self.channels.items():
                # Vérifier si la chaîne a une méthode de création de playlist
                if hasattr(channel, "_create_concat_file"):
                    # On ne recréé pas toutes les playlists à chaque fois, on alterne
                    # 1/4 des chaînes à chaque cycle pour ne pas surcharger le système
                    if random.randint(1, 4) == 1:  
                        logger.debug(f"[{channel_name}] 🔄 Resynchronisation périodique de la playlist")
                        
                        # Créer un thread dédié pour resynchroniser et redémarrer si nécessaire
                        def resync_and_restart(ch_name, ch):
                            try:
                                # 1. Vérifier l'état actuel de la playlist
                                playlist_path = Path(ch.video_dir) / "_playlist.txt"
                                old_content = ""
                                if playlist_path.exists():
                                    with open(playlist_path, "r", encoding="utf-8") as f:
                                        old_content = f.read()
                                
                                # 2. Mettre à jour la playlist
                                ch._create_concat_file()
                                
                                # 3. Vérifier si la playlist a changé
                                new_content = ""
                                if playlist_path.exists():
                                    with open(playlist_path, "r", encoding="utf-8") as f:
                                        new_content = f.read()
                                
                                # 4. Redémarrer seulement si le contenu a changé
                                if old_content != new_content:
                                    logger.info(f"[{ch_name}] 🔄 Playlist modifiée, redémarrage du stream nécessaire")
                                    if hasattr(ch, "_restart_stream") and ch.process_manager.is_running():
                                        logger.info(f"[{ch_name}] 🔄 Redémarrage du stream après mise à jour périodique de la playlist")
                                        ch._restart_stream()
                                else:
                                    logger.debug(f"[{ch_name}] ✓ Playlist inchangée, pas de redémarrage nécessaire")
                            except Exception as e:
                                logger.error(f"[{ch_name}] ❌ Erreur pendant la resynchronisation: {e}")
                        
                        # Lancer le thread de resynchronisation
                        threading.Thread(
                            target=resync_and_restart,
                            args=(channel_name, channel),
                            daemon=True
                        ).start()
                        
                        resync_count += 1
            
            if resync_count > 0:
                logger.info(f"✅ Resynchronisation et redémarrage de {resync_count} chaînes effectués")
                
        except Exception as e:
            logger.error(f"❌ Erreur resynchronisation playlists: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def stop(self):
        """Arrête proprement le gestionnaire IPTV"""
        logger.info("🛑 Arrêt du gestionnaire IPTV...")
        
        # Arrêt du scan périodique
        self.stop_periodic_scan.set()
        if hasattr(self, 'periodic_scan_thread'):
            self.periodic_scan_thread.join(timeout=5)

    def _process_channel_init_queue(self):
        """Thread qui traite la queue d'initialisation des chaînes en parallèle"""
        while not self.stop_init_thread.is_set():
            try:
                # Limite le nombre d'initialisations parallèles
                with self.init_threads_lock:
                    if self.active_init_threads >= self.max_parallel_inits:
                        time.sleep(0.5)
                        continue

                # Essaie de récupérer une chaîne de la queue
                try:
                    channel_data = self.channel_init_queue.get(timeout=5)
                except Empty:
                    time.sleep(0.5)
                    continue

                # Incrémente le compteur de threads actifs
                with self.init_threads_lock:
                    self.active_init_threads += 1

                # Lance un thread pour initialiser cette chaîne
                threading.Thread(
                    target=self._init_channel_async,
                    args=(channel_data,),
                    daemon=True
                ).start()

            except Exception as e:
                logger.error(f"❌ Erreur dans le thread d'initialisation: {e}")
                time.sleep(1)

    def _init_channel_async(self, channel_data):
        """Initialise une chaîne de manière asynchrone"""
        try:
            channel_name = channel_data["name"]
            channel_dir = channel_data["dir"]
            from_queue = channel_data.get("from_queue", True)  # Par défaut, on suppose que c'est de la queue

            logger.info(f"[{channel_name}] 🔄 Initialisation asynchrone de la chaîne")

            # Crée l'objet chaîne
            channel = IPTVChannel(
                channel_name,
                str(channel_dir),
                hls_cleaner=self.hls_cleaner,
                use_gpu=self.use_gpu,
                stats_collector=self.stats_collector,
            )

            # Ajoute la référence au manager
            channel.manager = self

            # Ajoute la chaîne au dictionnaire
            with self.scan_lock:
                self.channels[channel_name] = channel
                self.channel_ready_status[channel_name] = False  # Pas encore prête

            # Attente que la chaîne soit prête (max 10 secondes)
            ready = False
            for _ in range(10):
                if hasattr(channel, "ready_for_streaming") and channel.ready_for_streaming:
                    with self.scan_lock:
                        self.channel_ready_status[channel_name] = True
                    logger.info(f"✅ Chaîne {channel_name} prête pour le streaming")
                    ready = True
                    break
                time.sleep(1)

            if not ready:
                logger.warning(f"⚠️ Timeout d'initialisation pour la chaîne {channel_name}")

            logger.info(f"[{channel_name}] ✅ Initialisation terminée, en attente du démarrage automatique groupé")

        except Exception as e:
            logger.error(f"❌ Erreur initialisation de la chaîne {channel_data.get('name')}: {e}")
        finally:
            # Décrémente le compteur de threads actifs
            with self.init_threads_lock:
                self.active_init_threads -= 1

            # Marque la tâche comme terminée UNIQUEMENT si elle vient de la queue
            if channel_data.get("from_queue", True):
                self.channel_init_queue.task_done()

    def _watchers_loop(self):
        """Thread de surveillance des watchers"""
        while not self.stop_watchers.is_set():
            try:
                # Vérifier l'état des chaînes
                for channel_name, channel in self.channels.items():
                    if not channel.ready_for_streaming:
                        continue

                    # Récupérer le nombre de watchers actifs
                    active_watchers = self._active_watchers.get(channel_name, set())
                    watcher_count = len(active_watchers)
                    
                    # Mettre à jour les stats
                    if hasattr(channel, "watchers_count"):
                        channel.watchers_count = watcher_count
                        
                    # Mettre à jour le timestamp du dernier watcher si on en a
                    if watcher_count > 0:
                        channel.last_watcher_time = time.time()

                # Attendre avant la prochaine vérification
                time.sleep(WATCHERS_LOG_CYCLE)

            except Exception as e:
                logger.error(f"❌ Erreur surveillance watchers: {e}")
                time.sleep(5)  # Pause en cas d'erreur

    def _get_current_watchers(self):
        """Récupère les watchers actifs depuis le client_monitor uniquement"""
        try:
            current_watchers = {}
            
            # Récupérer les watchers depuis le client_monitor uniquement
            if hasattr(self, 'client_monitor') and hasattr(self.client_monitor, 'watchers'):
                current_time = time.time()
                for ip, data in self.client_monitor.watchers.items():
                    # Validation stricte de l'IP
                    try:
                        # Vérifier le format de base
                        if not ip or not re.match(r'^(\d{1,3}\.){3}\d{1,3}$', ip):
                            logger.warning(f"⚠️ Format IP invalide ignoré: {ip}")
                            continue
                            
                        # Vérifier que chaque partie est un nombre valide
                        ip_parts = ip.split('.')
                        if not all(0 <= int(part) <= 255 for part in ip_parts):
                            logger.warning(f"⚠️ Valeurs IP hors limites ignorées: {ip}")
                            continue
                    except ValueError:
                        logger.warning(f"⚠️ IP avec valeurs non numériques ignorée: {ip}")
                        continue
                        
                    channel = data.get("current_channel")
                    last_seen = data.get("last_seen", 0)
                    
                    # Ne considérer que les watchers actifs dans les 30 dernières secondes
                    if channel and (current_time - last_seen) < 30:
                        if channel not in current_watchers:
                            current_watchers[channel] = set()
                        current_watchers[channel].add(ip)
                        logger.debug(f"👁️ Watcher actif depuis client_monitor: {channel} sur {ip}")

            return current_watchers

        except Exception as e:
            logger.error(f"❌ Erreur récupération watchers: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {}

    def auto_start_ready_channels(self):
        """Démarre automatiquement toutes les chaînes prêtes avec un délai entre chaque démarrage"""
        logger.info("🚀 Démarrage automatique des chaînes prêtes...")
        
        # Attendre que plus de chaînes soient prêtes
        for attempt in range(2):  # 2 tentatives maximum
            ready_channels = []
            with self.scan_lock:
                for name, is_ready in self.channel_ready_status.items():
                    if is_ready and name in self.channels:
                        channel = self.channels[name]
                        if channel.ready_for_streaming:
                            ready_channels.append(name)
                            logger.info(f"✅ Chaîne {name} prête pour le démarrage automatique")

            if len(ready_channels) >= len(self.channels) * 0.5:  # Au moins 50% des chaînes sont prêtes
                break

            logger.info(f"⏳ Seulement {len(ready_channels)}/{len(self.channels)} chaînes prêtes, attente supplémentaire ({attempt+1}/2)...")
            time.sleep(5)  # 5 secondes entre les tentatives

        # Trier pour prévisibilité
        ready_channels.sort()
        logger.info(f"📋 Liste des chaînes à démarrer: {ready_channels}")

        # Limiter le CPU pour éviter saturation
        max_parallel = 4
        groups = [ready_channels[i:i + max_parallel] for i in range(0, len(ready_channels), max_parallel)]

        for group_idx, group in enumerate(groups):
            logger.info(f"🚀 Démarrage du groupe {group_idx+1}/{len(groups)} ({len(group)} chaînes)")

            # Démarrer chaque chaîne du groupe avec un petit délai entre elles
            for i, channel_name in enumerate(group):
                delay = i * 0.1  # Réduit de 0.5s à 0.1s entre chaque chaîne
                logger.info(f"[{channel_name}] ⏱️ Démarrage programmé dans {delay} secondes")
                
                # Vérifier que la chaîne est toujours prête avant de la démarrer
                if channel_name in self.channels and self.channels[channel_name].ready_for_streaming:
                    threading.Timer(delay, self._start_channel, args=[channel_name]).start()
                else:
                    logger.warning(f"⚠️ La chaîne {channel_name} n'est plus prête pour le démarrage")

            # Attendre avant le prochain groupe
            if group_idx < len(groups) - 1:
                wait_time = 1  # Réduit de max_parallel à 1 seconde
                logger.info(f"⏳ Attente de {wait_time}s avant le prochain groupe...")
                time.sleep(wait_time)

        if ready_channels:
            logger.info(f"✅ {len(ready_channels)} chaînes programmées pour démarrage automatique")
        else:
            logger.warning("⚠️ Aucune chaîne prête à démarrer")

    def _start_channel(self, channel_name):
        """Démarre une chaîne spécifique"""
        try:
            if channel_name in self.channels:
                channel = self.channels[channel_name]
                if channel.ready_for_streaming:
                    logger.info(f"[{channel_name}] 🚀 Démarrage automatique...")
                    success = channel.start_stream()
                    if success:
                        logger.info(f"[{channel_name}] ✅ Démarrage automatique réussi")
                    else:
                        logger.error(f"[{channel_name}] ❌ Échec du démarrage automatique")
                else:
                    logger.warning(f"[{channel_name}] ⚠️ Non prête pour le streaming, démarrage ignoré")
            else:
                logger.error(f"[{channel_name}] ❌ Chaîne non trouvée dans le dictionnaire")
        except Exception as e:
            logger.error(f"[{channel_name}] ❌ Erreur lors du démarrage automatique: {e}")
            import traceback
            logger.error(traceback.format_exc())

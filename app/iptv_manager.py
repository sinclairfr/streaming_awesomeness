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
from file_event_handler import FileEventHandler
from ready_content_handler import ReadyContentHandler
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
from typing import Optional
from datetime import datetime


class IPTVManager:
    """Gestionnaire centralisé des chaînes IPTV"""

    def __init__(self, content_dir: str, use_gpu: bool = False):
        # Assurons-nous que la valeur de USE_GPU est bien prise de l'environnement
        use_gpu_env = os.getenv("USE_GPU", "false").lower() == "true"
        
        # Configuration
        self.content_dir = content_dir
        self.use_gpu = use_gpu or use_gpu_env
        self.channels = {}
        self.channel_ready_status = {}
        self.log_path = NGINX_ACCESS_LOG
        
        # >>> FIX: Initialize hls_cleaner EARLY <<<
        # On initialise le nettoyeur HLS avec le bon chemin
        self.hls_cleaner = HLSCleaner("/app/hls")
        # Le nettoyage HLS initial sera fait dans _clean_startup
        
        # >>> STEP 1: Clean startup FIRST to reset status file
        logger.info("Initialisation du gestionnaire IPTV amélioré")
        self._clean_startup()
        # <<< END STEP 1
        
        # Initialize ready_event_handler first
        try:
            self.ready_event_handler = ReadyContentHandler(self)
            logger.info("✅ ReadyContentHandler initialized")
        except Exception as e:
            logger.error(f"❌ Error initializing ReadyContentHandler: {e}")
            self.ready_event_handler = None
        
        # Initialize file_event_handler
        try:
            self.file_event_handler = FileEventHandler(self)
            logger.info("✅ FileEventHandler initialized")
        except Exception as e:
            logger.error(f"❌ Error initializing FileEventHandler: {e}")
            self.file_event_handler = None
        
        # Initialisation de last_position pour le suivi des logs
        self.last_position = 0
        if os.path.exists(self.log_path):
            with open(self.log_path, "r") as f:
                f.seek(0, 2)
                self.last_position = f.tell()
                logger.info(f"📝 Position initiale de lecture des logs: {self.last_position} bytes")
        

        
        # Verrou et cooldown pour les scans
        self.scan_lock = threading.RLock()
        self.scan_cooldown = 60
        self.last_scan_time = 0
        self.scan_queue = Queue()
        self.failing_channels = set()

        # Queue pour les chaînes à initialiser en parallèle
        self.channel_init_queue = Queue()
        self.max_parallel_inits = 15
        self.active_init_threads = 0
        self.init_threads_lock = threading.RLock()

        # Timeout d'inactivité pour arrêter un stream (en secondes)
        self.channel_inactivity_timeout = 300 # 5 minutes
        self.last_inactivity_check = 0

        # >>> STEP 2: Initialize channel status manager AFTER cleaning
        self.channel_status = None  # Initialiser à None
        # >>> FIX: Call init_channel_status_manager method <<<
        self.init_channel_status_manager() 
        # <<< END STEP 2

        # MODIFIÉ: StatsCollector et ClientMonitor seront initialisés après que les chaînes soient prêtes
        try:
            # Initialiser un StatsCollector vide immédiatement pour éviter les erreurs d'attribut manquant
            self.stats_collector = StatsCollector()
            # Passer le callback update_watchers même avant l'initialisation complète
            self.stats_collector.update_watchers_callback = self.update_watchers
            logger.info("📊 StatsCollector initialisé avec succès dès le départ")
        except Exception as e:
            logger.error(f"❌ Erreur lors de l'initialisation initiale de StatsCollector: {e}")
            # Créer un objet vide pour éviter les erreurs None
            from types import SimpleNamespace
            self.stats_collector = SimpleNamespace()
            self.stats_collector.update_watchers_callback = lambda *args, **kwargs: None  # Fonction vide
            
        self.client_monitor = None

        # Moniteur FFmpeg
        self.ffmpeg_monitor = FFmpegMonitor(self.channels)
        self.ffmpeg_monitor.start()

        # On initialise le nettoyeur HLS avec le bon chemin
        # HLS Cleaner is already initialized above

        # Initialisation des threads
        self.stop_scan_thread = threading.Event()
        self.stop_init_thread = threading.Event()
        self.stop_periodic_scan = threading.Event()  # Pour arrêter le scan périodique
        self.periodic_scan_interval = 300  # 5 minutes entre chaque scan périodique

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

        # Le thread de nettoyage a été supprimé car le nettoyage est basé sur les logs via ClientMonitor
        # self.cleanup_thread.start()
        # logger.info("🧹 Thread de nettoyage des watchers démarré")

        # Force un scan initial
        logger.info("🔍 Forçage du scan initial des chaînes...")
        self._do_scan(force=True)
        logger.info("✅ Scan initial terminé")

        # >>> STEP 3: REMOVE second _clean_startup call
        # _clean_startup called only once at the beginning now
        # self._clean_startup()
        # <<< END STEP 3

        last_scan_time = time.time()
        last_summary_time = time.time()
        last_resync_time = time.time()
        last_log_watchers_time = time.time()
        last_process_logs_time = time.time()  # Pour traiter les logs régulièrement

    def request_scan(self, force: bool = False):
        """Demande un scan en le mettant dans la queue"""
        self.scan_queue.put(force)
        logger.debug("Scan demandé" + (" (forcé)" if force else ""))

    def _scan_worker(self):
        """Thread qui gère les scans de manière centralisée"""
        logger.info("🔍 Démarrage du thread de scan worker")
        while not self.stop_scan_thread.is_set():
            try:
                # Attendre une demande de scan ou le délai périodique
                try:
                    force = self.scan_queue.get(timeout=30)  # 30 secondes de délai par défaut
                except Empty:  # Fixed: Using imported Empty instead of Queue.Empty
                    force = False  # Scan périodique normal

                current_time = time.time()
                
                # Vérifier le cooldown sauf si scan forcé
                if not force and (current_time - self.last_scan_time) < 15:
                    logger.debug("⏭️ Scan ignoré (cooldown)")
                    continue

                with self.scan_lock:
                    logger.info("🔍 Démarrage du scan" + (" forcé" if force else ""))
                    self._do_scan(force)
                    self.last_scan_time = time.time()

                # --- Vérification périodique des chaînes inactives ---
                if current_time - self.last_inactivity_check > 60: # Vérifier toutes les 60 secondes
                    self._check_inactive_channels()
                    self.last_inactivity_check = current_time
                # -----------------------------------------------------

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

            # Ensure content directory has proper permissions
            try:
                os.chmod(content_path, 0o777)
                logger.debug(f"📂 Permissions 777 appliquées au dossier de contenu lors du scan: {content_path}")
            except Exception as chmod_err:
                logger.warning(f"⚠️ Impossible de modifier les permissions lors du scan: {chmod_err}")

            # Scan des dossiers de chaînes
            channel_dirs = [d for d in content_path.iterdir() if d.is_dir()]
            logger.info(f"📂 {len(channel_dirs)} dossiers de chaînes trouvés: {[d.name for d in channel_dirs]}")
            
            # Pour suivre les nouvelles chaînes détectées cette fois-ci
            found_in_this_scan = set()

            for channel_dir in channel_dirs:
                # Ensure each channel directory has proper permissions
                try:
                    os.chmod(channel_dir, 0o777)
                    logger.debug(f"📂 Permissions 777 appliquées au dossier de chaîne: {channel_dir}")
                    
                    # Also set permissions for ready_to_stream and processed directories
                    ready_dir = channel_dir / "ready_to_stream"
                    if ready_dir.exists():
                        os.chmod(ready_dir, 0o777)
                        logger.debug(f"📂 Permissions 777 appliquées à {ready_dir}")
                        
                    processed_dir = channel_dir / "processed"
                    if processed_dir.exists():
                        os.chmod(processed_dir, 0o777)
                        logger.debug(f"📂 Permissions 777 appliquées à {processed_dir}")
                except Exception as chmod_err:
                    logger.warning(f"⚠️ Impossible de modifier les permissions du dossier {channel_dir}: {chmod_err}")
                
                channel_name = channel_dir.name
                found_in_this_scan.add(channel_name)
                
                # Check if channel is already known or being initialized
                with self.scan_lock:
                    if channel_name in self.channels:
                        # Existing channel - maybe log refresh if forced?
                        if force:
                             logger.info(f"🔄 Scan forcé : Chaîne existante {channel_name} - rafraîchissement éventuel géré par la chaîne elle-même.")
                        continue # Skip adding to init queue if already known
                    else:
                         # Add placeholder ONLY if not already present
                         self.channels[channel_name] = None
                         logger.debug(f"[{channel_name}] Added placeholder to self.channels.")

                # Nouvelle chaîne détectée (ou placeholder ajouté)
                logger.info(f"✅ Nouvelle chaîne détectée (ou placeholder ajouté): {channel_name}")
                
                # ALWAYS use the queue to respect parallel limits
                logger.info(f"⏳ Mise en file d'attente pour initialisation de la chaîne {channel_name}")
                self.channel_init_queue.put({
                    "name": channel_name,
                    "dir": channel_dir,
                    "from_queue": True  # Marquer comme venant de la queue
                })

            # --- Remove channels that exist in self.channels but were not found in this scan --- 
            # Careful with race conditions if init is slow
            # with self.scan_lock:
            #     current_known_channels = set(self.channels.keys())
            #     removed_channels = current_known_channels - found_in_this_scan
            #     for removed_name in removed_channels:
            #         logger.info(f"🗑️ Chaîne {removed_name} non trouvée dans le scan, suppression...")
            #         channel_obj = self.channels.pop(removed_name, None)
            #         if channel_obj and hasattr(channel_obj, 'stop_stream_if_needed'):
            #             channel_obj.stop_stream_if_needed() # Try to stop if object exists
            #         # Also remove from channel_status?
            #         if self.channel_status:
            #             self.channel_status.remove_channel(removed_name)
            # ----------------------------------------------------------------------------------

            # Mise à jour de la playlist maître (peut être appelée trop tôt, mais OK)
            self._update_master_playlist()
            
            # Remove the delayed_start_streams logic as _init_channel_async handles starting
                
        except Exception as e:
            logger.error(f"❌ Erreur scan des chaînes: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def stop(self):
        """Arrête proprement le gestionnaire IPTV"""
        logger.info("🛑 Arrêt du gestionnaire IPTV...")
        
        # Utiliser la méthode complète de nettoyage pour aussi vider les viewers
        self.cleanup_manager()
        
        logger.info("🛑 Gestionnaire IPTV arrêté avec succès")

    def update_watchers(self, channel_name: str, watcher_count: int, active_ips_list: list, path: str = "/hls/", source: str = 'unknown'):
        """
        Met à jour le statut d'une chaîne (nombre de watchers, IPs) suite à une notification
        du ClientMonitor (basé sur les logs Nginx).
        Met directement à jour le ChannelStatusManager.
        
        Args:
            channel_name: Nom de la chaîne.
            watcher_count: Nombre actuel de watchers pour cette chaîne.
            active_ips_list: Liste des IPs des watchers actifs.
            path: (Ignoré, conservé pour compatibilité potentielle)
            source: Source de la mise à jour: 'nginx_log', 'nginx_log_immediate', etc.
        """
        # Ignorer la playlist maître
        if channel_name == "master_playlist":
            return True

        # Vérifier si le ChannelStatusManager est disponible
        if not hasattr(self, "channel_status") or self.channel_status is None:
            # Log une seule fois pour éviter le spam
            if not getattr(self, "_logged_csm_missing", False):
                logger.warning("⚠️ ChannelStatusManager non disponible dans update_watchers. Impossible de mettre à jour le statut.")
                self._logged_csm_missing = True
            return False
        else:
            # Réinitialiser le flag de log si CSM est à nouveau disponible
            self._logged_csm_missing = False

        try:
            # Forcer une mise à jour MÊME si les viewers n'ont pas changé
            # C'est crucial pour maintenir les canaux actifs à jour
            
            # Log différent selon que c'est une mise à jour immédiate ou périodique
            if source == 'nginx_log_immediate':
                logger.info(f"[{channel_name}] 🔄 Mise à jour IMMÉDIATE: {watcher_count} watchers")
            else:
                logger.debug(f"[{channel_name}] Callback update_watchers: {watcher_count} watchers (Source: {source})")
            
            # Préparer les données pour ChannelStatusManager
            # On suppose que si ClientMonitor notifie, la chaîne est considérée 'live'
            # ou du moins potentiellement active si des watchers sont présents.
            channel_obj = self.channels.get(channel_name)
            is_live_status = False
            if channel_obj:
                is_live_status = (hasattr(channel_obj, 'is_streaming') and channel_obj.is_streaming()) or \
                              (hasattr(channel_obj, 'is_ready') and channel_obj.is_ready())
            
            # Pour les canaux avec des watchers actifs, toujours les marquer comme actifs
            # même si le canal n'est pas techniquement "streaming"
            if watcher_count > 0:
                is_live_status = True
            
            status_data = {
                "is_live": is_live_status,
                "viewers": watcher_count,
                "watchers": active_ips_list,
                "last_updated": datetime.now().isoformat()
            }
            
            # Si c'est une mise à jour immédiate ou si un spectateur est présent, forcer un save sans debounce
            force_immediate_save = (source == 'nginx_log_immediate') or (watcher_count > 0)
            
            # Mettre à jour directement le ChannelStatusManager
            success = self.channel_status.update_channel(
                channel_name, 
                status_data,
                force_save=force_immediate_save
            )
            
            if success:
                if source == 'nginx_log_immediate':
                    logger.info(f"[{channel_name}] ✅ Mise à jour IMMÉDIATE réussie dans ChannelStatusManager")
                else:
                    logger.debug(f"[{channel_name}] ✅ Statut mis à jour dans ChannelStatusManager (Viewers: {watcher_count})")
                return True
            else:
                logger.warning(f"[{channel_name}] ⚠️ Échec de la mise à jour du statut via ChannelStatusManager.")
                return False
                
        except Exception as e:
            logger.error(f"[{channel_name}] ❌ Erreur dans update_watchers: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def _log_channels_summary(self):
        """Génère et affiche un récapitulatif de l'état des chaînes basé sur ChannelStatusManager"""
        try:
            # Utiliser self.channel_status comme source de vérité
            if not hasattr(self, 'channel_status') or not self.channel_status:
                logger.info("📊 ChannelStatusManager non disponible pour le récapitulatif")
                return

            # Lire l'état actuel des chaînes depuis ChannelStatusManager
            # Accès direct, le manager interne gère les verrous pour les écritures
            current_statuses = self.channel_status.channels
            manager_known_channels = list(self.channels.keys()) # Chaînes connues par IPTVManager

            if not manager_known_channels:
                logger.info("📊 Aucune chaîne gérée par IPTVManager pour le récapitulatif")
                return

            # Organiser les chaînes par état basé sur current_statuses
            live_with_viewers = []
            live_without_viewers = []
            not_live_channels = []

            for name in sorted(manager_known_channels):
                status = current_statuses.get(name, {}) # Obtenir le statut ou un dict vide
                is_live = status.get('is_live', False)
                viewers_count = status.get('viewers', 0)
                watchers_list = status.get('watchers', [])

                channel_info = {
                    "name": name,
                    "viewers": viewers_count,
                    "is_live": is_live,
                    "watchers": watchers_list
                }

                if is_live:
                    if viewers_count > 0:
                        live_with_viewers.append(channel_info)
                    else:
                        live_without_viewers.append(channel_info)
                else:
                    # Inclut les chaînes explicitement non live et celles inconnues du status manager
                    not_live_channels.append(channel_info)

            # Construire le récapitulatif
            summary_lines = ["📊 RÉCAPITULATIF DES CHAÎNES (via ChannelStatusManager):"]

            # Afficher les chaînes live avec viewers
            if live_with_viewers:
                active_parts = []
                for ch in live_with_viewers:
                    active_parts.append(f"🟢 {ch['name']}: {ch['viewers']} viewers")
                summary_lines.append(
                    "CHAÎNES LIVE AVEC VIEWERS: " + " | ".join(active_parts)
                )
            else:
                summary_lines.append("CHAÎNES LIVE AVEC VIEWERS: Aucune")

            # Afficher les chaînes live sans viewers
            if live_without_viewers:
                inactive_parts = []
                for ch in live_without_viewers[:10]: # Limiter pour éviter logs trop longs
                    inactive_parts.append(f"{ch['name']}")
                remaining = len(live_without_viewers) - 10
                if remaining > 0:
                    inactive_parts.append(f"et {remaining} autres")
                summary_lines.append(
                    f"CHAÎNES LIVE SANS VIEWERS: {len(live_without_viewers)} ({', '.join(inactive_parts)})"
                )
            else:
                summary_lines.append("CHAÎNES LIVE SANS VIEWERS: Aucune")

            # Afficher les chaînes non-live ou inconnues
            if not_live_channels:
                stopped_names = [ch['name'] for ch in not_live_channels[:10]] # Limiter aussi
                remaining = len(not_live_channels) - 10
                if remaining > 0:
                    stopped_names.append(f"et {remaining} autres")
                summary_lines.append(f"CHAÎNES NON-LIVE / INCONNUES: {len(not_live_channels)} ({', '.join(stopped_names)})")
            else:
                summary_lines.append("CHAÎNES NON-LIVE / INCONNUES: Aucune")

            # Stats globales basées sur ChannelStatusManager
            total_viewers = sum(ch["viewers"] for ch in live_with_viewers)
            total_live_streams = len(live_with_viewers) + len(live_without_viewers)
            total_known_channels = len(manager_known_channels)
            summary_lines.append(
                f"TOTAL: {total_viewers} viewers sur {total_live_streams} streams live ({total_known_channels} chaînes gérées)"
            )

            # Afficher le récapitulatif
            logger.info("\n".join(summary_lines))

        except Exception as e:
            logger.error(f"❌ Erreur génération récapitulatif: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def _clean_startup(self):
        """Nettoyage initial optimisé"""
        try:
            logger.info("🧹 Nettoyage initial... (appel unique au démarrage)")

            # Ensure content directory has proper permissions
            logger.info(f"📂 Vérification des permissions du dossier de contenu: {self.content_dir}")
            content_path = Path(self.content_dir)
            if not content_path.exists():
                content_path.mkdir(parents=True, exist_ok=True)
                logger.info(f"📂 Création du dossier de contenu: {content_path}")
            
            try:
                os.chmod(content_path, 0o777)
                logger.info(f"📂 Permissions 777 appliquées au dossier de contenu: {content_path}")
                
                # Ensure channel directories have proper permissions too
                for channel_dir in content_path.iterdir():
                    if channel_dir.is_dir():
                        os.chmod(channel_dir, 0o777)
                        logger.debug(f"📂 Permissions 777 appliquées à {channel_dir}")
                        
                        # Also set permissions for ready_to_stream and processed directories
                        ready_dir = channel_dir / "ready_to_stream"
                        if ready_dir.exists():
                            os.chmod(ready_dir, 0o777)
                            logger.debug(f"📂 Permissions 777 appliquées à {ready_dir}")
                            
                        processed_dir = channel_dir / "processed"
                        if processed_dir.exists():
                            os.chmod(processed_dir, 0o777)
                            logger.debug(f"📂 Permissions 777 appliquées à {processed_dir}")
            except Exception as chmod_err:
                logger.warning(f"⚠️ Impossible de modifier les permissions du dossier de contenu: {chmod_err}")

            # Force reset of channel status file at startup
            logger.info("🔄 Réinitialisation du fichier de statut des chaînes...")
            stats_dir = Path(os.path.dirname(CHANNELS_STATUS_FILE))
            stats_dir.mkdir(parents=True, exist_ok=True)
            try:
                os.chmod(stats_dir, 0o777)
            except Exception as chmod_err:
                logger.warning(f"Could not chmod stats dir {stats_dir}: {chmod_err}")

            # Create a fresh status file with zero viewers
            with open(CHANNELS_STATUS_FILE, 'w') as f:
                json.dump({
                    'channels': {},
                    'last_updated': int(time.time()),
                    'active_viewers': 0
                }, f, indent=2)
            try:
                os.chmod(CHANNELS_STATUS_FILE, 0o666)
            except Exception as chmod_err:
                logger.warning(f"Could not chmod status file {CHANNELS_STATUS_FILE}: {chmod_err}")

            logger.info("✅ Fichier de statut réinitialisé")

            # >>> STEP 4: REMOVE call to _reset_channel_statuses
            # Reset is done by overwriting the file above. Updates will come via push.
            # if hasattr(self, "channel_status") and self.channel_status:
            #    self._reset_channel_statuses()
            # <<< END STEP 4

            # Nettoyage des dossiers HLS
            self.hls_cleaner.initial_cleanup()

            # Start the cleaner thread only if it's not already alive
            if hasattr(self.hls_cleaner, 'cleanup_thread') and not self.hls_cleaner.cleanup_thread.is_alive():
                self.hls_cleaner.start()
                logger.info("✅ HLSCleaner thread started.")
            else:
                logger.info("ℹ️ HLSCleaner thread already running or finished, not starting again.")

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
                logger.debug(f"🔍 Scanning content directory: {content_path} (exists: {content_path.exists()})")
                
                # Get all subdirectories 
                channel_dirs = [d for d in content_path.iterdir() if d.is_dir()]
                
                # Debug the found directories
                logger.debug(f"📂 Found {len(channel_dirs)} potential channel directories: {[d.name for d in channel_dirs]}")

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
        """Gère la mise à jour périodique de la playlist principale"""
        logger.info("🔄 Démarrage thread de mise à jour de la playlist principale")
        
        # S'assurer que la playlist existe avec des permissions correctes dès le départ
        playlist_path = os.path.abspath("/app/hls/playlist.m3u")
        if not os.path.exists(playlist_path):
            try:
                # Créer un contenu minimal
                with open(playlist_path, "w", encoding="utf-8") as f:
                    f.write("#EXTM3U\n")
                os.chmod(playlist_path, 0o777)  # Permissions larges pour le debug
                logger.info(f"✅ Playlist initiale créée: {playlist_path}")
            except Exception as e:
                logger.error(f"❌ Erreur création playlist initiale: {e}")
        
        # Mise à jour immédiate au démarrage
        try:
            self._update_master_playlist()
            logger.info("✅ Première mise à jour de la playlist effectuée")
        except Exception as e:
            logger.error(f"❌ Erreur première mise à jour playlist: {e}")
        
        # Mises à jour fréquentes au démarrage
        for _ in range(3):
            try:
                # Attendre un peu entre les mises à jour
                time.sleep(10)
                self._update_master_playlist()
                logger.info("✅ Mise à jour de démarrage de la playlist effectuée")
            except Exception as e:
                logger.error(f"❌ Erreur mise à jour playlist de démarrage: {e}")
        
        # Continuer avec des mises à jour périodiques
        while True:
            try:
                self._update_master_playlist()
                time.sleep(60)  # On attend 60s avant la prochaine mise à jour
            except Exception as e:
                logger.error(f"❌ Erreur maj master playlist: {e}")
                logger.error(traceback.format_exc())
                time.sleep(60)  # On attend même en cas d'erreur

    def _update_master_playlist(self):
        """Effectue la mise à jour de la playlist principale"""
        playlist_path = os.path.abspath("/app/hls/playlist.m3u")
        logger.debug(f"🔄 Master playlist maj.: {playlist_path}")
        
        # Utiliser le verrou de scan pour protéger l'accès aux chaînes
        with self.scan_lock:
            try:
                # On sauvegarde d'abord le contenu actuel au cas où
                existing_content = "#EXTM3U\n"
                if os.path.exists(playlist_path) and os.path.getsize(playlist_path) > 0:
                    try:
                        with open(playlist_path, "r", encoding="utf-8") as f:
                            existing_content = f.read()
                        logger.debug(f"✅ Contenu actuel sauvegardé: {len(existing_content)} octets")
                    except Exception as e:
                        logger.error(f"❌ Erreur lecture playlist existante: {e}")
                
                # Préparation du nouveau contenu
                content = "#EXTM3U\n"

                # Build ready_channels based on channel objects status
                ready_channels = []
                # *** REMOVED redundant scan_lock, using self.lock now ***
                # with self.scan_lock: 
                for name, channel in sorted(self.channels.items()):
                    # Ensure channel object exists and check its ready status
                    if channel and hasattr(channel, 'ready_for_streaming') and channel.ready_for_streaming:
                        # Trust the ready_for_streaming status if start_stream succeeded
                        # No need to check for hls_playlist_path.exists() here, as it might not be created instantly
                        ready_channels.append((name, channel))
                        logger.debug(f"[{name}] ✅ Chaîne prête pour la playlist maître (status: ready_for_streaming=True)")
                    elif channel:
                        logger.debug(f"[{name}] ⏳ Chaîne non prête pour la playlist maître (ready_for_streaming={getattr(channel, 'ready_for_streaming', 'N/A')})")
                    else:
                         logger.debug(f"[{name}] ⏳ Chaîne non initialisée (objet None), non ajoutée à la playlist maître.")

                # Écriture des chaînes prêtes
                server_url = os.getenv("SERVER_URL", "192.168.10.183")
                if ready_channels:
                    for name, _ in ready_channels:
                        content += f'#EXTINF:-1 tvg-id="{name}" tvg-name="{name}",{name}\n'
                        content += f"http://{server_url}/hls/{name}/playlist.m3u8\n"
                else:
                    # Si aucune chaîne n'est prête, ajouter un commentaire
                    content += "# Aucune chaîne active pour le moment\n"
                    logger.warning("⚠️ Aucune chaîne active détectée pour la playlist")
                
                # Loguer le contenu à écrire pour vérification
                logger.debug(f"📝 Contenu de la playlist à écrire:\n{content}")
                
                # Écrire dans un fichier temporaire d'abord
                temp_path = f"{playlist_path}.tmp"
                with open(temp_path, "w", encoding="utf-8") as f:
                    f.write(content)
                    # *** ADDED: Ensure file is written to disk before checking size ***
                    f.flush()
                    os.fsync(f.fileno())
                    # *** END ADDED ***
                
                # Vérifier que le fichier temporaire a été créé correctement
                # *** ADDED: Short delay to allow filesystem sync ***
                time.sleep(0.1) 
                # *** END ADDED ***
                if os.path.exists(temp_path) and os.path.getsize(temp_path) > 0:
                    # Remplacer l'ancien fichier
                    os.replace(temp_path, playlist_path)
                    logger.debug(f"✅ Playlist remplacée avec succès")
                else:
                    logger.error(f"❌ Fichier temporaire vide ou non créé: {temp_path}")
                    # Ne pas remplacer l'ancien fichier si le temporaire est vide
                    raise Exception("Fichier temporaire vide ou non créé")
                
                # Vérifier permissions et que le fichier a bien été écrit
                os.chmod(playlist_path, 0o777)  # Permissions larges pour le debug
                
                # Vérification que le fichier a été correctement écrit
                if os.path.exists(playlist_path):
                    size = os.path.getsize(playlist_path)
                    logger.debug(f"✅ Playlist écrite: {playlist_path}, taille: {size} octets")
                    
                    # Lire le contenu pour vérification
                    with open(playlist_path, "r", encoding="utf-8") as f:
                        read_content = f.read()
                        if read_content == content:
                            logger.debug("✅ Contenu vérifié, identique à ce qui devait être écrit")
                        else:
                            logger.error("❌ Contenu lu différent du contenu qui devait être écrit")
                            logger.error(f"📄 Contenu lu:\n{read_content}")
                            # Essayer d'écrire directement
                            with open(playlist_path, "w", encoding="utf-8") as f:
                                f.write(content)
                            logger.info("🔄 Tentative d'écriture directe effectuée")
                else:
                    logger.error(f"❌ Fichier non trouvé après écriture: {playlist_path}")
                    # Recréer avec le contenu existant
                    with open(playlist_path, "w", encoding="utf-8") as f:
                        f.write(existing_content)
                    logger.warning("🔄 Restauration du contenu précédent")

                # Use len(self.channels) which includes all potentially initializing channels (placeholders)
                total_channels_known_by_manager = len(self.channels)
                # Count non-None channels for a more accurate 'loaded' count
                loaded_channels_count = sum(1 for ch in self.channels.values() if ch is not None)
                logger.info(
                    f"✅ Playlist mise à jour avec {len(ready_channels)} chaînes prêtes sur {loaded_channels_count} chargées ({total_channels_known_by_manager} total connu par manager)"
                )
            except Exception as e:
                logger.error(f"❌ Erreur mise à jour playlist: {e}")
                logger.error(traceback.format_exc())
                
                # En cas d'erreur, vérifier si le fichier existe toujours
                if not os.path.exists(playlist_path) or os.path.getsize(playlist_path) == 0:
                    # Restaurer le contenu précédent s'il existe
                    if existing_content and len(existing_content) > 8:  # Plus que juste "#EXTM3U\n"
                        try:
                            with open(playlist_path, "w", encoding="utf-8") as f:
                                f.write(existing_content)
                            os.chmod(playlist_path, 0o777)
                            logger.info("✅ Contenu précédent restauré")
                        except Exception as restore_e:
                            logger.error(f"❌ Erreur restauration contenu: {restore_e}")
                    
                    # Si pas de contenu précédent ou erreur, créer une playlist minimale
                    if not os.path.exists(playlist_path) or os.path.getsize(playlist_path) == 0:
                        try:
                            with open(playlist_path, "w", encoding="utf-8") as f:
                                f.write("#EXTM3U\n# Playlist de secours\n")
                            os.chmod(playlist_path, 0o777)
                            logger.info("✅ Playlist minimale créée en fallback")
                        except Exception as inner_e:
                            logger.error(f"❌ Échec création playlist minimale: {inner_e}")
            
            # Removed the redundant stream start logic from here

    def cleanup_manager(self):
        """Cleanup everything before shutdown"""
        logger.info("Début du nettoyage...")
        
        # Stop all threads first
        self.stop_scan_thread.set()
        self.stop_init_thread.set()

        # Vider tous les viewers du fichier de statut avant arrêt
        try:
            if self.channel_status is not None:
                logger.info("🧹 Vidage de tous les viewers du fichier channel_status.json avant arrêt...")
                flush_success = self.channel_status.flush_all_viewers()
                if flush_success:
                    logger.info("✅ Tous les viewers ont été vidés avec succès du fichier de statut")
                else:
                    logger.warning("⚠️ Échec du vidage des viewers avant arrêt")
        except Exception as e:
            logger.error(f"❌ Erreur lors du vidage des viewers: {e}")

        # Stop components that might be None
        try:
            if self.channel_status is not None:
                self.channel_status.stop()
                logger.info("✅ Channel status manager stopped")
        except Exception as e:
            logger.error(f"❌ Erreur lors de l'arrêt du channel status manager: {e}")

        try:
            if self.stats_collector is not None:
                self.stats_collector.stop()
                logger.info("📊 StatsCollector arrêté")
        except Exception as e:
            logger.error(f"❌ Erreur lors de l'arrêt du StatsCollector: {e}")

        try:
            if self.hls_cleaner is not None:
                self.hls_cleaner.stop_cleaner()
                logger.info("🧹 HLS Cleaner arrêté")
        except Exception as e:
            logger.error(f"❌ Erreur lors de l'arrêt du HLS Cleaner: {e}")

        # Join threads with timeout
        threads_to_join = [
            (self.channel_init_thread, "Channel init thread"),
            (self.scan_thread, "Scan thread"),
            # (self.cleanup_thread, "Cleanup thread"),
        ]

        for thread, name in threads_to_join:
            try:
                if thread and thread.is_alive():
                    thread.join(timeout=5)
                    logger.info(f"✅ {name} stopped")
            except Exception as e:
                logger.error(f"❌ Erreur lors de l'arrêt du thread {name}: {e}")

        # Stop observers
        try:
            if hasattr(self, "observer"):
                self.observer.stop()
                self.observer.join(timeout=5)
                logger.info("✅ Main observer stopped")
        except Exception as e:
            logger.error(f"❌ Erreur lors de l'arrêt de l'observer principal: {e}")

        try:
            if hasattr(self, "ready_observer"):
                self.ready_observer.stop()
                self.ready_observer.join(timeout=5)
                logger.info("✅ Ready observer stopped")
        except Exception as e:
            logger.error(f"❌ Erreur lors de l'arrêt du ready observer: {e}")
            
        # Clean up channels
        for name, channel in self.channels.items():
            try:
                if channel is not None:
                    channel._clean_processes()
                    logger.info(f"✅ Channel {name} cleaned up")
            except Exception as e:
                logger.error(f"❌ Erreur lors du nettoyage du canal {name}: {e}")

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
                # ADDED: Check if channel object is None
                if channel is None:
                    logger.debug(f"_setup_ready_observer: Skipping channel {name} as object is None.")
                    continue
                    
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

    def _update_channel_status(self, initial_call=False):
        """Update channel status for dashboard"""
        try:
            if not self.channel_status:
                logger.warning("⚠️ Channel status manager not initialized")
                return False
                
            # Ensure stats directory exists and has proper permissions
            stats_dir = Path(os.path.dirname(CHANNELS_STATUS_FILE))
            stats_dir.mkdir(parents=True, exist_ok=True)
            # Relaxed permissions for debugging/docker volume issues
            try:
                os.chmod(stats_dir, 0o777)
            except Exception as chmod_err:
                 logger.warning(f"Could not chmod stats dir {stats_dir}: {chmod_err}")

            
            # Prepare channel status data
            channels_dict = {}
            # Use scan_lock for iterating self.channels safely
            with self.scan_lock:
                for channel_name, channel in self.channels.items():
                    # Only include channels that have been initialized (are not None)
                    if channel and hasattr(channel, 'is_ready'):
                        # Get active watchers for this channel from the reliable source
                        active_channel_watchers = self.channels.get(channel_name, set())
                        viewers_count = len(active_channel_watchers)
                        watchers_list = list(active_channel_watchers)

                        status_data = {
                            # Base status
                            "active": channel.is_ready(),
                            "streaming": channel.is_streaming() if hasattr(channel, 'is_streaming') else False,
                            # Viewer info - Use data derived from self.channels
                            "viewers": viewers_count,
                            "watchers": watchers_list
                        }
                        channels_dict[channel_name] = status_data
                    # else: # Log channels that are skipped during update
                        # logger.debug(f"_update_channel_status: Skipping channel '{channel_name}' (not initialized or no is_ready attr)")

            
            # If no channels are ready or available, don't wipe the status
            if not channels_dict:
                # Avoid logging this message too frequently if manager starts with no channels
                log_key = "_last_no_channels_log_time"
                now = time.time()
                if not hasattr(self, log_key) or now - getattr(self, log_key) > 60:
                     logger.info("🤷 No initialized channels found, skipping status update.")
                     setattr(self, log_key, now)
                return True # Indicate success as no update was needed

            # Update status with retry logic
            max_retries = 3
            retry_delay = 1
            
            for attempt in range(max_retries):
                try:
                    # Call the method responsible for writing the combined status
                    success = self.channel_status.update_all_channels(channels_dict)
                    if success:
                        logger.debug(f"✅ Channel status file updated successfully ({'initial' if initial_call else 'periodic'})")
                        return True
                    else:
                        logger.warning(f"⚠️ Failed to update channel status file (attempt {attempt + 1}/{max_retries})")
                        if attempt < max_retries - 1:
                            time.sleep(retry_delay)
                except Exception as e:
                    logger.error(f"❌ Error calling channel_status.update_all_channels (attempt {attempt + 1}/{max_retries}): {e}")
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
            
            logger.error("❌ Failed to update channel status file after all retries")
            return False
            
        except Exception as e:
            logger.error(f"❌ Error in _update_channel_status: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def _check_channels_timeout(self):
        """Vérifie périodiquement les timeouts des chaînes sans watchers"""
        try:
            # Use list() to avoid issues if dict changes during iteration
            for channel_name, channel in list(self.channels.items()): 
                # ---> ADD CHECK FOR NONE <--- 
                if channel is not None and hasattr(channel, 'check_watchers_timeout'):
                    channel.check_watchers_timeout()
                elif channel is None:
                    logger.debug(f"_check_channels_timeout: Skipping channel {channel_name} as object is None.")
        except Exception as e:
            logger.error(f"❌ Erreur vérification timeouts: {e}")

    def run_manager_loop(self):
        """Boucle principale du gestionnaire IPTV"""
        logger.info("🔄 Démarrage de la boucle principale du gestionnaire")
        
        # Démarrer les composants dans le bon ordre
        if not hasattr(self, "observer") or not self.observer.is_alive():
            self.observer.start()
            logger.info("👁️ Observateur démarré")
            
        try:
            last_scan_time = time.time()
            last_summary_time = time.time()
            last_resync_time = time.time()
            last_log_watchers_time = time.time()
            last_process_logs_time = time.time()  # Pour traiter les logs régulièrement
            
            while True:
                current_time = time.time()
                
                # >>> NOUVEAU: Traiter les logs access.log régulièrement
                if current_time - last_process_logs_time > 0.5 and hasattr(self, "client_monitor") and self.client_monitor is not None:
                    self.client_monitor.process_new_logs()
                    last_process_logs_time = current_time
                # <<< FIN NOUVEAU
                
                # Scan périodique 
                if current_time - last_scan_time > SUMMARY_CYCLE:
                    self._periodic_scan()
                    last_scan_time = current_time
                    
                # Affichage périodique du récapitulatif
                if current_time - last_summary_time > SUMMARY_CYCLE:
                    self._log_channels_summary()
                    last_summary_time = current_time
                
                # Resynchronisation générale de toutes les playlists
                if current_time - last_resync_time > 600:  # Toutes les 10 minutes
                    self._resync_all_playlists()
                    last_resync_time = current_time
                
                # Log périodique des watchers
                if current_time - last_log_watchers_time > WATCHERS_LOG_CYCLE and hasattr(self, "_log_watchers_status"):
                    self._log_watchers_status()
                    last_log_watchers_time = current_time
                
                # Dormir un peu pour éviter de surcharger le CPU
                time.sleep(0.1)
                
        except KeyboardInterrupt:
            logger.info("👋 Interruption clavier, arrêt du gestionnaire")
            self.stop()
        except Exception as e:
            logger.critical(f"❌ ERREUR MAJEURE dans la boucle principale: {e}")
            import traceback
            logger.critical(traceback.format_exc())
            self.stop()
            
        logger.info("🛑 Boucle principale du gestionnaire terminée")

    def _periodic_scan(self):
        """Effectue un scan périodique des chaînes pour détecter les changements"""
        if self.stop_periodic_scan.is_set():
            return

        try:
            logger.debug(f"🔄 Scan périodique des chaînes en cours...")
            self.scan_channels(force=True)
            
            # NOUVEAU: Resynchronisation périodique des playlists
            self._resync_all_playlists()
            
        except Exception as e:
            logger.error(f"❌ Erreur scan périodique: {e}")

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
                        logger.info(f"🔄 [{channel_name}] {resync_count} playlists resynchronisées")
                else:
                    logger.debug(f"[{channel_name}] ℹ️ Pas de méthode de création de playlist, aucune resynchronisation nécessaire")
                    
            if resync_count > 0:
                logger.info(f"✅ Resynchronisation et redémarrage de {resync_count} chaînes effectués")
                
        except Exception as e:
            logger.error(f"❌ Erreur resynchronisation des playlists: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def stop(self):
        """Arrête proprement le gestionnaire IPTV"""
        logger.info("🛑 Arrêt du gestionnaire IPTV...")
        
        # Utiliser la méthode complète de nettoyage pour aussi vider les viewers
        self.cleanup_manager()
        
        logger.info("🛑 Gestionnaire IPTV arrêté avec succès")

    def _process_channel_init_queue(self):
        """Process the queue of channels to initialize"""
        logger.info("🔄 Démarrage du thread de traitement de la queue d'initialisation des chaînes")
        while not self.stop_init_thread.is_set():
            try:
                # Limite le nombre d'initialisations parallèles
                with self.init_threads_lock:
                    active_threads = self.active_init_threads
                    if active_threads >= self.max_parallel_inits:
                        logger.debug(f"[INIT_QUEUE] ⏳ Limite d'initialisations parallèles atteinte ({active_threads}/{self.max_parallel_inits}), attente...")
                        time.sleep(0.5)
                        continue

                # Essaie de récupérer une chaîne de la queue
                channel_data = None
                try:
                    logger.debug("[INIT_QUEUE] ⏱️ Attente d'un élément dans la queue...")
                    channel_data = self.channel_init_queue.get(timeout=5)
                    channel_name = channel_data.get('name', 'unknown')
                    logger.info(f"[INIT_QUEUE] 📥 Récupération de {channel_name} depuis la queue")
                except Empty:
                    logger.debug("[INIT_QUEUE] 📪 Queue vide, attente...")
                    time.sleep(0.5)
                    continue # Continue to next iteration to check stop_event
                except Exception as q_err:
                    logger.error(f"[INIT_QUEUE] ❌ Erreur get() sur la queue: {q_err}")
                    time.sleep(1) # Wait a bit before retrying
                    continue

                # If we got an item from the queue
                if channel_data:
                    channel_name = channel_data.get('name', 'unknown')
                    # Incrémente le compteur de threads actifs
                    with self.init_threads_lock:
                        self.active_init_threads += 1
                        logger.debug(f"[INIT_QUEUE] ➕ [{channel_name}] Incrémentation du compteur de threads actifs: {self.active_init_threads}/{self.max_parallel_inits}")

                    # Lance un thread pour initialiser cette chaîne
                    logger.info(f"[INIT_QUEUE] 🧵 [{channel_name}] Démarrage d'un thread d'initialisation")
                    threading.Thread(
                        target=self._init_channel_async,
                        args=(channel_data,),
                        daemon=True,
                        name=f"Init-{channel_name}" # Add thread name
                    ).start()

            except Exception as e:
                logger.error(f"[INIT_QUEUE] ❌ Erreur majeure dans la boucle _process_channel_init_queue: {e}")
                logger.error(traceback.format_exc()) # Log full traceback
                time.sleep(5) # Wait longer after a major loop error

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

            # Vérifie si la chaîne est prête immédiatement après l'initialisation
            is_ready_after_init = hasattr(channel, "ready_for_streaming") and channel.ready_for_streaming
            
            if is_ready_after_init:
                logger.info(f"✅ Chaîne {channel_name} prête immédiatement après initialisation.")
                # Ajoute la chaîne au dictionnaire sous verrou
                with self.scan_lock:
                    self.channels[channel_name] = channel
                    # Remove reliance on self.channel_ready_status dictionary
                
                # Démarrer immédiatement le stream si la chaîne est prête
                logger.info(f"[{channel_name}] 🚀 Démarrage immédiat du stream")
                if hasattr(channel, "start_stream"):
                    success = channel.start_stream()
                    if success:
                        logger.info(f"[{channel_name}] ✅ Stream démarré avec succès")
                        # Trigger master playlist update AFTER successful start
                        if hasattr(self, "_update_master_playlist"):
                            logger.info(f"[{channel_name}] 🔄 Mise à jour de la playlist maître après démarrage")
                            # Call the update function directly
                            self._update_master_playlist()
                    else:
                        logger.error(f"[{channel_name}] ❌ Échec du démarrage du stream")
                else:
                    logger.warning(f"[{channel_name}] ⚠️ Channel does not have start_stream method")
            else:
                 # La chaîne n'est pas prête (e.g., _scan_videos a échoué dans __init__)
                 logger.warning(f"⚠️ Chaîne {channel_name} non prête après initialisation (ready_for_streaming={is_ready_after_init}). Ne sera pas ajoutée ni démarrée.")
                 # Optionnel: Ajouter quand même au dictionnaire avec un statut non prêt?
                 # with self.scan_lock:
                 #    self.channels[channel_name] = channel # ou None?

            logger.info(f"[{channel_name}] ✅ Traitement d'initialisation terminé (État Prêt: {is_ready_after_init})")

        except Exception as e:
            logger.error(f"❌ Erreur initialisation de la chaîne {channel_data.get('name')}: {e}")
            # Make sure to add placeholder if exception happens before adding the channel object
            channel_name_for_exc = channel_data.get('name')
            if channel_name_for_exc:
                with self.scan_lock:
                    if channel_name_for_exc not in self.channels:
                        self.channels[channel_name_for_exc] = None # Placeholder on error
        finally:
            # Décrémente le compteur de threads actifs
            with self.init_threads_lock:
                self.active_init_threads -= 1

            # Marque la tâche comme terminée UNIQUEMENT si elle vient de la queue
            if channel_data.get("from_queue", True):
                self.channel_init_queue.task_done()

    def init_stats_collector_and_client_monitor(self):
        """Initialise le StatsCollector et le ClientMonitor une fois que les chaînes sont prêtes"""
        logger.info("🚀 Initialisation de StatsCollector et ClientMonitor maintenant que les chaînes sont prêtes...")
        
        # Initialisation du StatsCollector
        if self.stats_collector is None:
            try:
                logger.info(">>> INITIALIZING STATS COLLECTOR <<<")
                self.stats_collector = StatsCollector()
                logger.info("📊 StatsCollector initialisé")
                # Passer le callback update_watchers pour que StatsCollector puisse mettre à jour les spectateurs directement
                self.stats_collector.update_watchers_callback = self.update_watchers
            except Exception as e:
                logger.error(f"❌ Erreur initialisation StatsCollector: {e}")
                self.stats_collector = None
        
        # Initialisation du ClientMonitor
        if self.client_monitor is None:
            try:
                self.client_monitor = ClientMonitor(
                    log_path=self.log_path,
                    update_watchers_callback=self.update_watchers,
                    manager=self,
                    stats_collector=self.stats_collector
                )
                # Au lieu de démarrer le thread, on initialise juste la position
                self.client_monitor.start()
                logger.info("👁️ ClientMonitor initialisé et prêt")
            except Exception as e:
                logger.error(f"❌ Erreur initialisation ClientMonitor: {e}")
                self.client_monitor = None

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
        max_parallel = 8
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
            
            # NOUVEAU: Initialiser StatsCollector et ClientMonitor maintenant que les chaînes sont prêtes
            self.init_stats_collector_and_client_monitor()
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

    def get_current_channel_status(self):
        """
        Retourne l'état actuel de toutes les chaînes, basé sur ChannelStatusManager.
        """
        try:
            if not hasattr(self, 'channel_status') or not self.channel_status:
                logger.warning("⚠️ Tentative d'accès au statut des chaînes mais ChannelStatusManager n'est pas prêt.")
                return {}

            # Accès direct aux données de statut gérées par ChannelStatusManager
            # Aucune copie nécessaire ici car on ne fait que lire.
            status_data = self.channel_status.channels
            
            # Créer une copie pour éviter de retourner la référence interne 
            # et potentiellement filtrer pour ne retourner que les chaînes connues du manager?
            # Pour l'instant, retournons tout ce que le status manager connaît.
            return status_data.copy() 
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la récupération du statut actuel des chaînes: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {}

    def get_channel_segment_duration(self, channel_name: str) -> Optional[float]:
        """Retrieves the configured HLS segment duration (hls_time) for a specific channel."""
        # Use scan_lock as channel dictionary might be modified during scans/inits
        with self.scan_lock:
            channel = self.channels.get(channel_name)
            if channel and hasattr(channel, 'command_builder') and hasattr(channel.command_builder, 'hls_time'):
                duration = getattr(channel.command_builder, 'hls_time', None)
                if isinstance(duration, (int, float)) and duration > 0:
                    logger.debug(f"[{channel_name}] Found segment duration: {duration}")
                    return float(duration)
                else:
                    logger.warning(f"[{channel_name}] Found invalid segment duration type/value: {duration} in command_builder")
                    return None
            elif channel:
                logger.warning(f"[{channel_name}] Channel object found but missing 'command_builder' or 'hls_time' attribute.")
                return None
            else:
                # Channel not found or is a placeholder (None)
                logger.warning(f"[{channel_name}] Channel not found or not fully initialized in IPTVManager.channels.")
                return None

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
            # Use relaxed permissions here too
            try:
                os.chmod(stats_dir, 0o777)
            except Exception as chmod_err:
                 logger.warning(f"Could not chmod stats dir {stats_dir}: {chmod_err}")
            
            # Créer le fichier de statut s'il n'existe pas ou est vide/invalide
            should_create_file = True
            if os.path.exists(CHANNELS_STATUS_FILE):
                try:
                    if os.path.getsize(CHANNELS_STATUS_FILE) > 0:
                         with open(CHANNELS_STATUS_FILE, 'r') as f:
                             json.load(f) # Try to parse existing json
                         should_create_file = False # File exists and is valid JSON
                    # else: file exists but is empty, will overwrite
                except (json.JSONDecodeError, OSError) as e:
                     logger.warning(f"Existing status file {CHANNELS_STATUS_FILE} is invalid or unreadable ({e}). Will overwrite.")
                     # File exists but is invalid, will overwrite
                     
            if should_create_file:
                 logger.info(f"Creating/Overwriting initial status file: {CHANNELS_STATUS_FILE}")
                 with open(CHANNELS_STATUS_FILE, 'w') as f:
                     # Initial state with 0 viewers
                     json.dump({
                         'channels': {},
                         'last_updated': int(time.time()),
                         'active_viewers': 0
                     }, f, indent=2)
                 try:
                     os.chmod(CHANNELS_STATUS_FILE, 0o666)
                 except Exception as chmod_err:
                     logger.warning(f"Could not chmod status file {CHANNELS_STATUS_FILE}: {chmod_err}")
            
            # Initialiser le gestionnaire de statuts
            self.channel_status = ChannelStatusManager(
                status_file=CHANNELS_STATUS_FILE
            )
            
            # Faire une mise à jour initiale avec retry, PASSING initial_call=True
            # Note: _update_channel_status needs TimeTracker, ensure it's initialized before calling this if needed
            # For now, we assume initialization is enough, actual update might happen later
            # max_retries = 3
            # retry_delay = 1
            # for attempt in range(max_retries):
            #     try:
            #         # Pass initial_call=True here
            #         success = self._update_channel_status(initial_call=True) # Depends on TimeTracker
            #         if success:
            #             logger.info("✅ Channel status manager initialized for dashboard (initial status set)")
            #             return
            #         else:
            #             logger.warning(f"⚠️ Failed to update channel status during init (attempt {attempt + 1}/{max_retries})")
            #             if attempt < max_retries - 1:
            #                 time.sleep(retry_delay)
            #     except Exception as e:
            #         logger.error(f"❌ Error calling _update_channel_status during init (attempt {attempt + 1}/{max_retries}): {e}")
            #         if attempt < max_retries - 1:
            #             time.sleep(retry_delay)
            # logger.error("❌ Failed to initialize channel status manager after all retries")
            # self.channel_status = None
            logger.info("✅ ChannelStatusManager initialisé. La mise à jour initiale se fera plus tard.")
            
        except Exception as e:
            logger.error(f"❌ Error initializing channel status manager: {e}")
            import traceback
            logger.error(traceback.format_exc())
            self.channel_status = None
            
    def _check_inactive_channels(self):
        """
        Vérifie les chaînes actuellement en streaming mais sans viewers (selon ChannelStatusManager)
        et les arrête si elles sont inactives depuis trop longtemps.
        """
        if not hasattr(self, "channel_status") or self.channel_status is None:
            logger.debug("[_check_inactive_channels] ChannelStatusManager non disponible, skip.")
            return

        current_time = time.time()
        channels_to_stop = []

        # Utiliser le lock du ChannelStatusManager pour lire son état
        with self.channel_status._lock:
            for channel_name, status_data in self.channel_status.channels.items():
                viewers = status_data.get("viewers", 0)
                is_live = status_data.get("is_live", False)
                last_updated_str = status_data.get("last_updated")
                
                # Ne vérifier que les chaînes considérées comme live mais sans viewers
                if is_live and viewers == 0:
                    # Vérifier si la chaîne existe dans notre manager
                    channel_obj = self.channels.get(channel_name)
                    if channel_obj and hasattr(channel_obj, 'is_streaming') and channel_obj.is_streaming():
                        # Initialiser last_active_time si ce n'est pas fait
                        if not hasattr(channel_obj, 'last_time_with_viewers'):
                            channel_obj.last_time_with_viewers = current_time
                            
                        # Vérifier le délai d'inactivité
                        inactive_duration = current_time - channel_obj.last_time_with_viewers
                        if inactive_duration > self.channel_inactivity_timeout:
                            logger.info(f"[{channel_name}] ⏱️ Inactive depuis {inactive_duration:.0f}s (> {self.channel_inactivity_timeout}s) sans viewers. Arrêt demandé.")
                            channels_to_stop.append(channel_name)
                        else:
                             logger.debug(f"[{channel_name}] Inactive depuis {inactive_duration:.0f}s sans viewers, mais < timeout.")
                    # else: Channel is not streaming according to its object, no need to stop
                elif viewers > 0:
                    # Mettre à jour le timestamp si des viewers sont présents
                    channel_obj = self.channels.get(channel_name)
                    if channel_obj:
                        channel_obj.last_time_with_viewers = current_time 

        # Arrêter les chaînes identifiées (en dehors du lock de channel_status)
        for name in channels_to_stop:
            channel_obj = self.channels.get(name)
            if channel_obj and hasattr(channel_obj, 'stop_stream_if_needed'):
                 # Vérifier à nouveau si des viewers sont revenus entre temps?
                 # Pour l'instant, on arrête si la décision a été prise.
                 logger.info(f"[{name}] 🛑 Arrêt du stream pour inactivité.")
                 channel_obj.stop_stream_if_needed()
            else:
                 logger.warning(f"[{name}] Impossible d'arrêter le stream (objet non trouvé ou méthode manquante)." )   
            

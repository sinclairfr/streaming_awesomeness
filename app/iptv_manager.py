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

        # >>> STEP 2: Initialize channel status manager AFTER cleaning
        self.channel_status = None  # Initialiser à None
        self.init_channel_status_manager()
        # <<< END STEP 2

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
        # Le nettoyage HLS initial sera fait dans _clean_startup

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

        # >>> STEP 3: REMOVE second _clean_startup call
        # _clean_startup called only once at the beginning now
        # self._clean_startup()
        # <<< END STEP 3

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
            logger.info(f"📂 {len(channel_dirs)} dossiers de chaînes trouvés: {[d.name for d in channel_dirs]}")
            
            # Pour suivre les nouvelles chaînes détectées cette fois-ci
            found_in_this_scan = set()

            for channel_dir in channel_dirs:
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

    def update_watchers(self, channel_name: str, watcher_count: int, active_ips_list: list, path: str = "/hls/", source: str = 'unknown'):
        """Met à jour UNIQUEMENT ChannelStatusManager avec les données fournies."""
        # ---> GATEKEEPER <-----
        if source != 'tracker':
            # Keep this warning
            logger.warning(f"[{channel_name}] ⚠️ Ignored update_watchers call from unknown source ('{source}'). Count={watcher_count}, IPs={active_ips_list}")
            return # Ignore calls not coming from the reliable tracker push
        # ---> END GATEKEEPER <-----
        
        try:
            # Keep checks for IP-like names and channel existence
            is_likely_ip = False
            if channel_name and '.' in channel_name:
                parts = channel_name.split('.')
                if len(parts) == 4 and all(p.isdigit() for p in parts):
                    is_likely_ip = True
            if is_likely_ip:
                logger.debug(f"⏭️ Ignoré mise à jour watchers pour IP-like name: {channel_name}")
                return
            if channel_name not in self.channels:
                logger.warning(f"[UPDATE_WATCHERS] ⚠️ Chaîne '{channel_name}' non connue du manager. Update ignoré.")
                return
                
            # Vérifier si certains watchers ont changé de canal et les supprimer des autres canaux
            if hasattr(self, "channel_status") and self.channel_status:
                # Vérifier si certains de ces IPs étaient actifs sur d'autres canaux
                current_channels = {}
                # Créer une copie des canaux pour éviter des modifications pendant l'itération
                all_channels = self.channel_status.channels.copy()
                
                # Pour chaque IP active sur ce canal
                for ip in active_ips_list:
                    # Rechercher dans tous les autres canaux si cette IP y était active
                    for other_channel, channel_data in all_channels.items():
                        # Ne pas vérifier le canal actuel
                        if other_channel == channel_name:
                            continue
                            
                        # Vérifier si l'IP est dans la liste des watchers de l'autre canal
                        other_watchers = channel_data.get('watchers', [])
                        if ip in other_watchers:
                            logger.info(f"🔄 IP {ip} détectée sur {channel_name} mais était aussi sur {other_channel}, notification de changement")
                            
                            # CORRECTION: Ne retirer que l'IP qui a changé de canal, pas tous les viewers
                            new_watchers = [w for w in other_watchers if w != ip]
                            
                            # Mettre à jour l'autre canal sans cette IP
                            other_data = channel_data.copy()
                            other_data['watchers'] = new_watchers
                            other_data['viewers'] = len(new_watchers)
                            
                            # Appliquer la mise à jour à l'autre canal
                            self.channel_status.update_channel(other_channel, other_data)
                            logger.info(f"🧹 IP {ip} retirée de {other_channel} après changement vers {channel_name}")

            # Mise à jour du statut via ChannelStatusManager
            if hasattr(self, "channel_status") and self.channel_status is not None:
                watchers_list_for_json = list(active_ips_list) if isinstance(active_ips_list, (list, set)) else []
                viewers_count_from_arg = watcher_count
                
                is_active = False
                channel = self.channels.get(channel_name)
                if channel:
                     is_active = bool(getattr(channel, "ready_for_streaming", False))
                else: 
                     logger.warning(f"[{channel_name}] Channel object not found in update_watchers for is_live check.")

                # NE PAS ÉCRASER LES VIEWERS EXISTANTS, MAIS FUSIONNER
                current_data = self.channel_status.channels.get(channel_name, {})
                current_watchers = current_data.get('watchers', [])
                
                # Ne mettre à jour que si les listes diffèrent pour éviter des écritures inutiles
                existing_set = set(current_watchers)
                new_set = set(watchers_list_for_json)
                
                if existing_set != new_set:
                    # Ajouter tous les viewers actuels ET nouveaux (sans doublons)
                    merged_watchers = list(existing_set.union(new_set))
                    
                    channel_data = {
                        'is_live': is_active,
                        'viewers': len(merged_watchers),
                        'watchers': merged_watchers,
                    }
                    
                    update_successful = self.channel_status.update_channel(
                        channel_name,
                        channel_data
                    )
                    
                    if not update_successful:
                        logger.warning(f"[{channel_name}] ⚠️ Échec de la mise à jour du statut via ChannelStatusManager")
                    else:
                        logger.debug(f"[{channel_name}] ✅ Statut CSM mis à jour: viewers={len(merged_watchers)}, watchers={merged_watchers}")
                else:
                    logger.debug(f"[{channel_name}] ℹ️ Pas de changement dans la liste de viewers, mise à jour ignorée")

        except Exception as e:
            logger.error(f"❌ Erreur mise à jour watchers pour {channel_name}: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def _reset_channel_statuses(self):
        """Reset all channel statuses to inactive with zero viewers at startup"""
        if hasattr(self, "channel_status") and self.channel_status: # Check if initialized
            logger.info("🔄 [RESET] Resetting all channel statuses...") # Add log
            updated_count = 0 # Track if any update happened
            # Need to acquire lock? Probably not, this should run early before threads modify channel_status heavily
            channels_to_reset = list(self.channels.keys()) # Get keys first in case dict changes
            logger.info(f"🔄 [RESET] Found {len(channels_to_reset)} channels to reset: {channels_to_reset}")
            for name in channels_to_reset:
                logger.info(f"🔄 [RESET] Attempting reset for {name}")
                # Directly call update_channel on the status manager instance
                # It handles saving if needed
                reset_success = self.channel_status.update_channel(
                    name,
                    is_live=False,
                    viewers=0,
                    watchers=[] # Explicitly provide empty watchers list
                )
                if reset_success:
                     updated_count += 1 # Mark that at least one update was attempted
                     logger.info(f"🔄 [RESET] Reset successful for {name}")
                else:
                     logger.warning(f"⚠️ [RESET] Reset call for {name} returned False (either no change needed or save failed)")

            if updated_count > 0:
                logger.info(f"✅ [RESET] {updated_count} channel statuses reset via ChannelStatusManager")
            else:
                logger.info("ℹ️ [RESET] No channel status needed resetting or ChannelStatusManager not ready.")
        else:
            logger.warning("⚠️ [RESET] ChannelStatusManager not available during reset attempt.")

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
            logger.info("🧹 Nettoyage initial... (appel unique au démarrage)")

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
        
        # *** ADDED: Acquire lock to prevent concurrent updates ***
        with self.lock:
            # *** END ADDED ***
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
        self.stop_watchers.set()
        self.stop_status_update.set()

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
            (self.watchers_thread, "Watchers thread"),
            (self.cleanup_thread, "Cleanup thread"),
            (self.status_update_thread, "Status update thread")
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
                        # Ajouter le paramètre source='tracker' pour passer la vérification de sécurité
                        self.update_watchers(channel, count, list(ips), "/hls/", source='tracker')

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
        
        # Vérifier si TimeTracker est disponible pour communiquer avec son buffer
        has_time_tracker_buffer = False
        for channel_name, channel in self.channels.items():
            if hasattr(channel, "time_tracker") and hasattr(channel.time_tracker, "is_being_removed"):
                has_time_tracker_buffer = True
                break
        
        # Durée d'inactivité plus longue pour éviter les suppressions prématurées
        inactivity_threshold = 300  # 5 minutes d'inactivité

        # Identifier les watchers inactifs
        with self.lock:
            for (channel, ip), last_seen_time in self.watchers.items():
                # Période d'inactivité
                inactivity_time = current_time - last_seen_time
                
                # Si l'IP est dans le buffer de suppression de TimeTracker, on ne la supprime pas encore
                if has_time_tracker_buffer:
                    time_tracker = next((ch.time_tracker for ch_name, ch in self.channels.items() 
                                         if hasattr(ch, "time_tracker")), None)
                    if time_tracker and hasattr(time_tracker, "is_being_removed") and time_tracker.is_being_removed(ip):
                        logger.debug(f"⏱️ Manager: Watcher {ip} dans le buffer de TimeTracker, suppression différée")
                        continue
                
                # Si pas d'activité depuis plus de la période d'inactivité définie
                if inactivity_time > inactivity_threshold:
                    inactive_watchers.append((channel, ip))
                    logger.debug(f"⏱️ Manager: Watcher {ip} inactif depuis {inactivity_time:.1f}s sur {channel}")

            # Si aucun watcher inactif, on arrête ici
            if not inactive_watchers:
                return
                
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
                    logger.info(f"🧹 Manager: Suppression du watcher inactif: {ip} sur {channel} (inactif depuis plus de {inactivity_threshold}s)")

            # Mettre à jour les compteurs de watchers pour les chaînes affectées
            for channel in channels_to_update:
                if channel in self.channels:
                    watcher_count = len(self._active_watchers.get(channel, set()))
                    
                    # Ne pas mettre à jour si ça fait passer de quelque chose à zéro
                    old_count = getattr(self.channels[channel], 'watchers_count', 0)
                    
                    if watcher_count > 0 or old_count == 0:
                        self.channels[channel].watchers_count = watcher_count
                        self.channels[channel].last_watcher_time = current_time
                        logger.info(f"[{channel}] 👁️ Manager: Mise à jour après nettoyage: {watcher_count} watchers actifs (ancien: {old_count})")
                    else:
                        # Si on passe de viewers à zéro, on log mais on ne met pas à jour pour éviter les arrêts intempestifs
                        logger.warning(f"[{channel}] ⚠️ Manager: Détection chute de viewers à zéro, vérification supplémentaire nécessaire")

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
                        active_channel_watchers = self._active_watchers.get(channel_name, set())
                        viewers_count = len(active_channel_watchers)
                        watchers_list = list(active_channel_watchers)

                        status_data = {
                            # Base status
                            "active": channel.is_ready(),
                            "streaming": channel.is_streaming() if hasattr(channel, 'is_streaming') else False,
                            # Viewer info - Use data derived from self._active_watchers
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

    def _status_update_loop(self):
        """Background thread to periodically update channel status"""
        update_interval = 60  # Update every 60 seconds (Reduced frequency as push updates are primary)
        logger.info("🔄 Démarrage de la boucle de mise à jour des statuts (périodicité: {update_interval}s)")
        
        while not self.stop_status_update.is_set():
            try:
                # Vérifier que le channel_status est bien initialisé
                if self.channel_status is None:
                    logger.warning("⚠️ Channel status manager non initialisé, tentative de réinitialisation")
                    self.init_channel_status_manager()
                    time.sleep(update_interval) # Wait before next check
                    continue

                # Ensure stats directory exists and has proper permissions
                # This might be redundant if init always ensures it
                # stats_dir = Path(os.path.dirname(CHANNELS_STATUS_FILE))
                # stats_dir.mkdir(parents=True, exist_ok=True)
                # os.chmod(stats_dir, 0o777)
                
                # Update channel status with retry logic - COMMENTED OUT as primary updates are pushed
                logger.debug("Status update loop: Skipping active status update (handled by push). Loop remains for potential future periodic tasks.")
                # max_retries = 3
                # retry_delay = 1
                # 
                # for attempt in range(max_retries):
                #     try:
                #         # success = self._update_channel_status() # COMMENTED OUT
                #         success = True # Assume success if we are not calling it
                #         if success:
                #             # logger.debug("✅ Channel status updated successfully") # No longer accurate here
                #             break
                #         else:
                #             # logger.warning(f"⚠️ Failed to update channel status (attempt {attempt + 1}/{max_retries})")
                #             # if attempt < max_retries - 1:
                #             #     time.sleep(retry_delay)
                #     except Exception as e:
                #         # logger.error(f"❌ Error updating channel status (attempt {attempt + 1}/{max_retries}): {e}")
                #         # if attempt < max_retries - 1:
                #         #     time.sleep(retry_delay)
                
                # Sleep until next cycle
                self.stop_status_update.wait(update_interval) # Use wait for cleaner interruption
                
            except Exception as e:
                logger.error(f"❌ Error in status update loop: {e}")
                import traceback
                logger.error(traceback.format_exc())
                self.stop_status_update.wait(30) # Wait 30s after error

    def init_channel_status_manager(self):
        """Initialize the channel status manager for dashboard"""
        try:
            # Si déjà initialisé, ne pas réinitialiser
            if self.channel_status is not None:
                logger.info("ℹ️ Channel status manager déjà initialisé, skip")
                return

            logger.info("🚀 Initialisation du gestionnaire de statuts des chaînes")

            # Créer le dossier stats s'il n'existe pas (peut être redondant avec _clean_startup)
            stats_dir = Path(os.path.dirname(CHANNELS_STATUS_FILE))
            stats_dir.mkdir(parents=True, exist_ok=True)
            try:
                os.chmod(stats_dir, 0o777)
            except Exception as chmod_err:
                 logger.warning(f"Could not chmod stats dir {stats_dir}: {chmod_err}")

            # >>> STEP 5: Load/Create file, but DO NOT call _update_channel_status
            # Le fichier doit exister (et être vide/propre) grâce à _clean_startup
            should_create_file = True
            if os.path.exists(CHANNELS_STATUS_FILE):
                try:
                    if os.path.getsize(CHANNELS_STATUS_FILE) > 0:
                         with open(CHANNELS_STATUS_FILE, 'r') as f:
                             json.load(f) # Try to parse existing json
                         should_create_file = False # File exists and is valid JSON
                         logger.info(f"Fichier de statut existant trouvé: {CHANNELS_STATUS_FILE}")
                    # else: file exists but is empty, will overwrite
                except (json.JSONDecodeError, OSError) as e:
                     logger.warning(f"Existing status file {CHANNELS_STATUS_FILE} is invalid or unreadable ({e}). Will overwrite.")
                     # File exists but is invalid, will overwrite

            if should_create_file:
                 logger.info(f"Création/Écrasement du fichier de statut initial: {CHANNELS_STATUS_FILE}")
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
            # <<< END STEP 5

            # Initialiser le gestionnaire de statuts
            from channel_status_manager import ChannelStatusManager
            self.channel_status = ChannelStatusManager(
                status_file=CHANNELS_STATUS_FILE
            )
            logger.info("✅ Channel status manager initialized for dashboard (NO initial status push)")

            # >>> STEP 6: REMOVE initial status update call
            # Status will be populated by push updates via update_watchers -> update_channel
            # max_retries = 3
            # retry_delay = 1
            # 
            # for attempt in range(max_retries):
            #     try:
            #         # Pass initial_call=True here
            #         success = self._update_channel_status(initial_call=True)
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
            # 
            # logger.error("❌ Failed to initialize channel status manager after all retries")
            # self.channel_status = None
            # <<< END STEP 6

        except Exception as e:
            logger.error(f"❌ Error initializing channel status manager: {e}")
            import traceback

            logger.error(traceback.format_exc())
            self.channel_status = None

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
        try:
            # Initialize channel status manager
            self.init_channel_status_manager()
            
            # >>> TEMP DEBUG: DISABLED
# This code was automatically disabled because it was causing all channels
# to show the same fake viewer (IP: 192.168.10.104).
# <<< END TEMP DEBUG
            
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

    def _watchers_loop(self):
        """Thread de surveillance des watchers"""
        while not self.stop_watchers.is_set():
            try:
                # Vérifier l'état des chaînes
                # Use list(self.channels.items()) to create a temporary copy
                # in case the dictionary is modified during iteration by another thread
                for channel_name, channel in list(self.channels.items()):
                    # ADDED: Check if channel object exists before accessing attributes
                    if channel is None:
                        logger.debug(f"_watchers_loop: Skipping channel {channel_name} as object is None (likely initializing)")
                        continue

                    if not channel.ready_for_streaming:
                        logger.debug(f"_watchers_loop: Skipping channel {channel_name} as not ready_for_streaming")
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

    def get_current_channel_status(self):
        """Retrieves the live status from the ChannelStatusManager."""
        if self.channel_status:
            # Return the data managed by ChannelStatusManager
            # Adjust the structure if needed based on what the API should return
            return {
                'channels': self.channel_status.channels,
                'last_updated': self.channel_status.last_updated,
                'active_viewers': self.channel_status.active_viewers
            }
        else:
            logger.warning("ChannelStatusManager not initialized when calling get_current_channel_status.")
            # Return a default empty structure
            return {
                'channels': {},
                'last_updated': int(time.time()),
                'active_viewers': 0
            }

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

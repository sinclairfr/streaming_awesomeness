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
from event_handler import ChannelEventHandler, ReadyContentHandler
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
)
from stats_collector import StatsCollector


class IPTVManager:
    """
    Gestionnaire principal du service IPTV - version améliorée avec:
    - Meilleure gestion des dossiers
    - Lancement non bloquant des chaînes
    - Meilleure détection et gestion des fichiers
    """

    def __init__(self, content_dir: str, use_gpu: bool = False):
        # Assurons-nous que la valeur de USE_GPU est bien prise de l'environnement
        use_gpu_env = os.getenv("USE_GPU", "false").lower() == "true"
        if use_gpu != use_gpu_env:
            logger.warning(
                f"⚠️ Valeur USE_GPU en paramètre ({use_gpu}) différente de l'environnement ({use_gpu_env}), on utilise celle de l'environnement"
            )
            use_gpu = use_gpu_env

        self.use_gpu = use_gpu
        logger.info(
            f"✅ Accélération GPU: {'ACTIVÉE' if self.use_gpu else 'DÉSACTIVÉE'}"
        )

        self.ensure_hls_directory()  # Sans argument pour le dossier principal

        self.content_dir = content_dir
        self.use_gpu = use_gpu
        self.channels = {}
        self.channel_ready_status = {}  # Pour suivre l'état de préparation des chaînes
        self.log_path = NGINX_ACCESS_LOG  # Ajout du chemin du log

        # Queue pour les chaînes à initialiser en parallèle
        self.channel_init_queue = Queue()
        self.max_parallel_inits = 3  # Nombre max d'initialisations parallèles
        self.active_init_threads = 0
        self.init_threads_lock = threading.Lock()

        # Moniteur FFmpeg
        self.ffmpeg_monitor = FFmpegMonitor(self.channels)
        self.ffmpeg_monitor.start()

        # On initialise le nettoyeur HLS avec le bon chemin
        self.hls_cleaner = HLSCleaner("/app/hls")
        self.hls_cleaner.initial_cleanup()
        self.hls_cleaner.start()

        self.scan_lock = threading.Lock()
        self.failing_channels = set()

        logger.info("Initialisation du gestionnaire IPTV amélioré")
        self._clean_startup()

        # Observer
        self.observer = Observer()
        event_handler = ChannelEventHandler(self)

        # Surveillance du dossier racine en mode récursif
        self.observer.schedule(event_handler, self.content_dir, recursive=True)
        logger.info(
            f"👁️ Observer configuré pour surveiller {self.content_dir} en mode récursif"
        )

        # NOUVEAU: Observer pour les dossiers ready_to_stream
        self.ready_observer = Observer()
        self.ready_event_handler = ReadyContentHandler(self)

        # Démarrage du thread d'initialisation des chaînes
        self.stop_init_thread = threading.Event()
        self.channel_init_thread = threading.Thread(
            target=self._process_channel_init_queue, daemon=True
        )
        self.channel_init_thread.start()

        # statistiques
        self.stats_collector = StatsCollector()
        # Vérifier si le StatsCollector est correctement initialisé
        if not hasattr(self.stats_collector, "stats") or not self.stats_collector.stats:
            logger.warning("⚠️ Réinitialisation du StatsCollector qui était invalide")
            self.stats_collector = StatsCollector()

        # Forcer une sauvegarde initiale des deux fichiers
        self.stats_collector.save_stats()
        self.stats_collector.save_user_stats()
        logger.info("✅ StatsCollector initialisé et vérifié")

        # Moniteur clients
        logger.info(f"🚀 Démarrage du client_monitor avec {NGINX_ACCESS_LOG}")
        self.client_monitor = ClientMonitor(
            NGINX_ACCESS_LOG, self.update_watchers, self, self.stats_collector
        )

        # Vérification explicite qu'on a bien accès au fichier
        if os.path.exists(NGINX_ACCESS_LOG):
            try:
                with open(NGINX_ACCESS_LOG, "r") as f:
                    # On lit juste les dernières lignes pour voir si ça fonctionne
                    f.seek(max(0, os.path.getsize(NGINX_ACCESS_LOG) - 1000))
                    last_lines = f.readlines()
                    logger.info(
                        f"✅ Lecture réussie du fichier de log, {len(last_lines)} dernières lignes trouvées"
                    )
            except Exception as e:
                logger.error(f"❌ Erreur lors de la lecture du fichier de log: {e}")
                
        # Démarrage du client_monitor une seule fois
        self.client_monitor.start()
        logger.info("✅ ClientMonitor démarré")

        # Thread de surveillance du log
        self._log_monitor_thread = threading.Thread(
            target=self._check_client_monitor, daemon=True
        )
        self._log_monitor_thread.start()

        # Moniteur ressources
        self.resource_monitor = ResourceMonitor()
        self.resource_monitor.start()

        # Thread de mise à jour de la playlist maître
        self.master_playlist_updater = threading.Thread(
            target=self._manage_master_playlist, daemon=True
        )
        self.master_playlist_updater.start()

        # Thread qui vérifie les watchers
        self.watchers_thread = threading.Thread(target=self._watchers_loop, daemon=True)
        self.running = True

    def _check_client_monitor(self):
        """Vérifie périodiquement l'état du client_monitor"""
        while True:
            try:
                logger.info("🔍 Vérification de l'état du client_monitor...")
                if (
                    not hasattr(self, "client_monitor")
                    or not self.client_monitor.is_alive()
                ):
                    logger.critical("🚨 client_monitor n'est plus actif!")
                    # Tentative de redémarrage
                    logger.info("🔄 Tentative de redémarrage du client_monitor...")
                    self.client_monitor = ClientMonitor(
                        NGINX_ACCESS_LOG, self.update_watchers, self, self.stats_collector
                    )
                    self.client_monitor.start()
            except Exception as e:
                logger.error(f"❌ Erreur vérification client_monitor: {e}")
            time.sleep(60)  # Vérification toutes les minutes

    def _stats_reporting_loop(self):
        """Thread qui génère périodiquement un rapport des statistiques"""
        stats_report_interval = int(
            os.getenv("STATS_REPORT_INTERVAL", "300")
        )  # Toutes les 5 minutes par défaut

        while self.running:
            try:
                time.sleep(stats_report_interval)

                if not hasattr(self, "stats_collector") or not self.stats_collector:
                    logger.warning(
                        "⚠️ StatsCollector non disponible, rapport impossible"
                    )
                    continue

                # Générer le rapport
                self._generate_stats_report()

            except Exception as e:
                logger.error(f"❌ Erreur dans la boucle de rapport stats: {e}")
                time.sleep(60)  # Attente plus longue en cas d'erreur

    def _generate_stats_report(self):
        """Génère un rapport de statistiques basé sur les données du StatsCollector"""
        try:
            stats = self.stats_collector

            # Récupérer les statistiques globales
            global_stats = stats.stats.get("global", {})
            total_watchers = global_stats.get("total_watchers", 0)
            peak_watchers = global_stats.get("peak_watchers", 0)
            total_watch_time = global_stats.get("total_watch_time", 0)

            # Formater le temps de visionnage
            hours = int(total_watch_time // 3600)
            minutes = int((total_watch_time % 3600) // 60)
            seconds = int(total_watch_time % 60)
            watch_time_formatted = f"{hours}h {minutes}m {seconds}s"

            # Générer le rapport
            report_lines = [
                "📊 RAPPORT DE STATISTIQUES",
                f"Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}",
                f"Spectateurs actifs: {total_watchers}",
                f"Pic de spectateurs: {peak_watchers}",
                f"Temps de visionnage total: {watch_time_formatted}",
            ]

            # Statistiques par chaîne
            channels_stats = []
            for name, channel_data in stats.stats.get("channels", {}).items():
                current_watchers = channel_data.get("current_watchers", 0)
                peak_watchers = channel_data.get("peak_watchers", 0)
                watch_time = channel_data.get("total_watch_time", 0)

                # Ne montrer que les chaînes avec activité
                if current_watchers > 0 or peak_watchers > 0 or watch_time > 0:
                    # Formater le temps de visionnage
                    hours = int(watch_time // 3600)
                    minutes = int((watch_time % 3600) // 60)
                    watch_time_formatted = f"{hours}h {minutes}m"

                    channels_stats.append(
                        {
                            "name": name,
                            "current": current_watchers,
                            "peak": peak_watchers,
                            "watch_time": watch_time,
                            "watch_time_formatted": watch_time_formatted,
                        }
                    )

            # Trier par nombre de spectateurs actuels
            channels_stats.sort(key=lambda x: (x["current"], x["peak"]), reverse=True)

            if channels_stats:
                report_lines.append("\nSTATISTIQUES PAR CHAÎNE:")
                for ch in channels_stats:
                    report_lines.append(
                        f"- {ch['name']}: {ch['current']} actifs (pic: {ch['peak']}, visionnage: {ch['watch_time_formatted']})"
                    )

            # Statistiques par utilisateur (IP)
            user_stats = stats.user_stats.get("users", {})

            if user_stats:
                # Limiter à 10 utilisateurs les plus actifs pour éviter des logs trop longs
                top_users = sorted(
                    [(ip, data) for ip, data in user_stats.items()],
                    key=lambda x: x[1].get("total_watch_time", 0),
                    reverse=True,
                )[:10]

                if top_users:
                    report_lines.append("\nTOP 10 UTILISATEURS (par IP):")
                    for ip, data in top_users:
                        watch_time = data.get("total_watch_time", 0)
                        hours = int(watch_time // 3600)
                        minutes = int((watch_time % 3600) // 60)
                        watch_time_formatted = f"{hours}h {minutes}m"

                        # Si on a des chaînes pour cet utilisateur
                        user_channels = len(data.get("channels", {}))
                        favorite = "aucune"

                        for ch_name, ch_data in data.get("channels", {}).items():
                            if ch_data.get("favorite", False):
                                favorite = ch_name
                                break

                        report_lines.append(
                            f"- {ip}: {watch_time_formatted} sur {user_channels} chaînes (favorite: {favorite})"
                        )

            # Afficher le rapport
            logger.info("\n".join(report_lines))

        except Exception as e:
            logger.error(f"❌ Erreur génération rapport stats: {e}")

    def _process_channel_init_queue(self):
        """Traite la queue d'initialisation des chaînes en parallèle"""
        while not self.stop_init_thread.is_set():
            try:
                # Limite le nombre d'initialisations parallèles
                with self.init_threads_lock:
                    if self.active_init_threads >= self.max_parallel_inits:
                        time.sleep(0.5)
                        continue

                # Essaie de récupérer une chaîne de la queue
                try:
                    channel_data = self.channel_init_queue.get(block=False)
                except Empty:
                    time.sleep(0.5)
                    continue

                # Incrémente le compteur de threads actifs
                with self.init_threads_lock:
                    self.active_init_threads += 1

                # Lance un thread pour initialiser cette chaîne
                threading.Thread(
                    target=self._init_channel_async, args=(channel_data,), daemon=True
                ).start()

            except Exception as e:
                logger.error(f"Erreur dans le thread d'initialisation: {e}")
                time.sleep(1)

    def _init_channel_async(self, channel_data):
        """Initialise une chaîne de manière asynchrone"""
        try:
            channel_name = channel_data["name"]
            channel_dir = channel_data["dir"]

            logger.info(f"[{channel_name}] - Initialisation asynchrone de la chaîne: ")

            # Crée l'objet chaîne
            channel = IPTVChannel(
                channel_name,
                str(channel_dir),
                hls_cleaner=self.hls_cleaner,
                use_gpu=self.use_gpu,
                stats_collector=self.stats_collector,  # Ajout du stats_collector
            )

            # Ajoute la chaîne au dictionnaire
            with self.scan_lock:
                self.channels[channel_name] = channel
                self.channel_ready_status[channel_name] = False  # Pas encore prête

            # Attente que la chaîne soit prête (max 5 secondes)
            for _ in range(5):
                if (
                    hasattr(channel, "ready_for_streaming")
                    and channel.ready_for_streaming
                ):
                    with self.scan_lock:
                        self.channel_ready_status[channel_name] = True
                    logger.info(f"✅ Chaîne {channel_name} prête pour le streaming")
                    break
                time.sleep(1)

        except Exception as e:
            logger.error(
                f"Erreur initialisation de la chaîne {channel_data.get('name')}: {e}"
            )
        finally:
            # Décrémente le compteur de threads actifs
            with self.init_threads_lock:
                self.active_init_threads -= 1

            # Marque la tâche comme terminée
            self.channel_init_queue.task_done()

    def auto_start_ready_channels(self):
        """Démarre automatiquement toutes les chaînes prêtes avec un délai entre chaque démarrage"""
        logger.info("🚀 Démarrage automatique des chaînes prêtes...")

        # Attendre que plus de chaînes soient prêtes
        for attempt in range(2):  # Réduit de 3 à 2 tentatives
            ready_channels = []
            with self.scan_lock:
                for name, is_ready in self.channel_ready_status.items():
                    if is_ready and name in self.channels:
                        channel = self.channels[name]
                        if channel.ready_for_streaming:
                            ready_channels.append(name)

            if (
                len(ready_channels) >= len(self.channels) * 0.5
            ):  # Au moins 50% des chaînes sont prêtes
                break

            logger.info(
                f"⏳ Seulement {len(ready_channels)}/{len(self.channels)} chaînes prêtes, attente supplémentaire ({attempt+1}/2)..."
            )
            time.sleep(5)  # Réduit de 10 à 5 secondes

        # Trier pour prévisibilité
        ready_channels.sort()

        # Limiter le CPU pour éviter saturation
        max_parallel = 4
        groups = [
            ready_channels[i : i + max_parallel]
            for i in range(0, len(ready_channels), max_parallel)
        ]

        for group_idx, group in enumerate(groups):
            logger.info(
                f"🚀 Démarrage du groupe {group_idx+1}/{len(groups)} ({len(group)} chaînes)"
            )

            # Démarrer chaque chaîne du groupe avec un petit délai entre elles
            for i, channel_name in enumerate(group):
                delay = i * 1  # Réduit de 3 à 1 seconde entre chaque chaîne
                threading.Timer(delay, self._start_channel, args=[channel_name]).start()
                logger.info(
                    f"[{channel_name}] ⏱️ Démarrage programmé dans {delay} secondes"
                )

            # Attendre avant le prochain groupe
            if group_idx < len(groups) - 1:
                time.sleep(max_parallel * 2)  # Réduit de 5 à 2 secondes par chaîne entre les groupes

        if ready_channels:
            logger.info(
                f"✅ {len(ready_channels)} chaînes programmées pour démarrage automatique"
            )
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
                        logger.error(
                            f"[{channel_name}] ❌ Échec du démarrage automatique"
                        )
                else:
                    logger.warning(
                        f"[{channel_name}] ⚠️ Non prête pour le streaming, démarrage ignoré"
                    )
        except Exception as e:
            logger.error(
                f"[{channel_name}] ❌ Erreur lors du démarrage automatique: {e}"
            )

    def _watchers_loop(self):
        """Surveille l'activité des watchers et arrête les streams inutilisés"""
        last_log_time = 0
        last_health_check = 0
        last_summary_time = 0  # Nouveau compteur pour le récapitulatif
        log_cycle = WATCHERS_LOG_CYCLE  # Augmenter à 60s au lieu de 10s
        summary_cycle = SUMMARY_CYCLE  # 5 minutes par défaut

        while True:
            try:
                current_time = time.time()
                channels_to_stop = []

                # Ajout du health check toutes les 5 minutes
                if current_time - last_health_check > 300:  # 5 minutes
                    for channel_name, channel in self.channels.items():
                        if hasattr(channel, "channel_health_check"):
                            try:
                                channel.channel_health_check()
                            except Exception as e:
                                logger.error(
                                    f"[{channel_name}] ❌ Erreur health check: {e}"
                                )
                    last_health_check = current_time

                # Générer le récapitulatif des chaînes
                if current_time - last_summary_time > summary_cycle:
                    self._log_channels_summary()
                    last_summary_time = current_time

                # Pour chaque chaîne, on vérifie l'inactivité
                for channel_name, channel in self.channels.items():
                    if not hasattr(channel, "last_watcher_time"):
                        continue

                    # On calcule l'inactivité
                    inactivity_duration = current_time - channel.last_watcher_time

                    # Si inactif depuis plus de TIMEOUT_NO_VIEWERS
                    if inactivity_duration > TIMEOUT_NO_VIEWERS:
                        if channel.process_manager.is_running():
                            logger.warning(
                                f"[{channel_name}] ⚠️ Stream inactif depuis {inactivity_duration:.1f}s, arrêt programmé"
                            )
                            channels_to_stop.append(channel)

                # Arrêt des chaînes sans watchers (avec un délai pour éviter les cascades)
                for i, channel in enumerate(channels_to_stop):
                    # Ajout d'un petit délai entre les arrêts (0.5s entre chaque)
                    time.sleep(i * 0.5)
                    channel.stop_stream_if_needed()

                # Log périodique des watchers actifs (moins fréquent)
                if current_time - last_log_time > log_cycle:
                    active_channels = []
                    for name, channel in sorted(self.channels.items()):
                        if (
                            hasattr(channel, "watchers_count")
                            and channel.watchers_count > 0
                        ):
                            active_channels.append(f"{name}: {channel.watchers_count}")

                    if active_channels:
                        logger.info(
                            f"👥 Chaînes avec viewers: {', '.join(active_channels)}"
                        )
                    last_log_time = current_time

                time.sleep(10)  # Vérification toutes les 10s

            except Exception as e:
                logger.error(f"❌ Erreur watchers_loop: {e}")
                time.sleep(10)

    def force_watch_time_update(self, channel_name=None):
        """Force l'ajout de temps de visionnage pour TOUTES les chaînes avec des watchers"""
        try:
            logger.info("🔥 FORCE WATCH TIME UPDATE 🔥")
            
            # Si StatsCollector n'existe pas, rien à faire
            if not hasattr(self, "stats_collector") or not self.stats_collector:
                logger.error("❌ PAS DE STATS_COLLECTOR DISPONIBLE!")
                return False
                
            # Si un channel spécifique est demandé
            if channel_name:
                if channel_name in self.channels:
                    # Récupérer les IPs actives pour ce canal
                    active_ips = self._get_active_watcher_ips(channel_name)
                    if active_ips:
                        # Ajouter le temps pour chaque IP active
                        for ip in active_ips:
                            logger.info(f"🔥 FORCE 10.0 SECONDES POUR {channel_name} (IP: {ip})")
                            self.stats_collector.add_watch_time(channel_name, ip, 10.0)
                        self.stats_collector.save_stats()
                        self.stats_collector.save_user_stats()
                        return True
                return False
                
            # Pour toutes les chaînes avec des watchers
            updates = 0
            for name, channel in self.channels.items():
                # Récupérer les IPs actives pour ce canal
                active_ips = self._get_active_watcher_ips(name)
                if active_ips:
                    # Ajouter le temps pour chaque IP active
                    for ip in active_ips:
                        logger.info(f"🔥 FORCE 10.0 SECONDES POUR {name} (IP: {ip})")
                        self.stats_collector.add_watch_time(name, ip, 10.0)
                    updates += 1
                    
            if updates > 0:
                self.stats_collector.save_stats()
                self.stats_collector.save_user_stats()
                return True
                
            return False
        except Exception as e:
            logger.error(f"❌ Erreur force_watch_time_update: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def _update_watcher(self, ip, channel, request_type, user_agent, line):
        logger.info(f"🔄 Mise à jour du watcher: {ip} sur {channel}")
        """Met à jour les informations d'un watcher spécifique"""
        with self.lock:
            current_time = time.time()
            
            # Si le watcher n'existe pas, créer un nouveau minuteur
            if ip not in self.watchers:
                if self.stats_collector:
                    timer = WatcherTimer(channel, ip, self.stats_collector)
                    self.watchers[ip] = {
                        "timer": timer,
                        "last_seen": current_time,
                        "type": request_type,
                        "user_agent": user_agent,
                        "last_channel": channel,  # Historique
                        "current_channel": channel  # Chaîne actuelle
                    }
                    logger.info(f"🆕 Nouveau watcher détecté: {ip} sur {channel}")
            else:
                # Vérifier si la chaîne a changé
                old_channel = self.watchers[ip].get("current_channel")
                if old_channel != channel:
                    logger.info(f"🔄 Changement de chaîne pour {ip}: {old_channel} -> {channel}")
                    # Arrêter l'ancien minuteur
                    if "timer" in self.watchers[ip]:
                        self.watchers[ip]["timer"].stop()
                        logger.info(f"⏱️ Arrêt du minuteur pour {ip} sur {old_channel}")
                    # Créer un nouveau minuteur pour la nouvelle chaîne
                    timer = WatcherTimer(channel, ip, self.stats_collector)
                    self.watchers[ip]["timer"] = timer
                    self.watchers[ip]["last_channel"] = old_channel
                    self.watchers[ip]["current_channel"] = channel  # MAJ chaîne actuelle

                # Mise à jour des infos
                self.watchers[ip]["last_seen"] = current_time
                self.watchers[ip]["type"] = request_type
                self.watchers[ip]["user_agent"] = user_agent

            # Traiter la requête
            if request_type == "segment":
                self._handle_segment_request(channel, ip, line, user_agent)

            # Mettre à jour le compteur de watchers pour cette chaîne
            self._update_channel_watchers_count(channel)
    def _get_active_watcher_ips(self, channel_name):
        """Récupère les IPs des watchers actifs pour une chaîne"""
        active_ips = []
        
        # Si on a accès au client_monitor et ses watchers
        if hasattr(self, "client_monitor") and hasattr(self.client_monitor, "watchers"):
            # Parcourir tous les watchers du client_monitor
            for (ch, ip), data in self.client_monitor.watchers.items():
                if ch == channel_name:
                    active_ips.append(ip)
        
        return active_ips

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
                summary_lines.append(f"CHAÎNES ARRÊTÉES: {len(stopped_channels)}")

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
        """Nettoie avant de démarrer"""
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
        Scanne le contenu pour détecter les nouveaux dossiers (chaînes).
        Version améliorée avec limitation de fréquence
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

                channel_dirs = [d for d in content_path.iterdir() if d.is_dir()]

                logger.info(f"📡 Scan des chaînes disponibles...")
                for channel_dir in channel_dirs:
                    channel_name = channel_dir.name

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
        logger.info("Début du nettoyage...")

        # Arrêt du StatsCollector
        if hasattr(self, "stats_collector"):
            self.stats_collector.stop()

        # Arrêt du thread d'initialisation
        self.stop_init_thread.set()

        if hasattr(self, "channel_init_thread") and self.channel_init_thread.is_alive():
            self.channel_init_thread.join(timeout=5)

        if hasattr(self, "hls_cleaner"):
            self.hls_cleaner.stop()

        if hasattr(self, "observer"):
            self.observer.stop()
            self.observer.join()

        if hasattr(self, "ready_observer"):
            self.ready_observer.stop()
            self.ready_observer.join()

        for name, channel in self.channels.items():
            channel._clean_processes()

        if hasattr(self, "scan_thread_stop"):
            self.scan_thread_stop.set()

        if hasattr(self, "scan_thread") and self.scan_thread.is_alive():
            self.scan_thread.join(timeout=5)

        logger.info("Nettoyage terminé")

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
        """Parse une ligne de log nginx pour extraire les informations pertinentes"""
        try:
            # Format typique d'une ligne de log nginx :
            # IP - - [DATE] "METHOD /path HTTP/1.1" STATUS SIZE "REFERER" "USER_AGENT"
            parts = line.split()
            if len(parts) < 7:
                return None, None, None, False, None

            ip = parts[0]
            request = parts[6]  # La requête complète entre guillemets
            path = request.split()[1] if len(request.split()) > 1 else ""

            # Vérifier si c'est une requête HLS
            if not ("/hls/" in path and (".m3u8" in path or ".ts" in path)):
                return None, None, None, False, None

            # Extraire le nom de la chaîne du chemin
            # Format attendu: /hls/CHANNEL_NAME/playlist.m3u8 ou /hls/CHANNEL_NAME/segment.ts
            channel = None
            if "/hls/" in path:
                parts = path.split("/")
                if len(parts) >= 3:
                    channel = parts[2]

            # Déterminer le type de requête
            request_type = "m3u8" if ".m3u8" in path else "ts" if ".ts" in path else None

            return ip, channel, request_type, True, path

        except Exception as e:
            logger.error(f"❌ Erreur parsing log: {e}")
            return None, None, None, False, None

    def process_iptv_log_lines(self):
        logger.info(f"[IPTV_MANAGER] 🔄 Début process_iptv_log_lines")
        """Traite les nouvelles lignes ajoutées au fichier de log nginx"""
        try:
            # Vérification de l'existence du fichier
            if not os.path.exists(self.log_path):
                logger.error(f"❌ Fichier log introuvable: {self.log_path}")
                return False

            # Initialisation de la position si c'est la première exécution
            if not hasattr(self, "last_position"):
                # On se met à la fin du fichier pour ne traiter que les nouvelles lignes
                with open(self.log_path, "r") as f:
                    f.seek(0, 2)  # Positionnement à la fin
                    self.last_position = f.tell()
                return True

            file_size = os.path.getsize(self.log_path)

            # Si le fichier a été rotaté (taille plus petite qu'avant)
            if file_size < self.last_position:
                logger.warning(f"⚠️ Détection rotation log: {self.log_path}")
                self.last_position = 0  # On repart du début

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
                    logger.info(
                        f"[{channel}] 👁️ MAJ watchers: {count} actifs - {list(ips)}"
                    )
                    self.update_watchers(channel, count, "/hls/")

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
            # Fallback au mode legacy
            self._legacy_watchers_loop()

    def _legacy_watchers_loop(self):
        """Mode de surveillance legacy quand watchdog n'est pas disponible"""
        logger.info("🔄 Démarrage du mode de surveillance legacy des logs")
        last_position = 0
        
        while True:
            try:
                if os.path.exists(self.log_path):
                    with open(self.log_path, "r") as f:
                        f.seek(last_position)
                        new_lines = f.readlines()
                        if new_lines:
                            self.process_iptv_log_lines()
                            last_position = f.tell()
                time.sleep(1)  # Vérification toutes les secondes
            except Exception as e:
                logger.error(f"❌ Erreur surveillance legacy: {e}")
                time.sleep(5)  # Attente plus longue en cas d'erreur

    def run_manager_loop(self):
        try:
            # Démarrer la boucle de surveillance des watchers
            if not self.watchers_thread.is_alive():
                self.watchers_thread.start()
                logger.info("🔄 Boucle de surveillance des watchers démarrée")

            # Démarrer la surveillance des logs nginx
            nginx_monitor_thread = threading.Thread(target=self._monitor_nginx_logs, daemon=True)
            nginx_monitor_thread.start()
            logger.info("🔍 Surveillance des logs nginx démarrée")

            logger.debug("📥 Scan initial des chaînes...")
            self.scan_channels(initial=True)  # Marquer comme scan initial

            logger.debug("🕵️ Démarrage de l'observer...")
            if not self.observer.is_alive():
                self.observer.start()

            # Configurer l'observateur pour ready_to_stream
            self._setup_ready_observer()

            # Attente suffisamment longue pour l'initialisation des chaînes
            logger.info(
                "⏳ Attente de 30 secondes pour l'initialisation des chaînes..."
            )
            time.sleep(30)

            # Démarrage automatique des chaînes prêtes
            self.auto_start_ready_channels()

            while True:
                time.sleep(1)

        except KeyboardInterrupt:
            self.cleanup_manager()
        except Exception as e:
            logger.error(f"🔥 Erreur manager : {e}")
            self.cleanup_manager()

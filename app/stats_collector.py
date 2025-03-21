# stats_collector.py
import os
import json
import time
import threading
from pathlib import Path
from config import logger


class StatsCollector:
    """
    # Collecte et sauvegarde des statistiques de lecture des chaînes
    # Permet l'analyse future des habitudes de visionnage
    """

    # Dans stats_collector.py

    def __init__(self, stats_dir="/app/stats"):
        self.stats_dir = Path(stats_dir)
        self.stats_dir.mkdir(parents=True, exist_ok=True)

        self.stats_file = self.stats_dir / "channel_stats.json"
        self.stats = self._load_stats()

        self.lock = threading.Lock()
        self.save_interval = 300  # 5 minutes

        # Démarrage du thread de sauvegarde périodique
        self.stop_save_thread = threading.Event()
        self.save_thread = threading.Thread(target=self._save_loop, daemon=True)
        self.save_thread.start()

        self.user_stats_file = self.stats_dir / "user_stats.json"
        self.user_stats = self._load_user_stats()

        # Forcer une sauvegarde initiale des deux fichiers
        self.save_stats()
        self.save_user_stats()

        logger.info(
            f"📊 StatsCollector initialisé (sauvegarde dans {self.stats_file}, user stats dans {self.user_stats_file})"
        )

    def add_watch_time(self, channel_name, ip, duration):
        """Ajoute du temps de visionnage pour une IP sur une chaîne"""
        with self.lock:
            # Log initial avec la durée reçue
            current_time = int(time.time())
            logger.debug(f"[STATS_COLLECTOR_DEBUG] Début add_watch_time - Channel: {channel_name}, IP: {ip}, Duration: {duration:.1f}s")
            logger.info(f"[STATS_COLLECTOR] ⏱️ Ajout de {duration:.1f}s pour {ip} sur {channel_name} (timestamp: {current_time})")

            # S'assurer que la structure channel_stats existe
            if channel_name not in self.stats["channels"]:
                logger.debug(f"[STATS_COLLECTOR_DEBUG] Création nouvelle structure pour channel: {channel_name}")
                self.stats["channels"][channel_name] = {
                    "current_watchers": 0,
                    "peak_watchers": 0,
                    "peak_time": current_time,
                    "total_watch_time": 0,
                    "session_count": 0,
                    "total_segments": 0,
                    "watchlist": {},
                }

            channel_stats = self.stats["channels"][channel_name]

            # Log avant mise à jour
            old_time = channel_stats["total_watch_time"]
            old_global_time = self.stats["global"]["total_watch_time"]
            logger.debug(f"[STATS_COLLECTOR_DEBUG] Avant mise à jour - Channel time: {old_time:.1f}s, Global time: {old_global_time:.1f}s")

            # Mise à jour du temps de visionnage global
            channel_stats["total_watch_time"] += duration
            self.stats["global"]["total_watch_time"] += duration

            # Log après mise à jour avec plus de détails
            logger.info(
                f"[STATS_COLLECTOR] 📊 {channel_name}: temps passé de {old_time:.1f}s à {channel_stats['total_watch_time']:.1f}s "
                f"(global: {old_global_time:.1f}s → {self.stats['global']['total_watch_time']:.1f}s) "
                f"[timestamp: {current_time}]"
            )

            # Mise à jour du temps de visionnage dans la watchlist
            if "watchlist" not in channel_stats:
                logger.debug(f"[STATS_COLLECTOR_DEBUG] Création watchlist pour channel: {channel_name}")
                channel_stats["watchlist"] = {}

            if ip not in channel_stats["watchlist"]:
                logger.debug(f"[STATS_COLLECTOR_DEBUG] Nouveau watcher {ip} sur {channel_name}")
                channel_stats["watchlist"][ip] = {
                    "first_seen": current_time,
                    "last_seen": current_time,
                    "total_time": 0,
                }
                logger.info(f"[STATS_COLLECTOR] 👤 Nouveau watcher {ip} sur {channel_name} [timestamp: {current_time}]")

            old_watchlist_time = channel_stats["watchlist"][ip]["total_time"]
            channel_stats["watchlist"][ip]["total_time"] += duration
            channel_stats["watchlist"][ip]["last_seen"] = current_time

            # Log de la watchlist avec plus de détails
            logger.info(
                f"[STATS_COLLECTOR] 👥 {channel_name}: {ip} temps passé de {old_watchlist_time:.1f}s à {channel_stats['watchlist'][ip]['total_time']:.1f}s "
                f"[timestamp: {current_time}]"
            )

            # Mise à jour des stats quotidiennes
            today = time.strftime("%Y-%m-%d")
            if "daily" not in self.stats:
                logger.debug(f"[STATS_COLLECTOR_DEBUG] Création structure daily pour date: {today}")
                self.stats["daily"] = {}

            if today not in self.stats["daily"]:
                logger.debug(f"[STATS_COLLECTOR_DEBUG] Initialisation stats quotidiennes pour {today}")
                self.stats["daily"][today] = {
                    "peak_watchers": 0,
                    "total_watch_time": 0,
                    "channels": {},
                }

            daily_stats = self.stats["daily"][today]
            if "channels" not in daily_stats:
                daily_stats["channels"] = {}

            if channel_name not in daily_stats["channels"]:
                logger.debug(f"[STATS_COLLECTOR_DEBUG] Initialisation stats quotidiennes pour channel: {channel_name}")
                daily_stats["channels"][channel_name] = {
                    "peak_watchers": 0,
                    "total_watch_time": 0,
                }

            # Log avant mise à jour quotidienne
            old_daily_time = daily_stats["channels"][channel_name]["total_watch_time"]
            old_daily_global = daily_stats["total_watch_time"]
            logger.debug(f"[STATS_COLLECTOR_DEBUG] Avant mise à jour quotidienne - Channel: {old_daily_time:.1f}s, Global: {old_daily_global:.1f}s")

            # Mise à jour du temps de visionnage quotidien
            daily_stats["channels"][channel_name]["total_watch_time"] += duration
            daily_stats["total_watch_time"] += duration

            # Log après mise à jour quotidienne
            logger.info(
                f"[STATS_COLLECTOR] 📅 {today} - {channel_name}: temps quotidien passé de {old_daily_time:.1f}s à {daily_stats['channels'][channel_name]['total_watch_time']:.1f}s "
                f"(global quotidien: {old_daily_global:.1f}s → {daily_stats['total_watch_time']:.1f}s)"
            )

            # Sauvegarde périodique
            if not hasattr(self, "last_forced_save"):
                self.last_forced_save = current_time

            if current_time - self.last_forced_save > 300:  # Sauvegarde toutes les 5 minutes
                logger.debug(f"[STATS_COLLECTOR_DEBUG] Déclenchement sauvegarde forcée après 5 minutes d'activité")
                threading.Thread(target=self.save_stats, daemon=True).start()
                self.last_forced_save = current_time
                logger.info("[STATS_COLLECTOR] 💾 Sauvegarde forcée des stats après 5 minutes d'activité")

            logger.debug(f"[STATS_COLLECTOR_DEBUG] Fin add_watch_time - Channel: {channel_name}, IP: {ip}")

    def _save_loop(self):
        """Sauvegarde périodique des statistiques"""
        while not self.stop_save_thread.is_set():
            try:
                # Attente entre sauvegardes
                time.sleep(self.save_interval)

                # Sauvegarde des stats principales
                self.save_stats()

                # Sauvegarde des stats utilisateur
                self.save_user_stats()

            except Exception as e:
                logger.error(f"❌ Erreur sauvegarde périodique des stats: {e}")

    def _load_user_stats(self):
        """Charge les stats utilisateurs ou crée un nouveau fichier"""
        if self.user_stats_file.exists():
            try:
                with open(self.user_stats_file, "r") as f:
                    return json.load(f)
            except json.JSONDecodeError:
                logger.warning(
                    f"⚠️ Fichier stats utilisateurs corrompu, création d'un nouveau"
                )

        # Structure initiale
        return {
            "users": {},  # Par IP
            "last_updated": int(time.time()),
        }

    def save_user_stats(self):
        """Sauvegarde les statistiques par utilisateur"""
        with self.lock:
            try:
                # Vérification avant sauvegarde
                if not hasattr(self, "user_stats") or not self.user_stats:
                    logger.warning(
                        "⚠️ user_stats n'existe pas ou est vide lors de la sauvegarde"
                    )
                    self.user_stats = {"users": {}, "last_updated": int(time.time())}

                # S'assurer que le dossier existe
                os.makedirs(os.path.dirname(self.user_stats_file), exist_ok=True)

                # Mise à jour du timestamp
                self.user_stats["last_updated"] = int(time.time())

                # Log pour comprendre ce qu'on sauvegarde
                user_count = len(self.user_stats.get("users", {}))
                watch_times = {}
                for ip, data in self.user_stats.get("users", {}).items():
                    watch_times[ip] = data.get("total_watch_time", 0)

                logger.info(
                    f"💾 Sauvegarde des stats utilisateurs: {user_count} utilisateurs avec temps: {watch_times}"
                )

                # Sauvegarde effective
                with open(self.user_stats_file, "w") as f:
                    json.dump(self.user_stats, f, indent=2)

                logger.info(
                    f"✅ Stats utilisateurs sauvegardées dans {self.user_stats_file}"
                )
                return True
            except Exception as e:
                logger.error(f"❌ Erreur sauvegarde stats utilisateurs: {e}")
                import traceback

                logger.error(traceback.format_exc())
                return False

    # Dans stats_collector.py, modifiez la méthode update_user_stats:

    def update_user_stats(self, ip, channel_name, duration, user_agent=None):
        """Met à jour les stats par utilisateur"""
        with self.lock:
            # Log initial
            logger.info(f"👤 Mise à jour stats utilisateur: {ip} sur {channel_name} (+{duration:.1f}s)")

            # S'assurer que user_stats et users existent
            if not hasattr(self, "user_stats"):
                logger.warning("⚠️ Initialisation de user_stats")
                self.user_stats = {"users": {}, "last_updated": int(time.time())}

            if "users" not in self.user_stats:
                self.user_stats["users"] = {}

            # Init pour cet utilisateur si nécessaire
            if ip not in self.user_stats["users"]:
                self.user_stats["users"][ip] = {
                    "first_seen": int(time.time()),
                    "last_seen": int(time.time()),
                    "total_watch_time": 0,
                    "channels": {},
                    "user_agent": user_agent,
                }
                logger.info(f"👤 Nouvel utilisateur détecté: {ip}")

            user = self.user_stats["users"][ip]
            old_total_time = user["total_watch_time"]
            user["last_seen"] = int(time.time())
            user["total_watch_time"] += duration

            # Log de la mise à jour du temps total
            logger.info(
                f"⏱️ {ip}: temps total passé de {old_total_time:.1f}s à {user['total_watch_time']:.1f}s"
            )

            # MAJ de l'user agent si fourni
            if user_agent:
                user["user_agent"] = user_agent

            # S'assurer que channels existe
            if "channels" not in user:
                user["channels"] = {}

            # Init pour cette chaîne si nécessaire
            if channel_name not in user["channels"]:
                user["channels"][channel_name] = {
                    "first_seen": int(time.time()),
                    "last_seen": int(time.time()),
                    "total_watch_time": 0,
                    "favorite": False,
                }
                logger.info(f"📺 {ip}: nouvelle chaîne {channel_name}")

            # MAJ des stats de la chaîne
            channel = user["channels"][channel_name]
            old_channel_time = channel["total_watch_time"]
            channel["last_seen"] = int(time.time())
            channel["total_watch_time"] += duration

            # Log de la mise à jour du temps par chaîne
            logger.info(
                f"📺 {ip} sur {channel_name}: temps passé de {old_channel_time:.1f}s à {channel['total_watch_time']:.1f}s"
            )

            # Détermination de la chaîne favorite
            if len(user["channels"]) > 1:
                favorite_channel = max(
                    user["channels"].items(), key=lambda x: x[1]["total_watch_time"]
                )[0]

                for ch_name, ch_data in user["channels"].items():
                    ch_data["favorite"] = ch_name == favorite_channel

            # Sauvegarde périodique
            if (
                not hasattr(self, "last_user_save")
                or time.time() - self.last_user_save > 300
            ):
                threading.Thread(target=self.save_user_stats, daemon=True).start()
                self.last_user_save = time.time()
                logger.info("💾 Sauvegarde forcée des stats utilisateur après 5 minutes d'activité")

    def _load_stats(self):
        """Charge les stats existantes ou crée un nouveau fichier"""
        if self.stats_file.exists():
            try:
                with open(self.stats_file, "r") as f:
                    stats = json.load(f)
                    
                    # Vérification et correction des timestamps dans le futur
                    current_time = int(time.time())
                    
                    # Correction des timestamps globaux
                    if "global" in stats:
                        if stats["global"].get("peak_time", 0) > current_time:
                            stats["global"]["peak_time"] = current_time
                        if stats["global"].get("last_updated", 0) > current_time:
                            stats["global"]["last_updated"] = current_time
                    
                    # Correction des timestamps des chaînes
                    if "channels" in stats:
                        for channel in stats["channels"].values():
                            if channel.get("peak_time", 0) > current_time:
                                channel["peak_time"] = current_time
                    
                    # Correction des timestamps quotidiens
                    if "daily" in stats:
                        for date, daily_data in stats["daily"].items():
                            for channel in daily_data.get("channels", {}).values():
                                if channel.get("peak_time", 0) > current_time:
                                    channel["peak_time"] = current_time
                    
                    # Correction du timestamp de dernière sauvegarde
                    if stats.get("last_daily_save", 0) > current_time:
                        stats["last_daily_save"] = current_time
                    
                    return stats
            except json.JSONDecodeError:
                logger.warning(f"⚠️ Fichier de stats corrompu, création d'un nouveau")

        # Structure initiale des stats
        return {
            "channels": {},
            "global": {
                "total_watchers": 0,
                "peak_watchers": 0,
                "peak_time": int(time.time()),
                "total_watch_time": 0,
                "last_updated": int(time.time()),
            },
            "daily": {},
            "last_daily_save": int(time.time()),
        }

    def save_stats(self):
        """Sauvegarde les statistiques dans le fichier JSON"""
        with self.lock:
            try:
                # Mise à jour du timestamp
                self.stats["global"]["last_updated"] = int(time.time())

                # Sauvegarde dans le fichier principal
                with open(self.stats_file, "w") as f:
                    json.dump(self.stats, f, indent=2)

                # Sauvegarde d'une copie horodatée toutes les 24h
                now = time.time()
                last_daily = self.stats.get("last_daily_save", 0)
                if now - last_daily > 86400:  # 24h
                    date_str = time.strftime("%Y-%m-%d")
                    daily_file = self.stats_dir / f"channel_stats_{date_str}.json"
                    with open(daily_file, "w") as f:
                        json.dump(self.stats, f, indent=2)
                    self.stats["last_daily_save"] = now
                    logger.info(f"📊 Sauvegarde quotidienne des stats: {daily_file}")

                logger.debug(
                    f"📊 Statistiques sauvegardées ({len(self.stats['channels'])} chaînes)"
                )
                return True
            except Exception as e:
                logger.error(f"❌ Erreur sauvegarde stats: {e}")
                return False

    def update_channel_watchers(self, channel_name, watchers_count):
        """Met à jour les stats de watchers pour une chaîne"""
        with self.lock:
            # Init des stats pour cette chaîne si nécessaire
            if channel_name not in self.stats["channels"]:
                self.stats["channels"][channel_name] = {
                    "current_watchers": 0,
                    "peak_watchers": 0,
                    "peak_time": 0,
                    "total_watch_time": 0,
                    "session_count": 0,
                    "total_segments": 0,
                    "watchlist": {},  # Pour suivre les IPs et leurs temps de visionnage
                }

            # Mise à jour des stats
            channel_stats = self.stats["channels"][channel_name]
            old_watchers = channel_stats["current_watchers"]
            channel_stats["current_watchers"] = watchers_count

            # Mise à jour du pic si nécessaire
            if watchers_count > channel_stats["peak_watchers"]:
                channel_stats["peak_watchers"] = watchers_count
                channel_stats["peak_time"] = int(time.time())

            # Si le nombre de watchers augmente, c'est une nouvelle session
            if watchers_count > old_watchers:
                channel_stats["session_count"] += watchers_count - old_watchers

            # Mise à jour des stats globales
            total_current_watchers = sum(
                ch["current_watchers"] for ch in self.stats["channels"].values()
            )
            self.stats["global"]["total_watchers"] = total_current_watchers

            # Mise à jour du pic global si nécessaire
            if (
                self.stats["global"]["total_watchers"]
                > self.stats["global"]["peak_watchers"]
            ):
                self.stats["global"]["peak_watchers"] = self.stats["global"][
                    "total_watchers"
                ]
                self.stats["global"]["peak_time"] = int(time.time())

            # Mise à jour des stats quotidiennes
            today = time.strftime("%Y-%m-%d")
            if today not in self.stats["daily"]:
                self.stats["daily"][today] = {
                    "peak_watchers": 0,
                    "total_watch_time": 0,
                    "channels": {},
                }

            daily_stats = self.stats["daily"][today]

            # Mise à jour du pic quotidien
            if self.stats["global"]["total_watchers"] > daily_stats["peak_watchers"]:
                daily_stats["peak_watchers"] = self.stats["global"]["total_watchers"]

            # Init des stats de la chaîne pour aujourd'hui
            if channel_name not in daily_stats["channels"]:
                daily_stats["channels"][channel_name] = {
                    "peak_watchers": 0,
                    "total_watch_time": 0,
                }

            # Mise à jour du pic de la chaîne pour aujourd'hui
            daily_channel = daily_stats["channels"][channel_name]
            if watchers_count > daily_channel["peak_watchers"]:
                daily_channel["peak_watchers"] = watchers_count

            # Sauvegarde si grosse variation
            if (
                abs(watchers_count - old_watchers) > 5
                or watchers_count > channel_stats["peak_watchers"] - 3
            ):
                threading.Thread(target=self.save_stats, daemon=True).start()

    def update_segment_stats(self, channel_name, segment_id, size):
        """Met à jour les stats de segments pour une chaîne"""
        with self.lock:
            # Init des stats pour cette chaîne si nécessaire
            if channel_name not in self.stats["channels"]:
                self.update_channel_watchers(channel_name, 0)

            # Mise à jour du nombre total de segments
            self.stats["channels"][channel_name]["total_segments"] += 1

            # Mise à jour des stats de segments
            segment_history = self.stats["channels"][channel_name].get(
                "segment_history", []
            )
            segment_history.append(
                {"segment_id": segment_id, "size": size, "time": int(time.time())}
            )

            # On garde que les 100 derniers segments pour limiter la taille
            self.stats["channels"][channel_name]["segment_history"] = segment_history[
                -100:
            ]

    def cleanup(self):
        logger.info("Début du nettoyage...")

        # Arrêt du StatsCollector
        if hasattr(self, "stats_collector"):
            self.stats_collector.stop()
            logger.info("📊 StatsCollector arrêté")

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

    def stop(self):
        """Arrête le thread de sauvegarde et fait une dernière sauvegarde"""
        self.stop_save_thread.set()
        if self.save_thread.is_alive():
            self.save_thread.join(timeout=5)

        # Sauvegarde finale
        self.save_stats()
        logger.info("📊 StatsCollector arrêté, stats sauvegardées")

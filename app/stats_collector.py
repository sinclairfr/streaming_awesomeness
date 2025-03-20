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
            # Vérification que la durée est positive
            if duration <= 0:
                logger.warning(
                    f"⚠️ Tentative d'ajout d'une durée négative ou nulle: {duration}s pour {ip} sur {channel_name}"
                )
                duration = max(0.1, duration)  # Force au moins 0.1s

            # Log pour debug
            logger.info(
                f"⏱️ Ajout de {duration:.1f}s de visionnage pour {ip} sur {channel_name}"
            )

            # Init des stats pour cette chaîne si nécessaire
            if channel_name not in self.stats["channels"]:
                self.update_channel_watchers(channel_name, 0)

            # Mise à jour du temps de visionnage
            channel_stats = self.stats["channels"][channel_name]
            channel_stats["total_watch_time"] += duration

            # Mise à jour des stats globales
            self.stats["global"]["total_watch_time"] += duration

            # Mise à jour des stats utilisateur
            if not hasattr(self, "user_stats"):
                logger.warning("⚠️ user_stats non initialisé, création...")
                self.user_stats = {"users": {}, "last_updated": int(time.time())}

            if "users" not in self.user_stats:
                self.user_stats["users"] = {}

            # Vérification/création de l'entrée utilisateur
            if ip not in self.user_stats["users"]:
                self.user_stats["users"][ip] = {
                    "first_seen": int(time.time()),
                    "last_seen": int(time.time()),
                    "total_watch_time": 0,
                    "channels": {},
                }

            # Mise à jour utilisateur
            self.user_stats["users"][ip]["total_watch_time"] += duration
            self.user_stats["users"][ip]["last_seen"] = int(time.time())

            # Vérification/création de l'entrée chaîne pour cet utilisateur
            if channel_name not in self.user_stats["users"][ip]["channels"]:
                self.user_stats["users"][ip]["channels"][channel_name] = {
                    "first_seen": int(time.time()),
                    "last_seen": int(time.time()),
                    "total_watch_time": 0,
                    "favorite": False,
                }

            # Mise à jour chaîne pour cet utilisateur
            self.user_stats["users"][ip]["channels"][channel_name][
                "total_watch_time"
            ] += duration
            self.user_stats["users"][ip]["channels"][channel_name]["last_seen"] = int(
                time.time()
            )

            # Mise à jour de la watchlist
            watchlist = channel_stats.get("watchlist", {})
            if ip not in watchlist:
                watchlist[ip] = {
                    "first_seen": int(time.time()),
                    "last_seen": int(time.time()),
                    "total_time": 0,
                }

            watchlist[ip]["total_time"] += duration
            watchlist[ip]["last_seen"] = int(time.time())

            # Mise à jour de la watchlist
            channel_stats["watchlist"] = watchlist

            # Mise à jour des stats quotidiennes
            today = time.strftime("%Y-%m-%d")
            if today not in self.stats["daily"]:
                self.stats["daily"][today] = {
                    "peak_watchers": 0,
                    "total_watch_time": 0,
                    "channels": {},
                }

            # S'assurer que le canal existe dans les stats quotidiennes
            if channel_name not in self.stats["daily"][today]["channels"]:
                self.stats["daily"][today]["channels"][channel_name] = {
                    "peak_watchers": 0,
                    "total_watch_time": 0,
                }

            # Mise à jour du temps de visionnage quotidien
            self.stats["daily"][today]["channels"][channel_name][
                "total_watch_time"
            ] += duration
            self.stats["daily"][today]["total_watch_time"] += duration

            # Forcer une sauvegarde si modification significative
            if duration > 10:  # S'il y a un ajout de plus de 10 secondes
                threading.Thread(target=self.save_stats, daemon=True).start()
                threading.Thread(target=self.save_user_stats, daemon=True).start()

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
                # S'assurer que le dossier existe
                os.makedirs(os.path.dirname(self.user_stats_file), exist_ok=True)

                # Mise à jour du timestamp
                self.user_stats["last_updated"] = int(time.time())

                # Log pour comprendre ce qu'on sauvegarde
                logger.info(
                    f"💾 Sauvegarde des stats utilisateurs: {len(self.user_stats['users'])} utilisateurs"
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

    def update_user_stats(self, ip, channel_name, duration, user_agent=None):
        """Met à jour les stats par utilisateur"""
        with self.lock:
            # Init pour cet utilisateur si nécessaire
            if ip not in self.user_stats["users"]:
                self.user_stats["users"][ip] = {
                    "first_seen": int(time.time()),
                    "last_seen": int(time.time()),
                    "total_watch_time": 0,
                    "channels": {},
                    "user_agent": user_agent,
                }

            user = self.user_stats["users"][ip]
            user["last_seen"] = int(time.time())
            user["total_watch_time"] += duration

            # MAJ de l'user agent si fourni
            if user_agent:
                user["user_agent"] = user_agent

            # Init pour cette chaîne si nécessaire
            if channel_name not in user["channels"]:
                user["channels"][channel_name] = {
                    "first_seen": int(time.time()),
                    "last_seen": int(time.time()),
                    "total_watch_time": 0,
                    "favorite": False,
                }

            # MAJ des stats de la chaîne
            channel = user["channels"][channel_name]
            channel["last_seen"] = int(time.time())
            channel["total_watch_time"] += duration

            # Détermination de la chaîne favorite
            if len(user["channels"]) > 1:
                favorite_channel = max(
                    user["channels"].items(), key=lambda x: x[1]["total_watch_time"]
                )[0]

                for ch_name, ch_data in user["channels"].items():
                    ch_data["favorite"] = ch_name == favorite_channel

            # Sauvegarde si changements importants
            if duration > 300:  # Plus de 5 minutes
                threading.Thread(target=self.save_user_stats, daemon=True).start()

    def _load_stats(self):
        """Charge les stats existantes ou crée un nouveau fichier"""
        if self.stats_file.exists():
            try:
                with open(self.stats_file, "r") as f:
                    return json.load(f)
            except json.JSONDecodeError:
                logger.warning(f"⚠️ Fichier de stats corrompu, création d'un nouveau")

        # Structure initiale des stats
        return {
            "channels": {},
            "global": {
                "total_watchers": 0,
                "peak_watchers": 0,
                "peak_time": 0,
                "total_watch_time": 0,
                "last_updated": int(time.time()),
            },
            "daily": {},
            "last_daily_save": 0,
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

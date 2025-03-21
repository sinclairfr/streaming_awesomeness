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
        """Initialise le collecteur de statistiques"""
        self.stats_dir = Path(stats_dir)
        self.stats_dir.mkdir(parents=True, exist_ok=True)

        self.stats_file = self.stats_dir / "channel_stats.json"
        self.user_stats_file = self.stats_dir / "user_stats.json"

        # Initialisation des structures de données
        self.stats = {}  # {channel: {total_watch_time, unique_viewers, watchlist, last_update}}
        self.global_stats = {
            "total_watch_time": 0.0,
            "unique_viewers": set(),
            "last_update": time.time()
        }
        self.user_stats = {}  # {ip: {total_watch_time, channels_watched, last_seen, user_agent}}
        self.daily_stats = {}  # {date: {channel: {total_watch_time, unique_viewers}}}
        self.last_save_time = time.time()
        self.lock = threading.Lock()
        self.save_interval = 300  # 5 minutes

        # Chargement des stats existantes
        self.stats = self._load_stats()
        self.user_stats = self._load_user_stats()

        # Démarrage du thread de sauvegarde périodique
        self.stop_save_thread = threading.Event()
        self.save_thread = threading.Thread(target=self._save_loop, daemon=True)
        self.save_thread.start()

        # Forcer une sauvegarde initiale des deux fichiers
        self.save_stats()
        self.save_user_stats()

        logger.info(
            f"📊 StatsCollector initialisé (sauvegarde dans {self.stats_file}, user stats dans {self.user_stats_file})"
        )
    def add_watch_time(self, channel, ip, duration):
        """Ajoute du temps de visionnage pour un watcher avec limitation de fréquence"""
        try:
            current_time = time.time()
            
            # Vérifie la dernière mise à jour pour cette paire channel/ip
            update_key = f"{channel}:{ip}"
            if not hasattr(self, "_last_update_times"):
                self._last_update_times = {}
                
            # Limite les mises à jour à une fois par seconde maximum
            if update_key in self._last_update_times:
                last_update = self._last_update_times[update_key]
                elapsed = current_time - last_update
                
                # Si moins d'une seconde depuis la dernière mise à jour, ajuster la durée
                if elapsed < 1.0:
                    # On ignore cette mise à jour trop rapprochée
                    logger.debug(f"[STATS] Mise à jour trop rapide pour {channel}:{ip} (interval: {elapsed:.2f}s), ignorée")
                    return
                
                # Ajuster la durée en fonction du temps réel écoulé
                if elapsed < duration and duration > 2.0:
                    adjusted_duration = max(elapsed, 1.0)  # Au moins 1 seconde
                    logger.debug(f"[STATS] Durée ajustée pour {channel}:{ip}: {duration:.1f}s → {adjusted_duration:.1f}s")
                    duration = adjusted_duration
            
            # Enregistrer le moment de cette mise à jour
            self._last_update_times[update_key] = current_time
            
            # Initialisation des stats si nécessaire
            if channel not in self.stats:
                self.stats[channel] = {
                    "total_watch_time": 0.0,
                    "unique_viewers": set(),
                    "watchlist": {},  # {ip: total_time}
                    "last_update": time.time()
                }

            # Mise à jour des stats du canal
            channel_stats = self.stats[channel]
            channel_stats["total_watch_time"] += duration
            channel_stats["unique_viewers"].add(ip)
            channel_stats["last_update"] = time.time()

            # Initialiser watchlist si elle n'existe pas
            if "watchlist" not in channel_stats:
                channel_stats["watchlist"] = {}

            # Mise à jour de la watchlist pour cette IP
            if ip not in channel_stats["watchlist"]:
                channel_stats["watchlist"][ip] = 0.0
                logger.debug(f"[STATS] 📊 Nouvelle IP ajoutée à la watchlist: {ip} sur {channel}")
            channel_stats["watchlist"][ip] += duration

            # Mise à jour des stats globales
            if "global" not in self.stats:
                self.stats["global"] = {
                    "total_watch_time": 0.0,
                    "unique_viewers": set(),
                    "last_update": time.time()
                }
            
            global_stats = self.stats["global"]
            global_stats["total_watch_time"] += duration
            global_stats["unique_viewers"].add(ip)
            global_stats["last_update"] = time.time()

            # Mise à jour des stats utilisateur (avec vérification des champs manquants)
            if ip not in self.user_stats:
                # Structure complète pour nouvel utilisateur
                self.user_stats[ip] = {
                    "total_watch_time": 0.0,
                    "channels_watched": set(),
                    "last_seen": time.time(),
                    "user_agent": None,
                    "channels": {}
                }
            
            user = self.user_stats[ip]
            user["total_watch_time"] = user.get("total_watch_time", 0.0) + duration
            
            # Vérifier si channels_watched existe et l'initialiser si nécessaire
            if "channels_watched" in user:
                # Convert list to set if needed
                if isinstance(user["channels_watched"], list):
                    user["channels_watched"] = set(user["channels_watched"])
            else:
                user["channels_watched"] = set()
            user["channels_watched"].add(channel)
            
            user["last_seen"] = time.time()

            # S'assurer que channels existe
            if "channels" not in user:
                user["channels"] = {}

            # Init pour cette chaîne si nécessaire
            if channel not in user["channels"]:
                user["channels"][channel] = {
                    "first_seen": int(time.time()),
                    "last_seen": int(time.time()),
                    "total_watch_time": 0,
                    "favorite": False,
                }

            # MAJ des stats de la chaîne
            channel_data = user["channels"][channel]
            channel_data["last_seen"] = int(time.time())
            channel_data["total_watch_time"] += duration

            # Détermination de la chaîne favorite
            if len(user["channels"]) > 1:
                favorite_channel = max(
                    user["channels"].items(), key=lambda x: x[1]["total_watch_time"]
                )[0]

                for ch_name, ch_data in user["channels"].items():
                    ch_data["favorite"] = ch_name == favorite_channel

            # Mise à jour des stats quotidiennes
            self._update_daily_stats(channel, ip, duration)

            # Sauvegarde périodique (moins fréquente)
            if not hasattr(self, "last_save_time") or current_time - self.last_save_time > 60:  # 1 minute
                threading.Thread(target=self.save_stats, daemon=True).start()
                threading.Thread(target=self.save_user_stats, daemon=True).start()
                self.last_save_time = current_time
                
        except Exception as e:
            logger.error(f"❌ Erreur mise à jour stats: {e}")
            import traceback
            logger.error(traceback.format_exc())
            
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
                    loaded_data = json.load(f)
                    
                    # Création d'une structure vide
                    clean_stats = {
                        "last_updated": int(time.time()),
                    }
                    
                    # Extraction des données utilisateurs depuis n'importe quel niveau d'imbrication
                    def extract_users(data_obj):
                        if not isinstance(data_obj, dict):
                            return {}
                        
                        extracted = {}
                        
                        # Si l'objet contient une clé "users" qui est un dictionnaire
                        if "users" in data_obj and isinstance(data_obj["users"], dict):
                            # Parcourir les utilisateurs de premier niveau
                            for ip, user_data in data_obj["users"].items():
                                if ip != "users" and isinstance(user_data, dict):
                                    # C'est un vrai utilisateur
                                    extracted[ip] = user_data
                            
                            # Récursion pour extraire les utilisateurs des niveaux imbriqués
                            nested_users = extract_users(data_obj["users"])
                            # Fusionner avec les utilisateurs déjà extraits
                            for ip, user_data in nested_users.items():
                                extracted[ip] = user_data
                                
                        return extracted
                    
                    # Extraire tous les utilisateurs et les ajouter à clean_stats
                    users = extract_users(loaded_data)
                    for ip, user_data in users.items():
                        clean_stats[ip] = user_data
                    
                    return clean_stats
                    
            except json.JSONDecodeError:
                logger.warning(f"⚠️ Fichier stats utilisateurs corrompu, création d'un nouveau")

        # Structure initiale
        return {
            "last_updated": int(time.time()),
        }
    def save_user_stats(self):
        """Sauvegarde les statistiques par utilisateur"""
        with self.lock:
            try:
                # Vérifications de base
                if not hasattr(self, "user_stats") or not self.user_stats:
                    self.user_stats = {"users": {}, "last_updated": int(time.time())}

                # S'assurer que le dossier existe
                os.makedirs(os.path.dirname(self.user_stats_file), exist_ok=True)

                # Préparation des données sérialisables - STRUCTURE SIMPLIFIÉE SANS IMBRICATION
                serializable_stats = {
                    "users": {},
                    "last_updated": int(time.time())
                }
                
                # Parcourir le dictionnaire user_stats pour extraire uniquement les données utilisateurs
                for ip, user_data in self.user_stats.items():
                    # Ignorer les clés de métadonnées comme "last_updated"
                    if ip == "last_updated":
                        continue
                        
                    # Convertir les données utilisateur en format sérialisable
                    serializable_user = {}
                    for key, val in user_data.items():
                        # Convertir les sets en listes
                        if isinstance(val, set):
                            serializable_user[key] = list(val)
                        else:
                            serializable_user[key] = val
                    
                    # Ajouter cet utilisateur au dictionnaire principal
                    serializable_stats["users"][ip] = serializable_user

                # Sauvegarde effective
                with open(self.user_stats_file, "w") as f:
                    json.dump(serializable_stats, f, indent=2)

                logger.info(f"✅ Stats utilisateurs sauvegardées dans {self.user_stats_file}")
                return True
            except Exception as e:
                logger.error(f"❌ Erreur sauvegarde stats utilisateurs: {e}")
                import traceback
                logger.error(traceback.format_exc())
                return False
    
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
                    loaded_stats = json.load(f)
                    
                    # Conversion de la structure chargée vers la nouvelle structure
                    stats = {}
                    global_stats = {
                        "total_watch_time": 0.0,
                        "unique_viewers": set(),
                        "last_update": time.time()
                    }
                    
                    # Conversion des stats des chaînes
                    if "channels" in loaded_stats:
                        for channel_name, channel_data in loaded_stats["channels"].items():
                            stats[channel_name] = {
                                "total_watch_time": float(channel_data.get("total_watch_time", 0)),
                                "unique_viewers": set(channel_data.get("unique_viewers", [])),
                                "watchlist": channel_data.get("watchlist", {}),
                                "last_update": time.time()
                            }
                    
                    # Conversion des stats globales
                    if "global" in loaded_stats:
                        global_data = loaded_stats["global"]
                        global_stats["total_watch_time"] = float(global_data.get("total_watch_time", 0))
                        global_stats["unique_viewers"] = set(global_data.get("unique_viewers", []))
                        global_stats["last_update"] = time.time()
                    
                    # Mise à jour des attributs
                    self.stats = stats
                    self.global_stats = global_stats
                    
                    logger.info(f"📊 Stats chargées: {len(stats)} chaînes, {len(global_stats['unique_viewers'])} spectateurs uniques")
                    return stats
                    
            except json.JSONDecodeError:
                logger.warning(f"⚠️ Fichier de stats corrompu, création d'un nouveau")

        # Structure initiale des stats
        return {}

    def save_stats(self):
        """Sauvegarde les statistiques dans le fichier JSON"""
        with self.lock:
            try:
                # Préparation des données à sauvegarder
                stats_to_save = {
                    "channels": {},
                    "global": {
                        "total_watch_time": self.global_stats["total_watch_time"],
                        "unique_viewers": list(self.global_stats["unique_viewers"]),
                        "last_update": self.global_stats["last_update"]
                    }
                }

                # Conversion des stats des chaînes
                for channel_name, channel_data in self.stats.items():
                    # Skip global stats, they're handled separately
                    if channel_name == "global":
                        continue
                        
                    # Ensure all required keys exist
                    channel_stats = {
                        "total_watch_time": channel_data.get("total_watch_time", 0),
                        "unique_viewers": list(channel_data.get("unique_viewers", set())),
                        "last_update": channel_data.get("last_update", time.time())
                    }
                    
                    # Add watchlist only if it exists
                    if "watchlist" in channel_data:
                        channel_stats["watchlist"] = channel_data["watchlist"]

                    stats_to_save["channels"][channel_name] = channel_stats

                # Sauvegarde dans le fichier principal
                with open(self.stats_file, "w") as f:
                    json.dump(stats_to_save, f, indent=2)

                # Sauvegarde d'une copie horodatée toutes les 24h
                now = time.time()
                last_daily = getattr(self, "last_daily_save", 0)
                if now - last_daily > 86400:  # 24h
                    date_str = time.strftime("%Y-%m-%d")
                    daily_file = self.stats_dir / f"channel_stats_{date_str}.json"
                    with open(daily_file, "w") as f:
                        json.dump(stats_to_save, f, indent=2)
                    self.last_daily_save = now
                    logger.info(f"📊 Sauvegarde quotidienne des stats: {daily_file}")

                logger.info(
                    f"📊 Statistiques sauvegardées ({len(self.stats)} chaînes, {len(self.global_stats['unique_viewers'])} spectateurs uniques)"
                )
                return True
            except Exception as e:
                logger.error(f"❌ Erreur sauvegarde stats: {e}")
                import traceback
                logger.error(traceback.format_exc())
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

    def cleanup_stats(self):
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
        """Arrête proprement le thread de sauvegarde"""
        try:
            logger.info("🛑 Arrêt du StatsCollector...")
            self.stop_save_thread.set()
            if hasattr(self, "save_thread") and self.save_thread.is_alive():
                self.save_thread.join(timeout=5)  # Attendre max 5 secondes
            # Dernière sauvegarde avant l'arrêt
            self.save_stats()
            self.save_user_stats()
            logger.info("✅ StatsCollector arrêté proprement")
        except Exception as e:
            logger.error(f"❌ Erreur lors de l'arrêt du StatsCollector: {e}")

    def _update_daily_stats(self, channel, ip, duration):
        """Met à jour les statistiques quotidiennes"""
        try:
            # Obtenir la date actuelle
            today = time.strftime("%Y-%m-%d")

            # Initialiser les stats pour aujourd'hui si nécessaire
            if today not in self.daily_stats:
                self.daily_stats[today] = {
                    "total_watch_time": 0.0,
                    "unique_viewers": set(),
                    "channels": {}
                }

            # Mise à jour des stats globales pour aujourd'hui
            daily_stats = self.daily_stats[today]
            daily_stats["total_watch_time"] += duration
            daily_stats["unique_viewers"].add(ip)

            # Initialiser les stats pour ce canal si nécessaire
            if channel not in daily_stats["channels"]:
                daily_stats["channels"][channel] = {
                    "total_watch_time": 0.0,
                    "unique_viewers": set()
                }

            # Mise à jour des stats du canal
            channel_stats = daily_stats["channels"][channel]
            channel_stats["total_watch_time"] += duration
            channel_stats["unique_viewers"].add(ip)

            # Log pour debug
            logger.debug(
                f"📅 Stats quotidiennes mises à jour pour {channel}:\n"
                f"  - Temps ajouté: {duration:.1f}s\n"
                f"  - Total canal: {channel_stats['total_watch_time']:.1f}s\n"
                f"  - Total journalier: {daily_stats['total_watch_time']:.1f}s"
            )

            # Mise à jour des stats dans les fichiers JSON
            current_time = time.time()
            if not hasattr(self, "last_save_time"):
                self.last_save_time = current_time
            elif current_time - self.last_save_time > 300:  # 5 minutes
                # Sauvegarde des stats des chaînes
                self.save_stats()
                # Sauvegarde des stats utilisateurs
                self.save_user_stats()
                self.last_save_time = current_time
                logger.info("💾 Sauvegarde périodique des stats effectuée")

        except Exception as e:
            logger.error(f"❌ Erreur mise à jour stats quotidiennes: {e}")
            import traceback
            logger.error(traceback.format_exc())

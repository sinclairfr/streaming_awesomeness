# stats_collector.py
import os
import json
import time
import threading
import re
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
        self.stats, self.global_stats = self._load_stats()
        self.user_stats = self._load_user_stats()

        # Démarrage du thread de sauvegarde périodique
        self.stop_save_thread = threading.Event()
        self.save_thread = threading.Thread(target=self._save_loop, daemon=True)
        self.save_thread.start()

        # Forcer une sauvegarde initiale des deux fichiers
        # self.save_stats()  # Remove initial save
        # self.save_user_stats() # Remove initial save

        logger.info(
            f"📊 StatsCollector initialisé (sauvegarde dans {self.stats_file}, user stats dans {self.user_stats_file})"
        )

        # Ajout des métriques de performance
        self.buffer_issues = {}  # Dictionnaire pour stocker les problèmes de buffer par chaîne
        self.latency_issues = {}  # Dictionnaire pour stocker les problèmes de latence par chaîne
        self.performance_metrics = {}  # Dictionnaire pour stocker les métriques de performance par chaîne

    def add_watch_time(self, channel, ip, duration):
        """Ajoute du temps de visionnage pour un watcher avec limitation de fréquence"""
        with self.lock:
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
                # Removed the block updating self.stats["global"] as it's redundant
                # Global stats are now calculated correctly in save_stats from self.global_stats

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

                logger.debug(f"✅ Stats utilisateurs sauvegardées dans {self.user_stats_file}")
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
            logger.debug(f"👤 Mise à jour stats utilisateur: {ip} sur {channel_name} (+{duration:.1f}s)")

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
            # Removed conditional save block:
            # if (
            #     not hasattr(self, "last_user_save")
            #     or time.time() - self.last_user_save > 300
            # ):
            #     threading.Thread(target=self.save_user_stats, daemon=True).start()
            #     self.last_user_save = time.time()
            #     logger.info("💾 Sauvegarde forcée des stats utilisateur après 5 minutes d'activité")

    def _load_stats(self):
        """Charge les stats existantes ou crée un nouveau fichier"""
        if self.stats_file.exists():
            try:
                with open(self.stats_file, "r") as f:
                    loaded_stats = json.load(f)
                    
                    # Conversion de la structure chargée vers la nouvelle structure
                    stats = {}
                    # Initialize global_stats with defaults, to be overwritten if found in file
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
                    
                    # Mise à jour des attributs - REMOVED, assignment happens in __init__
                    # self.stats = stats
                    # self.global_stats = global_stats
                    
                    logger.debug(f"📊 Stats chargées: {len(stats)} chaînes, {len(global_stats['unique_viewers'])} spectateurs uniques")
                    return stats, global_stats # Modified: Return both
                    
            except json.JSONDecodeError:
                logger.warning(f"⚠️ Fichier de stats corrompu, création d'un nouveau")

        # Structure initiale des stats en cas d'échec du chargement ou fichier inexistant
        default_global_stats = {
            "total_watch_time": 0.0,
            "unique_viewers": set(),
            "last_update": time.time()
        }
        return {}, default_global_stats # Modified: Return default structures for both

    def save_stats(self):
        """Sauvegarde les statistiques dans le fichier JSON"""
        with self.lock:
            try:
                # Calculate global statistics from all channels in self.stats
                total_watch_time = 0.0
                all_viewers = set()
                
                for channel_name, channel_data in self.stats.items():
                    # Removed check for channel_name == "global"
                    total_watch_time += channel_data.get("total_watch_time", 0)
                    all_viewers.update(channel_data.get("unique_viewers", set()))
                
                # Update self.global_stats before saving
                self.global_stats["total_watch_time"] = total_watch_time
                self.global_stats["unique_viewers"] = all_viewers
                self.global_stats["last_update"] = time.time()
                
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
                    # Removed check for channel_name == "global"
                        
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

                logger.debug(
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
            if channel_name not in self.stats:
                logger.warning(f"Attempting to update watchers for non-existent channel {channel_name}. Skipping.")
                return

            # Access channel stats directly
            channel_stats = self.stats[channel_name]

            # Ensure necessary keys exist
            if "current_watchers" not in channel_stats: channel_stats["current_watchers"] = 0
            if "peak_watchers" not in channel_stats: channel_stats["peak_watchers"] = 0
            if "peak_time" not in channel_stats: channel_stats["peak_time"] = 0
            if "session_count" not in channel_stats: channel_stats["session_count"] = 0

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
            # This section recalculates global watchers based on 'current_watchers'
            # But 'current_watchers' isn't reliably maintained elsewhere.
            # It's better to calculate global watchers based on active connections/sessions if needed.
            # For now, commenting out the potentially inaccurate global update.

            # total_current_watchers = sum(
            #     ch.get("current_watchers", 0) for ch_name, ch in self.stats.items()
            # )
            # # Ensure global stats exist
            # if "global" not in self.stats: # This check might be wrong, global stats are separate
            #     # self.stats["global"] = { ... } # Initialize if needed, but global stats are in self.global_stats
            #     logger.warning("Accessing self.stats['global'] which might not be intended structure.")

            # # Access global stats correctly (assuming self.global_stats is the right place)
            # if not hasattr(self, 'global_stats'): self.global_stats = {} # Initialize if missing
            # if "total_watchers" not in self.global_stats: self.global_stats["total_watchers"] = 0
            # if "peak_watchers" not in self.global_stats: self.global_stats["peak_watchers"] = 0
            # if "peak_time" not in self.global_stats: self.global_stats["peak_time"] = 0

            # self.global_stats["total_watchers"] = total_current_watchers

            # # Mise à jour du pic global si nécessaire
            # if (
            #     self.global_stats["total_watchers"]
            #     > self.global_stats["peak_watchers"]
            # ):
            #     self.global_stats["peak_watchers"] = self.global_stats[
            #         "total_watchers"
            #     ]
            #     self.global_stats["peak_time"] = int(time.time())

            # Mise à jour des stats quotidiennes
            # This also relies on potentially inaccurate global/current watchers.
            # Daily stats should ideally aggregate watch time and unique viewers from add_watch_time.
            # Commenting out for now.

            # today = time.strftime("%Y-%m-%d")
            # # Ensure daily stats structure exists
            # if not hasattr(self, 'daily_stats'): self.daily_stats = {}
            # if today not in self.daily_stats:
            #     self.daily_stats[today] = {
            #         "peak_watchers": 0,
            #         "total_watch_time": 0, # This should be aggregated from watch time updates
            #         "channels": {},
            #     }

            # daily_stats = self.daily_stats[today]

            # # Mise à jour du pic quotidien (using potentially inaccurate global watchers)
            # # if self.global_stats.get("total_watchers", 0) > daily_stats.get("peak_watchers", 0):
            # #     daily_stats["peak_watchers"] = self.global_stats["total_watchers"]

            # # Init des stats de la chaîne pour aujourd'hui
            # if channel_name not in daily_stats["channels"]:
            #     daily_stats["channels"][channel_name] = {
            #         "peak_watchers": 0,
            #         "total_watch_time": 0, # Should aggregate from watch time
            #     }

            # # Mise à jour du pic de la chaîne pour aujourd'hui (using potentially inaccurate watchers_count)
            # daily_channel = daily_stats["channels"][channel_name]
            # if watchers_count > daily_channel.get("peak_watchers", 0):
            #     daily_channel["peak_watchers"] = watchers_count

    def update_segment_stats(self, channel_name, segment_id, size):
        """Met à jour les stats de segments pour une chaîne"""
        try:
            with self.lock:
                # Init des stats pour cette chaîne si nécessaire
                if channel_name not in self.stats:
                    logger.warning(f"Attempting to update segment stats for non-existent channel {channel_name}. Skipping.")
                    return

                # Access channel stats directly
                channel_data = self.stats[channel_name]

                # Ensure necessary keys exist
                if "total_segments" not in channel_data: channel_data["total_segments"] = 0
                if "segment_history" not in channel_data: channel_data["segment_history"] = []

                # Mise à jour du nombre total de segments
                channel_data["total_segments"] += 1

                # Mise à jour des stats de segments
                segment_history = channel_data.get("segment_history", [])
                segment_history.append(
                    {"segment_id": segment_id, "size": size, "time": int(time.time())}
                )

                # On garde que les 100 derniers segments pour limiter la taille
                channel_data["segment_history"] = segment_history[-100:]

        except Exception as e:
            logger.error(f"❌ Erreur mise à jour stats segments: {e}")
            import traceback
            logger.error(traceback.format_exc())

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

        except Exception as e:
            logger.error(f"❌ Erreur mise à jour stats quotidiennes: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def handle_segment_request(self, channel, ip, line, user_agent):
        """Gère une requête de segment et met à jour les statistiques"""
        with self.lock:
            try:
                # Identifier le segment
                segment_match = re.search(r"segment_(\d+)\.ts", line)
                segment_id = segment_match.group(1) if segment_match else "unknown"

                # Ajouter du temps de visionnage
                segment_duration = 4.0  # secondes par segment
                self.add_watch_time(channel, ip, segment_duration)
                
                # Mettre à jour les stats utilisateur
                if user_agent:
                    self.update_user_stats(ip, channel, segment_duration, user_agent)

                # Mettre à jour les stats de segments
                self.update_segment_stats(channel, segment_id, 0)  # Taille inconnue pour l'instant

                logger.debug(f"[{channel}] 📊 Segment {segment_id} traité pour {ip}")

            except Exception as e:
                logger.error(f"❌ Erreur traitement segment: {e}")

    def handle_playlist_request(self, channel, ip, elapsed, user_agent):
        """Gère une requête de playlist et met à jour les statistiques"""
        with self.lock:
            try:
                # Ajouter du temps de visionnage
                self.add_watch_time(channel, ip, elapsed)
                
                # Mettre à jour les stats utilisateur
                if user_agent:
                    self.update_user_stats(ip, channel, elapsed, user_agent)

                logger.debug(f"[{channel}] 📊 Playlist traitée pour {ip} ({elapsed:.1f}s)")

            except Exception as e:
                logger.error(f"❌ Erreur traitement playlist: {e}")

    def handle_channel_change(self, ip, old_channel, new_channel):
        """Gère un changement de chaîne et met à jour les statistiques"""
        with self.lock:
            try:
                # Mettre à jour les stats de l'ancienne chaîne
                if old_channel:
                    self.update_channel_watchers(old_channel, 0)  # Réduire le compteur

                # Mettre à jour les stats de la nouvelle chaîne
                if new_channel:
                    self.update_channel_watchers(new_channel, 1)  # Augmenter le compteur

                logger.debug(f"🔄 Changement de chaîne traité: {ip} de {old_channel} vers {new_channel}")

            except Exception as e:
                logger.error(f"❌ Erreur traitement changement de chaîne: {e}")

    def record_buffer_issue(self, channel_name: str, delay: float, viewers: int):
        """Enregistre un problème de buffer pour une chaîne"""
        try:
            if channel_name not in self.buffer_issues:
                self.buffer_issues[channel_name] = []
                
            # Ajouter l'incident avec timestamp
            self.buffer_issues[channel_name].append({
                'timestamp': time.time(),
                'delay': delay,
                'viewers': viewers
            })
            
            # Garder seulement les 100 derniers incidents
            if len(self.buffer_issues[channel_name]) > 100:
                self.buffer_issues[channel_name].pop(0)
                
            # Mettre à jour les métriques de performance
            if channel_name not in self.performance_metrics:
                self.performance_metrics[channel_name] = {
                    'buffer_issues_count': 0,
                    'avg_buffer_delay': 0,
                    'max_buffer_delay': 0
                }
                
            metrics = self.performance_metrics[channel_name]
            metrics['buffer_issues_count'] += 1
            metrics['avg_buffer_delay'] = sum(issue['delay'] for issue in self.buffer_issues[channel_name]) / len(self.buffer_issues[channel_name])
            metrics['max_buffer_delay'] = max(issue['delay'] for issue in self.buffer_issues[channel_name])
            
            logger.warning(
                f"[{channel_name}] 📊 Problème de buffer enregistré: "
                f"délai={delay:.2f}s, viewers={viewers}"
            )
            
        except Exception as e:
            logger.error(f"❌ Erreur enregistrement problème buffer: {e}")
            
    def record_latency_issue(self, channel_name: str, latency: float, viewers: int):
        """Enregistre un problème de latence pour une chaîne"""
        try:
            if channel_name not in self.latency_issues:
                self.latency_issues[channel_name] = []
                
            # Ajouter l'incident avec timestamp
            self.latency_issues[channel_name].append({
                'timestamp': time.time(),
                'latency': latency,
                'viewers': viewers
            })
            
            # Garder seulement les 100 derniers incidents
            if len(self.latency_issues[channel_name]) > 100:
                self.latency_issues[channel_name].pop(0)
                
            # Mettre à jour les métriques de performance
            if channel_name not in self.performance_metrics:
                self.performance_metrics[channel_name] = {
                    'latency_issues_count': 0,
                    'avg_latency': 0,
                    'max_latency': 0
                }
                
            metrics = self.performance_metrics[channel_name]
            metrics['latency_issues_count'] += 1
            metrics['avg_latency'] = sum(issue['latency'] for issue in self.latency_issues[channel_name]) / len(self.latency_issues[channel_name])
            metrics['max_latency'] = max(issue['latency'] for issue in self.latency_issues[channel_name])
            
            logger.warning(
                f"[{channel_name}] 📊 Problème de latence enregistré: "
                f"latence={latency:.2f}s, viewers={viewers}"
            )
            
        except Exception as e:
            logger.error(f"❌ Erreur enregistrement problème latence: {e}")
            
    def get_performance_metrics(self, channel_name: str = None):
        """Récupère les métriques de performance pour une chaîne ou toutes les chaînes"""
        try:
            if channel_name:
                return self.performance_metrics.get(channel_name, {})
            return self.performance_metrics
        except Exception as e:
            logger.error(f"❌ Erreur récupération métriques performance: {e}")
            return {}

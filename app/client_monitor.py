import os
import time
import threading
from pathlib import Path
from config import logger
from typing import Dict, Tuple, List, Set
import re
from config import TIMEOUT_NO_VIEWERS


class ClientMonitor(threading.Thread):
    def __init__(self, log_path, update_watchers_callback, manager):
        super().__init__(daemon=True)
        self.log_path = log_path
        self.update_watchers = update_watchers_callback
        self.manager = manager
        self.watchers = {}  # {(channel, ip): last_seen_time}
        self.segments_by_channel = {}  # {channel: {segment_id: last_requested_time}}
        self.lock = threading.Lock()

        # On augmente le seuil d'inactivité pour éviter les déconnexions trop rapides
        self.inactivity_threshold = TIMEOUT_NO_VIEWERS

        # Thread de nettoyage
        self.cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self.cleanup_thread.start()

        # Nouveau thread pour surveiller les sauts de segments
        self.segment_monitor_thread = threading.Thread(
            target=self._monitor_segment_jumps, daemon=True
        )
        self.segment_monitor_thread.start()

    def _cleanup_loop(self):
        """Nettoie les watchers inactifs plus fréquemment"""
        while True:
            time.sleep(10)
            self._cleanup_inactive()

    def _cleanup_inactive(self):
        """Nettoie les watchers inactifs avec un délai plus strict"""
        now = time.time()
        to_remove = []

        with self.lock:
            # Critères de nettoyage plus stricts
            for (channel, ip), data in self.watchers.items():
                if isinstance(data, dict):
                    watcher_time = data.get("time", 0)
                    watcher_type = data.get("type", "unknown")
                else:
                    watcher_time = data
                    watcher_type = "unknown"

                # Différents timeouts selon le type de requête
                if (
                    (watcher_type == "segment" and now - watcher_time > 40)
                    or (watcher_type == "playlist" and now - watcher_time > 20)
                    or (watcher_type == "unknown" and now - watcher_time > 30)
                ):
                    to_remove.append((channel, ip))

            affected_channels = set()

            for key in to_remove:
                channel, ip = key
                if key in self.watchers:  # Vérification supplémentaire
                    data = self.watchers[key]
                    if isinstance(data, dict):
                        watcher_time = data.get("time", 0)
                    else:
                        watcher_time = data

                    del self.watchers[key]
                    affected_channels.add(channel)
                    logger.info(
                        f"🗑️ Watcher supprimé: {ip} -> {channel} (inactif depuis {now - watcher_time:.1f}s)"
                    )

            # Pour chaque chaîne affectée, recalculer le nombre exact de watchers
            for channel in affected_channels:
                # Comptage précis par type de requête
                now = time.time()
                segment_watchers = set()
                playlist_watchers = set()

                for (ch, watcher_ip), data in self.watchers.items():
                    if ch != channel:
                        continue

                    if isinstance(data, dict):
                        watcher_time = data.get("time", 0)
                        watcher_type = data.get("type", "unknown")
                    else:
                        watcher_time = data
                        watcher_type = "unknown"

                    if watcher_type == "segment" and now - watcher_time < 40:
                        segment_watchers.add(watcher_ip)
                    elif watcher_type == "playlist" and now - watcher_time < 20:
                        playlist_watchers.add(watcher_ip)

                # Un watcher est actif s'il a demandé soit un segment récemment, soit une playlist
                active_ips = segment_watchers.union(playlist_watchers)
                count = len(active_ips)

                # Mise à jour explicite du compteur
                if count != self.get_channel_watchers(channel):
                    logger.info(
                        f"[{channel}] 🔄 Mise à jour forcée: {count} watchers actifs (seg: {len(segment_watchers)}, pl: {len(playlist_watchers)})"
                    )
                    self.update_watchers(channel, count, "/hls/")

    def _monitor_segment_jumps(self):
        """Surveille les sauts anormaux dans les segments"""
        while True:
            try:
                with self.lock:
                    for channel, segments in self.segments_by_channel.items():
                        if len(segments) < 2:
                            continue

                        # Récupération des segments ordonnés par ID
                        ordered_segments = sorted(
                            [
                                (int(seg_id), ts)
                                for seg_id, ts in segments.items()
                                if seg_id.isdigit()
                            ],
                            key=lambda x: x[0],
                        )

                        # Vérification des sauts
                        for i in range(1, len(ordered_segments)):
                            current_id, current_ts = ordered_segments[i]
                            prev_id, prev_ts = ordered_segments[i - 1]

                            # Si le saut est supérieur à 5 segments et récent (< 20 secondes)
                            if (
                                current_id - prev_id > 5
                                and time.time() - current_ts < 20
                            ):
                                logger.warning(
                                    f"⚠️ Saut détecté pour {channel}: segment {prev_id} → {current_id} "
                                    f"(saut de {current_id - prev_id} segments)"
                                )

                                # On peut notifier ici ou prendre des mesures
                                channel_obj = self.manager.channels.get(channel)
                                if channel_obj and hasattr(
                                    channel_obj, "report_segment_jump"
                                ):
                                    channel_obj.report_segment_jump(prev_id, current_id)
            except Exception as e:
                logger.error(f"❌ Erreur surveillance segments: {e}")

            time.sleep(10)  # Vérification toutes les 10 secondes

    def _check_log_file(self):
        """Vérifie périodiquement l'état du fichier de log et force une reconnexion si nécessaire"""
        while True:
            try:
                # Vérifier si le fichier existe
                if not os.path.exists(self.log_path):
                    logger.error(
                        f"❌ Log file introuvable lors de la vérification périodique: {self.log_path}"
                    )
                    # Forcer la reconnexion
                    self._force_reconnect = True

                # Vérifier la taille et les permissions
                try:
                    file_info = os.stat(self.log_path)
                    file_size = file_info.st_size

                    # Si le fichier est vide ou trop petit
                    if file_size < 10:
                        logger.warning(
                            f"⚠️ Fichier log anormalement petit: {file_size} bytes"
                        )

                    # Vérifier les permissions
                    if not os.access(self.log_path, os.R_OK):
                        logger.error(f"❌ Permissions insuffisantes sur le fichier log")
                        self._force_reconnect = True

                except Exception as e:
                    logger.error(f"❌ Erreur vérification stats fichier log: {e}")

                # Vérifier l'activité Nginx
                try:
                    nginx_running = False
                    for proc in psutil.process_iter(["name"]):
                        if "nginx" in proc.info["name"]:
                            nginx_running = True
                            break

                    if not nginx_running:
                        logger.critical(
                            f"🚨 Nginx semble ne pas être en cours d'exécution!"
                        )
                        # Tenter de voir si le containeur est en cours d'exécution
                        result = subprocess.run(
                            ["docker", "ps", "--filter", "name=iptv-nginx"],
                            capture_output=True,
                            text=True,
                        )
                        logger.info(f"État containeur Nginx: {result.stdout}")
                except Exception as e:
                    logger.error(f"❌ Erreur vérification Nginx: {e}")

            except Exception as e:
                logger.error(f"❌ Erreur dans _check_log_file: {e}")

            time.sleep(60)  # Vérification toutes les minutes

    def get_channel_watchers(self, channel):
        """Récupère le nombre actuel de watchers pour une chaîne"""
        if hasattr(self.manager, "channels") and channel in self.manager.channels:
            return getattr(self.manager.channels[channel], "watchers_count", 0)
        return 0

    def _parse_access_log(self, line):
        """
        Analyse une ligne de log nginx plus précisément pour détecter l'activité
        Retourne (ip, channel, request_type, is_valid_request)
        """
        try:
            # Format typique: IP - - [date] "GET /hls/channel/playlist.m3u8 HTTP/1.1" 200 ...
            parts = line.split()
            if len(parts) < 7:
                return None, None, None, False

            ip = parts[0]

            # Extraire la requête (entre guillemets)
            request_raw = " ".join(parts[5:8]).strip('"')
            request_parts = request_raw.split()

            if len(request_parts) < 2 or not "/hls/" in request_parts[1]:
                return None, None, None, False

            request_path = request_parts[1]
            status_code = parts[8] if len(parts) > 8 else "???"

            # Déterminer le type de requête
            request_type = "unknown"
            if request_path.endswith(".m3u8"):
                request_type = "playlist"
            elif request_path.endswith(".ts"):
                request_type = "segment"

            # Extraire le nom de la chaîne
            import re

            match = re.search(r"/hls/([^/]+)/", request_path)
            if not match:
                return ip, None, request_type, False

            channel = match.group(1)

            # Considérer valide uniquement les requêtes avec statut 200 ou 206
            is_valid = status_code in ["200", "206"]

            return ip, channel, request_type, is_valid
        except Exception as e:
            logger.error(f"❌ Erreur analyse log: {e}")
            return None, None, None, False

    def run(self):
        """On surveille les requêtes clients"""
        logger.info("👀 Démarrage de la surveillance des requêtes...")

        retry_count = 0
        max_retries = 5

        while True:
            try:
                # Vérifier si le fichier existe
                if not os.path.exists(self.log_path):
                    logger.error(f"❌ Log file introuvable: {self.log_path}")
                    # Attendre et réessayer
                    time.sleep(10)
                    retry_count += 1
                    if retry_count > max_retries:
                        logger.critical(
                            f"❌ Impossible de trouver le fichier de log après {max_retries} tentatives"
                        )
                        # Redémarrage forcé du monitoring
                        time.sleep(30)
                        retry_count = 0
                    continue

                logger.info(f"📖 Ouverture du log: {self.log_path}")

                # Tester si le fichier est accessible en lecture
                try:
                    with open(self.log_path, "r") as test_file:
                        last_pos = test_file.seek(0, 2)  # Se positionne à la fin
                        logger.info(
                            f"✅ Fichier de log accessible, taille: {last_pos} bytes"
                        )
                except Exception as e:
                    logger.error(f"❌ Impossible de lire le fichier de log: {e}")
                    time.sleep(10)
                    continue

                with open(self.log_path, "r") as f:
                    # Se positionne à la fin du fichier
                    f.seek(0, 2)

                    # Log pour debug
                    logger.info(f"👁️ Monitoring actif sur {self.log_path}")
                    retry_count = 0

                    last_activity_time = time.time()
                    last_heartbeat_time = time.time()
                    last_cleanup_time = time.time()

                    while True:
                        line = f.readline().strip()
                        current_time = time.time()

                        # Vérification périodique pour nettoyer les watchers inactifs
                        if (
                            current_time - last_cleanup_time > 10
                        ):  # Tous les 10 secondes
                            self._cleanup_inactive()
                            last_cleanup_time = current_time

                        # Heartbeat périodique pour s'assurer que le monitoring est actif
                        if (
                            current_time - last_heartbeat_time > 60
                        ):  # Toutes les minutes
                            logger.info(
                                f"💓 ClientMonitor actif, dernière activité il y a {current_time - last_activity_time:.1f}s"
                            )
                            last_heartbeat_time = current_time

                            # Vérification périodique du fichier
                            try:
                                if not os.path.exists(self.log_path):
                                    logger.error(
                                        f"❌ Fichier log disparu: {self.log_path}"
                                    )
                                    break

                                # Vérification que le fichier n'a pas été tronqué
                                current_pos = f.tell()
                                file_size = os.path.getsize(self.log_path)
                                if current_pos > file_size:
                                    logger.error(
                                        f"❌ Fichier log tronqué: position {current_pos}, taille {file_size}"
                                    )
                                    break
                            except Exception as e:
                                logger.error(f"❌ Erreur vérification fichier log: {e}")
                                break

                        if not line:
                            # Si pas d'activité depuis longtemps, forcer une relecture des dernières lignes
                            if current_time - last_activity_time > 300:  # 5 minutes
                                logger.warning(
                                    f"⚠️ Pas d'activité depuis 5 minutes, relecture forcée des dernières lignes"
                                )
                                f.seek(
                                    max(0, f.tell() - 10000)
                                )  # Retour en arrière de 10Ko
                                last_activity_time = current_time

                            time.sleep(0.1)  # Réduit la charge CPU
                            continue

                        # Une ligne a été lue, mise à jour du temps d'activité
                        last_activity_time = current_time

                        # Analyse plus précise de la ligne
                        ip, channel, request_type, is_valid = self._parse_access_log(
                            line
                        )

                        if not channel or not is_valid:
                            continue  # Ignorer les requêtes invalides ou sans chaîne

                        request_path = (
                            request_type  # Pour compatibilité avec le code existant
                        )

                        # Log détaillé pour debug
                        logger.debug(
                            f"🔍 [{channel}] {request_type} ({ip}) - Valide: {is_valid}"
                        )

                        with self.lock:
                            # Mise à jour du timestamp pour ce watcher
                            if channel and ip:
                                # Stockage précis du type de requête et du temps
                                self.watchers[(channel, ip)] = {
                                    "time": time.time(),
                                    "type": request_type,
                                }

                                # Si c'est une requête de segment, on l'enregistre
                                if request_type == "segment":
                                    segment_match = re.search(
                                        r"segment_(\d+)\.ts", line
                                    )
                                    segment_id = (
                                        segment_match.group(1)
                                        if segment_match
                                        else "unknown"
                                    )

                                    self.segments_by_channel.setdefault(channel, {})[
                                        segment_id
                                    ] = time.time()

                                    # Ajouter du temps de visionnage (chaque segment = ~4 secondes)
                                    if hasattr(self.manager, "stats_collector"):
                                        self.manager.stats_collector.add_watch_time(
                                            channel, ip, 4
                                        )

                                # Si c'est une requête de playlist, considérer comme un heartbeat
                                if request_type == "playlist":
                                    # Heartbeat playlist = ~0.5 secondes de visionnage (rafraîchissement)
                                    if hasattr(self.manager, "stats_collector"):
                                        self.manager.stats_collector.add_watch_time(
                                            channel, ip, 0.5
                                        )

                                # Calcul des watchers vraiment actifs pour ce channel
                                # Un watcher est considéré actif s'il a demandé un segment dans les 20 dernières secondes
                                # ou une playlist dans les 10 dernières secondes
                                active_watchers = 0
                                now = time.time()
                                active_ips = set()

                                for (ch, watcher_ip), data in self.watchers.items():
                                    if ch != channel:
                                        continue

                                    watcher_time = (
                                        data["time"] if isinstance(data, dict) else data
                                    )
                                    watcher_type = (
                                        data.get("type", "unknown")
                                        if isinstance(data, dict)
                                        else "unknown"
                                    )

                                    # Règles de timeout plus strictes:
                                    # - Segments: 30s max (un client doit demander un segment au moins toutes les 30s)
                                    # - Playlist: 15s max (un client demande typiquement la playlist toutes les 2-10s)
                                    if (
                                        watcher_type == "segment"
                                        and now - watcher_time < 30
                                    ) or (
                                        watcher_type == "playlist"
                                        and now - watcher_time < 15
                                    ):
                                        if watcher_ip not in active_ips:
                                            active_ips.add(watcher_ip)
                                            active_watchers += 1

                                # Mise à jour du compteur uniquement si ça a changé
                                if active_watchers != self.get_channel_watchers(
                                    channel
                                ):
                                    logger.info(
                                        f"[{channel}] 👁️ MAJ watchers: {active_watchers} (analyse précise)"
                                    )
                                    self.update_watchers(
                                        channel, active_watchers, request_path
                                    )

            except Exception as e:
                logger.error(f"❌ Erreur fatale dans client_monitor: {e}")
                import traceback

                logger.error(traceback.format_exc())

                # Attente avant de réessayer
                time.sleep(10)

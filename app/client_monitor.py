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

        # Callback pour mettre à jour les watchers dans le manager
        self.update_watchers = update_watchers_callback
        # Manager pour accéder aux segments et aux canaux
        self.manager = manager

        # dictionnaire pour stocker les watchers actifs
        self.watchers = {}  # {(channel, ip): last_seen_time}

        # dictionnaire pour stocker les segments demandés par chaque canal
        self.segments_by_channel = {}  # {channel: {segment_id: last_requested_time}}

        # Pour éviter les accès concurrents
        self.lock = threading.Lock()

        self.inactivity_threshold = TIMEOUT_NO_VIEWERS

        # Pour surveiller les sauts de segments
        self.segment_monitor_thread = threading.Thread(
            target=self._monitor_segment_jumps, daemon=True
        )
        self.segment_monitor_thread.start()

        # Thread de nettoyage
        self.cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self.cleanup_thread.start()

        # self.log_check_thread.start()

    def _cleanup_loop(self):
        """Nettoie les watchers inactifs plus fréquemment"""
        while True:
            time.sleep(10)
            self._cleanup_inactive()

    def _cleanup_inactive(self):
        """Version simplifiée du nettoyage"""
        now = time.time()
        timeout = 60  # Un seul timeout de 60 secondes, point.

        to_remove = []
        affected_channels = set()

        with self.lock:
            # Trouver les watchers inactifs
            for (channel, ip), last_seen in self.watchers.items():
                if now - last_seen > timeout:
                    to_remove.append((channel, ip))
                    affected_channels.add(channel)

            # Supprimer les watchers inactifs
            for key in to_remove:
                if key in self.watchers:
                    logger.info(f"🗑️ Watcher supprimé: {key[1]} → {key[0]}")
                    del self.watchers[key]

            # Recalculer le nombre de watchers pour chaque chaîne affectée
            for channel in affected_channels:
                active_ips = set()
                for (ch, ip), last_seen in self.watchers.items():
                    if ch == channel and now - last_seen < timeout:
                        active_ips.add(ip)

                count = len(active_ips)
                self.update_watchers(channel, count, "/hls/")

    def _monitor_segment_jumps(self):
        """Surveille les sauts anormaux dans les segments et ainsi detectés les bugs"""
        # Boucle infinie pour surveiller les segments
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

    def get_channel_watchers(self, channel):
        """Récupère le nombre actuel de watchers pour une chaîne"""
        if hasattr(self.manager, "channels") and channel in self.manager.channels:
            return getattr(self.manager.channels[channel], "watchers_count", 0)
        return 0

    def _parse_access_log(self, line):
        """Version simplifiée qui extrait seulement l'essentiel"""
        # Si pas de /hls/ dans la ligne, on ignore direct
        if "/hls/" not in line:
            return None, None, None, False, None

        # Si pas un GET ou un HEAD, on ignore
        if not ("GET /hls/" in line or "HEAD /hls/" in line):
            return None, None, None, False, None

        # Extraction IP (en début de ligne)
        ip = line.split(" ")[0]

        # Extraction du canal (format: /hls/CHANNEL/...)
        channel = None
        parts = line.split("/hls/")
        if len(parts) > 1:
            channel_part = parts[1].split("/")[0]
            if channel_part:
                channel = channel_part

        # Type de requête
        request_type = (
            "playlist" if ".m3u8" in line else "segment" if ".ts" in line else "unknown"
        )

        # Statut HTTP (format: " 200 ")
        status_code = "???"
        status_match = re.search(r'" (\d{3}) ', line)
        if status_match:
            status_code = status_match.group(1)

        # Validité
        is_valid = status_code in ["200", "206"]

        # Log plus détaillé pour debug
        if channel and is_valid:
            logger.info(
                f"📱 Client actif: {ip} → {channel} ({request_type}) - Status: {status_code}"
            )

        return ip, channel, request_type, is_valid, None

    def _check_log_file_exists(self, retry_count, max_retries):
        """Vérifie si le fichier de log existe et est accessible"""
        if not os.path.exists(self.log_path):
            logger.error(f"❌ Log file introuvable: {self.log_path}")

            # Abandon après trop d'essais
            if retry_count > max_retries:
                logger.critical(
                    f"❌ Impossible de trouver le fichier de log après {max_retries} tentatives"
                )
                # Redémarrage forcé du monitoring
                time.sleep(30)
                return False
            return False

        # Test d'accès en lecture
        try:
            with open(self.log_path, "r") as test_file:
                last_pos = test_file.seek(0, 2)  # Se positionne à la fin
                logger.info(f"✅ Fichier de log accessible, taille: {last_pos} bytes")
        except Exception as e:
            logger.error(f"❌ Impossible de lire le fichier de log: {e}")
            return False

        return True

    def _periodic_scan_thread(self):
        """Thread dédié au scan initial uniquement"""
        try:
            # Attente initiale pour laisser le système démarrer
            time.sleep(20)

            # Un seul scan complet au démarrage
            logger.info("🔄 Scan initial des chaînes...")
            self.scan_channels(force=True)

            # Configuration unique de l'observateur ready_to_stream
            self._setup_ready_observer()

            # Ensuite, on bloque jusqu'à l'arrêt
            self.scan_thread_stop.wait()

        except Exception as e:
            logger.error(f"❌ Erreur dans le thread de scan: {e}")

        logger.info("🛑 Thread de scan arrêté")

    def _monitor_log_file(self):
        """Lit et traite le fichier de log ligne par ligne"""
        with open(self.log_path, "r") as f:
            # Se positionne à la fin du fichier
            f.seek(0, 2)

            # Log pour debug
            logger.info(f"👁️ Monitoring actif sur {self.log_path}")

            last_activity_time = time.time()
            last_heartbeat_time = time.time()
            last_cleanup_time = time.time()

            while True:
                line = f.readline().strip()
                current_time = time.time()

                # Vérifications périodiques
                if self._handle_periodic_tasks(
                    current_time,
                    last_cleanup_time,
                    last_heartbeat_time,
                    last_activity_time,
                    f,
                ):
                    last_cleanup_time = current_time
                    last_heartbeat_time = current_time

                # Si pas de nouvelle ligne, attendre
                if not line:
                    # Vérifier l'inactivité prolongée
                    if current_time - last_activity_time > 300:  # 5 minutes
                        f.seek(max(0, f.tell() - 10000))  # Retour en arrière de 10Ko
                        last_activity_time = current_time
                        logger.warning(
                            f"⚠️ Pas d'activité depuis 5 minutes, relecture forcée des dernières lignes"
                        )

                    time.sleep(0.1)  # Réduit la charge CPU
                    continue

                # Une ligne a été lue, mise à jour du temps d'activité
                last_activity_time = current_time

                # Traiter la ligne
                self._process_log_line(line)

    def _process_log_line(self, line):
        """Traite une ligne du log de façon simple et brutale"""
        # Parse la ligne
        ip, channel, request_type, is_valid, _ = self._parse_access_log(line)

        # Si pas valide, on sort
        if not is_valid or not channel:
            return

        # MAJ du compteur directement
        try:
            # On stocke simplement l'heure actuelle
            current_time = time.time()

            with self.lock:
                # On stocke l'heure pour ce watcher
                self.watchers[(channel, ip)] = current_time

                # Pour chaque chaîne, on compte les watchers actifs (dernier accès < 60 secondes)
                active_ips = set()
                for (ch, watcher_ip), last_seen in self.watchers.items():
                    if ch == channel and current_time - last_seen < 60:
                        active_ips.add(watcher_ip)

                # Nombre de watchers pour cette chaîne
                count = len(active_ips)

                # MAJ forcée du compteur de watchers
                logger.info(
                    f"[{channel}] 👁️ MAJ watchers: {count} watchers actifs - {list(active_ips)}"
                )
                self.update_watchers(channel, count, "/hls/")

        except Exception as e:
            logger.error(f"❌ Erreur traitement ligne: {e}")

    def _handle_periodic_tasks(
        self,
        current_time,
        last_cleanup_time,
        last_heartbeat_time,
        last_activity_time,
        file_handle,
    ):
        """Gère les tâches périodiques (nettoyage, heartbeat, vérification fichier)"""
        tasks_executed = False

        # Nettoyage périodique des watchers inactifs
        if current_time - last_cleanup_time > 10:  # Tous les 10 secondes
            self._cleanup_inactive()
            tasks_executed = True

        # Heartbeat périodique
        if current_time - last_heartbeat_time > 60:  # Toutes les minutes
            logger.info(
                f"💓 ClientMonitor actif, dernière activité il y a {current_time - last_activity_time:.1f}s"
            )
            tasks_executed = True

            # Vérification périodique du fichier
            if not self._verify_log_file_integrity(file_handle):
                return False

        return tasks_executed

    def _verify_log_file_integrity(self, file_handle):
        """Vérifie que le fichier de log est toujours valide"""
        try:
            # Vérifier que le fichier existe toujours
            if not os.path.exists(self.log_path):
                logger.error(f"❌ Fichier log disparu: {self.log_path}")
                return False

            # Vérifier que le fichier n'a pas été tronqué
            current_pos = file_handle.tell()
            file_size = os.path.getsize(self.log_path)
            if current_pos > file_size:
                logger.error(
                    f"❌ Fichier log tronqué: position {current_pos}, taille {file_size}"
                )
                return False

            return True
        except Exception as e:
            logger.error(f"❌ Erreur vérification fichier log: {e}")
            return False

    def _update_watcher(self, ip, channel, request_type, user_agent, line):
        """Met à jour les informations d'un watcher spécifique"""
        with self.lock:
            # Stockage précis du type de requête et du temps
            self.watchers[(channel, ip)] = {
                "time": time.time(),
                "type": request_type,
            }

            # Log pour debug
            logger.debug(f"🔍 Requête: {ip} → {channel} ({request_type})")

            # Si c'est une requête de segment, on l'enregistre
            if request_type == "segment":
                self._handle_segment_request(channel, ip, line, user_agent)
            # Pour les playlists, considérer comme un heartbeat
            elif request_type == "playlist":
                self._handle_playlist_request(channel, ip)

            # Mise à jour du compteur global
            self._update_channel_watchers_count(channel)

    def _handle_segment_request(self, channel, ip, line, user_agent):
        """Traite une requête de segment"""
        segment_match = re.search(r"segment_(\d+)\.ts", line)
        segment_id = segment_match.group(1) if segment_match else "unknown"

        # Enregistrer le segment dans l'historique
        self.segments_by_channel.setdefault(channel, {})[segment_id] = time.time()

        # Ajouter du temps de visionnage (chaque segment = ~4 secondes)
        if hasattr(self.manager, "stats_collector"):
            segment_duration = 4  # Durée approximative en secondes
            self.manager.stats_collector.add_watch_time(channel, ip, segment_duration)
            # Mise à jour des stats utilisateur si disponible
            if user_agent:
                self.manager.stats_collector.update_user_stats(
                    ip, channel, segment_duration, user_agent
                )

    def _handle_playlist_request(self, channel, ip):
        """Traite une requête de playlist"""
        # Heartbeat playlist = ~0.5 secondes de visionnage
        if hasattr(self.manager, "stats_collector"):
            self.manager.stats_collector.add_watch_time(channel, ip, 0.5)

    def _update_channel_watchers_count(self, channel):
        """Calcule et met à jour le nombre de watchers actifs pour une chaîne"""
        # Calcul des watchers actifs par IP
        active_ips = set()

        for (ch, watcher_ip), data in self.watchers.items():
            if ch != channel:
                continue

            # On considère un watcher actif s'il a une activité récente
            watcher_time = data["time"] if isinstance(data, dict) else data
            watcher_type = (
                data.get("type", "unknown") if isinstance(data, dict) else "unknown"
            )

            # Différents délais selon le type de requête
            if (
                (watcher_type == "segment" and time.time() - watcher_time < 60)
                or (watcher_type == "playlist" and time.time() - watcher_time < 30)
                or (watcher_type == "unknown" and time.time() - watcher_time < 45)
            ):
                active_ips.add(watcher_ip)

        active_watchers = len(active_ips)

        # Mise à jour du compteur uniquement si ça a changé
        current_watchers = self.get_channel_watchers(channel)
        if active_watchers != current_watchers:
            logger.info(
                f"[{channel}] 👁️ MAJ watchers: {active_watchers} (actifs: {list(active_ips)})"
            )
            self.update_watchers(channel, active_watchers, "/hls/")

    def _prepare_log_file(self):
        """Vérifie que le fichier de log existe et est accessible"""
        if not os.path.exists(self.log_path):
            logger.error(f"❌ Log file introuvable: {self.log_path}")
            return False

        # Test d'accès en lecture
        try:
            with open(self.log_path, "r") as test_file:
                last_pos = test_file.seek(0, 2)  # Se positionne à la fin
                logger.info(f"✅ Fichier de log accessible, taille: {last_pos} bytes")
                return True
        except Exception as e:
            logger.error(f"❌ Impossible de lire le fichier de log: {e}")
            return False

    def _follow_log_file(self):
        """Lit et traite le fichier de log ligne par ligne"""
        with open(self.log_path, "r") as f:
            # Se positionne à la fin du fichier
            f.seek(0, 2)

            logger.info(f"👁️ Monitoring actif sur {self.log_path}")

            last_activity_time = time.time()
            last_heartbeat_time = time.time()
            last_cleanup_time = time.time()

            while True:
                line = f.readline().strip()
                current_time = time.time()

                # Tâches périodiques
                if current_time - last_cleanup_time > 10:
                    self._cleanup_inactive()
                    last_cleanup_time = current_time

                if current_time - last_heartbeat_time > 60:
                    logger.info(
                        f"💓 ClientMonitor actif, dernière activité il y a {current_time - last_activity_time:.1f}s"
                    )
                    last_heartbeat_time = current_time

                    if not self._check_file_integrity(f):
                        break

                # Si pas de nouvelle ligne, attendre
                if not line:
                    # Si inactivité prolongée, relire du début
                    if current_time - last_activity_time > 300:
                        f.seek(max(0, f.tell() - 10000))
                        last_activity_time = current_time

                    time.sleep(0.1)
                    continue

                # Une ligne a été lue
                last_activity_time = current_time

                # Traiter la ligne
                self._process_log_line(line)

    def _check_file_integrity(self, file_handle):
        """Vérifie que le fichier log n'a pas été tronqué ou supprimé"""
        try:
            if not os.path.exists(self.log_path):
                logger.error(f"❌ Fichier log disparu: {self.log_path}")
                return False

            current_pos = file_handle.tell()
            file_size = os.path.getsize(self.log_path)
            if current_pos > file_size:
                logger.error(
                    f"❌ Fichier log tronqué: position {current_pos}, taille {file_size}"
                )
                return False

            return True
        except Exception as e:
            logger.error(f"❌ Erreur vérification fichier log: {e}")
            return False

    def _update_watcher_info(self, ip, channel, request_type, user_agent, line):
        """Met à jour les informations d'un watcher"""
        with self.lock:
            current_time = time.time()

            # Mise à jour du watcher dans le dict
            self.watchers[(channel, ip)] = {
                "time": current_time,
                "type": request_type,
                "user_agent": user_agent,
            }

            logger.debug(f"🔍 Traitant requête: {ip} → {channel} ({request_type})")

            # Gestion specifique par type de requête
            if request_type == "segment":
                self._handle_segment_request(ip, channel, line, user_agent)
            elif request_type == "playlist":
                # Ajouter seulement un petit temps de visionnage
                if hasattr(self.manager, "stats_collector"):
                    self.manager.stats_collector.add_watch_time(channel, ip, 0.5)

            # CORRECTION IMPORTANTE: Mise à jour du nombre de watchers
            self._update_watcher_count(channel)

    def _handle_segment_request(self, ip, channel, line, user_agent):
        """Traite une requête de segment"""
        # Identifier le segment
        segment_match = re.search(r"segment_(\d+)\.ts", line)
        segment_id = segment_match.group(1) if segment_match else "unknown"

        # Enregistrer le segment
        self.segments_by_channel.setdefault(channel, {})[segment_id] = time.time()

        # Ajouter du temps de visionnage (4 secondes par segment environ)
        duration = 4.0  # secondes
        if hasattr(self.manager, "stats_collector"):
            self.manager.stats_collector.add_watch_time(channel, ip, duration)
            if user_agent:
                self.manager.stats_collector.update_user_stats(
                    ip, channel, duration, user_agent
                )

    def _update_watcher_count(self, channel):
        """Calcule et met à jour le nombre de watchers pour une chaîne"""
        # Trouver toutes les IPs actives pour cette chaîne
        active_ips = set()
        current_time = time.time()

        for (ch, ip), data in self.watchers.items():
            if ch != channel:
                continue

            # Extraire les infos
            if isinstance(data, dict):
                last_time = data.get("time", 0)
                req_type = data.get("type", "unknown")
            else:
                last_time = data
                req_type = "unknown"

            # Différents timeouts selon le type
            if (
                (req_type == "segment" and current_time - last_time < 60)
                or (req_type == "playlist" and current_time - last_time < 30)
                or (req_type == "unknown" and current_time - last_time < 45)
            ):
                active_ips.add(ip)

        # Nombre de watchers
        watcher_count = len(active_ips)

        # CORRECTION ICI: Mise à jour forcée même si le compte n'a pas changé
        # Plus de logs pour debug
        logger.info(
            f"[{channel}] 👁️ Watchers: {watcher_count} actifs - IPs: {list(active_ips)}"
        )

        # Mise à jour dans le manager
        self.update_watchers(channel, watcher_count, "/hls/")

    def run(self):
        """Démarre le monitoring basé sur inotify plutôt que des vérifications périodiques"""
        logger.info("👀 Démarrage de la surveillance des requêtes en temps réel...")

        try:
            # Vérification du fichier log
            if not self._prepare_log_file():
                logger.error(
                    "❌ Impossible d'accéder au fichier log, utilisation du mode fallback"
                )
                self._follow_log_file_legacy()
                return

            # Si pyinotify est disponible, on l'utilise
            try:
                import pyinotify

                # On initialise le watcher sur le fichier
                wm = pyinotify.WatchManager()

                class LogHandler(pyinotify.ProcessEvent):
                    def __init__(self, monitor):
                        self.monitor = monitor

                    def process_IN_MODIFY(self, event):
                        # Fichier modifié, on traite les nouvelles lignes
                        self.monitor.process_new_log_lines()

                # Démarrage du monitoring
                handler = LogHandler(self)
                notifier = pyinotify.Notifier(wm, handler)

                # Ajout du fichier à surveiller
                wm.add_watch(self.log_path, pyinotify.IN_MODIFY)

                logger.info(
                    f"📡 Surveillance en temps réel activée sur {self.log_path}"
                )

                # Boucle de surveillance (bloquante)
                notifier.loop()

            except ImportError:
                logger.warning(
                    "⚠️ Module pyinotify non disponible, utilisation du mode traditionnel"
                )
                self._follow_log_file_legacy()

        except Exception as e:
            logger.error(f"❌ Erreur fatale dans client_monitor: {e}")
            import traceback

            logger.error(traceback.format_exc())

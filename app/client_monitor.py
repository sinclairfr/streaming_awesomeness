import os
import time
import threading
from pathlib import Path
from config import logger
from typing import Dict, Tuple, List, Set
import re
from config import TIMEOUT_NO_VIEWERS

os.environ['TZ'] = 'Europe/Paris'
time.tzset()  # Applique le changement

class ClientMonitor(threading.Thread):

    # Timeouts pour les différents types de requêtes (en secondes)
    SEGMENT_TIMEOUT = 30
    PLAYLIST_TIMEOUT = 20
    UNKNOWN_TIMEOUT = 25

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

        # Thread de nettoyage
        self.cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self.cleanup_thread.start()

    def _cleanup_loop(self):
        """Nettoie les watchers inactifs et vérifie l'état des logs"""
        last_health_check = 0

        while True:
            try:
                time.sleep(10)
                self._cleanup_inactive()

                # Vérification périodique des logs (toutes les 5 minutes)
                current_time = time.time()
                if current_time - last_health_check > 300:  # 5 minutes
                    self.check_log_status()
                    last_health_check = current_time

            except Exception as e:
                logger.error(f"❌ Erreur dans cleanup_loop: {e}")
                time.sleep(30)  # Pause plus longue en cas d'erreur

    def _cleanup_inactive(self):
        """Version optimisée du nettoyage avec détection rapide des watchers inactifs"""
        current_time = time.time()
        affected_channels = set()

        for (channel, ip), data in list(self.watchers.items()):
            if isinstance(data, dict):
                last_seen = data.get("time", 0)
                req_type = data.get("type", "unknown")
            else:
                last_seen = data
                req_type = "unknown"

            timeout = (
                self.SEGMENT_TIMEOUT
                if req_type == "segment"
                else (
                    self.PLAYLIST_TIMEOUT
                    if req_type == "playlist"
                    else self.UNKNOWN_TIMEOUT
                )
            )

            if current_time - last_seen > timeout:
                del self.watchers[(channel, ip)]
                affected_channels.add(channel)

        # Recalculer le nombre de watchers pour chaque chaîne affectée
        for channel in affected_channels:
            # Compter les viewers actifs
            active_ips = set()
            for (ch, ip), data in self.watchers.items():
                if ch == channel:
                    # Refaire la vérification pour être cohérent
                    if isinstance(data, dict):
                        last_seen = data.get("time", 0)
                        req_type = data.get("type", "unknown")
                    else:
                        last_seen = data
                        req_type = "unknown"

                    timeout = (
                        self.SEGMENT_TIMEOUT
                        if req_type == "segment"
                        else (
                            self.PLAYLIST_TIMEOUT
                            if req_type == "playlist"
                            else self.UNKNOWN_TIMEOUT
                        )
                    )

                    if current_time - last_seen <= timeout:
                        active_ips.add(ip)

            count = len(active_ips)
            if count > 0 or channel in self.manager.channels:
                # On force la mise à jour, même si count=0
                logger.info(
                    f"[{channel}] 🔄 Mise à jour après nettoyage: {count} watchers actifs"
                )
                self.update_watchers(channel, count, "/hls/")

    def _print_channels_summary(self):
        """Affiche un récapitulatif des chaînes et de leur état"""
        try:
            active_channels = []
            stopped_channels = []
            total_viewers = 0

            for name, channel in self.channels.items():
                process_running = channel.process_manager.is_running()
                viewers = getattr(channel, "watchers_count", 0)

                # Debug: afficher les valeurs exactes des attributs
                logger.debug(
                    f"[{name}] État: {'actif' if process_running else 'arrêté'}, "
                    f"watchers_count={viewers}, "
                    f"last_watcher_time={getattr(channel, 'last_watcher_time', 0)}"
                )

                if process_running:
                    active_channels.append((name, viewers))
                    total_viewers += viewers
                else:
                    stopped_channels.append(name)

            # Affichage formaté avec couleurs
            logger.info("📊 RÉCAPITULATIF DES CHAÎNES:")

            if active_channels:
                active_str = ", ".join(
                    [f"{name} ({viewers}👁️)" for name, viewers in active_channels]
                )
                logger.info(f"CHAÎNES ACTIVES: {active_str}")
            else:
                logger.info("CHAÎNES ACTIVES: Aucune")

            logger.info(f"CHAÎNES ARRÊTÉES: {len(stopped_channels)}")
            logger.info(
                f"TOTAL: {total_viewers} viewers sur {len(active_channels)} streams actifs ({len(self.channels)} chaînes)"
            )

        except Exception as e:
            logger.error(f"❌ Erreur récapitulatif: {e}")

    def get_channel_watchers(self, channel):
        """Récupère le nombre actuel de watchers pour une chaîne"""
        if hasattr(self.manager, "channels") and channel in self.manager.channels:
            return getattr(self.manager.channels[channel], "watchers_count", 0)
        return 0

    def _parse_access_log(self, line):
        """Version améliorée qui extrait plus d'infos et ajoute des logs"""
        # Si pas de /hls/ dans la ligne, on ignore direct
        if "/hls/" not in line:
            return None, None, None, False, None

        # On récupère l'utilisateur agent si possible
        user_agent = None
        user_agent_match = re.search(r'"([^"]*)"$', line)
        if user_agent_match:
            user_agent = user_agent_match.group(1)

        # Si pas un GET ou un HEAD, on ignore
        if not ("GET /hls/" in line or "HEAD /hls/" in line):
            return None, None, None, False, None

        # Extraction IP (en début de ligne)
        ip = line.split(" ")[0]

        # Extraction du code HTTP
        status_code = "???"
        status_match = re.search(r'" (\d{3}) ', line)
        if status_match:
            status_code = status_match.group(1)

        # Extraction du canal spécifique
        channel = None
        if "/hls/playlist.m3u" in line:
            # C'est un accès à la playlist principale, pas une chaîne spécifique
            channel = "master_playlist"
            request_type = "playlist"
            logger.debug(f"📋 Détecté accès playlist principale par {ip}")
        else:
            # Format attendu: /hls/CHANNEL/...
            parts = line.split("/hls/")
            if len(parts) > 1:
                channel_part = parts[1].split("/")[0]
                # Vérification supplémentaire pour éviter les mauvaises extractions
                if channel_part and not channel_part.endswith(".m3u"):
                    channel = channel_part

                    # Type de requête
                    request_type = (
                        "playlist"
                        if ".m3u8" in line
                        else "segment" if ".ts" in line else "unknown"
                    )

        # Validité - note que 404 est valide pour le suivi même si le contenu n'existe pas
        is_valid = status_code in [
            "200",
            "206",
            "404",
        ]  # Ajout du 404 pour détecter les demandes de playlists manquantes

        return (
            ip,
            channel,
            request_type,
            is_valid,
            user_agent,
        )  # On renvoie aussi l'user-agent maintenant

    def _update_stats(self, ip, channel, request_type, segment_id=None):
        """Mise à jour des statistiques via le StatsCollector"""
        if not hasattr(self.manager, "stats_collector") or not self.manager.stats_collector:
            return

        stats = self.manager.stats_collector
        current_time = time.time()

        # Initialisation des dictionnaires de timestamps si nécessaire
        if not hasattr(self, "last_segment_time"):
            self.last_segment_time = {}
        if not hasattr(self, "last_playlist_time"):
            self.last_playlist_time = {}

        # Déterminer la durée à ajouter selon le type de requête
        if request_type == "segment":
            # Pour les segments, on utilise la durée réelle depuis la dernière requête
            if channel in self.last_segment_time:
                duration = current_time - self.last_segment_time[channel]
                # On limite la durée maximale à 10 secondes pour éviter les anomalies
                duration = min(duration, 10.0)
            else:
                duration = 4.0  # Durée par défaut si pas de timestamp précédent
            self.last_segment_time[channel] = current_time
            logger.debug(f"[{channel}] ⏱️ Ajout de {duration:.1f}s pour {ip} (segment)")
        elif request_type == "playlist":
            # Pour les playlists, on utilise un intervalle plus court car c'est un heartbeat
            if channel in self.last_playlist_time:
                duration = current_time - self.last_playlist_time[channel]
                # On limite la durée maximale à 5 secondes pour les heartbeats
                duration = min(duration, 5.0)
            else:
                duration = 0.5
            self.last_playlist_time[channel] = current_time
            logger.debug(f"[{channel}] ⏱️ Ajout de {duration:.1f}s pour {ip} (playlist)")
        else:
            # Pour les autres types, on utilise une durée minimale
            duration = 0.1
            logger.debug(f"[{channel}] ⏱️ Ajout de {duration:.1f}s pour {ip} (autre)")

        # Ajout du temps de visionnage
        stats.add_watch_time(channel, ip, duration)

        # Mise à jour des stats utilisateur
        user_agent = None  # Idéalement, récupérer l'user-agent de la requête
        stats.update_user_stats(ip, channel, duration, user_agent)

    def _process_log_line(self, line):
        """Traite une ligne du log sans déclencher de mise à jour immédiate"""
        # Parse la ligne
        ip, channel, request_type, is_valid, user_agent = self._parse_access_log(line)

        # Si pas valide, on sort
        if not is_valid or not channel:
            return

        # MAJ du timestamp uniquement
        try:
            current_time = time.time()
            logger.debug(f"[CLIENT_MONITOR_DEBUG] Traitement ligne: {line[:100]}...")

            # Créer une structure pour suivre le temps passé par utilisateur/chaîne
            if not hasattr(self, "watcher_times"):
                self.watcher_times = {}

            with self.lock:
                old_data = self.watchers.get((channel, ip), None)
                old_time = (
                    old_data.get("time", 0)
                    if isinstance(old_data, dict)
                    else old_data if old_data else 0
                )

                # Calculer le temps écoulé depuis la dernière requête (avec limites raisonnables)
                elapsed_time = 0
                if old_time > 0:
                    elapsed_time = min(
                        30, current_time - old_time
                    )  # Max 30 secondes entre requêtes
                    logger.debug(f"[CLIENT_MONITOR_DEBUG] Temps écoulé depuis dernière requête: {elapsed_time:.1f}s")

                # On stocke des informations complètes pour ce watcher
                self.watchers[(channel, ip)] = {
                    "time": current_time,
                    "type": request_type,
                    "user_agent": user_agent,
                    "elapsed": elapsed_time,
                }

                # CRITICAL FIX: Détermine la durée à ajouter en fonction du type de requête ET du temps écoulé
                # Pour les segments, on utilise le temps écoulé réel
                if request_type == "segment":
                    # Si c'est un segment, la durée est soit le temps écoulé, soit au moins 4s par segment
                    duration = max(elapsed_time, 4.0)
                    logger.debug(f"[CLIENT_MONITOR_DEBUG] Segment détecté - Durée calculée: {duration:.1f}s")
                elif request_type == "playlist":
                    # Pour les playlists, c'est un heartbeat: au moins 0.5s
                    duration = max(elapsed_time * 0.5, 0.5)
                    logger.debug(f"[CLIENT_MONITOR_DEBUG] Playlist détectée - Durée calculée: {duration:.1f}s")
                else:
                    # Pour les autres types, au moins 0.1s
                    duration = max(elapsed_time * 0.1, 0.1)
                    logger.debug(f"[CLIENT_MONITOR_DEBUG] Autre type de requête - Durée calculée: {duration:.1f}s")

                # Ajouter un log détaillé pour comprendre ce qui se passe
                logger.info(
                    f"[CLIENT_MONITOR] ⏱️ {channel}: Ajout de {duration:.1f}s pour {ip} (elapsed: {elapsed_time:.1f}s, type: {request_type})"
                )

                # CRITICAL FIX: Mise à jour explicite des stats ici
                # Vérification que stats_collector existe et appel avec les bonnes durées
                if (
                    hasattr(self.manager, "stats_collector")
                    and self.manager.stats_collector
                ):
                    logger.debug(f"[CLIENT_MONITOR_DEBUG] Mise à jour des stats via stats_collector")
                    self.manager.stats_collector.add_watch_time(channel, ip, duration)
                    self.manager.stats_collector.update_user_stats(
                        ip, channel, duration, user_agent
                    )
                    # Forcer une sauvegarde périodique
                    watcher_key = f"{channel}:{ip}"
                    if not hasattr(self, "last_save_times"):
                        self.last_save_times = {}

                    if (
                        watcher_key not in self.last_save_times
                        or current_time - self.last_save_times.get(watcher_key, 0) > 60
                    ):
                        # Forcer une sauvegarde toutes les 60 secondes par watcher
                        threading.Thread(
                            target=self.manager.stats_collector.save_stats, daemon=True
                        ).start()
                        threading.Thread(
                            target=self.manager.stats_collector.save_user_stats,
                            daemon=True,
                        ).start()
                        self.last_save_times[watcher_key] = current_time
                        logger.info(
                            f"[CLIENT_MONITOR] 💾 Sauvegarde forcée des stats pour {channel}:{ip}"
                        )

                # Grouper les mises à jour par chaîne
                if not hasattr(self, "modified_channels"):
                    self.modified_channels = set()
                self.modified_channels.add(channel)

        except Exception as e:
            logger.error(f"[CLIENT_MONITOR_ERROR] ❌ Erreur traitement ligne: {e}")
            import traceback
            logger.error(f"[CLIENT_MONITOR_ERROR] {traceback.format_exc()}")

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

    def process_new_log_lines(self):
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

                if not new_lines:
                    return True  # Pas de nouvelles lignes, tout est ok

                # Débogage: nombre de lignes traitées
                # logger.debug(f"Traitement de {len(new_lines)} nouvelles lignes")

                # Traitement des nouvelles lignes
                segment_requests = {}  # {channel_name: [segment_ids]}
                channel_updates = {}  # {channel_name: set(ips)}

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

                        # Si c'est un segment, on le stocke pour analyse
                        if request_type == "segment":
                            segment_match = re.search(r"segment_(\d+)\.ts", line)
                            if segment_match:
                                segment_id = segment_match.group(1)
                                if channel not in segment_requests:
                                    segment_requests[channel] = []
                                segment_requests[channel].append(segment_id)

                                # On met à jour aussi la dernière activité de segment
                                if (
                                    hasattr(self.manager, "channels")
                                    and channel in self.manager.channels
                                ):
                                    self.manager.channels[channel].last_segment_time = (
                                        current_time
                                    )

                        # Regrouper par chaîne pour ne faire qu'une mise à jour
                        if channel not in channel_updates:
                            channel_updates[channel] = set()
                        channel_updates[channel].add(ip)

                # Mise à jour groupée par chaîne
                for channel, ips in channel_updates.items():
                    if channel == "master_playlist":
                        continue  # On ignore la playlist maîtresse

                    count = len(ips)
                    logger.info(
                        f"[{channel}] 👁️ MAJ watchers: {count} actifs - {list(ips)}"
                    )
                    self.update_watchers(channel, count, "/hls/")

                # Loguer les requêtes de segments pour debugging
                for channel, segments in segment_requests.items():
                    if segments:
                        logger.debug(f"[{channel}] 📊 Segments demandés: {segments}")

                return True

        except Exception as e:
            logger.error(f"❌ Erreur traitement nouvelles lignes: {e}")
            import traceback

            logger.error(traceback.format_exc())
            return False

    def _apply_pending_updates(self):
        """Applique les mises à jour en attente pour éviter les doublons"""
        if not hasattr(self, "modified_channels") or not self.modified_channels:
            return

        current_time = time.time()

        # Pour éviter les mises à jour trop fréquentes
        if hasattr(self, "last_update_time"):
            elapsed = current_time - self.last_update_time
            if elapsed < 2:  # Au moins 2 secondes entre les mises à jour
                return

        channels_updated = 0

        with self.lock:
            for channel in list(self.modified_channels):
                # IMPORTANT: Ignorer la playlist principale
                if channel == "master_playlist":
                    continue

                # Calcule les watchers actifs pour cette chaîne
                active_ips = set()
                for (ch, watcher_ip), last_seen in self.watchers.items():
                    if ch == channel and current_time - last_seen < 60:
                        active_ips.add(watcher_ip)

                # Nombre de watchers pour cette chaîne
                count = len(active_ips)

                # Évite les logs si aucun changement réel
                old_count = self.get_channel_watchers(channel)
                if count != old_count:
                    logger.info(
                        f"[{channel}] 👁️ MAJ watchers: {count} actifs - {list(active_ips)}"
                    )
                    self.update_watchers(channel, count, "/hls/")
                    channels_updated += 1

            # Réinitialise les chaînes modifiées
            self.modified_channels.clear()
            self.last_update_time = current_time

        if channels_updated > 0:
            logger.debug(f"🔄 {channels_updated} chaînes mises à jour en mode groupé")

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
            current_time = time.time()
            
            # Stockage précis du type de requête et du temps
            self.watchers[(channel, ip)] = {
                "time": current_time,
                "type": request_type,
                "user_agent": user_agent
            }

            # Log pour debug
            logger.debug(f"🔍 Requête: {ip} → {channel} ({request_type})")

            # Mise à jour des statistiques AVANT tout autre traitement
            self._update_stats(ip, channel, request_type)

            # Si c'est une requête de segment, on l'enregistre
            if request_type == "segment":
                self._handle_segment_request(channel, ip, line, user_agent)
            # Pour les playlists, considérer comme un heartbeat
            elif request_type == "playlist":
                self._handle_playlist_request(channel, ip)

            # Mise à jour du compteur global
            self._update_channel_watchers_count(channel)

    def _handle_playlist_request(self, channel, ip):
        """Traite une requête de playlist"""
        # Heartbeat playlist = ~0.5 secondes de visionnage
        if hasattr(self.manager, "stats_collector"):
            self.manager.stats_collector.add_watch_time(channel, ip, 0.5)

    def _update_channel_watchers_count(self, channel):
        """Calcule et met à jour le nombre de watchers actifs pour une chaîne"""
        # Calcul des watchers actifs par IP
        active_ips = set()
        current_time = time.time()

        for (ch, watcher_ip), data in self.watchers.items():
            if ch != channel:
                continue

            # On considère un watcher actif s'il a une activité récente
            watcher_time = data["time"] if isinstance(data, dict) else data
            watcher_type = (
                data.get("type", "unknown") if isinstance(data, dict) else "unknown"
            )

            # Utiliser les mêmes timeouts que dans _cleanup_inactive
            timeout = (
                self.SEGMENT_TIMEOUT
                if watcher_type == "segment"
                else self.PLAYLIST_TIMEOUT if watcher_type == "playlist" else self.UNKNOWN_TIMEOUT
            )

            if current_time - watcher_time <= timeout:
                active_ips.add(watcher_ip)

        active_watchers = len(active_ips)

        # Mise à jour du compteur uniquement si ça a changé
        current_watchers = self.get_channel_watchers(channel)
        if active_watchers != current_watchers:
            logger.info(
                f"[{channel}] 👁️ MAJ watchers: {active_watchers} actifs - {list(active_ips)}"
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

    def _handle_segment_request(self, channel, ip, line, user_agent):
        """Traite une requête de segment"""
        # Identifier le segment
        segment_match = re.search(r"segment_(\d+)\.ts", line)
        segment_id = segment_match.group(1) if segment_match else "unknown"

        # Enregistrer le segment
        self.segments_by_channel.setdefault(channel, {})[segment_id] = time.time()

        # Ajouter du temps de visionnage (4 secondes par segment est une bonne estimation)
        segment_duration = 4.0  # secondes par segment, valeur typique pour HLS

        # S'assurer que le stats_collector est disponible via le manager
        if hasattr(self.manager, "stats_collector") and self.manager.stats_collector:
            self.manager.stats_collector.add_watch_time(channel, ip, segment_duration)
            if user_agent:
                self.manager.stats_collector.update_user_stats(
                    ip, channel, segment_duration, user_agent
                )

    def check_log_status(self):
        """Vérifie l'état du monitoring des logs et effectue des actions correctives si nécessaire"""
        try:
            # Vérifie que le fichier existe
            if not os.path.exists(self.log_path):
                logger.error(f"❌ Fichier log nginx introuvable: {self.log_path}")
                return False

            # Vérifie la taille du fichier
            file_size = os.path.getsize(self.log_path)

            # Vérifie que la position de lecture est valide
            if hasattr(self, "last_position"):
                if self.last_position > file_size:
                    logger.warning(
                        f"⚠️ Position de lecture ({self.last_position}) > taille du fichier ({file_size})"
                    )
                    self.last_position = 0
                    logger.info("🔄 Réinitialisation de la position de lecture")

                # Vérifie si de nouvelles données ont été ajoutées depuis la dernière lecture
                if file_size > self.last_position:
                    diff = file_size - self.last_position
                    logger.info(
                        f"📊 {diff} octets de nouveaux logs depuis la dernière lecture"
                    )
                else:
                    logger.info(f"ℹ️ Pas de nouveaux logs depuis la dernière lecture")

            # Tente de lire quelques lignes pour vérifier que le fichier est accessible
            try:
                with open(self.log_path, "r") as f:
                    f.seek(max(0, file_size - 1000))  # Lire les 1000 derniers octets
                    last_lines = f.readlines()
                    logger.info(
                        f"✅ Lecture réussie, {len(last_lines)} lignes récupérées"
                    )

                    # Analyse des dernières lignes pour vérifier qu'elles contiennent des requêtes HLS
                    hls_requests = sum(1 for line in last_lines if "/hls/" in line)
                    if hls_requests == 0 and len(last_lines) > 0:
                        logger.warning(
                            "⚠️ Aucune requête HLS dans les dernières lignes du log!"
                        )
                    else:
                        logger.info(
                            f"✅ {hls_requests}/{len(last_lines)} requêtes HLS détectées"
                        )

                    return True

            except Exception as e:
                logger.error(f"❌ Erreur lecture fichier log: {e}")
                return False

        except Exception as e:
            logger.error(f"❌ Erreur vérification logs: {e}")
            return False

    def _update_watcher_count(self, channel):
        """Calcule et met à jour le nombre de watchers pour une chaîne avec timeouts cohérents"""
        # Trouver toutes les IPs actives pour cette chaîne
        active_ips = set()
        ip_times = {}  # Pour stocker le temps d'activité de chaque IP
        current_time = time.time()

        # Utiliser les mêmes timeouts que dans _cleanup_inactive
        segment_timeout = 30  # 30 secondes
        playlist_timeout = 20  # 20 secondes
        unknown_timeout = 25  # 25 secondes

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

            # Appliquer le même timeout qu'au nettoyage
            timeout = (
                segment_timeout
                if req_type == "segment"
                else playlist_timeout if req_type == "playlist" else unknown_timeout
            )

            if current_time - last_time <= timeout:
                active_ips.add(ip)
                ip_times[ip] = round(current_time - last_time)

        # Nombre de watchers
        watcher_count = len(active_ips)

        # Préparer une chaîne avec les IPs et leurs temps d'activité
        ip_details = [f"{ip} (actif depuis {ip_times[ip]}s)" for ip in active_ips]
        ip_str = ", ".join(ip_details) if ip_details else "aucun"

        # Mise à jour forcée avec log
        logger.info(f"[{channel}] 👁️ Watchers: {watcher_count} actifs - IPs: {ip_str}")

        # Mise à jour dans le manager
        self.update_watchers(channel, watcher_count, "/hls/")

    def _follow_log_file_legacy(self):
        """Version robuste pour suivre le fichier de log sans pyinotify"""
        try:
            # Position initiale - SE PLACER À LA FIN DU FICHIER, PAS AU DÉBUT
            position = os.path.getsize(self.log_path)

            logger.info(
                f"👁️ Mode surveillance legacy actif sur {self.log_path} (démarrage à position {position})"
            )

            # Variables pour le heartbeat et les nettoyages
            last_activity_time = time.time()
            last_heartbeat_time = time.time()
            last_cleanup_time = time.time()
            last_update_time = time.time()

            while True:
                # Vérifier que le fichier existe toujours
                if not os.path.exists(self.log_path):
                    logger.error(f"❌ Fichier log disparu: {self.log_path}")
                    time.sleep(5)
                    continue

                # Taille actuelle du fichier
                current_size = os.path.getsize(self.log_path)

                # Si le fichier a grandi, lire les nouvelles lignes
                if current_size > position:
                    with open(self.log_path, "r") as f:
                        f.seek(position)
                        new_lines = f.readlines()
                        position = f.tell()  # Nouvelle position

                        # Log pour debug - montre la différence de taille
                        if new_lines:
                            logger.debug(
                                f"📝 Traitement de {len(new_lines)} nouvelles lignes (pos: {position}/{current_size})"
                            )

                        # Regrouper par chaîne pour traiter d'un coup
                        channel_updates = {}

                        # Traitement des nouvelles lignes en batch
                        for line in new_lines:
                            if line.strip():
                                # Pré-traitement
                                ip, channel, request_type, is_valid, _ = (
                                    self._parse_access_log(line.strip())
                                )

                                # Si valide, ajouter à notre dictionnaire de chaînes
                                if is_valid and channel:
                                    # On stocke l'heure ACTUELLE, pas celle du log
                                    self.watchers[(channel, ip)] = time.time()
                                    if channel not in channel_updates:
                                        channel_updates[channel] = set()
                                    channel_updates[channel].add(ip)

                                # On marque l'activité
                                last_activity_time = time.time()

                        # Une fois toutes les lignes traitées, mettre à jour les chaînes
                        current_time = time.time()
                        if (
                            current_time - last_update_time > 2
                        ):  # Au moins 2 secondes entre les mises à jour
                            # Mise à jour groupée par chaîne
                            for channel, ips in channel_updates.items():
                                if channel == "master_playlist":
                                    continue
                                count = len(ips)
                                # Vérifier changement réel avant de loguer
                                old_count = self.get_channel_watchers(channel)

                                # On force à update même si pas de changement
                                logger.info(
                                    f"[{channel}] 👁️ MAJ watchers: {count} actifs - {list(ips)}"
                                )
                                self.update_watchers(channel, count, "/hls/")

                            # Réinitialiser le temps de dernière mise à jour
                            last_update_time = current_time

                # Si le fichier a été tronqué (rotation de logs)
                elif current_size < position:
                    logger.warning(f"⚠️ Fichier log tronqué, redémarrage lecture")
                    position = 0
                    continue

                current_time = time.time()

                # Nettoyage périodique des watchers inactifs
                if current_time - last_cleanup_time > 10:
                    self._cleanup_inactive()
                    last_cleanup_time = current_time

                # Heartbeat périodique
                if current_time - last_heartbeat_time > 60:
                    active_count = len(set(ch for (ch, _), _ in self.watchers.items()))
                    logger.info(
                        f"💓 ClientMonitor actif (pos: {position}/{current_size}), {active_count} chaînes actives"
                    )
                    last_heartbeat_time = current_time

                # Pause courte avant la prochaine vérification
                time.sleep(0.5)

        except Exception as e:
            logger.error(f"❌ Erreur mode legacy: {e}")
            import traceback

            logger.error(traceback.format_exc())

            # Tentative de récupération après pause
            time.sleep(10)
            self._follow_log_file_legacy()

    def run(self):
        """Démarre le monitoring en mode direct (legacy)"""
        logger.info("👀 Démarrage de la surveillance des requêtes...")

        try:
            # Vérification du fichier de log
            if not os.path.exists(self.log_path):
                logger.error(f"❌ Fichier log introuvable: {self.log_path}")
                time.sleep(5)
                return self.run()

            # Important: initialiser la position à la fin du fichier, pas au début
            with open(self.log_path, "r") as f:
                f.seek(0, 2)  # Se positionner à la fin du fichier
                position = f.tell()
                logger.info(
                    f"📝 Positionnement initial à la fin du fichier: {position} bytes"
                )

                # Afficher les dernières lignes du fichier pour vérification
                last_pos = max(0, position - 500)  # Remonter de 500 bytes
                f.seek(last_pos)
                last_lines = f.readlines()
                if last_lines:
                    logger.info(f"📋 Dernière ligne du log: {last_lines[-1][:100]}")

            # Utilisation directe du mode legacy
            self._follow_log_file_legacy()

        except Exception as e:
            logger.error(f"❌ Erreur démarrage surveillance: {e}")
            import traceback

            logger.error(traceback.format_exc())
            time.sleep(10)
            self.run()

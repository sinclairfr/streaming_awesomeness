import os
import time
import threading
from pathlib import Path
from config import logger
from typing import Dict, Tuple, List, Set
import re
from config import TIMEOUT_NO_VIEWERS
from watcher_timer import WatcherTimer

os.environ['TZ'] = 'Europe/Paris'
time.tzset()  # Applique le changement

class ClientMonitor(threading.Thread):

    # Timeouts pour les différents types de requêtes (en secondes)
    SEGMENT_TIMEOUT = 30
    PLAYLIST_TIMEOUT = 20
    UNKNOWN_TIMEOUT = 25

    def __init__(self, log_path, update_watchers_callback, manager, stats_collector=None):
        super().__init__(daemon=True)
        self.log_path = log_path

        # Callback pour mettre à jour les watchers dans le manager
        self.update_watchers = update_watchers_callback
        # Manager pour accéder aux segments et aux canaux
        self.manager = manager
        # StatsCollector pour les statistiques
        self.stats_collector = stats_collector

        # dictionnaire pour stocker les watchers actifs avec leurs minuteurs
        self.watchers = {}  # {ip: {"timer": WatcherTimer, "last_seen": time, "type": str, "current_channel": str}}

        # dictionnaire pour stocker les segments demandés par chaque canal
        self.segments_by_channel = {}  # {channel: {segment_id: last_requested_time}}

        # Pour éviter les accès concurrents
        self.lock = threading.Lock()

        self.inactivity_threshold = TIMEOUT_NO_VIEWERS

        # Thread de nettoyage
        self.cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self.cleanup_thread.start()

    def run(self):
        """Méthode principale du thread"""
        logger.info("🚀 Démarrage du ClientMonitor")
        self.run_client_monitor()

    def _cleanup_loop(self):
        """Nettoie les watchers inactifs et vérifie l'état des logs"""
        last_health_check = 0

        while True:
            try:
                time.sleep(10)
                self._cleanup_inactive()

                # Vérification périodique des logs (toutes les  minutes)
                current_time = time.time()
                if current_time - last_health_check > 60:  #1 minute
                    self.check_log_status()
                    last_health_check = current_time

            except Exception as e:
                logger.error(f"❌ Erreur dans cleanup_loop: {e}")
                time.sleep(30)  # Pause plus longue en cas d'erreur

    def _cleanup_inactive(self):
        """Nettoie les watchers inactifs"""
        current_time = time.time()
        inactive_watchers = []

        # Identifier les watchers inactifs
        for ip, watcher in self.watchers.items():
            # Vérifier le type de requête pour le timeout approprié
            request_type = watcher.get("type", "unknown")
            timeout = {
                "segment": 300,  # 5 minutes pour les segments
                "playlist": 180,  # 3 minutes pour les playlists
                "unknown": 240   # 4 minutes pour les autres types
            }.get(request_type, 240)

            # Si pas de requête depuis le timeout approprié
            if current_time - watcher["last_seen"] > timeout:
                logger.debug(f"⏱️ Watcher {ip} inactif depuis {current_time - watcher['last_seen']:.1f}s (type: {request_type})")
                inactive_watchers.append(ip)

        # Supprimer les watchers inactifs
        for ip in inactive_watchers:
            watcher = self.watchers[ip]
            # Arrêter le minuteur si présent
            if "timer" in watcher:
                watcher["timer"].stop()
                logger.info(f"⏱️ Arrêt du minuteur pour {ip} sur {watcher.get('last_channel', 'unknown')}")
            
            # Supprimer le watcher
            del self.watchers[ip]
            logger.info(f"🧹 Suppression du watcher inactif: {ip}")

        # Mettre à jour les compteurs de watchers pour toutes les chaînes affectées
        affected_channels = set()
        for ip in inactive_watchers:
            if ip in self.watchers:
                affected_channels.add(self.watchers[ip].get("last_channel"))

        for channel in affected_channels:
            if channel:
                self._update_channel_watchers_count(channel)

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
        request_type = None

        # Format attendu: /hls/CHANNEL/...
        channel_match = re.search(r'/hls/([^/]+)/', line)
        if channel_match:
            channel = channel_match.group(1)
            # Type de requête
            request_type = (
                "playlist"
                if ".m3u8" in line
                else "segment" if ".ts" in line else "unknown"
            )
            logger.debug(f"📋 Détecté accès {request_type} pour {channel} par {ip}")

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

    def _process_log_line(self, line):
        """Traite une ligne de log nginx"""
        try:
            # Parse la ligne
            ip, channel, request_type, is_valid, user_agent = self._parse_access_log(line)

            # Si la ligne n'est pas valide ou pas de channel, on ignore
            if not is_valid or not channel:
                return

            # Si c'est la playlist principale, on ignore
            if channel == "master_playlist":
                return

            # Vérifier que la chaîne existe dans le manager
            if not hasattr(self.manager, "channels"):
                logger.error("❌ Le manager n'a pas d'attribut 'channels'")
                return

            # Log des chaînes disponibles
            logger.debug(f"📋 Chaînes disponibles: {list(self.manager.channels.keys())}")
            logger.debug(f"🔍 Recherche de la chaîne: {channel}")

            if channel not in self.manager.channels:
                logger.debug(f"⚠️ Chaîne {channel} non trouvée dans le manager, ignorée")
                return

            # Log détaillé de la requête
            logger.debug(f"📝 Requête détaillée: IP={ip}, Channel={channel}, Type={request_type}, User-Agent={user_agent}")

            # Mise à jour du watcher avec la nouvelle structure
            self._update_watcher(ip, channel, request_type, user_agent, line)

            # Si c'est une requête de playlist, on force une mise à jour des watchers
            if request_type == "playlist":
                logger.debug(f"🔄 Requête playlist détectée pour {channel} par {ip}")
                self._update_channel_watchers_count(channel)

        except Exception as e:
            logger.error(f"❌ Erreur traitement ligne log: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def _update_watcher(self, ip, channel, request_type, user_agent, line):
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
                        "current_channel": channel
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
                    self.watchers[ip]["current_channel"] = channel
                else:
                    # Même chaîne, vérifier le temps écoulé depuis la dernière requête
                    last_seen = self.watchers[ip].get("last_seen", 0)
                    if current_time - last_seen < 1.0 and request_type == "playlist":
                        # Ignorer les mises à jour trop rapprochées pour éviter le spam de stats
                        logger.debug(f"Ignoré mise à jour rapprochée pour {ip} sur {channel} (interval: {current_time - last_seen:.2f}s)")
                        return

                # Mise à jour des infos
                self.watchers[ip]["last_seen"] = current_time
                self.watchers[ip]["type"] = request_type
                self.watchers[ip]["user_agent"] = user_agent

            # Traiter la requête - ajouter du temps de visionnage uniquement pour les segments
            # ou pour les playlists avec un intervalle raisonnable
            if request_type == "segment":
                self._handle_segment_request(channel, ip, line, user_agent)
            elif request_type == "playlist":
                # Pour les playlists, on n'ajoute du temps que si ça fait au moins 3 secondes
                # depuis la dernière requête, pour éviter les doublons dus aux rafales de requêtes
                last_seen = self.watchers[ip].get("last_seen", 0)
                if current_time - last_seen >= 3.0 and self.stats_collector:
                    # Ajouter un temps plus raisonnable basé sur l'intervalle réel
                    elapsed = min(current_time - last_seen, 5.0)
                    self.stats_collector.add_watch_time(channel, ip, elapsed)
                    logger.debug(f"[{channel}] Ajout de {elapsed:.1f}s pour {ip} (intervalle: {current_time - last_seen:.1f}s)")

            # Mettre à jour le compteur de watchers pour cette chaîne
            active_ips = set()
            ip_times = {}  # Pour stocker le temps d'activité de chaque IP

            for ip in self.watchers:
                if self.watchers[ip].get("current_channel") == channel and "timer" in self.watchers[ip]:
                    # Vérifier si le timer est actif
                    timer = self.watchers[ip]["timer"]
                    if timer.is_running():
                        active_ips.add(ip)
                        ip_times[ip] = timer.get_total_time()
            
            # Nombre de segments pour cette chaîne
            segment_count = len(self.segments_by_channel.get(channel, {}))
            
            # Afficher les watchers actifs (moins fréquemment)
            if active_ips and (not hasattr(self, "last_watchers_log") or current_time - self.last_watchers_log > 10.0):
                # Trier par temps de visionnage
                sorted_ips = sorted(
                    [(ip, ip_times.get(ip, 0)) for ip in active_ips],
                    key=lambda x: x[1],
                    reverse=True
                )
                
                # Afficher les top 5
                top_watchers = [f"{ip} ({time:.1f}s)" for ip, time in sorted_ips[:5]]
                logger.info(f"[{channel}] 👁️ Top watchers: {', '.join(top_watchers)}")
                
                # Log total
                logger.info(
                    f"[{channel}] 📊 {len(active_ips)} watchers actifs, {segment_count} segments"
                )
                
                self.last_watchers_log = current_time

            # Appeler le callback du manager avec le nombre de watchers actifs
            if hasattr(self, 'update_watchers') and callable(self.update_watchers):
                self.update_watchers(channel, len(active_ips), "/hls/")
            else:
                logger.debug(f"[{channel}] ❌ Callback update_watchers non disponible")

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

    def process_client_log_lines(self):
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

                # Traitement des nouvelles lignes
                for line in new_lines:
                    if not line.strip():
                        continue

                    # Traiter la ligne avec la nouvelle structure
                    self._process_log_line(line.strip())

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
                ip_times = {}  # Pour stocker le temps d'activité de chaque IP

                for ip, data in self.watchers.items():
                    if data.get("current_channel") == channel and "timer" in data:
                        # Vérifier si le timer est actif
                        timer = data["timer"]
                        if timer.is_running():
                            active_ips.add(ip)
                            ip_times[ip] = timer.get_total_time()
                
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
            active_count = 0
            for ip, data in self.watchers.items():
                if isinstance(data, dict) and "timer" in data:
                    timer = data["timer"]
                    if timer.is_running():
                        active_count += 1
                        
            logger.info(
                f"💓 Heartbeat: {active_count} watchers actifs, {len(self.segments_by_channel)} chaînes en suivi"
            )
            last_heartbeat_time = current_time

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
            # Vérifier si c'est la même chaîne que la dernière vue par cette IP
            # On utilise uniquement l'IP comme clé pour le suivi des chaînes
            if ip in self.watchers:
                last_channel = self.watchers[ip].get("current_channel")
                logger.debug(f"📊 Traitement segment pour {ip}: dernière chaîne={last_channel}, chaîne actuelle={channel}, segment={segment_id}")
                
                if last_channel != channel:
                    # Si la chaîne a changé, on arrête l'ancien minuteur
                    if "timer" in self.watchers[ip]:
                        self.watchers[ip]["timer"].stop()
                        logger.info(f"⏱️ Arrêt du minuteur pour {ip} sur {last_channel}")
                    
                    # On met à jour la dernière chaîne vue
                    self.watchers[ip]["current_channel"] = channel
                    logger.info(f"🔄 Changement de chaîne détecté pour {ip}: {last_channel} -> {channel}")
            
            logger.debug(f"📈 Ajout de {segment_duration}s de temps de visionnage pour {ip} sur {channel}")
            self.manager.stats_collector.add_watch_time(channel, ip, segment_duration)
            if user_agent:
                self.manager.stats_collector.update_user_stats(
                    ip, channel, segment_duration, user_agent
                )

    def _handle_playlist_request(self, channel, ip):
        """Traite une requête de playlist"""
        # Cette méthode est maintenant vide car tout est géré dans _process_log_line
        pass

    def _update_channel_watchers_count(self, channel):
        """Calcule et met à jour le nombre de watchers pour une chaîne"""
        with self.lock:
            # Vérifier si on doit logger (au moins 2 secondes entre les logs)
            current_time = time.time()
            if hasattr(self, "last_watchers_log") and current_time - self.last_watchers_log < 2.0:
                return
                
            # Compter les watchers actifs pour cette chaîne
            active_ips = set()
            ip_times = {}  # Pour stocker le temps d'activité de chaque IP

            for ip, data in self.watchers.items():
                if data.get("current_channel") == channel and "timer" in data:
                    # Vérifier si le timer est actif
                    timer = data["timer"]
                    if timer.is_running():
                        active_ips.add(ip)
                        ip_times[ip] = timer.get_total_time()
            
            # Nombre de watchers
            watcher_count = len(active_ips)
            
            # Log uniquement si le nombre a changé
            if not hasattr(self, "last_watcher_counts"):
                self.last_watcher_counts = {}
            
            if channel not in self.last_watcher_counts or self.last_watcher_counts[channel] != watcher_count:
                logger.info(f"[{channel}] 👁️ Watchers: {watcher_count} actifs - IPs: {', '.join(active_ips)}")
                self.last_watcher_counts[channel] = watcher_count
                self.last_watchers_log = current_time
            
            # Mise à jour dans le manager
            if hasattr(self, 'update_watchers') and callable(self.update_watchers):
                self.update_watchers(channel, watcher_count, "/hls/")
            else:
                logger.error(f"[{channel}] ❌ Callback update_watchers non disponible")

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
        with self.lock:
            # Compter les watchers actifs pour cette chaîne
            active_ips = set()
            ip_times = {}  # Pour stocker le temps d'activité de chaque IP

            for ip, data in self.watchers.items():
                if data.get("current_channel") == channel:
                    active_ips.add(ip)
                    ip_times[ip] = data["timer"].get_total_time()

            # Nombre de watchers
            watcher_count = len(active_ips)

            # Préparer une chaîne avec les IPs et leurs temps d'activité
            ip_details = [f"{ip} (actif depuis {ip_times[ip]:.1f}s)" for ip in active_ips]
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

                        # Traitement des nouvelles lignes
                        for line in new_lines:
                            if line.strip():
                                # Traiter la ligne avec la nouvelle structure
                                self._process_log_line(line.strip())

                        # On marque l'activité
                        last_activity_time = time.time()

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
                    active_count = 0
                    for ip, data in self.watchers.items():
                        if isinstance(data, dict) and "timer" in data:
                            timer = data["timer"]
                            if timer.is_running():
                                active_count += 1
                    
                    logger.info(
                        f"💓 Heartbeat: {active_count} watchers actifs, {len(self.segments_by_channel)} chaînes en suivi"
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

    def run_client_monitor(self):
        """Démarre le monitoring en mode moderne avec fallback sur legacy au besoin"""
        logger.info("👀 Démarrage de la surveillance des requêtes...")

        try:
            # Vérification du fichier de log
            if not os.path.exists(self.log_path):
                logger.error(f"❌ Fichier log introuvable: {self.log_path}")
                time.sleep(5)
                return self.run_client_monitor()

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

            # Essayer d'utiliser le mode moderne
            try:
                logger.info("🔄 Utilisation du mode de surveillance moderne...")
                self._follow_log_file()
            except Exception as e:
                logger.error(f"❌ Erreur mode moderne: {e}")
                logger.warning("⚠️ Fallback sur le mode legacy")
                # Fallback au mode legacy en cas d'erreur
                self._follow_log_file_legacy()

        except Exception as e:
            logger.error(f"❌ Erreur démarrage surveillance: {e}")
            import traceback

            logger.error(traceback.format_exc())
            time.sleep(10)
            self.run_client_monitor()
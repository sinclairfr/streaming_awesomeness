import os
import time
import threading
from pathlib import Path
from config import logger
from typing import Dict, Tuple, List, Set
import re
from watcher_timer import WatcherTimer
from log_utils import parse_access_log

os.environ['TZ'] = 'Europe/Paris'
time.tzset()  # Applique le changement

class ClientMonitor(threading.Thread):

    # Timeouts pour les différents types de requêtes (en secondes)
    SEGMENT_TIMEOUT = 30
    PLAYLIST_TIMEOUT = 20
    UNKNOWN_TIMEOUT = 25
    WATCHER_INACTIVITY_TIMEOUT = 60  # Timeout pour considérer un watcher comme inactif
    
    # Ajouter des timeouts spécifiques pour le nettoyage
    CLEANUP_SEGMENT_TIMEOUT = 300  # 5 minutes pour les segments
    CLEANUP_PLAYLIST_TIMEOUT = 180  # 3 minutes pour les playlists  
    CLEANUP_UNKNOWN_TIMEOUT = 240   # 4 minutes pour les autres types

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

        # Événement pour l'arrêt propre du thread de nettoyage
        self.stop_event = threading.Event()

        logger.info(f"⏱️ Timeouts configurés - Watcher inactif: {self.WATCHER_INACTIVITY_TIMEOUT}s, Segment: {self.SEGMENT_TIMEOUT}s, Playlist: {self.PLAYLIST_TIMEOUT}s")

        # Thread de nettoyage
        self.cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self.cleanup_thread.start()
        
        # Pour le logging périodique
        self.last_cleanup_log = time.time()

    def run(self):
        """Méthode principale du thread"""
        logger.info("🚀 Démarrage du ClientMonitor")
        self.run_client_monitor()

    def _cleanup_loop(self):
        """Nettoie les watchers inactifs et vérifie l'état des logs"""
        while not self.stop_event.is_set():
            try:
                current_time = time.time()
                inactive_watchers = []

                # Identifier les watchers inactifs
                for ip, watcher in list(self.watchers.items()):
                    # Vérifier le type de requête pour le timeout approprié
                    request_type = watcher.get("type", "unknown")
                    timeout = {
                        "segment": self.CLEANUP_SEGMENT_TIMEOUT,
                        "playlist": self.CLEANUP_PLAYLIST_TIMEOUT,
                        "unknown": self.CLEANUP_UNKNOWN_TIMEOUT
                    }.get(request_type, self.CLEANUP_UNKNOWN_TIMEOUT)
                    
                    last_activity = watcher.get("last_activity", 0)
                    if current_time - last_activity > timeout:
                        inactive_watchers.append(ip)
                        logger.info(f"⏱️ Watcher inactif détecté: {ip} sur {watcher.get('current_channel', 'unknown')} (dernière activité: {current_time - last_activity:.1f}s)")

                # Supprimer les watchers inactifs
                for ip in inactive_watchers:
                    watcher = self.watchers.get(ip, {})  # Utilisation de get pour éviter les KeyError
                    channel = watcher.get("current_channel", "unknown")
                    
                    # Arrêter le minuteur si présent
                    if "timer" in watcher:
                        watcher["timer"].stop()
                        logger.info(f"⏱️ Arrêt du minuteur pour {ip} sur {channel}")
                    
                    # Supprimer le watcher
                    if ip in self.watchers:
                        del self.watchers[ip]
                        logger.info(f"🧹 Suppression du watcher inactif: {ip} de {channel}")

                        # Mettre à jour le compteur de watchers pour la chaîne
                        if channel != "unknown":
                            self._update_channel_watchers_count(channel)

                # Log périodique des watchers actifs
                if not hasattr(self, "last_cleanup_log") or current_time - self.last_cleanup_log > 30:
                    active_channels = {}
                    for ip, watcher in self.watchers.items():
                        channel = watcher.get("current_channel", "unknown")
                        if channel not in active_channels:
                            active_channels[channel] = set()
                        active_channels[channel].add(ip)

                    for channel, watchers in active_channels.items():
                        logger.info(f"[{channel}] 👥 {len(watchers)} watchers actifs: {', '.join(watchers)}")
                    
                    self.last_cleanup_log = current_time

                time.sleep(5)  # Vérification toutes les 5s

            except Exception as e:
                logger.error(f"❌ Erreur cleanup_loop: {e}")
                time.sleep(5)

    def _print_channels_summary(self):
        """Affiche un résumé des chaînes et de leurs watchers"""
        try:
            active_channels = {}
            for ip, watcher in self.watchers.items():
                channel = watcher.get("current_channel", "unknown")
                if channel not in active_channels:
                    active_channels[channel] = set()
                active_channels[channel].add(ip)
            
            if active_channels:
                logger.info("📊 Résumé des chaînes:")
                for channel, watchers in active_channels.items():
                    logger.info(f"[{channel}] 👥 {len(watchers)} watchers: {', '.join(watchers)}")
            else:
                logger.info("📊 Aucun watcher actif")
        except Exception as e:
            logger.error(f"❌ Erreur résumé chaînes: {e}")

    def get_channel_watchers(self, channel):
        """Récupère le nombre actuel de watchers pour une chaîne"""
        if hasattr(self.manager, "channels") and channel in self.manager.channels:
            return getattr(self.manager.channels[channel], "watchers_count", 0)
        return 0

    def _parse_access_log(self, line):
        """Version harmonisée qui utilise la fonction utilitaire"""
        return parse_access_log(line)

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
                        "current_channel": channel,
                        "last_activity": current_time
                    }
                    # Démarrer le timer explicitement
                    timer.start()
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
                    # Démarrer le nouveau timer explicitement
                    timer.start()
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
                self.watchers[ip]["last_activity"] = current_time

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
            time.sleep(5)  # Réduit de 20s à 5s

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
            self._cleanup_loop()
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
        with self.lock:
            current_time = time.time()
            
            # Identifier le segment
            segment_match = re.search(r"segment_(\d+)\.ts", line)
            segment_id = segment_match.group(1) if segment_match else "unknown"

            # Enregistrer le segment
            self.segments_by_channel.setdefault(channel, {})[segment_id] = current_time

            # Créer ou mettre à jour le watcher
            if ip not in self.watchers:
                # Toujours créer un timer, même sans stats_collector
                timer = WatcherTimer(channel, ip, self.stats_collector)
                self.watchers[ip] = {
                    "timer": timer,
                    "last_seen": current_time,
                    "type": "segment",
                    "user_agent": user_agent,
                    "current_channel": channel,
                    "last_activity": current_time
                }
                logger.info(f"🆕 Nouveau watcher détecté: {ip} sur {channel}")
                # Forcer une mise à jour immédiate du compteur
                self._update_watcher_count(channel)
            else:
                # Mise à jour du watcher existant
                watcher = self.watchers[ip]
                watcher["last_seen"] = current_time
                watcher["last_activity"] = current_time
                watcher["type"] = "segment"
                
                # Si le canal a changé, créer un nouveau timer
                if watcher.get("current_channel") != channel:
                    if "timer" in watcher:
                        watcher["timer"].stop()
                    timer = WatcherTimer(channel, ip, self.stats_collector)
                    watcher["timer"] = timer
                    watcher["current_channel"] = channel
                    logger.info(f"🔄 Changement de chaîne pour {ip}: {channel}")
                    # Forcer une mise à jour des compteurs pour les deux chaînes
                    self._update_watcher_count(watcher.get("current_channel"))
                    self._update_watcher_count(channel)

            # S'assurer que le timer est démarré
            if "timer" in self.watchers[ip]:
                self.watchers[ip]["timer"].start()

            # Ajouter du temps de visionnage si stats_collector existe
            if self.stats_collector:
                segment_duration = 4.0  # secondes par segment
                self.stats_collector.add_watch_time(channel, ip, segment_duration)
                if user_agent:
                    self.stats_collector.update_user_stats(ip, channel, segment_duration, user_agent)

            # Mettre à jour le compteur de watchers
            self._update_watcher_count(channel)

    def _handle_playlist_request(self, channel, ip):
        """Traite une requête de playlist"""
        # Cette méthode est maintenant vide car tout est géré dans _process_log_line
        pass

    def _update_channel_watchers_count(self, channel):
        """Met à jour le compteur de watchers pour une chaîne"""
        try:
            # Compter les watchers actifs pour cette chaîne
            active_watchers = set()
            current_time = time.time()
            
            for ip, data in self.watchers.items():
                # Validation stricte de l'IP
                try:
                    # Vérifier le format de base
                    ip_pattern = r'^(\d{1,3}\.){3}\d{1,3}$'
                    if not ip or not re.match(ip_pattern, ip):
                        logger.warning(f"⚠️ Format IP invalide ignoré dans le compteur: {ip}")
                        continue
                        
                    # Vérifier que chaque partie est un nombre valide
                    ip_parts = ip.split('.')
                    if not all(0 <= int(part) <= 255 for part in ip_parts):
                        logger.warning(f"⚠️ Valeurs IP hors limites ignorées dans le compteur: {ip}")
                        continue
                except ValueError:
                    logger.warning(f"⚠️ IP avec valeurs non numériques ignorée dans le compteur: {ip}")
                    continue
                
                # Vérifier que le watcher est actif et sur la bonne chaîne
                if (data.get("current_channel") == channel and 
                    current_time - data.get("last_activity", 0) < self.SEGMENT_TIMEOUT and
                    "timer" in data and data["timer"].is_running()):
                    active_watchers.add(ip)

            # Mettre à jour le compteur dans le manager via le callback
            if self.update_watchers:
                self.update_watchers(channel, len(active_watchers), "/hls/")
                logger.debug(f"[{channel}] 👥 Mise à jour compteur: {len(active_watchers)} watchers actifs")
                
                # Log pour debug
                if active_watchers:
                    logger.debug(f"[{channel}] 👥 {len(active_watchers)} watchers actifs: {', '.join(active_watchers)}")

        except Exception as e:
            logger.error(f"❌ Erreur mise à jour compteur watchers pour {channel}: {e}")
            import traceback
            logger.error(traceback.format_exc())

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
                    self._cleanup_loop()
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
        """Met à jour le compteur de watchers pour une chaîne"""
        try:
            if not channel or not hasattr(self.manager, "update_watchers"):
                return

            # Compter les watchers actifs pour cette chaîne
            active_watchers = set()
            current_time = time.time()
            
            for ip, data in self.watchers.items():
                # Validation stricte de l'IP
                try:
                    # Vérifier le format de base
                    ip_pattern = r'^(\d{1,3}\.){3}\d{1,3}$'
                    if not ip or not re.match(ip_pattern, ip):
                        logger.warning(f"⚠️ Format IP invalide ignoré dans le compteur: {ip}")
                        continue
                        
                    # Vérifier que chaque partie est un nombre valide
                    ip_parts = ip.split('.')
                    if not all(0 <= int(part) <= 255 for part in ip_parts):
                        logger.warning(f"⚠️ Valeurs IP hors limites ignorées dans le compteur: {ip}")
                        continue
                except ValueError:
                    logger.warning(f"⚠️ IP avec valeurs non numériques ignorée dans le compteur: {ip}")
                    continue
                    
                # Vérifier que le watcher est actif et sur la bonne chaîne
                if (data.get("current_channel") == channel and 
                    current_time - data.get("last_activity", 0) < self.SEGMENT_TIMEOUT and
                    "timer" in data and data["timer"].is_running()):
                    active_watchers.add(ip)

            # Mettre à jour le compteur dans le manager
            self.manager.update_watchers(channel, len(active_watchers), "/hls/")
            
            # Log pour debug
            if active_watchers:
                logger.debug(f"[{channel}] 👥 {len(active_watchers)} watchers actifs: {', '.join(active_watchers)}")

        except Exception as e:
            logger.error(f"❌ Erreur mise à jour watchers pour {channel}: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def run_client_monitor(self):
        """Méthode principale qui suit le fichier de log nginx"""
        logger.info("🔄 Démarrage du suivi des logs nginx")
        
        while not self.stop_event.is_set():
            try:
                # Vérifier que le fichier de log existe
                if not os.path.exists(self.log_path):
                    logger.error(f"❌ Fichier de log introuvable: {self.log_path}")
                    time.sleep(5)
                    continue

                # Ouvrir et suivre le fichier
                with open(self.log_path, 'r') as f:
                    # Aller à la fin du fichier
                    f.seek(0, 2)
                    
                    while not self.stop_event.is_set():
                        line = f.readline()
                        if not line:
                            time.sleep(0.1)  # Petite pause pour ne pas surcharger le CPU
                            continue
                            
                        # Traiter la ligne
                        if "/hls/" in line:
                            ip, channel, request_type, is_valid, user_agent = self._parse_access_log(line)
                            if channel and ip and is_valid:
                                if request_type == "segment":
                                    self._handle_segment_request(channel, ip, line, user_agent)
                                elif request_type == "playlist":
                                    self._handle_playlist_request(channel, ip)
                                
                                # Log pour le debug
                                logger.debug(f"📝 Traité: {request_type} pour {channel} par {ip}")

            except Exception as e:
                logger.error(f"❌ Erreur suivi logs: {e}")
                import traceback
                logger.error(traceback.format_exc())
                time.sleep(5)  # Pause avant de réessayer
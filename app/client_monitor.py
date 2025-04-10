import os
import time
import threading
from pathlib import Path
from config import logger
from typing import Dict, Tuple, List, Set, Optional
import re
from log_utils import parse_access_log
from time_tracker import TimeTracker
import json

os.environ['TZ'] = 'Europe/Paris'
time.tzset()  # Applique le changement

class ClientMonitor(threading.Thread):

    def __init__(self, log_path, update_watchers_callback, manager, stats_collector=None):
        super().__init__(daemon=True)
        self.log_path = log_path

        # Callback pour mettre à jour les watchers dans le manager
        self.update_watchers = update_watchers_callback
        # Manager pour accéder aux segments et aux canaux
        self.manager = manager
        # StatsCollector pour les statistiques
        self.stats_collector = stats_collector

        # Initialiser le TimeTracker
        self.time_tracker = TimeTracker(stats_collector)

        # dictionnaire pour stocker les watchers actifs (utilisé principalement pour le user_agent)
        self.watchers: Dict[str, Dict] = {}  # {ip: {"last_seen", "type", "user_agent", "current_channel", "last_activity"}}

        # dictionnaire pour stocker les segments demandés par chaque canal (Est-ce encore utilisé?)
        self.segments_by_channel = {}  # {channel: {segment_id: last_requested_time}}

        # *** ADDED: Initialize _channel_watchers ***
        self._channel_watchers: Dict[str, int] = {} # {channel: count}
        # *** END ADDED ***

        # Pour éviter les accès concurrents
        self.lock = threading.Lock()

        # Événement pour l'arrêt propre du thread de nettoyage (peut-être plus nécessaire?)
        self.stop_event = threading.Event()

        # Initialisation des attributs de nettoyage (peut-être plus nécessaire?)
        self.last_cleanup_log = time.time()
        # self.last_cleanup_time = time.time()
        # self.cleanup_interval = 30

        # logger.info(f"⏱️ Timeouts configurés - Watcher inactif: {self.WATCHER_INACTIVITY_TIMEOUT}s, Segment: {self.SEGMENT_TIMEOUT}s, Playlist: {self.PLAYLIST_TIMEOUT}s")
        logger.info("⏱️ Timeouts de watchers maintenant gérés par TimeTracker.")

        # Thread de nettoyage - DÉSACTIVÉ
        # self.cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        # self.cleanup_thread.start()
        
        # Pour le logging périodique (déplacé dans la boucle principale?)
        # self.last_cleanup_log = time.time()

    def run(self):
        """Méthode principale du thread"""
        logger.info("🚀 Démarrage du ClientMonitor")
        self.run_client_monitor() # Assurez-vous que run_client_monitor gère le nouveau logging périodique si nécessaire

    def _cleanup_loop(self):
        """Nettoie les watchers inactifs et vérifie l'état des logs - DÉSACTIVÉ"""
        logger.info("🧹 Boucle de nettoyage interne de ClientMonitor désactivée (gérée par TimeTracker).")
        return # Ne rien faire
        # while not self.stop_event.is_set():
        #     try:
        #         # Utiliser le TimeTracker pour le nettoyage
        #         self.time_tracker.cleanup_inactive_watchers()
        #         
        #         # Forcer la mise à jour des compteurs pour toutes les chaînes (REDONDANT?)
        #         # ... (code existant)
        #         
        #         # Log périodique des watchers actifs (REDONDANT?)
        #         # ... (code existant)
        #         
        #         time.sleep(5)  # Vérification toutes les 5s
        #         
        #     except Exception as e:
        #         logger.error(f"❌ Erreur cleanup_loop: {e}")
        #         time.sleep(5)

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

            # Log plus détaillé pour le diagnostic
            if is_valid and channel and "/hls/" in line:
                logger.info(f"📝 TRAITEMENT LIGNE: ip={ip}, channel={channel}, type={request_type}, valid={is_valid}")

            # Si la ligne n'est pas valide ou pas de channel, on ignore
            if not is_valid or not channel:
                if "/hls/" in line and not is_valid:
                    logger.warning(f"⚠️ Ligne non valide ignorée: {line[:100]}...")
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
                self.watchers[ip] = {
                    "last_seen": current_time,
                    "type": request_type,
                    "user_agent": user_agent,
                    "current_channel": channel,
                    "last_activity": current_time
                }
                logger.info(f"🆕 Nouveau watcher détecté: {ip} sur {channel}")
                # Forcer une mise à jour du compteur de watchers
                self._update_channel_watchers_count(channel)
            else:
                # Vérifier si la chaîne a changé
                old_channel = self.watchers[ip].get("current_channel")
                if old_channel != channel:
                    logger.info(f"🔄 Changement de chaîne pour {ip}: {old_channel} -> {channel}")
                    # Notifier le StatsCollector du changement de chaîne
                    if self.stats_collector:
                        self.stats_collector.handle_channel_change(ip, old_channel, channel)
                    self.watchers[ip]["current_channel"] = channel
                    
                    # Mettre à jour les compteurs pour l'ancienne et la nouvelle chaîne
                    if old_channel:
                        self._update_channel_watchers_count(old_channel)
                    self._update_channel_watchers_count(channel)
                else:
                    # Même chaîne, vérifier le temps écoulé depuis la dernière requête
                    last_seen = self.watchers[ip].get("last_seen", 0)
                    if current_time - last_seen < 0.5 and request_type == "playlist":  # Réduit de 1.0 à 0.5 secondes
                        # Ignorer les mises à jour trop rapprochées pour éviter le spam
                        logger.debug(f"Ignoré mise à jour rapprochée pour {ip} sur {channel} (interval: {current_time - last_seen:.2f}s)")
                        # On met quand même à jour last_activity pour garder le watcher actif
                        self.watchers[ip]["last_activity"] = current_time
                        return

                # Mise à jour des infos
                self.watchers[ip]["last_seen"] = current_time
                self.watchers[ip]["type"] = request_type
                self.watchers[ip]["user_agent"] = user_agent
                self.watchers[ip]["last_activity"] = current_time

            # Déléguer la collecte des statistiques au StatsCollector
            if self.stats_collector:
                if request_type == "segment":
                    self.stats_collector.handle_segment_request(channel, ip, line, user_agent)
                elif request_type == "playlist":
                    # Pour les playlists, on n'ajoute du temps que si ça fait au moins 1 seconde
                    # depuis la dernière requête, pour éviter les doublons
                    last_seen = self.watchers[ip].get("last_seen", 0)
                    if current_time - last_seen >= 1.0:  # Réduit de 3.0 à 1.0 secondes
                        elapsed = min(current_time - last_seen, 5.0)
                        self.stats_collector.handle_playlist_request(channel, ip, elapsed, user_agent)

            # Si c'est un segment ou une playlist, forcer la mise à jour du compteur
            if request_type in ["segment", "playlist"]:
                # >>> COMMENT OUT THIS CALL - Updates are now pushed via _push_tracker_status_to_manager
                # self._update_channel_watchers_count(channel)
                logger.debug(f"[_update_watcher] Skipping redundant call to _update_channel_watchers_count for {channel}")
                pass # Explicitly do nothing here now

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
            if elapsed < 1:  # Au moins 1 seconde entre les mises à jour (réduit de 2 à 1)
                return

        channels_updated = 0

        # Vérifier également les chaînes avec des watchers actifs via TimeTracker
        all_channels_to_update = set(self.modified_channels)
        
        # Ajouter toutes les chaînes avec des watchers actifs dans TimeTracker
        if hasattr(self.time_tracker, "_active_segments"):
            for channel in self.time_tracker._active_segments:
                if channel and channel != "master_playlist":
                    all_channels_to_update.add(channel)

        with self.lock:
            for channel_to_update in list(all_channels_to_update):
                # IMPORTANT: Ignorer la playlist principale
                if channel_to_update == "master_playlist":
                    continue

                # Calcule les watchers actifs pour cette chaîne via TimeTracker (source unique de vérité)
                active_ips = self.time_tracker.get_active_watchers(channel_to_update, include_buffer=True)
                count = len(active_ips)

                # Obtenir l'ancien compte depuis le manager
                old_count = self.get_channel_watchers(channel_to_update)

                # Décider si une mise à jour est nécessaire
                needs_update = (count != old_count)

                if needs_update:
                    # Loguer le changement détecté
                    active_ips_list = sorted(list(active_ips)) # Already have the list here
                    if old_count == 0 and count > 0: # Devenu actif
                        logger.info(f"[{channel_to_update}] ✅ Watchers ACTIVÉS: {count}")
                        if len(active_ips_list) <= 10:
                             logger.info(f"[{channel_to_update}] 👥 IPs actives: {', '.join(active_ips_list)}")
                        else:
                             logger.info(f"[{channel_to_update}] 👥 {count} IPs actives (trop nombreuses pour log)")
                    elif old_count > 0 and count == 0: # Devenu inactif
                        logger.info(f"[{channel_to_update}] 🅾️ Watchers DÉSACTIVÉS (précédent: {old_count})")
                    else: # Changement de compte (X -> Y, ambos > 0)
                        logger.info(f"[{channel_to_update}] 🔄 Changement watchers: {old_count} → {count}")
                        if len(active_ips_list) <= 10:
                             logger.info(f"[{channel_to_update}] 👥 IPs actives: {', '.join(active_ips_list)}")
                        else:
                             logger.info(f"[{channel_to_update}] 👥 {count} IPs actives (trop nombreuses pour log)")

                    # Effectuer la mise à jour via le callback
                    if self.update_watchers:
                        # MODIFIED: Pass both count AND the list of IPs
                        self.update_watchers(channel_to_update, count, active_ips_list, "/hls/") 
                        channels_updated += 1
                    else:
                        logger.warning(f"[{channel_to_update}] ⚠️ Callback update_watchers non disponible")

                # Log périodique (optionnel, gardons le en DEBUG pour l'instant)
                # elif count > 0 and (current_time - self.last_update_time > 60):

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
            logger.debug(f"[{channel}] 🔍 Traitement requête segment pour {ip}")
            
            # Mettre à jour le watcher dans le dictionnaire
            current_time = time.time()
            if ip not in self.watchers:
                self.watchers[ip] = {
                    "last_seen": current_time,
                    "type": "segment",
                    "user_agent": user_agent,
                    "current_channel": channel,
                    "last_activity": current_time
                }
                logger.info(f"🆕 Nouveau watcher détecté: {ip} sur {channel}")
            else:
                # Mettre à jour les informations du watcher
                self.watchers[ip]["last_seen"] = current_time
                self.watchers[ip]["type"] = "segment"
                self.watchers[ip]["user_agent"] = user_agent
                self.watchers[ip]["current_channel"] = channel
                self.watchers[ip]["last_activity"] = current_time
            
            # *** MODIFIED: Record activity with calculated expiry based on segment duration ***
            try:
                # Assume manager provides the segment duration (hls_time) used by FFmpeg for this channel
                segment_duration = self.manager.get_channel_segment_duration(channel) 

                if segment_duration and isinstance(segment_duration, (int, float)) and segment_duration > 0:
                    # Calculate expiry: segment duration + 20% buffer
                    expiry_duration = segment_duration * 1.2 
                    logger.debug(f"[{channel}] Calculated expiry for {ip}: {expiry_duration:.2f}s (segment: {segment_duration}s)")
                    # Call TimeTracker with the calculated expiry duration
                    # IMPORTANT: TimeTracker.record_activity must be modified to accept expiry_duration
                    self.time_tracker.record_activity(ip, channel, expiry_duration=expiry_duration)
                else:
                    # Fallback if duration is invalid or not found
                    if segment_duration is None:
                         logger.warning(f"[{channel}] Segment duration not found via manager for {ip}. Falling back to default TimeTracker timeout.")
                    else:
                         logger.warning(f"[{channel}] Invalid segment duration ({segment_duration}) from manager for {ip}. Falling back to default TimeTracker timeout.")
                    self.time_tracker.record_activity(ip, channel) # Fallback call

            except AttributeError:
                 logger.error(f"[{channel}] Manager missing 'get_channel_segment_duration' method. Cannot calculate expiry for {ip}. Falling back.")
                 self.time_tracker.record_activity(ip, channel) # Fallback call
            except Exception as e:
                 logger.error(f"[{channel}] Error getting segment duration or recording activity for {ip}: {e}")
                 self.time_tracker.record_activity(ip, channel) # Fallback call
            # *** END MODIFIED ***

            # Obtenir les watchers actifs et mettre à jour le statut
            try:
                active_watchers = self.time_tracker.get_active_watchers(channel, include_buffer=True)
                calculated_count = len(active_watchers)
                
                # Obtenir le compte précédent pour comparaison
                old_count = self.get_channel_watchers(channel)
                
                # Mettre à jour seulement si le nombre a changé ou toutes les 60 secondes
                last_update_ts = getattr(self, "_last_update_time", {}).get(channel, 0)
                force_update = current_time - last_update_ts > 60

                if old_count != calculated_count or force_update:
                    # Mettre à jour le timestamp de la dernière MAJ
                    if not hasattr(self, "_last_update_time"):
                        self._last_update_time = {}
                    self._last_update_time[channel] = current_time
                    
                    # Log et mise à jour via callback
                    if self.update_watchers:
                        # Log based on the type of change
                        if calculated_count != old_count:
                            if old_count == 0 and calculated_count > 0: # Became active
                                logger.info(f"[{channel}] 👁️ Watchers devenus actifs: {calculated_count}")
                                if len(active_watchers) <= 10:
                                     logger.info(f"[{channel}] 👥 IPs actives (TimeTracker): {', '.join(sorted(list(active_watchers)))}")
                                else:
                                     logger.info(f"[{channel}] 👥 {calculated_count} IPs actives (TimeTracker) - trop nombreuses pour log")
                            elif old_count > 0 and calculated_count == 0: # Became inactive
                                logger.info(f"[{channel}] 👁️ Watchers tombés à 0 (précédent: {old_count})")
                            else: # Count changed but still > 0
                                logger.debug(f"[{channel}] 👁️ Changement nombre watchers: {old_count} → {calculated_count}")
                        elif force_update and calculated_count > 0: # Periodic update, no change
                            logger.debug(f"[{channel}] ⏱️ MAJ périodique: {calculated_count} watchers actifs (TimeTracker)")

                        # Envoyer la mise à jour avec la liste des watchers actifs
                        self.update_watchers(channel, calculated_count, list(active_watchers), "/hls/")
                    else:
                        logger.warning(f"[{channel}] ⚠️ Callback update_watchers non disponible pour MAJ {calculated_count} watchers.")

            except Exception as e:
                logger.error(f"❌ Erreur mise à jour compteur watchers pour {channel}: {e}")
                import traceback
                logger.error(traceback.format_exc())

    def _update_channel_watchers_count(self, channel):
        """
        Met à jour le compteur de watchers pour une chaîne en se basant uniquement sur TimeTracker.
        """
        try:
            current_time = time.time()
            
            # Obtenir les watchers actifs (y compris buffer) directement depuis TimeTracker
            active_watchers = self.time_tracker.get_active_watchers(channel, include_buffer=True)
            calculated_count = len(active_watchers)
            
            # Obtenir le compte précédent pour comparaison
            old_count = self.get_channel_watchers(channel)
            
            # Mettre à jour seulement si le nombre a changé ou toutes les 60 secondes
            last_update_ts = getattr(self, "_last_update_time", {}).get(channel, 0)
            force_update = current_time - last_update_ts > 60

            if old_count != calculated_count or force_update:
                # Mettre à jour le timestamp de la dernière MAJ
                if not hasattr(self, "_last_update_time"):
                    self._last_update_time = {}
                self._last_update_time[channel] = current_time
                
                # Log et mise à jour via callback
                if self.update_watchers:
                    # Log based on the type of change
                    if calculated_count != old_count:
                        if old_count == 0 and calculated_count > 0: # Became active
                            logger.info(f"[{channel}] 👁️ Watchers devenus actifs: {calculated_count}")
                            if len(active_watchers) <= 10:
                                 logger.info(f"[{channel}] 👥 IPs actives (TimeTracker): {', '.join(sorted(list(active_watchers)))}")
                            else:
                                 logger.info(f"[{channel}] 👥 {calculated_count} IPs actives (TimeTracker) - trop nombreuses pour log")
                        elif old_count > 0 and calculated_count == 0: # Became inactive
                            logger.info(f"[{channel}] 👁️ Watchers tombés à 0 (précédent: {old_count})")
                        else: # Count changed but still > 0
                            logger.debug(f"[{channel}] 👁️ Changement nombre watchers: {old_count} → {calculated_count}")
                    elif force_update and calculated_count > 0: # Periodic update, no change
                        logger.debug(f"[{channel}] ⏱️ MAJ périodique: {calculated_count} watchers actifs (TimeTracker)")

                    # Envoyer la mise à jour avec la liste des watchers actifs
                    self.update_watchers(channel, calculated_count, list(active_watchers), "/hls/")
                else:
                    logger.warning(f"[{channel}] ⚠️ Callback update_watchers non disponible pour MAJ {calculated_count} watchers.")

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

    def run_client_monitor(self):
        """Méthode principale qui suit le fichier de log nginx"""
        logger.info("🔄 Démarrage du suivi des logs nginx dans ClientMonitor")

        # Enlever les timeouts ici, TimeTracker gère cela
        # self.SEGMENT_TIMEOUT = 300
        # self.PLAYLIST_TIMEOUT = 300
        # self.WATCHER_INACTIVITY_TIMEOUT = 600
        logger.info("⏱️ Timeouts gérés par TimeTracker.")
        
        # Add a periodic force update during initialization
        def delayed_force_update():
            # Wait for system to initialize
            time.sleep(20)
            logger.info("🔄 [INIT] Forcing first status update from TimeTracker")
            self.force_status_update()
            
            # Do one more update after a delay to catch any missed updates
            time.sleep(40)
            logger.info("🔄 [INIT] Forcing second status update from TimeTracker")
            self.force_status_update()
            
        # Start the delayed update thread
        force_update_thread = threading.Thread(target=delayed_force_update, daemon=True)
        force_update_thread.start()
        logger.info("🔄 [INIT] Scheduled delayed force updates")

        # Pas besoin de modified_channels avec l'approche simplifiée
        # self.modified_channels = set()
        # self.last_update_time = time.time()

        while not self.stop_event.is_set():
            try:
                if not os.path.exists(self.log_path):
                    logger.error(f"❌ Fichier de log introuvable: {self.log_path}")
                    time.sleep(5)
                    continue

                with open(self.log_path, 'r') as f:
                    f.seek(0, 2)
                    logger.info(f"👁️ ClientMonitor monitoring {self.log_path}")

                    while not self.stop_event.is_set():
                        line = f.readline()
                        if not line:  # This checks if readline returned an empty string (EOF)
                            time.sleep(0.1)
                            continue

                        # Strip the line after checking if it's None/empty
                        line = line.strip()
                        if not line:  # Skip empty lines after stripping
                            continue

                        if "/hls/" not in line:
                            continue

                        try:
                            # Improved logging to show raw line content with escaping for control chars
                            logger.info(f"---> RAW LINE READ: {repr(line)}")
                            ip, channel, request_type, is_valid, user_agent = self._parse_access_log(line)
                            logger.info(f"---> PARSED: ip={ip}, ch={channel}, type={request_type}, valid={is_valid}")
                            # <--- End Log
                            
                            # Si segment valide pour une chaîne connue
                            if (
                                is_valid
                                and ip # Make sure we have an IP
                                and channel
                                and channel != "master_playlist"
                                and hasattr(self.manager, "channels")
                                and channel in self.manager.channels
                            ):
                                if request_type == "segment":
                                    # 1. Enregistrer l'activité dans TimeTracker
                                    try:
                                        segment_duration = self.manager.get_channel_segment_duration(channel)
                                        if segment_duration and isinstance(segment_duration, (int, float)) and segment_duration > 0:
                                            expiry_duration = segment_duration * 1.2
                                            self.time_tracker.record_activity(ip, channel, expiry_duration=expiry_duration)
                                            logger.debug(f"[{channel}] Activity recorded for {ip} (expiry: {expiry_duration:.1f}s)")
                                        else:
                                            self.time_tracker.record_activity(ip, channel)
                                            logger.debug(f"[{channel}] Activity recorded for {ip} (default expiry)")
                                    except AttributeError:
                                        logger.error(f"[{channel}] Manager missing 'get_channel_segment_duration' method.")
                                        self.time_tracker.record_activity(ip, channel)
                                    except Exception as e:
                                        logger.error(f"[{channel}] Error recording activity for {ip}: {e}")
                                        self.time_tracker.record_activity(ip, channel)
                                    
                                    # 2. Gérer la mise à jour de statut (potentiellement double)
                                    self._handle_channel_activity_update(ip, channel)
                                    
                                elif request_type == "playlist":
                                    # Also record activity for playlist requests with default timeout
                                    logger.debug(f"[{channel}] Recording playlist activity for {ip}")
                                    self.time_tracker.record_activity(ip, channel)
                                    
                                    # Also update status for playlist requests to keep viewers active
                                    self._handle_channel_activity_update(ip, channel)

                            # The following condition is now redundant and should be removed
                            # elif is_valid and channel and request_type == "playlist":
                            #    logger.debug(f"Ignoring playlist request for status update: {channel} by {ip}")

                        except Exception as e:
                            logger.error(f"❌ Erreur traitement ligne HLS: {e}")
                            logger.error(f"LIGNE: {line[:200]}...")

            except Exception as e:
                logger.error(f"❌ Erreur suivi logs (boucle externe): {e}")
                import traceback
                logger.error(traceback.format_exc())
                time.sleep(5)

    def _handle_channel_activity_update(self, ip: str, current_channel: str):
        """Handles the status update when activity is detected for an IP on a channel."""
        previous_channel = None
        try:
            with self.lock:
                # Get previous channel for this IP, if known
                watcher_data = self.watchers.get(ip)
                if watcher_data:
                    previous_channel = watcher_data.get('current_channel')
                    # Update watcher's current channel and last seen time
                    watcher_data['current_channel'] = current_channel
                    watcher_data['last_activity'] = time.time() # Use last_activity consistent with _update_watcher
                    watcher_data['last_seen'] = time.time()
                else:
                    # Maybe add the watcher if completely new? Or rely on _update_watcher?
                    # For now, let's assume _update_watcher handles new watchers.
                    logger.warning(f"[_handle_channel_activity_update] Watcher {ip} not found in self.watchers, cannot determine previous channel.")

            # Force TimeTracker cleanup to ensure inactive viewers are removed immediately
            if hasattr(self, 'time_tracker') and self.time_tracker:
                logger.info(f"🧹 Forcing immediate cleanup due to channel activity")
                # Reset _last_cleanup_time to ensure it runs
                self.time_tracker._last_cleanup_time = 0
                self.time_tracker.cleanup_inactive_watchers()

            # If the channel changed, update the status of the PREVIOUS channel first
            if previous_channel and previous_channel != current_channel:
                logger.info(f"🔄 Detected channel change for {ip}: {previous_channel} -> {current_channel}. Pushing status update for {previous_channel}.")
                self._push_tracker_status_to_manager(previous_channel)
            
            # Always push the status for the CURRENT channel where activity was detected
            logger.debug(f"Activity detected for {ip} on {current_channel}. Pushing status update.")
            self._push_tracker_status_to_manager(current_channel)

        except Exception as e:
            logger.error(f"❌ Erreur dans _handle_channel_activity_update pour ip={ip}, channel={current_channel}: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def _push_tracker_status_to_manager(self, channel: str):
        """Gets current status from TimeTracker and pushes it to the manager."""
        try:
            if not self.update_watchers:
                logger.warning(f"[{channel}] Callback update_watchers non disponible.")
                return

            # Log TimeTracker state before getting watchers
            logger.info(f"[{channel}] 🔍 Getting watchers from TimeTracker (timeouts: {getattr(self.time_tracker, '_segment_timeout', 'N/A')}s)")
            
            active_watchers = self.time_tracker.get_active_watchers(channel, include_buffer=True)
            calculated_count = len(active_watchers)
            active_ips_list = list(active_watchers)

            # Always log this information for debugging
            if calculated_count > 0:
                logger.info(f"[{channel}] 📊 Current status: {calculated_count} viewers - IPs: {active_ips_list}")
            else:
                logger.info(f"[{channel}] 📊 No active viewers detected")

            # Pass source='tracker' to ensure it passes the gatekeeper
            self.update_watchers(channel, calculated_count, active_ips_list, "/hls/", source='tracker')
            logger.info(f"[{channel}] ✅ Status pushed to manager (watchers: {calculated_count})")

        except Exception as e:
            logger.error(f"❌ Erreur dans _push_tracker_status_to_manager pour {channel}: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def _update_channel_status(self, initial_call=False):
        """Update channel status for dashboard"""
        try:
            if not self.channel_status:
                logger.warning("⚠️ Channel status manager not initialized")
                return False
                
            # Ensure stats directory exists and has proper permissions
            stats_dir = Path(os.path.dirname(CHANNELS_STATUS_FILE))
            stats_dir.mkdir(parents=True, exist_ok=True)
            try:
                os.chmod(stats_dir, 0o777)
            except Exception as chmod_err:
                logger.warning(f"Could not chmod stats dir {stats_dir}: {chmod_err}")

            # Load current channel stats
            channel_stats_file = Path("/app/stats/channel_stats.json")
            if not channel_stats_file.exists():
                logger.error("❌ Channel stats file not found")
                return False

            try:
                with open(channel_stats_file, 'r') as f:
                    stats_data = json.load(f)
            except Exception as e:
                logger.error(f"❌ Error loading channel stats: {e}")
                return False

            # Prepare channel status data
            channels_dict = {}
            total_viewers = 0

            # Use scan_lock for iterating self.manager.channels safely
            with self.manager.scan_lock:
                for channel_name, channel in self.manager.channels.items():
                    if channel and hasattr(channel, 'is_ready'):
                        # Get current watchers from TimeTracker
                        active_watchers = self.time_tracker.get_active_watchers(channel_name, include_buffer=True)
                        watcher_count = len(active_watchers)
                        total_viewers += watcher_count

                        # Get stats from channel_stats.json
                        channel_stats = stats_data.get("channels", {}).get(channel_name, {})
                        unique_viewers = channel_stats.get("unique_viewers", [])
                        watchlist = channel_stats.get("watchlist", {})

                        status_data = {
                            "active": channel.is_ready(),
                            "streaming": channel.is_streaming() if hasattr(channel, 'is_streaming') else False,
                            "viewers": watcher_count,
                            "watchers": list(active_watchers),
                            "unique_viewers": unique_viewers,
                            "total_watch_time": channel_stats.get("total_watch_time", 0),
                            "watchlist": watchlist
                        }
                        channels_dict[channel_name] = status_data

            # If no channels are ready or available, don't wipe the status
            if not channels_dict:
                log_key = "_last_no_channels_log_time"
                now = time.time()
                if not hasattr(self, log_key) or now - getattr(self, log_key) > 60:
                    logger.info("🤷 No initialized channels found, skipping status update.")
                    setattr(self, log_key, now)
                return True

            # Update status with retry logic
            max_retries = 3
            retry_delay = 1
            
            for attempt in range(max_retries):
                try:
                    # Call the method responsible for writing the combined status
                    success = self.channel_status.update_all_channels(channels_dict)
                    if success:
                        logger.debug(f"✅ Channel status file updated successfully ({'initial' if initial_call else 'periodic'})")
                        return True
                    else:
                        logger.warning(f"⚠️ Failed to update channel status file (attempt {attempt + 1}/{max_retries})")
                        if attempt < max_retries - 1:
                            time.sleep(retry_delay)
                except Exception as e:
                    logger.error(f"❌ Error calling channel_status.update_all_channels (attempt {attempt + 1}/{max_retries}): {e}")
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
            
            logger.error("❌ Failed to update channel status file after all retries")
            return False
            
        except Exception as e:
            logger.error(f"❌ Error in _update_channel_status: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def _status_update_loop(self):
        # ... existing code ...
        pass

    def init_channel_status_manager(self):
        """Initialize the channel status manager for dashboard"""
        try:
            # Si déjà initialisé, ne pas réinitialiser
            if self.channel_status is not None:
                logger.info("ℹ️ Channel status manager déjà initialisé, skip")
                return

            logger.info("🚀 Initialisation du gestionnaire de statuts des chaînes")
            
            # Créer le dossier stats s'il n'existe pas
            stats_dir = Path(os.path.dirname(CHANNELS_STATUS_FILE))
            stats_dir.mkdir(parents=True, exist_ok=True)
            # Use relaxed permissions here too
            try:
                os.chmod(stats_dir, 0o777)
            except Exception as chmod_err:
                 logger.warning(f"Could not chmod stats dir {stats_dir}: {chmod_err}")
            
            # Créer le fichier de statut s'il n'existe pas ou est vide/invalide
            should_create_file = True
            if os.path.exists(CHANNELS_STATUS_FILE):
                try:
                    if os.path.getsize(CHANNELS_STATUS_FILE) > 0:
                         with open(CHANNELS_STATUS_FILE, 'r') as f:
                             json.load(f) # Try to parse existing json
                         should_create_file = False # File exists and is valid JSON
                    # else: file exists but is empty, will overwrite
                except (json.JSONDecodeError, OSError) as e:
                     logger.warning(f"Existing status file {CHANNELS_STATUS_FILE} is invalid or unreadable ({e}). Will overwrite.")
                     # File exists but is invalid, will overwrite
                     
            if should_create_file:
                 logger.info(f"Creating/Overwriting initial status file: {CHANNELS_STATUS_FILE}")
                 with open(CHANNELS_STATUS_FILE, 'w') as f:
                     # Initial state with 0 viewers
                     json.dump({
                         'channels': {},
                         'last_updated': int(time.time()),
                         'active_viewers': 0
                     }, f, indent=2)
                 try:
                     os.chmod(CHANNELS_STATUS_FILE, 0o666)
                 except Exception as chmod_err:
                     logger.warning(f"Could not chmod status file {CHANNELS_STATUS_FILE}: {chmod_err}")
            
            # Initialiser le gestionnaire de statuts
            from channel_status_manager import ChannelStatusManager
            self.channel_status = ChannelStatusManager(
                status_file=CHANNELS_STATUS_FILE
            )
            
            # Faire une mise à jour initiale avec retry, PASSING initial_call=True
            max_retries = 3
            retry_delay = 1
            
            for attempt in range(max_retries):
                try:
                    # Pass initial_call=True here
                    success = self._update_channel_status(initial_call=True)
                    if success:
                        logger.info("✅ Channel status manager initialized for dashboard (initial status set)")
                        return
                    else:
                        logger.warning(f"⚠️ Failed to update channel status during init (attempt {attempt + 1}/{max_retries})")
                        if attempt < max_retries - 1:
                            time.sleep(retry_delay)
                except Exception as e:
                    logger.error(f"❌ Error calling _update_channel_status during init (attempt {attempt + 1}/{max_retries}): {e}")
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
            
            logger.error("❌ Failed to initialize channel status manager after all retries")
            self.channel_status = None
            
        except Exception as e:
            logger.error(f"❌ Error initializing channel status manager: {e}")
            import traceback
            logger.error(traceback.format_exc())
            self.channel_status = None

    def _check_channels_timeout(self):
        # ... rest of the file ...
        pass

    def force_status_update(self):
        """Debug method to force update all channels status from TimeTracker to the JSON file."""
        logger.info("🔍 [DEBUG] Forcing status update for all channels")
        
        try:
            if not hasattr(self, "manager") or not self.manager:
                logger.error("❌ Manager not available for status update")
                return False
                
            if not hasattr(self, "time_tracker") or not self.time_tracker:
                logger.error("❌ TimeTracker not available for status update")
                return False
                
            # Get all channels with active watchers from TimeTracker
            active_channels = set()
            if hasattr(self.time_tracker, "_active_segments"):
                for channel in self.time_tracker._active_segments:
                    if channel and channel != "master_playlist":
                        active_channels.add(channel)
                        
            # Add all known channels from manager
            if hasattr(self.manager, "channels"):
                for channel_name in self.manager.channels:
                    if channel_name and channel_name != "master_playlist":
                        active_channels.add(channel_name)
                        
            logger.info(f"🔍 [DEBUG] Found {len(active_channels)} channels to update: {sorted(list(active_channels))}")
            
            # Push updates for all channels
            for channel in sorted(active_channels):
                logger.info(f"🔍 [DEBUG] Forcing update for channel: {channel}")
                self._push_tracker_status_to_manager(channel)
                
            return True
            
        except Exception as e:
            logger.error(f"❌ Error in force_status_update: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
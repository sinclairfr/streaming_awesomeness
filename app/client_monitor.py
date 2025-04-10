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
        """Méthode stub - Le nettoyage est désormais géré par TimeTracker"""
        # Force un nettoyage via TimeTracker
        if hasattr(self, 'time_tracker') and self.time_tracker:
            self.time_tracker.cleanup_inactive_watchers()
            logger.debug("🧹 Nettoyage des viewers inactifs via TimeTracker")
        else:
            logger.warning("⚠️ TimeTracker non disponible pour le nettoyage")

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
                # Always pass source='tracker' here
                active_watchers = self.time_tracker.get_active_watchers(channel, include_buffer=False)
                self.update_watchers(channel, len(active_watchers), list(active_watchers), "/hls/", source='tracker')

        except Exception as e:
            logger.error(f"❌ Erreur traitement ligne log: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def _update_watcher(self, ip, channel, request_type, user_agent, line):
        """
        Met à jour les informations d'un watcher spécifique.
        
        Note: self.watchers est principalement utilisé pour:
        1. Détecter les changements de chaîne
        2. Stocker des métadonnées comme l'user_agent
        3. Conserver l'historique des activités
        
        Le TimeTracker est la source unique de vérité pour le comptage des viewers actifs.
        """
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
            else:
                # Vérifier si la chaîne a changé
                old_channel = self.watchers[ip].get("current_channel")
                if old_channel != channel:
                    logger.info(f"🔄 Changement de chaîne pour {ip}: {old_channel} -> {channel}")
                    # Notifier le StatsCollector du changement de chaîne
                    if self.stats_collector:
                        self.stats_collector.handle_channel_change(ip, old_channel, channel)
                    self.watchers[ip]["current_channel"] = channel
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

            # Déléguer le suivi du temps d'activité à TimeTracker (source de vérité)
            if request_type == "segment":
                try:
                    segment_duration = self.manager.get_channel_segment_duration(channel)
                    if segment_duration and isinstance(segment_duration, (int, float)) and segment_duration > 0:
                        expiry_duration = segment_duration * 1.2
                        self.time_tracker.record_activity(ip, channel, expiry_duration=expiry_duration)
                    else:
                        self.time_tracker.record_activity(ip, channel)
                except AttributeError:
                    logger.warning(f"[{channel}] Manager missing 'get_channel_segment_duration' method")
                    self.time_tracker.record_activity(ip, channel)
                except Exception as e:
                    logger.error(f"[{channel}] Error recording activity: {e}")
                    self.time_tracker.record_activity(ip, channel)
            elif request_type == "playlist":
                # Pour les playlists, on utilise le timeout par défaut
                self.time_tracker.record_activity(ip, channel)

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
                active_ips = self.time_tracker.get_active_watchers(channel_to_update, include_buffer=False)
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
                             logger.debug(f"[{channel_to_update}] 👥 IPs actives: {', '.join(active_ips_list)}")
                        else:
                             logger.debug(f"[{channel_to_update}] 👥 {count} IPs actives (trop nombreuses pour log)")
                    elif old_count > 0 and count == 0: # Devenu inactif
                        logger.info(f"[{channel_to_update}] 🅾️ Watchers DÉSACTIVÉS (précédent: {old_count})")
                    else: # Changement de compte (X -> Y, ambos > 0)
                        logger.info(f"[{channel_to_update}] 🔄 Changement watchers: {old_count} → {count}")
                        if len(active_ips_list) <= 10:
                             logger.debug(f"[{channel_to_update}] 👥 IPs actives: {', '.join(active_ips_list)}")
                        else:
                             logger.debug(f"[{channel_to_update}] 👥 {count} IPs actives (trop nombreuses pour log)")

                    # Effectuer la mise à jour via le callback
                    if self.update_watchers:
                        # MODIFIED: Pass both count AND the list of IPs
                        self.update_watchers(channel_to_update, count, active_ips_list, "/hls/", source='tracker') 
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
                active_watchers = self.time_tracker.get_active_watchers(channel, include_buffer=False)
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
                                     logger.debug(f"[{channel}] 👥 IPs actives (TimeTracker): {', '.join(sorted(list(active_watchers)))}")
                                else:
                                     logger.debug(f"[{channel}] 👥 {calculated_count} IPs actives (TimeTracker) - trop nombreuses pour log")
                            elif old_count > 0 and calculated_count == 0: # Became inactive
                                logger.info(f"[{channel}] 👁️ Watchers tombés à 0 (précédent: {old_count})")
                            else: # Count changed but still > 0
                                logger.debug(f"[{channel}] 👁️ Changement nombre watchers: {old_count} → {calculated_count}")
                        elif force_update and calculated_count > 0: # Periodic update, no change
                            logger.debug(f"[{channel}] ⏱️ MAJ périodique: {calculated_count} watchers actifs (TimeTracker)")

                        # Envoyer la mise à jour avec la liste des watchers actifs
                        self.update_watchers(channel, calculated_count, list(active_watchers), "/hls/", source='tracker')
                    else:
                        logger.warning(f"[{channel}] ⚠️ Callback update_watchers non disponible pour MAJ {calculated_count} watchers.")

            except Exception as e:
                logger.error(f"❌ Erreur mise à jour compteur watchers pour {channel}: {e}")
                import traceback
                logger.error(traceback.format_exc())

    def _update_channel_watchers_count(self, channel):
        """Met à jour le nombre de watchers pour un canal donné via une méthode plus précise"""
        active_watchers = self.get_channel_watchers(channel)
        count = len(active_watchers)
        
        # Historique de debug
        if not hasattr(self, '_channel_counts'):
            self._channel_counts = {}
        
        # Uniquement logger si le compte a changé
        old_count = self._channel_counts.get(channel, -1)
        if old_count != count:
            if count > 0:
                logger.info(f"[{channel}] 📊 Current status: {count} viewers - IPs: {list(active_watchers)}")
            else:
                logger.debug(f"[{channel}] 📊 Current status: No viewers")
            self._channel_counts[channel] = count
        
        # Si nous avons un callback pour mettre à jour le gestionnaire, l'appeler
        if self.update_watchers_callback and count > 0:
            try:
                # Crucial: toujours spécifier source='tracker' pour que la mise à jour soit acceptée
                # Fournir la liste d'IPs active_watchers pour tracker précisément
                success = self.update_watchers_callback(
                    channel, 
                    count, 
                    list(active_watchers),
                    "/hls/",
                    source='tracker'
                )
                logger.info(f"[{channel}] ✅ Status pushed to manager (watchers: {count})")
                
                # Ajouter une vérification de sécurité pour éviter de perdre des viewers
                if not success:
                    logger.warning(f"[{channel}] ⚠️ Échec de la mise à jour du statut via le manager")
                    
            except Exception as e:
                    logger.error(f"[{channel}] ❌ Error updating watchers: {e}")

    def _push_tracker_status_to_manager(self, channel: str):
        """Pousse le statut du tracker vers le manager pour un canal spécifique"""
        try:
            # Récupération des viewers actifs depuis les timestamps TimeTracker
            if not hasattr(self, 'time_tracker') or not self.time_tracker:
                logger.warning(f"[{channel}] ⚠️ TimeTracker non disponible, impossible de mettre à jour le statut")
                return False

            # Récupérer les watchers actifs pour ce canal avec les timeouts configurés
            active_watchers = self.time_tracker.get_active_watchers(channel)
            watchers_count = len(active_watchers)
            
            logger.info(f"[{channel}] 🔍 Getting watchers from TimeTracker (timeouts: N/As)")
            
            # Si nous avons des watchers actifs, mettre à jour le manager
            if watchers_count > 0:
                logger.info(f"[{channel}] 📊 Current status: {watchers_count} viewers - IPs: {list(active_watchers)}")
                
                # Crucial: toujours spécifier source='tracker' pour que la mise à jour soit acceptée
                if self.update_watchers_callback:
                    success = self.update_watchers_callback(
                        channel,
                        watchers_count,
                        list(active_watchers),
                        "/hls/",
                        source='tracker'
                    )
                    if success:
                        logger.info(f"[{channel}] ✅ Status pushed to manager (watchers: {watchers_count})")
                        return True
                    else:
                        logger.warning(f"[{channel}] ⚠️ Failed to push status to manager")
                        return False
                else:
                    logger.warning(f"[{channel}] ⚠️ No update_watchers_callback available")
                    return False
            else:
                # Même avec 0 viewers, on doit mettre à jour le statut pour certains canaux
                # Par exemple, quand un canal perd son dernier viewer
                if self.update_watchers_callback and channel in self._channels_with_activity:
                    logger.info(f"[{channel}] 📊 No active viewers, but had previous activity - updating")
                    success = self.update_watchers_callback(
                        channel,
                        0,
                        [],
                        "/hls/",
                        source='tracker'
                    )
                    if success:
                        # Retirer ce canal de la liste des canaux avec activité si la mise à jour a réussi
                        # et qu'on est certain qu'il n'y a plus d'activité
                        if channel in self._channels_with_activity:
                            self._channels_with_activity.remove(channel)
                        logger.info(f"[{channel}] ✅ Status updated to 0 viewers")
                        return True
                    else:
                        logger.warning(f"[{channel}] ⚠️ Failed to update status to 0 viewers")
                        return False
                else:
                    logger.debug(f"[{channel}] ℹ️ No viewers and no previous activity, skipping update")
                    return True

        except Exception as e:
            logger.error(f"[{channel}] ❌ Error in _push_tracker_status_to_manager: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def _update_channel_status(self, initial_call=False):
        """Update channel status for dashboard"""
        try:
            if not self.channel_status:
                logger.warning("⚠️ Channel status manager not initialized")
                return False
            
            # Forcer un nettoyage des viewers inactifs avant la mise à jour
            if hasattr(self, 'time_tracker') and self.time_tracker and hasattr(self.time_tracker, 'force_flush_inactive'):
                logger.info("🧹 Nettoyage forcé des viewers inactifs avant mise à jour du statut")
                self.time_tracker.force_flush_inactive()
                
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
                        active_watchers = self.time_tracker.get_active_watchers(channel_name, include_buffer=False)
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
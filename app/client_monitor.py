from pathlib import Path
import os
import time
from typing import Callable, Dict, Optional, Set, List
from config import logger, HLS_SEGMENT_DURATION, HLS_DIR
from log_utils import parse_access_log

class ClientMonitor:
    """
    Moniteur de clients ultra-simplifié qui lit access.log et met à jour les stats.
    
    Version sans thread, sans lock, sans complexité inutile.
    """

    def __init__(self, log_path, update_watchers_callback, manager, stats_collector=None):
        """
        Initialise le moniteur ultra-simplifié.
        
        Args:
            log_path: Chemin vers access.log
            update_watchers_callback: Callback simple pour IPTVManager
            manager: Référence vers IPTVManager (pour segment_duration)
            stats_collector: Référence vers StatsCollector pour les stats
        """
        self.log_path = log_path
        self.update_watchers = update_watchers_callback
        self.manager = manager
        self.stats_collector = stats_collector
        
        # Position dans le fichier de log
        self.last_position = 0
        
        # Dictionnaire simple {ip: channel} pour détecter les changements de chaîne
        self.viewers = {}
        
        # Intervalle entre mises à jour du statut
        self.update_interval = 1.5
        
        # État d'arrêt
        self.should_stop = False
        
        logger.info("🔍 ClientMonitor ultra-simplifié initialisé")

    def start(self):
        """Démarre le moniteur (non-bloquant)"""
        # Au lieu d'utiliser un thread, on initialise juste la position de lecture
        log_path_obj = Path(self.log_path)
        if log_path_obj.exists():
            with open(self.log_path, "r") as f:
                # Se positionner à la fin du fichier
                f.seek(0, 2)
                self.last_position = f.tell()
                logger.info(f"📝 Positionnement initial à la fin du fichier: {self.last_position} bytes")

        logger.info("🚀 ClientMonitor démarré - Pour une mise à jour, appelez process_new_logs()")

    def process_new_logs(self):
        """
        Traite les nouvelles lignes du fichier de log.
        Cette méthode doit être appelée régulièrement par la boucle principale.
        
        Returns:
            bool: True si de nouvelles lignes ont été traitées
        """
        log_path_obj = Path(self.log_path)
        has_new_lines = False
        current_viewers = {}  # Pour collecter les viewers actifs
            
        try:
            # Vérifier que le fichier existe
            if not log_path_obj.exists():
                return False
            
            # Lire les nouvelles lignes
            with open(self.log_path, "r", encoding="utf-8", errors="ignore") as file:
                # Vérifier si le fichier a été recréé (nouveau inode)
                if hasattr(self, "_last_inode") and self._last_inode != os.stat(self.log_path).st_ino:
                    logger.info("🔄 Nouveau fichier de log détecté, réinitialisation")
                    file.seek(0, 2)  # Aller à la fin
                    self.last_position = file.tell()
                    self._last_inode = os.stat(self.log_path).st_ino
                    return False
                
                # Stocker l'inode pour la prochaine fois
                self._last_inode = os.stat(self.log_path).st_ino
                
                # Se positionner au dernier point de lecture
                file.seek(self.last_position)
                
                # Lire les nouvelles lignes
                new_lines = file.readlines()
                if new_lines:
                    has_new_lines = True
                
                # Mettre à jour la position
                self.last_position = file.tell()
                
                # Traiter chaque ligne
                for line in new_lines:
                    self._process_line(line.strip(), current_viewers)
        
        except Exception as e:
            logger.error(f"❌ Erreur lecture access.log: {e}", exc_info=True)
            return False
            
        # Si des lignes ont été traitées, mettre à jour les statuts
        if has_new_lines and self.update_watchers:
            self._update_channel_viewers(current_viewers)
            
        return has_new_lines

    def _process_line(self, line, current_viewers):
        """
        Traite une ligne de log et met à jour les statistiques.
        Version ULTRA-SIMPLIFIÉE et CENTRALISÉE qui ne traite QUE les segments.
        
        Args:
            line: Ligne de log à traiter
            current_viewers: Dictionnaire {channel: set(ips)} à mettre à jour
        """
        # PREMIÈRE VÉRIFICATION ULTRA-RAPIDE: seulement segments .ts avec code 200
        if " 200 " not in line or ".ts" not in line or "segment" not in line:
            return
        
        # LOG EXPLICITE DES SEGMENTS RENCONTRÉS
        logger.info(f"🧠 SEGMENT DÉTECTÉ: {line}")
            
        # Toute la logique de parsing est dans parse_access_log
        # Cette fonction va appliquer tous les filtres nécessaires
        ip, channel, request_type, is_valid, path, user_agent = parse_access_log(line)
        
        # Si parse_access_log a rejeté la ligne, on arrête là
        if not is_valid or not channel:
            logger.info(f"❌ SEGMENT REJETÉ après parsing: {channel} (valide: {is_valid})")
            return
            
        logger.info(f"✅ SEGMENT ACCEPTÉ: {channel} demandé par {ip}")
            
        # Vérifier que c'est un canal valide
        if not self._is_channel_valid(channel):
            logger.info(f"⚠️ Canal invalide: {channel}")
            return
            
        # Mémoriser l'IP pour ce canal
        if channel not in current_viewers:
            current_viewers[channel] = set()
        current_viewers[channel].add(ip)
            
        # Monitorer les changements de chaîne
        previous_channel = self.viewers.get(ip)
        
        # SI CHANGEMENT DE CHAÎNE: gérer le changement
        if previous_channel != channel:
            # Log explicite du changement pour prouver la détection
            logger.warning(f"ZAP! Channel change detected. IP: {ip}, From: {previous_channel}, To: {channel}")
            
            # Mise à jour stats
            if previous_channel and self.stats_collector:
                self.stats_collector.handle_channel_change(ip, previous_channel, channel)
                
            # Retirer l'IP de l'ancienne chaîne dans current_viewers
            if previous_channel and previous_channel in current_viewers:
                current_viewers[previous_channel].discard(ip)
                # Si la chaîne n'a plus de spectateurs, la retirer
                if not current_viewers[previous_channel]:
                    del current_viewers[previous_channel]
                
            # Mettre à jour le canal actuel
            self.viewers[ip] = channel
            
            # Notifications immédiates
            if self.update_watchers:
                # Force la mise à jour pour la nouvelle chaîne
                self._notify_channel_immediately(channel, ip)
                # Et retire le spectateur de l'ancienne
                if previous_channel:
                    self._notify_channel_immediately(previous_channel, ip, remove=True)
                    
            # NETTOYAGE COMPLET pour éviter le "collage"
            logger.info(f"🧹 NETTOYAGE COMPLET des viewers après changement {previous_channel} → {channel}")
            # Vider tous les viewers sauf pour le canal actuel
            for ch in list(current_viewers.keys()):
                if ch != channel:
                    current_viewers[ch].discard(ip)
                    # Supprimer les chaînes sans spectateurs
                    if not current_viewers[ch]:
                        del current_viewers[ch]
            
            # S'assurer que l'IP est bien dans la nouvelle chaîne
            if channel not in current_viewers:
                current_viewers[channel] = set()
            current_viewers[channel].add(ip)
            
            # Force update immédiat
            self._update_channel_viewers(current_viewers)
            
            # Log confirmant la mise à jour
            logger.info(f"✓ Changement {previous_channel} → {channel} traité avec succès")
        
        # SI PAS DE CHANGEMENT: mise à jour simple et statistiques
        else:
            # Utiliser HLS_SEGMENT_DURATION pour la synchronisation
            segment_update_interval = float(os.getenv("HLS_SEGMENT_DURATION", "2.0"))
            last_update_key = f"_last_segment_update_{channel}_{ip}"
            current_time = time.time()
            last_update_time = getattr(self, last_update_key, 0)
            
            if current_time - last_update_time > segment_update_interval:
                # Mise à jour avec log explicite
                logger.debug(f"⏱️ Mise à jour synchronisée pour {channel} (spectateur: {ip})")
                self._notify_channel_immediately(channel, ip)
                setattr(self, last_update_key, current_time)
        
        # Mise à jour des stats
        if self.stats_collector:
            # Durée par défaut depuis l'environnement
            segment_duration = float(os.getenv("HLS_SEGMENT_DURATION", "2.0"))
            
            # Tenter d'obtenir la durée spécifique de ce canal
            try:
                duration = self.manager.get_channel_segment_duration(channel)
                if duration and duration > 0:
                    segment_duration = duration
            except:
                pass
            
            # Enregistrer l'activité avec la durée exacte du segment
            self.stats_collector._record_log_activity(ip, channel, user_agent, 0)
            logger.debug(f"⏱️ Stats mises à jour pour {channel}: +{segment_duration:.1f}s")

    def _is_channel_valid(self, channel):
        """
        Vérifie simplement si un canal existe dans le manager.
        Version simplifiée sans vérifications complexes.
        """
        return hasattr(self.manager, "channels") and channel in self.manager.channels
        
    def _update_channel_viewers(self, current_viewers):
        """
        Met à jour le statut de toutes les chaînes via le callback.
        Version améliorée pour une détection plus fiable des spectateurs actifs.
        """
        # Obtenir toutes les chaînes connues
        all_channels = set(self.manager.channels.keys()) if hasattr(self.manager, "channels") else set()
        
        # Vérifier la cohérence des viewers
        # Un spectateur ne peut être que sur une seule chaîne à la fois
        seen_ips = {}  # {ip: channel}
        for channel, viewers in current_viewers.items():
            for ip in list(viewers):  # Convertir en liste pour pouvoir modifier pendant l'itération
                if ip in seen_ips:
                    # L'IP est déjà vue sur une autre chaîne
                    old_channel = seen_ips[ip]
                    # Garder l'IP uniquement sur la chaîne la plus récente selon self.viewers
                    current_channel = self.viewers.get(ip)
                    if current_channel == channel:
                        # Retirer l'IP de l'ancienne chaîne
                        current_viewers[old_channel].discard(ip)
                        if not current_viewers[old_channel]:
                            del current_viewers[old_channel]
                        seen_ips[ip] = channel
                        logger.info(f"🔄 {ip} retiré de {old_channel} (actif sur {channel})")
                    else:
                        # Retirer l'IP de cette chaîne
                        viewers.discard(ip)
                        if not viewers:
                            del current_viewers[channel]
                        logger.info(f"🔄 {ip} retiré de {channel} (actif sur {old_channel})")
                else:
                    seen_ips[ip] = channel
        
        # Mettre à jour TOUTES les chaînes, pas seulement celles avec des spectateurs
        for channel in all_channels:
            if channel == "master_playlist":
                continue
                
            # Obtenir les viewers actuels pour cette chaîne
            viewers = list(current_viewers.get(channel, set()))
            
            # Vérifier self.viewers pour s'assurer que les spectateurs sont toujours actifs
            # Un spectateur n'est considéré actif que sur sa chaîne actuelle
            active_viewers = []
            for ip in viewers:
                if self.viewers.get(ip) == channel:
                    active_viewers.append(ip)
                else:
                    logger.debug(f"[{channel}] ⚠️ {ip} ignoré (actif sur {self.viewers.get(ip)})")
            
            # Compteur de spectateurs après l'ajout des viewers manquants
            count = len(active_viewers)
            
            # Mise à jour via callback avec log plus détaillé
            if count > 0:
                logger.info(f"[{channel}] 👁️ Mise à jour périodique: {count} spectateurs actifs")
            else:
                # Log de niveau debug pour les canaux sans spectateurs
                logger.debug(f"[{channel}] Mise à jour périodique: 0 spectateurs")
            
            # TOUJOURS envoyer la mise à jour, même quand count = 0
            # C'est crucial pour maintenir l'état cohérent des chaînes sans spectateurs
            self.update_watchers(channel, count, active_viewers, HLS_DIR, source="nginx_log")

    def _notify_channel_immediately(self, channel, ip, remove=False):
        """
        Notifie immédiatement un changement pour un canal.
        Version améliorée et plus robuste.
        """
        if not self.update_watchers:
            return
            
        # Collecter tous les IPs actuels pour ce canal
        viewers = [ip2 for ip2, ch in self.viewers.items() if ch == channel]
        
        # Si on veut retirer l'IP
        if remove and ip in viewers:
            viewers.remove(ip)
            # Retirer aussi l'IP du dictionnaire des viewers
            if ip in self.viewers and self.viewers[ip] == channel:
                del self.viewers[ip]
            logger.info(f"[{channel}] 🚫 Retrait explicite de {ip}")
        # Si on veut ajouter l'IP et qu'elle n'est pas déjà présente
        elif not remove and ip not in viewers:
            viewers.append(ip)
            # Mettre à jour le dictionnaire des viewers
            # Si l'IP était sur une autre chaîne, la retirer d'abord
            old_channel = self.viewers.get(ip)
            if old_channel and old_channel != channel:
                # Forcer une notification de retrait sur l'ancienne chaîne
                self._notify_channel_immediately(old_channel, ip, remove=True)
                logger.info(f"[{old_channel}] 🔄 Retrait forcé de {ip} (changement vers {channel})")
            self.viewers[ip] = channel
            
        # Notification TOUJOURS, même sans viewers pour mettre à jour correctement l'état
        # C'est crucial pour les channels sans spectateurs ou quand on en retire
        self.update_watchers(channel, len(viewers), viewers, HLS_DIR, source="nginx_log_immediate")
        
        # Log plus détaillé
        if remove:
            logger.info(f"[{channel}] 👁️ Notification immédiate (retrait): {len(viewers)} spectateurs actifs")
        else:
            logger.info(f"[{channel}] 👁️ Notification immédiate (ajout/mise à jour): {len(viewers)} spectateurs actifs")

    def stop(self):
        """Arrête proprement le moniteur"""
        self.should_stop = True
        logger.info("🛑 ClientMonitor arrêté")
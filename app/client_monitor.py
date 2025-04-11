import os
import time
from pathlib import Path
from config import logger
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
        Version simplifiée sans conditions complexes.
        
        Args:
            line: Ligne de log à traiter
            current_viewers: Dictionnaire {channel: set(ips)} à mettre à jour
        """
        try:
            # Parser la ligne
            ip, channel, request_type, is_valid, path, user_agent = parse_access_log(line)
            
            # Ignorer les lignes non valides ou sans canal
            if not is_valid or not channel or channel == "master_playlist" or "404" in line:
                return
                
            # Simple vérification : le canal doit exister dans le manager
            if not self._is_channel_valid(channel):
                return
                
            # Monitorer les changements de chaîne
            previous_channel = self.viewers.get(ip)
            if previous_channel != channel:
                if previous_channel and self.stats_collector:
                    # Notifier le changement
                    logger.info(f"🔄 Changement: {ip}: {previous_channel} → {channel}")
                    self.stats_collector.handle_channel_change(ip, previous_channel, channel)
                    
                # Mettre à jour le canal actuel
                self.viewers[ip] = channel
                
                # Notification immédiate
                if self.update_watchers:
                    self._notify_channel_immediately(channel, ip)
                    if previous_channel:
                        self._notify_channel_immediately(previous_channel, ip, remove=True)
            
            # Mettre à jour les stats pour l'affichage actuel
            if channel not in current_viewers:
                current_viewers[channel] = set()
            current_viewers[channel].add(ip)
            
            # Mettre à jour les stats de visionnage
            if self.stats_collector:
                if request_type == "segment":
                    # Durée du segment (par défaut 2s, ou configurable)
                    segment_duration = 2.0
                    try:
                        duration = self.manager.get_channel_segment_duration(channel)
                        if duration and duration > 0:
                            segment_duration = duration
                    except:
                        pass
                    
                    self.stats_collector._record_log_activity(ip, channel, user_agent, 0)  # bytes_transferred=0 car on veut juste incrémenter le temps
                    
                    # MODIFICATION ICI: Mise à jour immédiate du statut du canal pour chaque segment
                    # Cela garantit que le suivi des spectateurs est cohérent avec le suivi du temps de visionnage
                    if self.update_watchers:
                        self._notify_channel_immediately(channel, ip)
                        
                elif request_type == "playlist":
                    # Petit incrément pour les requêtes de playlist
                    self.stats_collector._record_log_activity(ip, channel, user_agent, 0)  # bytes_transferred=0 car on veut juste incrémenter le temps
                    
        except Exception as e:
            logger.error(f"❌ Erreur traitement ligne: {e}")

    def _is_channel_valid(self, channel):
        """
        Vérifie simplement si un canal existe dans le manager.
        Version simplifiée sans vérifications complexes.
        """
        return hasattr(self.manager, "channels") and channel in self.manager.channels
        
    def _update_channel_viewers(self, current_viewers):
        """
        Met à jour le statut de toutes les chaînes via le callback.
        Version simplifiée.
        """
        # Utiliser le dictionnaire self.viewers qui contient {ip: channel} pour savoir quelle chaîne
        # chaque spectateur regarde actuellement (celui-ci est mis à jour dans _process_line)
        
        # Créer un dictionnaire {channel: [ip1, ip2, ...]} à partir de self.viewers
        active_channel_viewers = {}
        for ip, channel in self.viewers.items():
            if channel not in active_channel_viewers:
                active_channel_viewers[channel] = []
            active_channel_viewers[channel].append(ip)
        
        # Maintenant, mettre à jour toutes les chaînes connues
        all_channels = set(self.manager.channels.keys()) if hasattr(self.manager, "channels") else set()
        
        for channel in all_channels:
            if channel == "master_playlist":
                continue
            
            # Pour chaque chaîne, utiliser seulement les IPs qui la regardent actuellement
            viewers = active_channel_viewers.get(channel, [])
            count = len(viewers)
            
            # Mise à jour via callback
            self.update_watchers(channel, count, viewers, "/hls/", source="active_viewers_only")
        
        # Log informatif
        active_channels = [c for c, ips in active_channel_viewers.items() if ips]
        if active_channels:
            logger.info(f"👥 {len(self.viewers)} spectateurs actifs sur {len(active_channels)} chaînes")

    def _notify_channel_immediately(self, channel, ip, remove=False):
        """
        Notifie immédiatement un changement pour un canal.
        Version ultra-simplifiée.
        """
        if not self.update_watchers:
            return
            
        # Collecter tous les IPs actuels pour ce canal
        viewers = [ip2 for ip2, ch in self.viewers.items() if ch == channel and ip2 != ip]
        
        # Ajouter l'IP en cours si on ne le supprime pas
        if not remove:
            viewers.append(ip)
            
        # Notification
        self.update_watchers(channel, len(viewers), viewers, "/hls/", source="nginx_log_immediate")
        logger.info(f"[{channel}] 🔄 Notification immédiate: {len(viewers)} watchers actifs")

    def stop(self):
        """Arrête proprement le moniteur"""
        self.should_stop = True
        logger.info("🛑 ClientMonitor arrêté")
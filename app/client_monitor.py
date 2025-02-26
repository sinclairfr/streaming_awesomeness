import os
import time
import threading
from pathlib import Path
from config import logger
from typing import Dict, Tuple, List, Set
import re
from config import (
    TIMEOUT_NO_VIEWERS
)

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
        self.segment_monitor_thread = threading.Thread(target=self._monitor_segment_jumps, daemon=True)
        self.segment_monitor_thread.start()

    def _cleanup_loop(self):
        """Nettoie les watchers inactifs"""
        while True:
            time.sleep(60)
            self._cleanup_inactive()

    def _cleanup_inactive(self):
        """Nettoie les watchers inactifs"""
        now = time.time()
        to_remove = []

        with self.lock:
            # Check for inactive watchers (>60s without request)
            for (channel, ip), last_seen in self.watchers.items():
                if now - last_seen > self.inactivity_threshold:
                    to_remove.append((channel, ip))

            affected_channels = set()

            for key in to_remove:
                channel, ip = key
                del self.watchers[key]
                affected_channels.add(channel)
                logger.info(f"🗑️ Watcher supprimé: {ip} -> {channel}")

            # On nettoie aussi les segments des chaînes affectées
            for channel in affected_channels:
                if channel in self.segments_by_channel:
                    # On garde les infos de segments des 5 dernières minutes max
                    current_segments = self.segments_by_channel[channel]
                    self.segments_by_channel[channel] = {
                        seg_id: ts for seg_id, ts in current_segments.items()
                        if now - ts < 300  # 5 minutes
                    }

            # Mise à jour des watchers pour les chaînes affectées
            for channel in affected_channels:
                count = len([1 for (ch, _), _ in self.watchers.items() if ch == channel])
                logger.warning(f"⚠️ Mise à jour {channel} : {count} watchers restants")
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
                            [(int(seg_id), ts) for seg_id, ts in segments.items() 
                             if seg_id.isdigit()],
                            key=lambda x: x[0]
                        )
                        
                        # Vérification des sauts
                        for i in range(1, len(ordered_segments)):
                            current_id, current_ts = ordered_segments[i]
                            prev_id, prev_ts = ordered_segments[i-1]
                            
                            # Si le saut est supérieur à 5 segments et récent (< 20 secondes)
                            if current_id - prev_id > 5 and time.time() - current_ts < 20:
                                logger.warning(
                                    f"⚠️ Saut détecté pour {channel}: segment {prev_id} → {current_id} "
                                    f"(saut de {current_id - prev_id} segments)"
                                )
                                
                                # On peut notifier ici ou prendre des mesures
                                channel_obj = self.manager.channels.get(channel)
                                if channel_obj and hasattr(channel_obj, "report_segment_jump"):
                                    channel_obj.report_segment_jump(prev_id, current_id)
            except Exception as e:
                logger.error(f"❌ Erreur surveillance segments: {e}")
                
            time.sleep(10)  # Vérification toutes les 10 secondes

    def run(self):
        """On surveille les requêtes clients"""
        logger.debug("👀 Démarrage de la surveillance des requêtes...")

        try:
            if not os.path.exists(self.log_path):
                logger.error(f"❌ Log file introuvable: {self.log_path}")
                return

            logger.debug(f"📖 Ouverture du log: {self.log_path}")
            with open(self.log_path, "r") as f:
                f.seek(0, 2)  # Se positionne à la fin du fichier
                while True:
                    line = f.readline().strip()
                    if not line:
                        time.sleep(0.5)
                        continue

                    # Debug pour voir les logs en entier (si besoin)
                    # logger.debug(f"📝 Ligne lue: {line}")

                    # On ne s'intéresse qu'aux requêtes HLS
                    if "GET /hls/" not in line:
                        continue

                    parts = line.split()
                    if len(parts) < 7:
                        continue

                    ip = parts[0]
                    request = parts[6].strip('"')

                    # On extrait le channel
                    match = re.search(r'/hls/([^/]+)/', request)
                    if not match:
                        continue

                    channel = match.group(1)
                    
                    # Extraction du numéro de segment si présent
                    segment_match = re.search(r'segment_(\d+)\.ts', request)
                    segment_id = segment_match.group(1) if segment_match else None
                    
                    # Pour le debug avancé
                    if segment_id:
                        logger.debug(f"🔍 Segment détecté: {channel} - segment_{segment_id}.ts demandé par {ip}")
                    else:
                        logger.debug(f"🔍 Requête HLS: {ip} -> {channel} ({request})")

                    with self.lock:
                        # Mise à jour du timestamp pour ce watcher
                        self.watchers[(channel, ip)] = time.time()
                        
                        # Si c'est une requête de segment, on l'enregistre
                        if segment_id:
                            self.segments_by_channel.setdefault(channel, {})[segment_id] = time.time()
                        
                        # Calcul des watchers actifs pour ce channel
                        active_watchers = len([1 for (ch, _), ts in self.watchers.items() 
                                             if ch == channel and time.time() - ts < self.inactivity_threshold])
                        
                        # Mise à jour des watchers si nécessaire
                        self.update_watchers(channel, active_watchers, request)

        except Exception as e:
            logger.error(f"❌ Erreur fatale dans client_monitor: {e}")
            import traceback
            logger.error(traceback.format_exc())
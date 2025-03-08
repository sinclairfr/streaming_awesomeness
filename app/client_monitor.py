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
        #self.segment_monitor_thread = threading.Thread(target=self._monitor_segment_jumps, daemon=True)
        #self.segment_monitor_thread.start()

    def _cleanup_loop(self):
        """Nettoie les watchers inactifs plus fréquemment"""
        while True:
            time.sleep(10) 
            self._cleanup_inactive()
            
    def _cleanup_inactive(self):
        """Nettoie les watchers inactifs"""
        now = time.time()
        to_remove = []

        with self.lock:
            inactivity_threshold = 30 

            for (channel, ip), last_seen in self.watchers.items():
                if now - last_seen > inactivity_threshold:
                    to_remove.append((channel, ip))

            affected_channels = set()

            for key in to_remove:
                channel, ip = key
                del self.watchers[key]
                affected_channels.add(channel)
                logger.info(f"🗑️ Watcher supprimé: {ip} -> {channel} (inactif depuis {now - last_seen:.1f}s)")

            # On fait une mise à jour COMPLÈTE de TOUTES les chaînes actives
            all_channels = set([ch for (ch, _), _ in self.watchers.items()])
            all_channels.update(affected_channels)
            
            for channel in all_channels:
                count = len([1 for (ch, _), _ in self.watchers.items() if ch == channel])
                logger.info(f"[{channel}] 🔄 Mise à jour forcée: {count} watchers actifs")
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

    def _check_log_file(self):
        """Vérifie périodiquement l'état du fichier de log et force une reconnexion si nécessaire"""
        while True:
            try:
                # Vérifier si le fichier existe
                if not os.path.exists(self.log_path):
                    logger.error(f"❌ Log file introuvable lors de la vérification périodique: {self.log_path}")
                    # Forcer la reconnexion
                    self._force_reconnect = True
                    
                # Vérifier la taille et les permissions
                try:
                    file_info = os.stat(self.log_path)
                    file_size = file_info.st_size
                    
                    # Si le fichier est vide ou trop petit
                    if file_size < 10:
                        logger.warning(f"⚠️ Fichier log anormalement petit: {file_size} bytes")
                    
                    # Vérifier les permissions
                    if not os.access(self.log_path, os.R_OK):
                        logger.error(f"❌ Permissions insuffisantes sur le fichier log")
                        self._force_reconnect = True
                        
                except Exception as e:
                    logger.error(f"❌ Erreur vérification stats fichier log: {e}")
                    
                # Vérifier l'activité Nginx
                try:
                    nginx_running = False
                    for proc in psutil.process_iter(['name']):
                        if 'nginx' in proc.info['name']:
                            nginx_running = True
                            break
                    
                    if not nginx_running:
                        logger.critical(f"🚨 Nginx semble ne pas être en cours d'exécution!")
                        # Tenter de voir si le containeur est en cours d'exécution
                        result = subprocess.run(["docker", "ps", "--filter", "name=iptv-nginx"], capture_output=True, text=True)
                        logger.info(f"État containeur Nginx: {result.stdout}")
                except Exception as e:
                    logger.error(f"❌ Erreur vérification Nginx: {e}")
                    
            except Exception as e:
                logger.error(f"❌ Erreur dans _check_log_file: {e}")
                
            time.sleep(60)  # Vérification toutes les minutes
            
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
                        logger.critical(f"❌ Impossible de trouver le fichier de log après {max_retries} tentatives")
                        # Redémarrage forcé du monitoring
                        time.sleep(30)
                        retry_count = 0
                    continue

                logger.info(f"📖 Ouverture du log: {self.log_path}")
                
                # Tester si le fichier est accessible en lecture
                try:
                    with open(self.log_path, "r") as test_file:
                        last_pos = test_file.seek(0, 2)  # Se positionne à la fin
                        logger.info(f"✅ Fichier de log accessible, taille: {last_pos} bytes")
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
                        if current_time - last_cleanup_time > 10:  # Tous les 10 secondes
                            self._cleanup_inactive()
                            last_cleanup_time = current_time
                        
                        # Heartbeat périodique pour s'assurer que le monitoring est actif
                        if current_time - last_heartbeat_time > 60:  # Toutes les minutes
                            logger.info(f"💓 ClientMonitor actif, dernière activité il y a {current_time - last_activity_time:.1f}s")
                            last_heartbeat_time = current_time
                            
                            # Vérification périodique du fichier
                            try:
                                if not os.path.exists(self.log_path):
                                    logger.error(f"❌ Fichier log disparu: {self.log_path}")
                                    break
                                    
                                # Vérification que le fichier n'a pas été tronqué
                                current_pos = f.tell()
                                file_size = os.path.getsize(self.log_path)
                                if current_pos > file_size:
                                    logger.error(f"❌ Fichier log tronqué: position {current_pos}, taille {file_size}")
                                    break
                            except Exception as e:
                                logger.error(f"❌ Erreur vérification fichier log: {e}")
                                break
                        
                        if not line:
                            # Si pas d'activité depuis longtemps, forcer une relecture des dernières lignes
                            if current_time - last_activity_time > 300:  # 5 minutes
                                logger.warning(f"⚠️ Pas d'activité depuis 5 minutes, relecture forcée des dernières lignes")
                                f.seek(max(0, f.tell() - 10000))  # Retour en arrière de 10Ko
                                last_activity_time = current_time
                                
                            time.sleep(0.1)  # Réduit la charge CPU
                            continue

                        # Une ligne a été lue, mise à jour du temps d'activité
                        last_activity_time = current_time

                        # On ne s'intéresse qu'aux requêtes /hls/ - IMPORTANT: Inclure les 404 aussi
                        if "/hls/" not in line:
                            continue

                        parts = line.split()
                        if len(parts) < 7:
                            logger.warning(f"⚠️ Format de ligne invalide: {line}")
                            continue

                        ip = parts[0]
                        request = parts[6].strip('"')
                        status_code = parts[8] if len(parts) > 8 else "???"

                        # On extrait le channel
                        match = re.search(r'/hls/([^/]+)/', request)
                        if not match:
                            logger.warning(f"⚠️ Impossible d'extraire le channel: {request}")
                            continue

                        channel = match.group(1)
                        
                        # Log explicite pour le debug - INCLURE TOUTES LES REQUÊTES, MÊME LES 404
                        logger.info(f"🔍 Requête détectée: {ip} -> {channel} ({request}) [Status: {status_code}]")
                        
                        # Extraction du numéro de segment si présent
                        segment_match = re.search(r'segment_(\d+)\.ts', request)
                        segment_id = segment_match.group(1) if segment_match else None
                        
                        # Pour le debug avancé
                        if segment_id:
                            logger.debug(f"🔍 Segment détecté: {channel} - segment_{segment_id}.ts demandé par {ip}")
                        else:
                            logger.debug(f"🔍 Requête playlist: {ip} -> {channel} ({request})")

                        with self.lock:
                            # Mise à jour du timestamp pour ce watcher
                            self.watchers[(channel, ip)] = time.time()
                            
                            # Si c'est une requête de segment, on l'enregistre
                            if segment_id:
                                self.segments_by_channel.setdefault(channel, {})[segment_id] = time.time()
                            
                            # Calcul des watchers actifs pour ce channel
                            active_watchers = len([1 for (ch, _), ts in self.watchers.items() 
                                                if ch == channel and time.time() - ts < self.inactivity_threshold * 2])
                            
                            # Mise à jour des watchers si nécessaire - MÊME POUR LES 404!
                            # C'est important car ça permet de démarrer un flux suite à une requête 404
                            self.update_watchers(channel, active_watchers, request)

            except Exception as e:
                logger.error(f"❌ Erreur fatale dans client_monitor: {e}")
                import traceback
                logger.error(traceback.format_exc())
                
                # Attente avant de réessayer
                time.sleep(10)
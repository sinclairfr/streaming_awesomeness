# ffmpeg_process_manager.py
import os
import time
import signal
import psutil
import threading
import subprocess
from pathlib import Path
from config import logger
import traceback

class FFmpegProcessManager:
    """
    # On centralise toute la gestion des processus FFmpeg
    # Démarrage, arrêt, surveillance, nettoyage, etc.
    """
    def __init__(self, channel_name, logger_instance=None):
        self.channel_name = channel_name
        self.process = None
        self.active_pids = set()  # Tous les PIDs FFmpeg associés à cette chaîne
        self.lock = threading.Lock()
        self.monitor_thread = None
        self.stop_monitoring = threading.Event()
        self.last_playback_time = time.time()
        self.playback_offset = 0
        self.total_duration = 0
        self.logger_instance = logger_instance  # Instance FFmpegLogger, optionnelle
        
        # Callbacks qu'on peut remplacer depuis l'extérieur
        self.on_process_died = None        # Callback quand le processus meurt
        self.on_position_update = None     # Callback quand la position est mise à jour
        self.on_segment_created = None     # Callback quand un segment est créé
        
        # Registre global des channels pour retrouver le parent depuis n'importe où
        if not hasattr(FFmpegProcessManager, 'all_channels'):
            FFmpegProcessManager.all_channels = {}
        

    def start_process(self, command, hls_dir):
        """
        # Démarre un processus FFmpeg avec la commande fournie
        # Renvoie True si démarré avec succès, False sinon
        """
        with self.lock:
            logger.debug(f"[{self.channel_name}] 🚀 Démarrage start_process")
            # On nettoie d'abord les processus existants
            self._clean_existing_processes()
            
            try:
                # On s'assure que le dossier HLS existe
                Path(hls_dir).mkdir(parents=True, exist_ok=True)
                
                # Préparation du log si disponible
                log_file = None
                if self.logger_instance:
                    log_path = self.logger_instance.get_main_log_file()
                    os.makedirs(os.path.dirname(log_path), exist_ok=True)
                    log_file = open(log_path, "a", buffering=1)
                    logger.info(f"[{self.channel_name}] 📝 Logs FFmpeg -> {log_path}")
                
                # Lancement du processus
                logger.info(f"[{self.channel_name}] 🚀 Lancement FFmpeg: {' '.join(command)}")
                
                if log_file:
                    # Avec redirection des logs
                    process = subprocess.Popen(
                        command,
                        stdout=log_file,
                        stderr=subprocess.STDOUT,
                        bufsize=1,
                        universal_newlines=True
                    )
                else:
                    # Sans redirection (stdout/stderr ignorés)
                    process = subprocess.Popen(
                        command,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL
                    )
                
                self.process = process
                self.active_pids.add(process.pid)
                
                # Vérification rapide du démarrage
                time.sleep(1)
                if self.process.poll() is not None:
                    logger.error(f"[{self.channel_name}] ❌ FFmpeg s'est arrêté immédiatement")
                    if log_file:
                        log_file.close()
                    return False
                
                # Démarrage du thread de surveillance
                self._start_monitoring(hls_dir)
                
                logger.info(f"[{self.channel_name}] ✅ FFmpeg démarré avec PID: {self.process.pid}")
                return True
                
            except Exception as e:
                logger.error(f"[{self.channel_name}] ❌ Erreur démarrage FFmpeg: {e}")
                logger.error(f"[{self.channel_name}] Détail: {traceback.format_exc()}")
                if 'log_file' in locals() and log_file:
                    log_file.close()
                return False
    
    def stop_process(self):
        """Arrête proprement le processus FFmpeg en cours, sans sauvegarder la position"""
        with self.lock:
            if not self.process:
                return
            
            try:
                # On arrête la surveillance
                if self.monitor_thread and self.monitor_thread.is_alive():
                    self.stop_monitoring.set()
                    # Évite le join() si c'est le thread courant
                    if self.monitor_thread != threading.current_thread():
                        self.monitor_thread.join(timeout=3)
                
                pid = self.process.pid
                logger.info(f"[{self.channel_name}] 🛑 Arrêt du processus FFmpeg {pid}")
                
                # Tentative d'arrêt propre avec SIGTERM
                self.process.terminate()
                
                # On attend un peu que ça se termine
                for _ in range(5):  # 5 secondes max
                    if self.process.poll() is not None:
                        logger.info(f"[{self.channel_name}] ✅ Processus {pid} terminé proprement")
                        break
                    time.sleep(1)
                
                # Si toujours en vie, on force avec SIGKILL
                if self.process.poll() is None:
                    logger.warning(f"[{self.channel_name}] ⚠️ Processus {pid} résistant, envoi de SIGKILL")
                    self.process.kill()
                    time.sleep(1)
                
                # On retire ce PID des actifs
                self.active_pids.discard(pid)
                
                # Nettoyage 
                self.process = None
                
                # On nettoie aussi les autres processus au cas où
                self._clean_orphan_processes()
                
                logger.info(f"[{self.channel_name}] 🧹 Processus FFmpeg nettoyé")
                return True
                    
            except Exception as e:
                logger.error(f"[{self.channel_name}] ❌ Erreur arrêt FFmpeg: {e}")
                self.process = None
                return False
            
    def _clean_existing_processes(self):
        """
        # Nettoie tous les processus FFmpeg existants pour cette chaîne
        """
        # Si on a un processus actif, on l'arrête proprement
        if self.process and self.process.poll() is None:
            self.stop_process()
        
        # On nettoie tous les autres processus qui pourraient traîner
        self._clean_orphan_processes()

    def _clean_orphan_processes(self, force_cleanup=True):
        """Nettoie brutalement les processus FFmpeg orphelins"""
        try:
            # Recherche tous les processus FFmpeg liés à cette chaîne
            pattern = f"/hls/{self.channel_name}/"
            processes_killed = 0
            
            # Utilise directement ps et grep pour trouver les PID, puis kill -9
            try:
                cmd = f"ps aux | grep ffmpeg | grep '{pattern}' | grep -v grep | awk '{{print $2}}'"
                pids = subprocess.check_output(cmd, shell=True).decode().strip().split('\n')
                
                for pid in pids:
                    if pid.strip():
                        try:
                            # Kill -9 direct, sans états d'âme
                            kill_cmd = f"kill -9 {pid}"
                            subprocess.run(kill_cmd, shell=True)
                            logger.info(f"[{self.channel_name}] 🔪 Processus {pid} tué avec kill -9")
                            processes_killed += 1
                            
                            # Petit délai entre les kills pour éviter les cascades
                            if processes_killed > 0:
                                time.sleep(0.5)
                        except Exception as e:
                            logger.error(f"[{self.channel_name}] Erreur kill -9 {pid}: {e}")
                
                # Si on a tué des processus, attendre un peu avant de continuer
                if processes_killed > 0:
                    time.sleep(1)
                    
            except Exception as e:
                logger.error(f"[{self.channel_name}] Erreur recherche processus: {e}")
                
            # Approche alternative avec psutil comme backup
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    if 'ffmpeg' in proc.info['name']:
                        cmdline = ' '.join(proc.info['cmdline'] or [])
                        if pattern in cmdline:
                            proc.kill()  # SIGKILL directement
                            logger.info(f"[{self.channel_name}] 🔪 Processus {proc.info['pid']} tué via psutil")
                            processes_killed += 1
                            time.sleep(0.5)  # Petit délai
                except Exception:
                    pass
                    
        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur nettoyage global: {e}")
    
    def _start_monitoring(self, hls_dir):
        """Démarre le thread de surveillance du processus FFmpeg"""
        if self.monitor_thread and self.monitor_thread.is_alive():
            # On arrête d'abord le thread existant
            self.stop_monitoring.set()
            # Évite le join sur le thread courant
            if self.monitor_thread != threading.current_thread():
                try:
                    self.monitor_thread.join(timeout=3)
                except RuntimeError:
                    logger.warning(f"[{self.channel_name}] Impossible de joindre le thread (thread courant)")
            self.stop_monitoring.clear()
        
        # On crée et démarre le nouveau thread
        self.monitor_thread = threading.Thread(
            target=self._monitor_process,
            args=(hls_dir,),
            daemon=True
        )
        self.monitor_thread.start()
        logger.debug(f"[{self.channel_name}] 👀 Surveillance FFmpeg démarrée")
    
    def _monitor_process(self, hls_dir):
        """
        # Surveille le processus FFmpeg et ses segments
        """
        last_segment_check = 0
        segment_count = 0
        hls_path = Path(hls_dir)
        last_frozen_check = 0
        
        while not self.stop_monitoring.is_set():
            try:
                # Vérification du processus
                if not self.process or self.process.poll() is not None:
                    logger.error(f"[{self.channel_name}] ❌ Processus FFmpeg terminé avec code: {self.process.returncode if self.process else 'None'}")
                    
                    # Callback en cas de mort du processus
                    if self.on_process_died:
                        self.on_process_died(self.process.returncode if self.process else None)
                    break
                
                # Vérification périodique des segments (toutes les 2 secondes)
                current_time = time.time()
                if current_time - last_segment_check >= 2:
                    # Comptage des segments
                    try:
                        segments = list(hls_path.glob("*.ts"))
                        new_count = len(segments)
                        
                        if new_count != segment_count:
                            logger.debug(f"[{self.channel_name}] 📊 Segments HLS: {new_count}")
                            
                            # Tri des segments par numéro
                            segments.sort(key=lambda x: int(x.stem.split('_')[-1]))
                            
                            # Callback pour le nouveau segment
                            if new_count > segment_count and self.on_segment_created and segments:
                                newest_segment = segments[-1]
                                self.on_segment_created(str(newest_segment), newest_segment.stat().st_size)
                            
                            segment_count = new_count
                    except Exception as e:
                        logger.error(f"[{self.channel_name}] ❌ Erreur vérification segments: {e}")
                    
                    last_segment_check = current_time
                
                # Vérification périodique du gel du flux (toutes les 30 secondes)
                if current_time - last_frozen_check >= 30:
                    # On ne vérifie que s'il y a des spectateurs
                    from_channel = None
                    for name, channel in FFmpegProcessManager.all_channels.items() if hasattr(FFmpegProcessManager, 'all_channels') else {}:
                        if hasattr(channel, 'process_manager') and channel.process_manager == self:
                            from_channel = channel
                            break
                    
                    if from_channel and hasattr(from_channel, 'watchers_count') and from_channel.watchers_count > 0:
                        if self._detect_frozen_stream():
                            # Si le flux est bloqué et qu'il y a des spectateurs, on redémarre
                            logger.warning(f"[{self.channel_name}] 🔄 Redémarrage du flux bloqué (spectateurs: {from_channel.watchers_count})")
                            if hasattr(from_channel, '_restart_stream'):
                                from_channel._restart_stream()
                    
                    last_frozen_check = current_time
                
                # On attend un peu avant la prochaine vérification
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"[{self.channel_name}] ❌ Erreur surveillance: {e}")
                time.sleep(1)
        
        logger.info(f"[{self.channel_name}] 👋 Fin de la surveillance FFmpeg") 
    
    def set_total_duration(self, duration):
        """
        # Définit la durée totale de la playlist
        """
        self.total_duration = duration
    
    def set_playback_offset(self, offset):
        """
        # Définit l'offset de lecture avec validation
        """
        # S'assurer que l'offset est valide et dans les limites
        if self.total_duration > 0:
            # Appliquer le modulo pour rester dans les limites
            self.playback_offset = offset % self.total_duration
            logger.debug(f"[{self.channel_name}] 🔄 Process manager: offset ajusté à {self.playback_offset:.2f}s (modulo {self.total_duration:.2f}s)")
        else:
            self.playback_offset = offset
            
        self.last_playback_time = time.time()        
    
    def get_playback_offset(self):
        """
        # Renvoie l'offset de lecture actuel
        """
        if self.total_duration <= 0:
            return 0
            
        # Calcul de l'offset actuel
        current_time = time.time()
        elapsed = current_time - self.last_playback_time
        current_offset = (self.playback_offset + elapsed) % self.total_duration
        
        return current_offset
    
    def is_running(self):
        """
        # Vérifie si le processus FFmpeg est en cours d'exécution
        """
        return self.process is not None and self.process.poll() is None

    def _detect_frozen_stream(self):
        """
        # Détecte si le stream est gelé/bloqué en surveillant les fichiers de progression FFmpeg
        # Retourne True si le stream semble bloqué
        """
        if not self.process or not self.logger_instance:
            return False
            
        try:
            progress_file = self.logger_instance.get_progress_file()
            if not progress_file or not Path(progress_file).exists():
                return False
                
            # Vérifier la dernière modification du fichier de progression
            mod_time = Path(progress_file).stat().st_mtime
            current_time = time.time()
            time_since_update = current_time - mod_time
            
            # Si le fichier de progression n'a pas été mis à jour depuis 30s, c'est suspect
            if time_since_update > 30:
                logger.warning(f"[{self.channel_name}] ⚠️ Fichier progression non mis à jour depuis {time_since_update:.1f}s")
                
                # On lit le contenu pour voir si la sortie est bloquée
                with open(progress_file, 'r') as f:
                    content = f.read()
                    last_position = 0
                    
                    # Chercher la dernière position
                    if 'out_time_ms=' in content:
                        position_lines = [l for l in content.split('\n') if 'out_time_ms=' in l]
                        if position_lines:
                            time_part = position_lines[-1].split('=')[1]
                            if time_part.isdigit():
                                last_position = int(time_part) / 1_000_000  # En secondes
                    
                    # Si on n'a pas de position ou une position statique, c'est bloqué
                    if last_position == 0 or (hasattr(self, 'last_checked_position') and 
                                            self.last_checked_position == last_position and 
                                            time_since_update > 60):  # 60s sans progression
                        # On stocke la position pour comparer la prochaine fois
                        self.last_checked_position = last_position
                        logger.error(f"[{self.channel_name}] 🚨 FLUX BLOQUÉ détecté! Position: {last_position:.2f}s, Inactivité: {time_since_update:.1f}s")
                        return True
                        
                    # Mise à jour de la position pour la prochaine vérification
                    self.last_checked_position = last_position
                
            return False
            
        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur détection flux bloqué: {e}")
            return False
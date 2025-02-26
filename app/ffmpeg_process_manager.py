# ffmpeg_process_manager.py
import os
import time
import signal
import psutil
import threading
import subprocess
from pathlib import Path
from config import logger


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

    def start_stream(self) -> bool:
        """Démarre le stream avec FFmpeg en utilisant les nouvelles classes"""
        try:
            logger.info(f"[{self.name}] 🚀 Démarrage du stream...")

            hls_dir = Path(f"/app/hls/{self.name}")
            hls_dir.mkdir(parents=True, exist_ok=True)
    
            concat_file = self._create_concat_file()
            if not concat_file or not concat_file.exists():
                logger.error(f"[{self.name}] ❌ _playlist.txt introuvable")
                return False

            start_offset = self.position_manager.get_start_offset()
            logger.info(f"[{self.name}] Décalage de démarrage: {start_offset}")
            
            logger.info(f"[{self.name}] Optimisation pour le matériel...")
            self.command_builder.optimize_for_hardware()
            logger.info(f"[{self.name}] Vérification mkv...")
            has_mkv = self.command_builder.detect_mkv_in_playlist(concat_file)

            logger.info(f"[{self.name}] Construction de la commande FFmpeg...")
            command = self.command_builder.build_command(
                input_file=concat_file,
                output_dir=hls_dir,
                playback_offset=start_offset,
                progress_file=self.logger.get_progress_file(),
                has_mkv=has_mkv
            )
            logger.info(f"[{self.name}] Commande FFmpeg: {command}")
            
            if not self.process_manager.start_process(command, hls_dir):
                logger.error(f"[{self.name}] ❌ Échec démarrage FFmpeg")
                return False
                
            self.position_manager.set_playing(True)
            
            logger.info(f"[{self.name}] ✅ Stream démarré avec succès")
            return True

        except Exception as e:
            logger.error(f"Erreur démarrage stream {self.name}: {e}")
            return False    
      
    def stop_process(self, save_position=True):
        """
        # Arrête proprement le processus FFmpeg en cours
        # Si save_position=True, sauvegarde la position de lecture
        """
        with self.lock:
            if not self.process:
                return
            
            try:
                # Sauvegarde de la position si demandé
                if save_position and hasattr(self, 'playback_offset') and self.total_duration > 0:
                    current_time = time.time()
                    elapsed = current_time - self.last_playback_time
                    self.playback_offset = (self.playback_offset + elapsed) % self.total_duration
                    self.last_playback_time = current_time
                    logger.info(f"[{self.channel_name}] 💾 Position sauvegardée: {self.playback_offset:.1f}s")
                
                # On arrête la surveillance
                if self.monitor_thread and self.monitor_thread.is_alive():
                    self.stop_monitoring.set()
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
    
    def _clean_orphan_processes(self):
        """
        # Nettoie les processus FFmpeg orphelins pour cette chaîne
        """
        try:
            # On récupère tous les processus FFmpeg actifs
            ffmpeg_processes = []
            
            for proc in psutil.process_iter(attrs=["pid", "name", "cmdline"]):
                try:
                    # Si c'est un processus FFmpeg
                    if "ffmpeg" in proc.info["name"].lower():
                        # Et s'il contient le nom de notre chaîne dans sa ligne de commande
                        cmd_args = proc.info["cmdline"] or []
                        cmd_str = " ".join(str(arg) for arg in cmd_args)
                        
                        if f"/hls/{self.channel_name}/" in cmd_str:
                            ffmpeg_processes.append(proc.info["pid"])
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            
            # On différencie le processus actuel (si on en a un)
            current_pid = self.process.pid if self.process and self.process.poll() is None else None
            
            # Pour chaque PID, on vérifie s'il faut le tuer
            for pid in ffmpeg_processes:
                if pid == current_pid:
                    continue  # On ne tue pas notre processus actif
                
                try:
                    # On essaie de tuer proprement avec SIGTERM
                    logger.info(f"[{self.channel_name}] 🔪 Nettoyage du processus orphelin {pid}")
                    os.kill(pid, signal.SIGTERM)
                    
                    # On donne 2 secondes pour mourir
                    time.sleep(2)
                    
                    # Si toujours en vie, on force avec SIGKILL
                    if psutil.pid_exists(pid):
                        logger.warning(f"[{self.channel_name}] ⚠️ Processus {pid} résistant, envoi de SIGKILL")
                        os.kill(pid, signal.SIGKILL)
                        
                    # On retire ce PID des actifs
                    self.active_pids.discard(pid)
                    
                except ProcessLookupError:
                    # Le processus n'existe plus
                    self.active_pids.discard(pid)
                except Exception as e:
                    logger.error(f"[{self.channel_name}] ❌ Erreur nettoyage processus {pid}: {e}")
        
        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur nettoyage global: {e}")
    
    def _start_monitoring(self, hls_dir):
        """
        # Démarre le thread de surveillance du processus FFmpeg
        """
        if self.monitor_thread and self.monitor_thread.is_alive():
            # On arrête d'abord le thread existant
            self.stop_monitoring.set()
            self.monitor_thread.join(timeout=3)
            self.stop_monitoring.clear()
        
        # On crée et démarre le nouveau thread
        self.monitor_thread = threading.Thread(
            target=self._monitor_process,
            args=(hls_dir,),
            daemon=True
        )
        self.monitor_thread.start()
        logger.info(f"[{self.channel_name}] 👀 Surveillance FFmpeg démarrée")
    
    def _monitor_process(self, hls_dir):
        """
        # Surveille le processus FFmpeg et ses segments
        """
        last_segment_check = 0
        segment_count = 0
        hls_path = Path(hls_dir)
        
        while not self.stop_monitoring.is_set():
            try:
                # Vérification du processus
                if not self.process or self.process.poll() is not None:
                    logger.error(f"[{self.channel_name}] ❌ Processus FFmpeg terminé avec code: {self.process.returncode if self.process else 'None'}")
                    
                    # Callback en cas de mort du processus
                    if self.on_process_died:
                        self.on_process_died(self.process.returncode if self.process else None)
                    
                    break
                
                # Mise à jour de la position de lecture (si on a un fichier de progression)
                if self.logger_instance and self.on_position_update:
                    self._update_playback_position(self.logger_instance.get_progress_file())
                
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
                
                # On attend un peu avant la prochaine vérification
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"[{self.channel_name}] ❌ Erreur surveillance: {e}")
                time.sleep(1)
        
        logger.info(f"[{self.channel_name}] 👋 Fin de la surveillance FFmpeg")
    
    def _update_playback_position(self, progress_file):
        """
        # Met à jour la position de lecture en lisant le fichier de progression
        """
        if not progress_file or not Path(progress_file).exists():
            return
        
        try:
            with open(progress_file, 'r') as f:
                content = f.read()
                
                # Extraction de la position temporelle
                if 'out_time_ms=' in content:
                    position_lines = [l for l in content.split('\n') if 'out_time_ms=' in l]
                    if position_lines:
                        # On prend la dernière ligne qui contient out_time_ms
                        last_line = position_lines[-1]
                        time_part = last_line.split('=')[1]
                        
                        # Conversion en secondes
                        if time_part.isdigit():
                            ms_value = int(time_part)
                            position_seconds = ms_value / 1_000_000
                            
                            # Appel du callback
                            if self.on_position_update:
                                self.on_position_update(position_seconds)
                        
        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur lecture position: {e}")
    
    def set_total_duration(self, duration):
        """
        # Définit la durée totale de la playlist
        """
        self.total_duration = duration
    
    def set_playback_offset(self, offset):
        """
        # Définit l'offset de lecture
        """
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
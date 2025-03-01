# ffmpeg_monitor.py
import psutil
import threading
import time
from pathlib import Path
from config import logger
import os
import signal
from config import (
    TIMEOUT_NO_VIEWERS,
    FFMPEG_LOG_LEVEL,
    logger
)

class FFmpegMonitor(threading.Thread):
    """
    # On centralise toute la surveillance des processus FFmpeg ici
    """
    def __init__(self, channels_dict):
        super().__init__(daemon=True)
        self.channels = channels_dict  # Référence au dict des chaînes
        self.stop_event = threading.Event()
        self.ffmpeg_log_dir = Path("/app/logs/ffmpeg")
        self.ffmpeg_log_dir.mkdir(parents=True, exist_ok=True)
    # Méthode à modifier dans FFmpegMonitor pour mieux gérer les sauts de segments
    def _check_all_ffmpeg_processes(self):
        """
        Parcourt tous les processus pour voir lesquels sont liés à FFmpeg,
        groupés par nom de chaîne. Puis gère le nettoyage des processus
        multiples (zombies) avec throttling.
        """
        # Limiter la fréquence d'exécution
        current_time = time.time()
        if hasattr(self, 'last_check_time') and current_time - self.last_check_time < 30:
            # Au maximum toutes les 30 secondes
            return
            
        setattr(self, 'last_check_time', current_time)
        
        ffmpeg_processes = {}
        
        # Scanne tous les processus système
        for proc in psutil.process_iter(attrs=["pid", "name", "cmdline"]):
            try:
                # Vérifie si c'est un processus ffmpeg
                if "ffmpeg" in proc.info["name"].lower():
                    cmd_args = proc.info["cmdline"] or []
                    cmd_str = " ".join(cmd_args)
                    
                    # Détecte le nom de la chaîne si "/hls/<channel_name>/" est présent
                    for channel_name in self.channels:
                        if f"/hls/{channel_name}/" in cmd_str:
                            ffmpeg_processes.setdefault(channel_name, []).append(proc.info["pid"])
                            break  # On s'arrête après la première correspondance
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        
        # Pour chaque chaîne, on vérifie s'il y a plusieurs PIDs ou s'ils sont inactifs
        for channel_name, pids in ffmpeg_processes.items():
            # On récupère la chaîne et calcule le temps depuis le dernier watcher
            channel = self.channels.get(channel_name)
            if not channel:
                continue
                
            time_since_last_watcher = current_time - channel.last_watcher_time
            
            # On log des infos de monitoring mais seulement une fois toutes les 5 minutes
            if not hasattr(self, 'last_inspect_time'):
                self.last_inspect_time = {}
            
            if channel_name not in self.last_inspect_time or current_time - self.last_inspect_time[channel_name] > 300:
                logger.info(
                    f"[{channel_name}] - nombre de process : {len(pids)} - temps depuis dernier watcher : {time_since_last_watcher:.1f}s"
                )
                self.last_inspect_time[channel_name] = current_time

            # Si on a plusieurs processus OU qu'on a dépassé le temps d'inactivité, on nettoie
            if len(pids) > 1 or time_since_last_watcher > TIMEOUT_NO_VIEWERS:
                if len(pids) > 1:
                    logger.warning(f"⚠️ {channel_name}: {len(pids)} processus FFmpeg actifs détectés")
                elif time_since_last_watcher > TIMEOUT_NO_VIEWERS:
                    logger.warning(f"⚠️ {channel_name}: Processus FFmpeg inactif depuis {time_since_last_watcher:.1f}s")
                
                self.process_manager._clean_orphan_processes(force_cleanup=True)
     
    def _watchers_loop(self):
        """Surveille l'activité des watchers et arrête les streams inutilisés"""
        while True:
            try:
                current_time = time.time()
                channels_checked = set()

                # Pour chaque chaîne, on vérifie l'activité
                # Vérification que self.channels n'est pas None avant d'itérer dessus
                if self.channels:
                    for channel_name, channel in self.channels.items():
                        if not hasattr(channel, 'last_watcher_time'):
                            continue

                        # On calcule l'inactivité
                        inactivity_duration = current_time - channel.last_watcher_time

                        # Si inactif depuis plus de TIMEOUT_NO_VIEWERS (120s par défaut)
                        if inactivity_duration > TIMEOUT_NO_VIEWERS:
                            if channel.process_manager.is_running():
                                logger.warning(
                                    f"[{channel_name}] ⚠️ Stream inactif depuis {inactivity_duration:.1f}s, on arrête FFmpeg"
                                )
                                channel.stop_stream_if_needed()

                        channels_checked.add(channel_name)

                    # On vérifie les processus FFmpeg orphelins
                    for proc in psutil.process_iter(attrs=["pid", "name", "cmdline"]):
                        try:
                            if "ffmpeg" in proc.info["name"].lower():
                                cmd_str = " ".join(str(arg) for arg in proc.info.get("cmdline", []))
                                
                                # Pour chaque chaîne, on vérifie si le process lui appartient
                                for channel_name in self.channels:
                                    if f"/hls/{channel_name}/" in cmd_str:
                                        if channel_name not in channels_checked:
                                            logger.warning(f"🔥 Process FFmpeg orphelin détecté pour {channel_name}, PID {proc.pid}")
                                            try:
                                                os.kill(proc.pid, signal.SIGKILL)
                                                logger.info(f"✅ Process orphelin {proc.pid} nettoyé")
                                            except:
                                                pass
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            continue

                time.sleep(10)  # Vérification toutes les 10s

            except Exception as e:
                logger.error(f"❌ Erreur watchers_loop: {e}")
                time.sleep(10)
                            
    def _is_process_active(self, channel_name: str, pid: int) -> bool   :
        """
        Vérifie si un processus est actif en fonction des watchers et du temps d'inactivité
        """
        channel = self.channels.get(channel_name)
        if not channel:
            return False

        # On récupère le statut des watchers
        watchers_active = channel.watchers_count > 0
        
        # On vérifie le temps d'inactivité
        current_time = time.time()
        time_since_last_watcher = current_time - channel.last_watcher_time
        
        # Si pas de watchers depuis plus de TIMEOUT_NO_VIEWERS ou watchers_count = 0
        if not watchers_active or time_since_last_watcher > TIMEOUT_NO_VIEWERS:
            try:
                # On tente de tuer le processus proprement
                process = psutil.Process(pid)
                process.terminate()
                time.sleep(1)  # On laisse une seconde pour la terminaison propre
                
                # Si toujours en vie, on force
                if process.is_running():
                    process.kill()
                    
                logger.info(f"[{channel_name}] Process {pid} nettoyé (inactif depuis {time_since_last_watcher:.1f}s)")
                    
            except psutil.NoSuchProcess:
                pass
            except Exception as e:
                logger.error(f"[{channel_name}] Erreur nettoyage process {pid}: {e}")
                
            return False
            
        return True
    
    def run(self):
        while not self.stop_event.is_set():
            try:
                self._check_all_ffmpeg_processes()
                time.sleep(10)  # On vérifie toutes les 10s
            except Exception as e:
                logger.error(f"❌ Erreur monitoring FFmpeg: {e}")
                time.sleep(10)

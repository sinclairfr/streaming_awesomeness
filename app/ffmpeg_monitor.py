# ffmpeg_monitor.py
import psutil
import threading
import time
from pathlib import Path
from config import logger
import os
import signal

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
        
    def run(self):
        while not self.stop_event.is_set():
            try:
                self._check_all_ffmpeg_processes()
                time.sleep(10)  # On vérifie toutes les 10s
            except Exception as e:
                logger.error(f"❌ Erreur monitoring FFmpeg: {e}")
                time.sleep(10)

    def _check_all_ffmpeg_processes(self):
        """On surveille tous les processus FFmpeg actifs"""
        ffmpeg_processes = {}
        
        # On scanne tous les processus FFmpeg
        for proc in psutil.process_iter(attrs=["pid", "name", "cmdline"]):
            try:
                if "ffmpeg" in proc.info["name"].lower():
                    if proc.info.get("cmdline"):
                        for arg in proc.info["cmdline"]:
                            if "/hls/" in str(arg):
                                channel_name = str(arg).split("/hls/")[1].split("/")[0]
                                if channel_name not in ffmpeg_processes:
                                    ffmpeg_processes[channel_name] = []
                                ffmpeg_processes[channel_name].append(proc.info["pid"])
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        # Pour chaque chaîne, on vérifie son état
        current_time = time.time()
        for channel_name, pids in ffmpeg_processes.items():
            if channel_name in self.channels:
                channel = self.channels[channel_name]
                time_since_last_watcher = current_time - channel.last_watcher_time
                
                # Si pas de watcher depuis 20s et plus d'un FFmpeg
                if time_since_last_watcher > 20 and len(pids) > 1:
                    logger.warning(f"⚠️ {channel_name}: {len(pids)} processus FFmpeg actifs mais inactif depuis {time_since_last_watcher:.1f}s")
                    self._cleanup_zombie_processes(channel_name, pids)
    
    def _cleanup_zombie_processes(self, channel_name: str, pids: list):
        """On nettoie les processus FFmpeg zombies en gardant le plus récent"""
        try:
            # On identifie le process le plus récent
            latest_pid = None
            latest_start_time = 0
            
            for pid in pids:
                try:
                    process = psutil.Process(pid)
                    if process.create_time() > latest_start_time:
                        latest_start_time = process.create_time()
                        latest_pid = pid
                except psutil.NoSuchProcess:
                    continue
                    
            logger.info(f"[{channel_name}] Conservation du processus le plus récent: {latest_pid}")
            
            # On tue tous les autres
            for pid in pids:
                if pid != latest_pid:
                    try:
                        logger.info(f"[{channel_name}] Kill du processus zombie: {pid}")
                        process = psutil.Process(pid)
                        process.terminate()  # SIGTERM d'abord
                    except psutil.NoSuchProcess:
                        continue
                        
            # On attend un peu
            time.sleep(2)
            
            # SIGKILL pour les survivants (sauf le plus récent)
            for pid in pids:
                if pid != latest_pid:
                    try:
                        process = psutil.Process(pid)
                        logger.warning(f"[{channel_name}] Kill forcé du processus: {pid}")
                        process.kill()  # SIGKILL
                    except psutil.NoSuchProcess:
                        continue
                        
        except Exception as e:
            logger.error(f"❌ Erreur nettoyage processus pour {channel_name}: {e}")
        """On nettoie les processus FFmpeg zombies"""
        try:
            current_time = time.time()
            processes_info = []
            
            # On analyse tous les processus
            for pid in pids:
                try:
                    process = psutil.Process(pid)
                    create_time = process.create_time()
                    processes_info.append((pid, create_time))
                except psutil.NoSuchProcess:
                    continue
                    
            # On trie par temps de création
            processes_info.sort(key=lambda x: x[1])
            
            # On garde le plus récent, on tue les autres
            if len(processes_info) > 1:
                newest_pid = processes_info[-1][0]
                logger.info(f"[{channel_name}] Conservation du processus le plus récent: {newest_pid}")
                
                for pid, _ in processes_info[:-1]:
                    try:
                        os.kill(pid, signal.SIGTERM)
                        logger.info(f"[{channel_name}] Nettoyage ancien processus: {pid}")
                    except ProcessLookupError:
                        continue
                        
        except Exception as e:
            logger.error(f"❌ Erreur nettoyage processus pour {channel_name}: {e}")
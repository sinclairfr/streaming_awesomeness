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
        
    def _check_all_ffmpeg_processes(self):
        """
        Parcourt tous les processus pour voir lesquels sont liés à FFmpeg,
        groupés par nom de chaîne. Puis gère le nettoyage des processus
        multiples (zombies).
        """
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
        
        current_time = time.time()
        
        # Pour chaque chaîne, on vérifie s'il y a plusieurs PIDs
        for channel_name, pids in ffmpeg_processes.items():
            # Log avant déduplication
            logger.debug(f"[{channel_name}] PIDs bruts (avant dedup) : {pids}")
            
            # Déduplication
            pids = list(set(pids))
            
            # Log après déduplication
            logger.debug(f"[{channel_name}] PIDs après dedup : {pids}")
            
            # On récupère la chaîne et calcule le temps depuis le dernier watcher
            channel = self.channels.get(channel_name)
            if not channel:
                continue
                
            time_since_last_watcher = current_time - channel.last_watcher_time
            
            # S'il y a plus d'un PID et qu'on n'a pas de watchers depuis TIMEOUT_NO_VIEWERS
            logger.info(
                f"On inspect la chaîne [{channel_name}], nombre de process : {len(pids)} "
                f"et temps depuis dernier watcher : {time_since_last_watcher:.1f}s"
            )

            if time_since_last_watcher > TIMEOUT_NO_VIEWERS or len(pids) > 1:
                logger.warning(
                    f"⚠️ {channel_name}: {len(pids)} processus FFmpeg actifs "
                    f"mais inactifs depuis {time_since_last_watcher:.1f}s"
                )
                self._cleanup_zombie_processes(channel_name, pids)
    
    def _cleanup_zombie_processes(self, channel_name: str, pids: list):
        """
        Nettoie les processus FFmpeg zombies pour une chaîne donnée,
        en conservant uniquement le plus récent.
        """
        if not pids:
            return

        logger.info(f"[{channel_name}] 🔍 Processus FFmpeg détectés avant nettoyage: {pids}")

        pids = list(set(pids))

        latest_pid = None
        latest_start_time = 0

        for pid in pids:
            try:
                if psutil.pid_exists(pid):
                    process = psutil.Process(pid)
                    ctime = process.create_time()
                    if ctime > latest_start_time:
                        latest_start_time = ctime
                        latest_pid = pid
            except psutil.NoSuchProcess:
                continue

        if latest_pid is None:
            logger.warning(f"[{channel_name}] Aucun processus valide trouvé.")
            return

        logger.info(f"[{channel_name}] ✅ Conservation du processus le plus récent: {latest_pid}")

        # Sauvegarde de l'offset avant kill forcé
        channel = self.channels.get(channel_name)
        if channel and hasattr(channel, 'process_manager') and hasattr(channel.process_manager, 'get_playback_offset'):
            try:
                # Utilise le process_manager pour sauvegarder la position
                channel.process_manager.save_position()
                logger.info(f"[{channel_name}] 💾 Position sauvegardée avant kill forcé")
            except Exception as e:
                logger.error(f"[{channel_name}] Erreur sauvegarde position: {e}")

        for pid in pids:
            if pid != latest_pid:
                try:
                    if psutil.pid_exists(pid):
                        logger.info(f"[{channel_name}] 🔪 Suppression du processus zombie: {pid}")
                        process = psutil.Process(pid)
                        process.terminate()  # SIGTERM
                except psutil.NoSuchProcess:
                    continue

        time.sleep(2)

        for pid in pids:
            if pid != latest_pid:
                try:
                    if psutil.pid_exists(pid):
                        logger.warning(f"[{channel_name}] 💀 Suppression forcée du processus: {pid}")
                        psutil.Process(pid).kill()
                except psutil.NoSuchProcess:
                    continue
                
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

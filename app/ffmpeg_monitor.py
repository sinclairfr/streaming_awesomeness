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
        
        # Pour chaque chaîne, on vérifie s'il y a plusieurs PIDs ou s'ils sont inactifs
        for channel_name, pids in ffmpeg_processes.items():
            # On récupère la chaîne et calcule le temps depuis le dernier watcher
            channel = self.channels.get(channel_name)
            if not channel:
                continue
                
            time_since_last_watcher = current_time - channel.last_watcher_time
            
            # On log des infos de monitoring
            logger.info(
                f"On inspect la chaîne [{channel_name}], nombre de process : {len(pids)} "
                f"et temps depuis dernier watcher : {time_since_last_watcher:.1f}s"
            )

            # Si on a plusieurs processus OU qu'on a dépassé le temps d'inactivité, on nettoie
            if len(pids) > 1 or time_since_last_watcher > TIMEOUT_NO_VIEWERS:
                if len(pids) > 1:
                    logger.warning(f"⚠️ {channel_name}: {len(pids)} processus FFmpeg actifs détectés")
                elif time_since_last_watcher > TIMEOUT_NO_VIEWERS:
                    logger.warning(f"⚠️ {channel_name}: Processus FFmpeg inactif depuis {time_since_last_watcher:.1f}s")
                
                self._cleanup_zombie_processes(channel_name, pids)
    
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
    #TODO déplacer ver ffmpeg_process_manager.py
    def _cleanup_zombie_processes(self, channel_name: str, pids: list):
        """
        Version améliorée et plus agressive pour nettoyer les processus FFmpeg zombies
        """
        if not pids:
            return

        logger.info(f"[{channel_name}] 🔍 Processus détectés: {pids}")
        
        # On récupère la chaîne
        channel = self.channels.get(channel_name)
        if not channel:
            logger.error(f"[{channel_name}] ❌ Chaîne introuvable")
            return
        
        inactivity_time = time.time() - channel.last_watcher_time
        
        # Si inactif depuis plus que le timeout configuré
        if inactivity_time > TIMEOUT_NO_VIEWERS:
            # On sauvegarde d'abord la position
            try:
                if hasattr(channel, 'position_manager'):
                    channel.position_manager.save_position()
                    logger.info(f"[{channel_name}] 💾 Position sauvegardée: {inactivity_time:.2f}s d'inactivité")
            except Exception as e:
                logger.error(f"[{channel_name}] Erreur sauvegarde: {e}")
            
            # On tue TOUS les processus FFmpeg pour cette chaîne sans pitié
            for pid in pids:
                try:
                    logger.warning(f"[{channel_name}] 💥 Kill BRUTAL du processus {pid}")
                    # On utilise directement le signal KILL pour être sûr
                    os.kill(pid, signal.SIGKILL)
                except Exception as e:
                    logger.error(f"[{channel_name}] Erreur kill {pid}: {e}")
            
            # On nettoie aussi via le channel pour être sûr
            if hasattr(channel, 'process_manager'):
                try:
                    logger.info(f"[{channel_name}] 🧹 Nettoyage via process_manager")
                    channel.process_manager.stop_process(save_position=False)  # Position déjà sauvegardée
                except Exception as e:
                    logger.error(f"[{channel_name}] Erreur process_manager: {e}")
            
            # On vérifie que tout est bien mort après 1 seconde
            time.sleep(1)
            zombie_pids = []
            for pid in pids:
                if psutil.pid_exists(pid):
                    zombie_pids.append(pid)
            
            # S'il reste des zombies, on refait un kill brutal
            if zombie_pids:
                logger.error(f"[{channel_name}] 🧟 Zombies persistants: {zombie_pids}")
                for pid in zombie_pids:
                    try:
                        os.kill(pid, signal.SIGKILL)
                        logger.warning(f"[{channel_name}] 💀 Kill ultime du zombie {pid}")
                    except:
                        pass
            
            return
        
        # Si on a plusieurs processus mais qu'ils sont toujours actifs
        if len(pids) > 1:
            # On garde le plus récent
            latest_pid = None
            latest_start_time = 0

            for pid in pids:
                try:
                    if psutil.pid_exists(pid):
                        p = psutil.Process(pid)
                        if p.create_time() > latest_start_time:
                            latest_start_time = p.create_time()
                            latest_pid = pid
                except:
                    continue

            if not latest_pid:
                return

            # On tue tous les autres
            for pid in pids:
                if pid != latest_pid:
                    try:
                        logger.info(f"[{channel_name}] 🔪 Kill du processus doublon {pid}")
                        os.kill(pid, signal.SIGKILL)
                    except:
                        pass    
                              
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

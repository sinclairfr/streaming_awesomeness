#!/usr/bin/env python3
# stuck_stream_monitor.py - Outil de détection et réparation des streams bloqués
# À exécuter via crontab, par exemple:

import os
import sys
import time
import psutil
import signal
import logging
import subprocess
from pathlib import Path

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - [%(levelname)s] - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("/app/logs/stream_monitor.log"),
    ],
)
logger = logging.getLogger("STREAM_MONITOR")

# Configuration
TIMEOUT_NO_VIEWERS = int(os.getenv("TIMEOUT_NO_VIEWERS", "3600"))  # 1h par défaut
HLS_DIR = "/app/hls"
ADDITIONAL_TIMEOUT = 1800  # 30 minutes supplémentaires de grâce


def get_ffmpeg_processes():
    """Identifie tous les processus FFmpeg et leurs chaînes associées"""
    ffmpeg_procs = {}

    for proc in psutil.process_iter(["pid", "name", "cmdline", "create_time"]):
        try:
            if proc.info["name"] and "ffmpeg" in proc.info["name"].lower():
                cmd_line = " ".join(proc.info["cmdline"] or [])
                
                # Recherche du pattern /hls/{channel}/
                import re
                channel_match = re.search(r'/hls/([^/]+)/', cmd_line)
                
                if channel_match:
                    channel_name = channel_match.group(1)
                    proc_info = {
                        "pid": proc.info["pid"],
                        "cmdline": cmd_line,
                        "runtime": time.time() - proc.info["create_time"],
                        "channel": channel_name
                    }
                    
                    if channel_name not in ffmpeg_procs:
                        ffmpeg_procs[channel_name] = []
                    
                    ffmpeg_procs[channel_name].append(proc_info)
        
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    
    return ffmpeg_procs


def check_channel_activity(channel_name):
    """Vérifie si une chaîne a des viewers actifs"""
    hls_path = Path(HLS_DIR) / channel_name
    
    # Si le dossier HLS n'existe pas, certainement pas d'activité
    if not hls_path.exists():
        return False
    
    # Vérifie les segments .ts pour estimer l'activité
    segments = list(hls_path.glob("*.ts"))
    if not segments:
        logger.warning(f"[{channel_name}] Aucun segment trouvé")
        return False
    
    # Vérifie l'âge du segment le plus récent
    newest_segment = max(segments, key=lambda p: p.stat().st_mtime)
    segment_age = time.time() - newest_segment.stat().st_mtime
    
    if segment_age > 60:  # Plus d'1 minute sans nouveau segment
        logger.warning(f"[{channel_name}] Dernier segment vieux de {segment_age:.1f}s")
        return False
    
    return True


def kill_processes(pids):
    """Tue les processus par PID avec un délai entre chaque"""
    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
            logger.info(f"Signal SIGTERM envoyé au processus {pid}")
            
            # Attendre un peu pour voir si le processus termine proprement
            time.sleep(1)
            
            # Vérifier si le processus existe toujours
            if psutil.pid_exists(pid):
                # Si toujours en vie, envoyer SIGKILL
                os.kill(pid, signal.SIGKILL)
                logger.info(f"Signal SIGKILL envoyé au processus {pid}")
            
            time.sleep(0.5)  # Petit délai entre les processus
            
        except (ProcessLookupError, PermissionError) as e:
            logger.error(f"Erreur suppression du processus {pid}: {e}")


def main():
    """Fonction principale de détection et correction"""
    logger.info("=== Début de la vérification des streams bloqués ===")
    
    try:
        # Récupère tous les processus FFmpeg
        ffmpeg_processes = get_ffmpeg_processes()
        logger.info(f"Trouvé {len(ffmpeg_processes)} chaînes avec des processus FFmpeg")
        
        killed_count = 0
        for channel_name, processes in ffmpeg_processes.items():
            # Vérifier si la chaîne a une activité récente
            has_activity = check_channel_activity(channel_name)
            process_count = len(processes)
            
            # Si plusieurs processus pour la même chaîne, c'est un problème
            if process_count > 1:
                logger.warning(f"[{channel_name}] Détecté {process_count} processus FFmpeg - conflit")
                
                # Trier par runtime (garder le plus récent, tuer les autres)
                processes.sort(key=lambda p: p["runtime"])
                
                # Garder le plus récent, tuer les autres
                pids_to_kill = [p["pid"] for p in processes[:-1]]
                logger.info(f"[{channel_name}] Suppression des {len(pids_to_kill)} processus les plus anciens")
                kill_processes(pids_to_kill)
                killed_count += len(pids_to_kill)
            
            # Vérifier si c'est un processus zombie (longue durée sans activité)
            oldest_process = max(processes, key=lambda p: p["runtime"])
            runtime_hours = oldest_process["runtime"] / 3600
            
            # On vérifie s'il y a des viewers actifs
            if not has_activity and runtime_hours > (TIMEOUT_NO_VIEWERS + ADDITIONAL_TIMEOUT) / 3600:
                logger.warning(
                    f"[{channel_name}] Processus bloqué détecté: {oldest_process['pid']} "
                    f"(durée: {runtime_hours:.1f}h sans activité)"
                )
                
                # Tuer ce processus
                pids_to_kill = [p["pid"] for p in processes]
                logger.info(f"[{channel_name}] Arrêt forcé des {len(pids_to_kill)} processus")
                kill_processes(pids_to_kill)
                killed_count += len(pids_to_kill)
        
        logger.info(f"=== Fin de la vérification: {killed_count} processus terminés ===")
        return 0
        
    except Exception as e:
        logger.error(f"Erreur dans le script de monitoring: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())
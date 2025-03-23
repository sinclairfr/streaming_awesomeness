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
    """Vérifie si une chaîne a des viewers actifs et détecte les problèmes de transition"""
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
    
    # Vérification plus stricte pour les segments récents
    if segment_age > 30:  # Réduit à 30 secondes au lieu de 60
        logger.warning(f"[{channel_name}] Dernier segment vieux de {segment_age:.1f}s")
        return False
    
    # Vérification des problèmes de transition vidéo
    try:
        # 1. Vérifier la playlist m3u8
        playlist_path = hls_path / "playlist.m3u8"
        if not playlist_path.exists():
            logger.warning(f"[{channel_name}] playlist.m3u8 introuvable")
            return False
        
        # Analyser la playlist pour extraire les segments référencés
        with open(playlist_path, "r") as f:
            playlist_lines = f.readlines()
        
        referenced_segments = []
        for line in playlist_lines:
            if line.strip().endswith(".ts"):
                referenced_segments.append(line.strip())
        
        if not referenced_segments:
            logger.warning(f"[{channel_name}] Aucun segment référencé dans la playlist")
            return False
        
        # 2. Vérifier les segments manquants (plus sensible: 20% au lieu de 30%)
        missing_segments = []
        for segment_name in referenced_segments:
            segment_path = hls_path / segment_name
            if not segment_path.exists():
                missing_segments.append(segment_name)
        
        # Si plus de 20% des segments référencés sont manquants, c'est un problème
        if missing_segments:
            missing_percent = (len(missing_segments) / len(referenced_segments)) * 100
            if missing_percent > 20:  # Seuil réduit à 20%
                logger.warning(f"[{channel_name}] ⚠️ {missing_percent:.1f}% des segments référencés sont manquants ({len(missing_segments)}/{len(referenced_segments)})")
                return False
        
        # 3. Vérifier les sauts dans la numérotation des segments
        segment_numbers = []
        for seg in segments:
            try:
                # Extrait le numéro du segment à partir du nom de fichier (ex: "segment_123.ts" -> 123)
                num = int(seg.stem.split('_')[1])
                segment_numbers.append((num, seg))
            except (ValueError, IndexError):
                continue
        
        if segment_numbers:
            # Tri par numéro de segment
            segment_numbers.sort(key=lambda x: x[0])
            
            # Vérification des sauts dans la numérotation (plus sensible: 3 au lieu de 5)
            jumps = []
            for i in range(1, len(segment_numbers)):
                current_num = segment_numbers[i][0]
                prev_num = segment_numbers[i-1][0]
                gap = current_num - prev_num
                if gap > 3:  # Saut de plus de 3 segments (était 5)
                    jumps.append((prev_num, current_num, gap))
            
            # Vérifier aussi les dates de modification pour détecter les saccades
            timestamp_jumps = []
            for i in range(1, len(segment_numbers)):
                current_segment = segment_numbers[i][1]
                prev_segment = segment_numbers[i-1][1]
                
                current_mtime = current_segment.stat().st_mtime
                prev_mtime = prev_segment.stat().st_mtime
                
                # Si l'écart temporel entre segments est anormal (> 5 secondes au lieu de 3)
                # Les écarts de 3-5s peuvent être normaux pour certaines chaînes
                if current_mtime - prev_mtime > 5:
                    timestamp_jumps.append((
                        segment_numbers[i-1][0],
                        segment_numbers[i][0],
                        round(current_mtime - prev_mtime, 1)
                    ))
            
            # Si on a des sauts significatifs récents ou des écarts temporels anormaux
            recent_jumps = []
            if segment_numbers:
                # Segments récents = 3 derniers segments (au lieu de 5)
                # Se concentrer sur les derniers segments pour éviter les faux positifs
                latest_segments = segment_numbers[-3:] if len(segment_numbers) > 3 else segment_numbers
                latest_nums = [num for num, _ in latest_segments]
                
                # Vérifier les sauts de numérotation récents
                recent_jumps = [j for j in jumps if j[1] in latest_nums]
                
                # Vérifier les sauts temporels récents, mais seulement s'ils sont importants (> 8s)
                recent_timestamp_jumps = [j for j in timestamp_jumps if j[1] in latest_nums and j[2] > 8]
                
                # Si on a des problèmes récents, signaler
                if recent_jumps:
                    jump_str = ", ".join([f"{prev}->{curr} (gap:{gap})" for prev, curr, gap in recent_jumps])
                    logger.warning(f"[{channel_name}] 🚨 Sauts de segments récents: {jump_str}")
                    return False
                
                if recent_timestamp_jumps:
                    jump_str = ", ".join([f"{prev}->{curr} (temps:{gap}s)" for prev, curr, gap in recent_timestamp_jumps])
                    logger.warning(f"[{channel_name}] 🚨 Sauts temporels anormaux importants: {jump_str}")
                    return False
            
            # 4. Vérifier les tailles des segments (pour détecter les segments corrompus)
            segment_sizes = []
            for _, segment in segment_numbers:
                segment_sizes.append(segment.stat().st_size)
            
            # Calculer la taille moyenne
            if segment_sizes:
                avg_size = sum(segment_sizes) / len(segment_sizes)
                
                # Vérifier les segments anormalement petits (< 30% de la moyenne)
                small_segments = []
                for i, (num, segment) in enumerate(segment_numbers):
                    if segment.stat().st_size < avg_size * 0.3:  # Segment anormalement petit
                        small_segments.append((num, segment.stat().st_size, round(segment.stat().st_size / avg_size * 100)))
                
                # Si les segments récents sont anormalement petits
                recent_small_segments = []
                for small_seg in small_segments:
                    if small_seg[0] in latest_nums:
                        recent_small_segments.append(small_seg)
                
                if recent_small_segments:
                    small_str = ", ".join([f"segment_{num} ({size}B, {percent}%)" for num, size, percent in recent_small_segments])
                    logger.warning(f"[{channel_name}] 🚨 Segments anormalement petits détectés: {small_str}")
                    return False
                    
        # 5. Vérifier les logs d'erreur nginx pour les problèmes de ce flux
        try:
            error_cmd = f"tail -n 100 /app/nginx_logs/error.log | grep -i '{channel_name}' | grep -i 'no such file' | wc -l"
            error_result = subprocess.run(error_cmd, shell=True, capture_output=True, text=True)
            error_count = int(error_result.stdout.strip() or '0')
            
            if error_count > 3:  # Plusieurs erreurs récentes dans les logs
                logger.warning(f"[{channel_name}] 🚨 {error_count} erreurs nginx 'no such file' récentes")
                return False
        except Exception as e:
            logger.debug(f"[{channel_name}] Erreur vérification logs nginx: {e}")
    
    except Exception as e:
        logger.error(f"[{channel_name}] Erreur analyse segments: {e}")
    
    # Si on arrive ici, l'activité semble normale
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
        restart_count = 0
        for channel_name, processes in ffmpeg_processes.items():
            # Vérifier si la chaîne a une activité récente et des problèmes de transition
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
            
            # Vérifier si l'activité est valide
            if not has_activity:
                # 1. Vérifier si c'est un processus zombie (longue durée sans activité)
                oldest_process = max(processes, key=lambda p: p["runtime"])
                runtime_hours = oldest_process["runtime"] / 3600
                
                # 2. Vérifier s'il y a des viewers actifs
                has_viewers = check_channel_has_viewers(channel_name)
                
                # Si le processus est bloqué ou en erreur de transition
                if runtime_hours > (TIMEOUT_NO_VIEWERS + ADDITIONAL_TIMEOUT) / 3600:
                    logger.warning(
                        f"[{channel_name}] Processus bloqué détecté: {oldest_process['pid']} "
                        f"(durée: {runtime_hours:.1f}h sans activité)"
                    )
                    
                    # Tuer ce processus
                    pids_to_kill = [p["pid"] for p in processes]
                    logger.info(f"[{channel_name}] Arrêt forcé des {len(pids_to_kill)} processus")
                    kill_processes(pids_to_kill)
                    killed_count += len(pids_to_kill)
                    
                    # Redémarrer le processus si nécessaire
                    if has_viewers:
                        if restart_channel(channel_name):
                            restart_count += 1
                
                # 3. Si problème de transition avec des viewers actifs, redémarrer
                elif has_viewers:
                    logger.warning(f"[{channel_name}] Problème de transition détecté avec viewers actifs")
                    
                    # On redémarre le stream pour corriger les problèmes de transition
                    pids_to_kill = [p["pid"] for p in processes]
                    logger.info(f"[{channel_name}] Redémarrage forcé pour problème de transition")
                    kill_processes(pids_to_kill)
                    killed_count += len(pids_to_kill)
                    
                    # Attendre un peu avant de redémarrer
                    time.sleep(2)
                    
                    # Redémarrer le processus
                    if restart_channel(channel_name):
                        restart_count += 1
                
                # 4. Si c'est la chaîne verite ou d'autres chaînes spécifiques avec problèmes récurrents
                elif channel_name in ["verite", "lahaine", "defunes"]:
                    logger.warning(f"[{channel_name}] ⚠️ Chaîne connue pour avoir des problèmes de transition - redémarrage forcé même sans viewers")
                    
                    # On redémarre le stream pour corriger les problèmes de transition
                    pids_to_kill = [p["pid"] for p in processes]
                    logger.info(f"[{channel_name}] Redémarrage forcé pour chaîne problématique")
                    kill_processes(pids_to_kill)
                    killed_count += len(pids_to_kill)
                    
                    # Attendre un peu avant de redémarrer
                    time.sleep(2)
                    
                    # Redémarrer le processus
                    if restart_channel(channel_name):
                        restart_count += 1
        
        logger.info(f"=== Fin de la vérification: {killed_count} processus terminés, {restart_count} streams redémarrés ===")
        return 0
        
    except Exception as e:
        logger.error(f"Erreur dans le script de monitoring: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


def check_channel_has_viewers(channel_name):
    """Vérifie si une chaîne a des viewers actifs en consultant les logs d'accès"""
    try:
        # Vérifier les logs nginx pour des accès récents
        cmd = f"tail -n 500 /app/nginx_logs/access.log | grep -i '{channel_name}' | grep -v 'playlist.m3u8' | wc -l"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        # Compte les accès récents aux segments .ts
        access_count = int(result.stdout.strip() or '0')
        
        # Si on a des accès récents, il y a probablement des viewers
        logger.info(f"[{channel_name}] - nombre d'accès récents : {access_count}")
        return access_count > 0
        
    except Exception as e:
        logger.error(f"[{channel_name}] Erreur vérification viewers: {e}")
        return False


def restart_channel(channel_name):
    """Relance un flux pour une chaîne spécifique via l'API client_monitor"""
    try:
        # On appelle un process Python directement plutôt qu'une API
        cmd = f"python3 /app/restart_channel.py {channel_name}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info(f"[{channel_name}] ✅ Stream relancé avec succès")
            return True
        else:
            logger.error(f"[{channel_name}] ❌ Erreur relance: {result.stderr}")
            return False
            
    except Exception as e:
        logger.error(f"[{channel_name}] ❌ Erreur restart_channel: {e}")
        return False


if __name__ == "__main__":
    sys.exit(main())
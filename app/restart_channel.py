#!/usr/bin/env python3
# restart_channel.py - Script pour redémarrer une chaîne spécifique

import os
import sys
import time
import logging
import subprocess
from pathlib import Path

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - [%(levelname)s] - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("/app/logs/restart_channel.log"),
    ],
)
logger = logging.getLogger("RESTART_CHANNEL")

def restart_channel(channel_name):
    """Redémarre un flux pour une chaîne spécifique"""
    try:
        logger.info(f"[{channel_name}] 🔄 Tentative de redémarrage")
        
        # 1. Arrêter tous les processus existants pour cette chaîne
        stop_cmd = f"pkill -f '/hls/{channel_name}/'"
        subprocess.run(stop_cmd, shell=True, capture_output=True)
        
        # Attendre que les processus se terminent
        time.sleep(2)
        
        # Vérifier que tous les processus sont bien arrêtés
        check_cmd = f"ps aux | grep -v grep | grep -i '/hls/{channel_name}/' | wc -l"
        check_result = subprocess.run(check_cmd, shell=True, capture_output=True, text=True)
        
        if int(check_result.stdout.strip() or '0') > 0:
            # Forcer l'arrêt avec SIGKILL
            logger.warning(f"[{channel_name}] Processus toujours en cours, envoi de SIGKILL")
            kill_cmd = f"ps aux | grep -v grep | grep -i '/hls/{channel_name}/' | awk '{{print $2}}' | xargs -r kill -9"
            subprocess.run(kill_cmd, shell=True, capture_output=True)
            time.sleep(1)
        
        # 2. Nettoyer les segments existants et créer un dossier propre
        hls_dir = Path(f"/app/hls/{channel_name}")
        
        # Sauvegarder l'ancienne playlist si nécessaire pour la déboguer
        backup_dir = Path(f"/app/logs/playlist_backups")
        backup_dir.mkdir(parents=True, exist_ok=True)
        
        playlist = hls_dir / "playlist.m3u8"
        if playlist.exists():
            try:
                # Sauvegarder avec horodatage
                timestamp = time.strftime("%Y%m%d-%H%M%S")
                backup_file = backup_dir / f"{channel_name}_{timestamp}.m3u8"
                with open(playlist, "r") as src, open(backup_file, "w") as dest:
                    dest.write(src.read())
                logger.info(f"[{channel_name}] Playlist sauvegardée: {backup_file}")
            except Exception as e:
                logger.warning(f"[{channel_name}] Impossible de sauvegarder playlist: {e}")
        
        # Supprimer et recréer le dossier HLS
        if hls_dir.exists():
            try:
                # Supprimer tous les fichiers dans le dossier
                for file in hls_dir.glob("*"):
                    try:
                        file.unlink()
                    except Exception as e:
                        logger.warning(f"[{channel_name}] Impossible de supprimer {file.name}: {e}")
            except Exception as e:
                logger.warning(f"[{channel_name}] Erreur nettoyage: {e}")
        else:
            # Créer le dossier s'il n'existe pas
            hls_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"[{channel_name}] Dossier HLS créé: {hls_dir}")
        
        # 3. Vérifier/reconstruire le fichier de playlist concatenée
        playlist_file = Path(f"/app/videos/{channel_name}/_playlist.txt")
        if not playlist_file.exists() or playlist_file.stat().st_size == 0:
            logger.warning(f"[{channel_name}] Playlist concat inexistante ou vide, tentative de reconstruction")
            try:
                # Vérifier les deux emplacements possibles des vidéos
                video_paths = [
                    Path(f"/app/videos/{channel_name}/ready_to_stream"),
                    Path(f"/mnt/videos/streaming_awesomeness/{channel_name}/ready_to_stream")
                ]
                
                # Trouver le premier chemin valide
                video_dir = None
                for path in video_paths:
                    if path.exists():
                        video_dir = path
                        logger.info(f"[{channel_name}] Dossier vidéo trouvé: {video_dir}")
                        break
                
                if video_dir:
                    # Créer le dossier parent si nécessaire
                    playlist_file.parent.mkdir(parents=True, exist_ok=True)
                    
                    # Créer une playlist de base à partir des fichiers MP4 disponibles
                    videos = list(video_dir.glob("*.mp4"))
                    if videos:
                        with open(playlist_file, "w") as f:
                            for video in videos:
                                f.write(f"file '{video}'\n")
                        logger.info(f"[{channel_name}] Playlist reconstruite avec {len(videos)} vidéos")
                    else:
                        # Chercher aussi les MKV
                        videos = list(video_dir.glob("*.mkv"))
                        if videos:
                            with open(playlist_file, "w") as f:
                                for video in videos:
                                    f.write(f"file '{video}'\n")
                            logger.info(f"[{channel_name}] Playlist reconstruite avec {len(videos)} vidéos MKV")
                        else:
                            logger.error(f"[{channel_name}] Aucun fichier vidéo trouvé dans {video_dir}")
                            
                            # Tenter de récupérer en créant un fichier vide pour FFmpeg
                            with open(playlist_file, "w") as f:
                                dummy_path = Path("/app/assets/dummy.mp4")
                                if dummy_path.exists():
                                    f.write(f"file '{dummy_path}'\n")
                                    logger.warning(f"[{channel_name}] Création d'une playlist avec la vidéo de secours")
                                    return True
                            return False
                else:
                    logger.error(f"[{channel_name}] Dossier vidéo introuvable dans tous les chemins testés")
                    
                    # Créer la structure minimale nécessaire
                    hls_dir = Path(f"/app/hls/{channel_name}")
                    hls_dir.mkdir(parents=True, exist_ok=True)
                    
                    # Créer un fichier de playlist vide mais valide pour éviter les erreurs
                    with open(hls_dir / "playlist.m3u8", "w") as f:
                        f.write("#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:2\n#EXT-X-MEDIA-SEQUENCE:0\n")
                    
                    return False
            except Exception as e:
                logger.error(f"[{channel_name}] Erreur reconstruction playlist: {e}")
                return False
        
        # 4. Lancer le stream via client_monitor
        restart_cmd = f"python3 /app/client_monitor.py restart_stream {channel_name}"
        result = subprocess.run(restart_cmd, shell=True, capture_output=True, text=True)
        
        if "Stream started" in result.stdout or "started successfully" in result.stdout:
            logger.info(f"[{channel_name}] ✅ Stream redémarré avec succès")
            return True
        
        # Alternative si la commande précédente échoue
        restart_cmd2 = f"docker exec iptv-manager python3 /app/client_monitor.py restart_stream {channel_name}"
        result2 = subprocess.run(restart_cmd2, shell=True, capture_output=True, text=True)
        
        if "Stream started" in result2.stdout or "started successfully" in result2.stdout:
            logger.info(f"[{channel_name}] ✅ Stream redémarré avec succès (méthode alternative)")
            return True
            
        # Si les deux méthodes échouent, invoquer le gestionnaire de processus directement
        with open("/app/pythonpath.txt", "w") as f:
            f.write("import sys\nsys.path.append('/app')\n")
        
        restart_cmd3 = (
            f"cd /app && python3 -c \"import sys; sys.path.append('/app'); "
            f"from iptv_manager import IPTVManager; IPTVManager().restart_channel('{channel_name}')\""
        )
        
        result3 = subprocess.run(restart_cmd3, shell=True, capture_output=True, text=True)
        
        logger.info(f"[{channel_name}] ⚙️ Résultat du redémarrage (méthode 3): {result3.returncode}")
        
        # Vérifier si le flux a redémarré
        time.sleep(5)  # Attendre un peu que le stream démarre
        check_cmd = f"ps aux | grep -v grep | grep -i '/hls/{channel_name}/' | wc -l"
        check_result = subprocess.run(check_cmd, shell=True, capture_output=True, text=True)
        
        if int(check_result.stdout.strip() or '0') > 0:
            logger.info(f"[{channel_name}] ✅ Stream redémarré et en cours d'exécution")
            
            # Attendre un peu plus pour vérifier que des segments sont générés
            time.sleep(10)
            segment_check = list(Path(f"/app/hls/{channel_name}").glob("*.ts"))
            
            if segment_check:
                logger.info(f"[{channel_name}] ✅ {len(segment_check)} segments générés avec succès")
                return True
            else:
                logger.warning(f"[{channel_name}] ⚠️ Stream redémarré mais aucun segment généré")
                return True  # On retourne True car le processus est bien démarré
        else:
            logger.error(f"[{channel_name}] ❌ Échec du redémarrage")
            return False
            
    except Exception as e:
        logger.error(f"[{channel_name}] ❌ Erreur lors du redémarrage: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 restart_channel.py <channel_name>")
        return 1
    
    channel_name = sys.argv[1]
    logger.info(f"=== Début du redémarrage de la chaîne '{channel_name}' ===")
    success = restart_channel(channel_name)
    
    if success:
        logger.info(f"=== Redémarrage de '{channel_name}' terminé avec succès ===")
        return 0
    else:
        logger.error(f"=== Échec du redémarrage de '{channel_name}' ===")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 
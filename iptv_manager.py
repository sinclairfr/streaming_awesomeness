import os
from pathlib import Path
import subprocess
import json
import hashlib
import shutil
import logging
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor
import threading
import time
import psutil
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import datetime

# Configuration du logging
logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='%(asctime)s - %(name)s - [%(levelname)s] - %(message)s'
)
logger = logging.getLogger(__name__)

SERVER_URL = os.getenv('SERVER_URL', '192.168.10.183')

class ChannelEventHandler(FileSystemEventHandler):
    def __init__(self, manager):
        self.manager = manager
        self.last_event_time = 0
        self.event_delay = 5  # Délai minimum entre deux événements (en secondes)
        super().__init__()

    def on_modified(self, event):
        if not event.is_directory:
            current_time = time.time()
            if current_time - self.last_event_time >= self.event_delay:
                logger.debug(f"Modification détectée: {event.src_path}")
                self.manager.scan_channels()
                self.last_event_time = current_time

    def on_created(self, event):
        if event.is_directory:
            current_time = time.time()
            if current_time - self.last_event_time >= self.event_delay:
                logger.info(f"Nouvelle chaîne détectée: {event.src_path}")
                self.manager.scan_channels()
                self.last_event_time = current_time
                
class IPTVChannel:
    def __init__(self, name: str, video_dir: str, cache_dir: str):
        self.name = name
        self.video_dir = video_dir
        self.cache_dir = cache_dir
        self.videos: List[Dict] = []
        self.current_video = 0
        self.total_duration = 0
        self.active_streams = 0
        self.ffmpeg_process = None
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.executor = ThreadPoolExecutor(max_workers=2)
        self.last_mtime = 0

    def _get_video_duration(self, video_path: str) -> float:
        """Obtient la durée d'une vidéo en secondes"""
        try:
            cmd = [
                "ffprobe",
                "-v", "quiet",
                "-print_format", "json",
                "-show_format",
                video_path
            ]

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )

            data = json.loads(result.stdout)
            duration = float(data["format"]["duration"])
            logger.debug(f"Durée de {video_path}: {duration} secondes")
            return duration

        except subprocess.CalledProcessError as e:
            logger.error(f"Erreur ffprobe pour {video_path}: {e.stderr}")
            return 0
        except json.JSONDecodeError as e:
            logger.error(f"Erreur de parsing JSON pour {video_path}: {e}")
            return 0
        except KeyError as e:
            logger.error(f"Format de données incorrect pour {video_path}: {e}")
            return 0
        except Exception as e:
            logger.error(f"Erreur inattendue pour {video_path}: {e}")
            return 0

    def _create_channel_directory(self):
        """Crée le répertoire HLS pour la chaîne"""
        channel_dir = f"hls/{self.name}"  # Utilisation d'un chemin relatif
        try:
            os.makedirs(channel_dir, exist_ok=True)
            logger.info(f"📁 Répertoire créé pour {self.name}: {channel_dir}")
        except PermissionError:
            logger.error(f"🚨 Permission refusée : impossible de créer {channel_dir}")
        except Exception as e:
            logger.error(f"⚠️ Erreur inattendue lors de la création de {channel_dir}: {e}")

    def _create_initial_playlist(self):
        """Crée la playlist initiale pour la chaîne"""
        try:
            playlist_path = f"hls/{self.name}/playlist.m3u8"  # Utilisation d'un chemin relatif
            with open(playlist_path, 'w') as f:
                f.write("#EXTM3U\n")
                f.write("#EXT-X-VERSION:3\n")
                f.write("#EXT-X-TARGETDURATION:6\n")
                f.write("#EXT-X-START:TIME-OFFSET=0\n")
                f.write("#EXT-X-MEDIA-SEQUENCE:0\n")
                f.write("#EXT-X-PLAYLIST-TYPE:EVENT\n")
            logger.debug(f"Playlist initiale créée pour {self.name}")
            return True
        except Exception as e:
            logger.error(f"Erreur lors de la création de la playlist pour {self.name}: {e}")
            return False    

    def _clean_processes(self):
        """Nettoie proprement les processus FFmpeg"""
        try:
            if self.ffmpeg_process:
                process = psutil.Process(self.ffmpeg_process.pid)
                for child in process.children(recursive=True):
                    child.terminate()
                process.terminate()
                process.wait(timeout=5)
                self.ffmpeg_process = None  # Réinitialiser le processus
                logger.info(f"Processus FFmpeg arrêté pour {self.name}")
        except psutil.NoSuchProcess:
            logger.debug(f"Aucun processus FFmpeg trouvé pour {self.name}")
        except Exception as e:
            logger.error(f"Erreur lors du nettoyage des processus pour {self.name}: {e}")
            
    def _is_process_running(self):
        """Vérifie si le processus FFmpeg est en cours d'exécution"""
        return (
            self.ffmpeg_process is not None and 
            self.ffmpeg_process.poll() is None
        )

    def _should_continue_streaming(self):
        """Vérifie si le streaming doit continuer"""
        return (
            not self.stop_event.is_set() and 
            self.active_streams > 0 and 
            bool(self.videos)
        )

    def _reset_stream_state(self):
        """Réinitialise l'état du stream"""
        with self.lock:
            self.ffmpeg_process = None
            self.active_streams = 0
            self.stop_event.set()  
            
    def scan_videos(self) -> bool:
        """Scanne les vidéos dans le répertoire et met à jour la liste"""
        try:
            video_extensions = ('.mp4', '.avi', '.mkv', '.mov')
            current_videos = []
            
            for file in sorted(Path(self.video_dir).glob('*.*'), key=lambda f: f.name):
                if file.suffix.lower() in video_extensions:
                    current_mtime = file.stat().st_mtime
                    file_path = str(file)
                    
                    # Vérifier si la vidéo existe déjà
                    existing_video = next(
                        (v for v in self.videos if v["path"] == file_path),
                        None
                    )
                    
                    if existing_video and existing_video.get("mtime") == current_mtime:
                        current_videos.append(existing_video)
                        logger.debug(f"Réutilisation des infos pour: {file.name}")
                    else:
                        duration = self._get_video_duration(file_path)
                        if duration > 0:
                            video_info = {
                                "path": file_path,
                                "duration": duration,
                                "mtime": current_mtime
                            }
                            current_videos.append(video_info)
                            logger.info(f"Nouvelle vidéo détectée: {file.name}")

            # Mettre à jour uniquement si la liste a changé
            if len(current_videos) != len(self.videos) or any(
                v1 != v2 for v1, v2 in zip(current_videos, self.videos)
            ):
                self.videos = current_videos
                self.total_duration = sum(v["duration"] for v in current_videos)
                self._create_channel_directory()
                self._create_initial_playlist()
                logger.info(f"Mise à jour des vidéos pour {self.name}: {len(current_videos)} fichiers")
                return True
                
            return False

        except Exception as e:
            logger.error(f"Erreur lors du scan des vidéos pour {self.name}: {e}")
            return False
    
    def start_stream(self):
        """Méthode de démarrage du stream avec conversion préalable"""
        with self.lock:
            try:
                if not self.videos:
                    logger.error(f"🚫 Aucune vidéo disponible pour {self.name}")
                    return False

                # Convertir d'abord tous les fichiers
                converted_videos = []
                for video in self.videos:
                    cached_path = self._get_cached_path(video["path"])
                    if cached_path:
                        video["cached_path"] = cached_path
                        converted_videos.append(video)
                    else:
                        logger.error(f"❌ Échec de la conversion pour {video['path']}")

                if not converted_videos:
                    logger.error(f"❌ Aucun fichier converti disponible pour {self.name}")
                    return False

                self.videos = converted_videos

                if self.ffmpeg_process and self.ffmpeg_process.poll() is None:
                    logger.debug(f"⚡ Flux déjà en cours pour {self.name}")
                    return True

                self.active_streams += 1
                if self.active_streams == 1:
                    logger.info(f"🚀 Démarrage du stream pour {self.name}")
                    self.stop_event.clear()
                    return self._start_ffmpeg()

                return True
            except Exception as e:
                logger.error(f"🔥 Erreur dans start_stream() pour {self.name}: {e}")
                return False

    def _get_cached_path(self, video_path: str) -> str:
        """Obtient ou crée une version optimisée du fichier dans le cache"""
        try:
            original_file = Path(video_path)
            file_hash = hashlib.md5(f"{video_path}_{original_file.stat().st_mtime}".encode()).hexdigest()
            cached_path = Path(self.cache_dir) / f"{file_hash}.mp4"

            # Vérifier si le fichier cache existe déjà et est valide
            if cached_path.exists() and cached_path.stat().st_size > 0:
                logger.info(f"✅ Utilisation du cache pour {original_file.name}")
                return str(cached_path.absolute())  # Retourne le chemin absolu

            logger.info(f"🔄 Conversion de {original_file.name} en MP4/H264")

            # Conversion nécessaire
            cmd = [
                "ffmpeg", "-y",
                "-i", video_path,
                "-c:v", "libx264",
                "-preset", "superfast",
                "-crf", "23",
                "-c:a", "aac",
                "-b:a", "192k",
                "-ac", "2",
                "-ar", "48000",
                "-movflags", "+faststart",
                str(cached_path.absolute())  # Utilise le chemin absolu
            ]

            # Exécuter la conversion avec monitoring
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )

            # Attendre la fin du processus
            stdout, stderr = process.communicate()
            if process.returncode != 0:
                logger.error(f"❌ Erreur lors de la conversion: {stderr}")
                if cached_path.exists():
                    cached_path.unlink()
                raise Exception(f"Erreur de conversion: {stderr}")

            # Vérifier le fichier converti
            if not cached_path.exists() or cached_path.stat().st_size == 0:
                raise Exception("Le fichier converti est invalide")

            logger.info(f"✅ Fichier converti avec succès: {cached_path}")
            return str(cached_path.absolute())  # Retourne le chemin absolu

        except Exception as e:
            logger.error(f"❌ Erreur lors de la conversion de {video_path}: {e}")
            return None
    
    def _convert_video(self, video_path: str) -> str:
        """Convertit une vidéo en MP4 dans son dossier d'origine"""
        try:
            original_file = Path(video_path)
            output_path = original_file.parent / f"{original_file.stem}.mp4"

            # Si c'est déjà un MP4 valide, pas besoin de conversion
            if original_file.suffix.lower() == '.mp4':
                probe_cmd = [
                    "ffprobe",
                    "-v", "error",
                    "-select_streams", "v:0",
                    "-show_entries", "stream=codec_name",
                    "-of", "json",
                    str(original_file)
                ]
                try:
                    probe_result = subprocess.run(probe_cmd, capture_output=True, text=True)
                    if probe_result.returncode == 0:
                        video_info = json.loads(probe_result.stdout)
                        if video_info.get('streams', [{}])[0].get('codec_name') == 'h264':
                            logger.info(f"✅ Le fichier {original_file.name} est déjà au bon format")
                            return str(original_file.absolute())
                except Exception as e:
                    logger.warning(f"Impossible de vérifier le format de {original_file.name}: {e}")

            # Conversion nécessaire
            logger.info(f"🔄 Conversion de {original_file.name}")

            # Créer un fichier temporaire pour la conversion
            temp_output = output_path.with_suffix('.temp.mp4')

            cmd = [
                "ffmpeg", "-y",
                "-i", str(original_file),
                "-c:v", "libx264",
                "-preset", "superfast",
                "-crf", "23",
                "-c:a", "aac",
                "-b:a", "192k",
                "-ac", "2",
                "-ar", "48000",
                "-movflags", "+faststart",
                str(temp_output)
            ]

            try:
                logger.info(f"Commande de conversion: {' '.join(cmd)}")
                process = subprocess.run(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    check=True
                )
                
                # Vérifier que le fichier temporaire est valide
                if temp_output.exists() and temp_output.stat().st_size > 0:
                    # Supprimer l'ancien fichier s'il n'est pas le fichier source
                    if output_path != original_file and output_path.exists():
                        output_path.unlink()
                    
                    # Renommer le fichier temporaire
                    temp_output.rename(output_path)
                    
                    # Si le fichier source n'est pas un MP4, le supprimer
                    if original_file.suffix.lower() != '.mp4':
                        original_file.unlink()
                        logger.info(f"🗑️ Ancien fichier supprimé : {original_file.name}")
                    
                    logger.info(f"✅ Conversion réussie: {output_path.name}")
                    return str(output_path.absolute())
                else:
                    logger.error(f"❌ Fichier converti invalide pour {original_file.name}")
                    if temp_output.exists():
                        temp_output.unlink()
                    return None

            except subprocess.CalledProcessError as e:
                logger.error(f"❌ Erreur lors de la conversion de {original_file.name}: {e.stderr}")
                if temp_output.exists():
                    temp_output.unlink()
                return None

            except Exception as e:
                logger.error(f"❌ Erreur inattendue lors de la conversion: {e}")
                if temp_output.exists():
                    temp_output.unlink()
                return None

        except Exception as e:
            logger.error(f"❌ Erreur lors de la conversion: {e}")
            return None

    def _convert_all_videos(self):
        """Convertit tous les fichiers vidéo de la chaîne"""
        try:
            logger.info(f"🔄 Début de la conversion des vidéos pour {self.name}")
            converted_files = []
            
            for video in self.videos:
                converted_path = self._convert_video(video["path"])
                if converted_path:
                    converted_files.append(converted_path)
                else:
                    logger.error(f"❌ Échec de la conversion pour {video['path']}")

            return converted_files

        except Exception as e:
            logger.error(f"❌ Erreur lors de la conversion des vidéos: {e}")
            return []
    
    def _start_ffmpeg(self):
        """Méthode de démarrage FFmpeg avec timeshift global"""
        try:
            logger.info(f"🟢 Initialisation FFmpeg pour {self.name}")
            
            if not self.videos:
                logger.error(f"❌ Aucune vidéo disponible pour {self.name}")
                return False

            hls_dir = f"hls/{self.name}"
            
            # Nettoyage complet avant de démarrer
            if not self._clean_hls_directory(hls_dir):
                logger.error("Échec du nettoyage du répertoire HLS")
                return False

            # Convertir les vidéos si nécessaire
            converted_files = self._convert_all_videos()
            if not converted_files:
                logger.error(f"❌ Aucun fichier converti disponible pour {self.name}")
                return False

            # Créer un fichier de concaténation temporaire
            concat_file = Path(self.video_dir) / "_playlist.txt"
            try:
                with open(concat_file, 'w', encoding='utf-8') as f:
                    for file_path in converted_files:
                        f.write(f"file '{file_path}'\n")
            except Exception as e:
                logger.error(f"Erreur lors de la création du fichier de concaténation: {e}")
                return False

            # Configuration FFmpeg optimisée pour timeshift
            # Construction de la playlist de concaténation différente
            concat_content = ""
            for file_path in converted_files:
                # Répéter chaque fichier plusieurs fois pour créer une boucle plus robuste
                for _ in range(3):  # Répéter chaque fichier 3 fois
                    concat_content += f"file '{file_path}'\n"
            
            with open(concat_file, 'w', encoding='utf-8') as f:
                f.write(concat_content)

            cmd = [
                "ffmpeg",
                "-hide_banner",
                "-loglevel", "info",  # Changé à info pour mieux diagnostiquer
                "-y",
                "-safe", "0",
                "-f", "concat",
                "-stream_loop", "-1",
                "-fflags", "+genpts+igndts",  # Ajout de flags pour une meilleure stabilité
                "-re",
                "-i", str(concat_file.absolute()),
                # Paramètres vidéo optimisés
                "-c:v", "libx264",
                "-preset", "ultrafast",  # Changé à ultrafast pour réduire la latence
                "-tune", "zerolatency",
                "-profile:v", "baseline",  # Profile plus simple pour plus de stabilité
                "-level", "3.0",
                "-b:v", "2000k",
                "-maxrate", "2500k",
                "-bufsize", "4000k",
                "-g", "60",  # Keyframe tous les 60 frames
                "-keyint_min", "60",
                "-sc_threshold", "0",  # Désactive les changements de scène
                # Paramètres audio
                "-c:a", "aac",
                "-b:a", "128k",
                "-ac", "2",
                "-ar", "44100",
                # Configuration HLS
                "-f", "hls",
                "-hls_time", "2",  # Réduit à 2 secondes pour plus de réactivité
                "-hls_list_size", "10",
                "-hls_flags", "delete_segments+append_list+program_date_time+independent_segments",
                "-hls_segment_type", "mpegts",
                "-start_number", "0",
                "-hls_allow_cache", "1",
                "-hls_segment_filename", f"{hls_dir}/segment_%d.ts",
                f"{hls_dir}/playlist.m3u8"
            ]

            logger.info(f"🖥️ Commande FFmpeg: {' '.join(cmd)}")

            try:
                # Démarrage avec pipe pour monitorer la sortie
                self.ffmpeg_process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True
                )
                
                # Vérification de démarrage
                time.sleep(2)
                if self.ffmpeg_process.poll() is not None:
                    stderr = self.ffmpeg_process.stderr.read()
                    logger.error(f"❌ FFmpeg s'est arrêté immédiatement. Erreur: {stderr}")
                    return False
                
                # Démarrer un thread pour monitorer la sortie FFmpeg
                def monitor_output():
                    error_count = 0
                    startup_grace_period = 10  # Grace period de 10 secondes au démarrage
                    last_segment_check = time.time()
                    segment_check_interval = 30  # Vérifie les segments toutes les 30 secondes
                    
                    # Attendre le démarrage initial
                    time.sleep(startup_grace_period)
                    
                    while self.ffmpeg_process and self.ffmpeg_process.poll() is None:
                        stderr_line = self.ffmpeg_process.stderr.readline()
                        if stderr_line:
                            line = stderr_line.strip()
                            if "error" in line.lower():
                                error_count += 1
                                logger.error(f"FFmpeg [{self.name}]: {line}")
                            else:
                                logger.debug(f"FFmpeg [{self.name}]: {line}")
                        
                        current_time = time.time()
                        if current_time - last_segment_check >= segment_check_interval:
                            last_segment_check = current_time
                            segments = list(Path(hls_dir).glob("segment_*.ts"))
                            if not segments and error_count > 0:  # Ne redémarre que s'il y a eu des erreurs
                                logger.error(f"Aucun segment trouvé pour {self.name} après {segment_check_interval}s, redémarrage")
                                self._clean_processes()
                                self.start_stream()
                                break

                    # Si le processus s'est arrêté sans erreur explicite
                    if self.ffmpeg_process and self.ffmpeg_process.poll() is not None:
                        logger.error(f"FFmpeg s'est arrêté pour {self.name}, redémarrage")
                        self._clean_processes()
                        self.start_stream()
                
                threading.Thread(target=monitor_output, daemon=True).start()
                
                logger.info(f"✅ FFmpeg démarré avec PID {self.ffmpeg_process.pid}")
                return True

            except Exception as e:
                logger.error(f"Erreur lors du démarrage de FFmpeg: {e}")
                return False

        except Exception as e:
            logger.error(f"🚨 Erreur grave dans _start_ffmpeg: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def _clean_hls_directory(self, hls_dir):
        """Nettoie et prépare le répertoire HLS avec configuration optimisée"""
        if not isinstance(hls_dir, (str, Path)):
            logger.error(f"Type de hls_dir invalide: {type(hls_dir)}")
            return False
        try:
            dir_path = Path(hls_dir)
            dir_path.mkdir(parents=True, exist_ok=True)
            
            # Nettoyer les anciens segments
            for pattern in ["*.ts", "*.m3u8"]:
                for file in dir_path.glob(pattern):
                    try:
                        file.unlink()
                    except Exception as e:
                        logger.error(f"Impossible de supprimer {file}: {e}")
            
            # Créer une playlist initiale optimisée
            playlist_path = dir_path / "playlist.m3u8"
            with open(playlist_path, 'w') as f:
                f.write("#EXTM3U\n")
                f.write("#EXT-X-VERSION:3\n")
                f.write("#EXT-X-TARGETDURATION:6\n")
                f.write("#EXT-X-MEDIA-SEQUENCE:0\n")
                f.write("#EXT-X-PLAYLIST-TYPE:EVENT\n")
                f.write(f"#EXT-X-PROGRAM-DATE-TIME:{datetime.datetime.utcnow().isoformat()}Z\n")
            
            return True
        except Exception as e:
            logger.error(f"Erreur lors du nettoyage du répertoire HLS: {e}")
            return False
class IPTVManager:
    def __init__(self, content_dir: str, cache_dir: str = "./cache"):
        self.content_dir = content_dir
        self.cache_dir = cache_dir
        self.channels: Dict[str, IPTVChannel] = {}
        self.last_update_time = 0
        self.last_scan_time = 0
        self.scan_delay = 10

        # Création des dossiers nécessaires
        os.makedirs(cache_dir, exist_ok=True)
        os.makedirs("./hls", exist_ok=True)

        # Configuration du Watchdog
        self.observer = Observer()
        self.observer.schedule(ChannelEventHandler(self), self.content_dir, recursive=True)

        # Scanner les chaînes immédiatement et forcer la génération de playlist
        self.scan_channels(force=True)  # Ajout du paramètre force
        self.generate_master_playlist()
        
        logger.info("📡 IPTV Manager initialisé avec Watchdog")
        
    def generate_master_playlist(self):
        """Génère la playlist principale au format M3U"""
        if time.time() - self.last_update_time < 5:  # Réduit à 5 secondes au lieu de 10
            return

        self.last_update_time = time.time()
        playlist_path = os.path.abspath("./hls/playlist.m3u")

        try:
            with open(playlist_path, "w", encoding='utf-8') as f:
                f.write("#EXTM3U\n")
                
                for name, channel in sorted(self.channels.items()):
                    logger.debug(f"Ajout de la chaîne {name} à la playlist")
                    total_duration = -1
                    f.write(f'#EXTINF:{total_duration} tvg-id="{name}" tvg-name="{name}",{name}\n')
                    f.write(f'http://{SERVER_URL}/hls/{name}/playlist.m3u8\n')

            logger.info(f"🎬 Playlist M3U mise à jour avec succès - {len(self.channels)} chaînes")
                
        except Exception as e:
            logger.error(f"Erreur lors de la génération de la playlist: {e}")
          
    def _create_empty_playlist(self):
        """Crée une playlist M3U vide initiale"""
        try:
            playlist_path = "./hls/playlist.m3u"
            with open(playlist_path, "w", encoding='utf-8') as f:
                f.write("#EXTM3U\n")
                f.write("#EXTINF-TVG-URL=\"http://localhost\"\n")
                f.write("#EXTINF-TVG-NAME=\"IPTV Local\"\n")
            logger.info("Playlist M3U initiale créée")
        except Exception as e:
            logger.error(f"Erreur lors de la création de la playlist initiale: {e}")

    def scan_channels(self, force=False):
        """Scanne et met à jour les chaînes"""
        current_time = time.time()
        if not force and current_time - self.last_scan_time < self.scan_delay:
            return

        self.last_scan_time = current_time
        changes_detected = False

        try:
            content_path = Path(self.content_dir)
            
            # Scanner tous les dossiers de chaînes
            for channel_dir in content_path.iterdir():
                if channel_dir.is_dir():
                    channel_name = channel_dir.name
                    
                    # Vérifier si la chaîne existe déjà
                    if channel_name in self.channels:
                        channel = self.channels[channel_name]
                        # Forcer le scan des vidéos
                        if channel.scan_videos():
                            changes_detected = True
                            # Redémarrer le stream si nécessaire
                            channel._clean_processes()
                            channel.start_stream()
                    else:
                        # Nouvelle chaîne
                        logger.info(f"Nouvelle chaîne détectée: {channel_name}")
                        channel = IPTVChannel(channel_name, str(channel_dir), self.cache_dir)
                        if channel.scan_videos():
                            self.channels[channel_name] = channel
                            changes_detected = True
                            channel.start_stream()

            # Mettre à jour la playlist principale si des changements sont détectés
            if changes_detected or force:
                self.generate_master_playlist()
                
        except Exception as e:
            logger.error(f"Erreur lors du scan des chaînes: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def run(self):
        """Démarre le gestionnaire IPTV avec Watchdog"""
        try:
            # Forcer un scan et une génération de playlist au démarrage
            self.scan_channels()
            self.generate_master_playlist()
            
            self.observer.start()

            while True:
                time.sleep(1)

        except KeyboardInterrupt:
            self.cleanup()

        except Exception as e:
            logger.error(f"🔥 Erreur dans le gestionnaire IPTV: {e}")
            self.cleanup()
            
    def cleanup(self):
        """Nettoyage propre à l'arrêt"""
        logger.info("Arrêt du gestionnaire...")
        self.observer.stop()
        self.observer.join()

        for channel in self.channels.values():
            channel._clean_processes()
            channel.executor.shutdown(wait=True)

if __name__ == "__main__":
    manager = IPTVManager("./content")
    manager.run()
import json
import shutil
import subprocess
from pathlib import Path
from config import logger
from queue import Queue
import threading

class VideoProcessor:
    def __init__(self, channel_dir: str):
        self.channel_dir = Path(channel_dir)
        self.processed_dir = self.channel_dir / "processed"
        self.processed_dir.mkdir(exist_ok=True)
        self.processing_queue = Queue()
        self.processed_files = []
        self.processing_thread = None
        self.processing_lock = threading.Lock()

    def process_videos_async(self, videos: list = None):
        """
        Lance le traitement asynchrone des vidéos.
        Si videos est None, scanne le dossier pour trouver les vidéos à traiter.
        """
        if self.processing_thread and self.processing_thread.is_alive():
            logger.warning("🏃 Traitement déjà en cours")
            return False

        if videos is None:
            videos = [
                f for f in self.channel_dir.glob("*.mp4")
                if not f.name.startswith("temp_")
                and f.parent == self.channel_dir
            ]

        if not videos:
            logger.warning("⚠️ Aucune vidéo à traiter")
            return False

        # On vide la queue et la liste des fichiers traités
        while not self.processing_queue.empty():
            self.processing_queue.get()
        self.processed_files.clear()

        # On remplit la queue
        for video in videos:
            self.processing_queue.put(video)

        # On lance le thread de traitement
        self.processing_thread = threading.Thread(
            target=self._process_queue,
            daemon=True
        )
        self.processing_thread.start()
        return True

    def _process_queue(self):
        """Traite les vidéos dans la queue"""
        while not self.processing_queue.empty():
            video = self.processing_queue.get()
            try:
                processed = self.process_video(video)
                if processed:
                    with self.processing_lock:
                        self.processed_files.append(processed)
            except Exception as e:
                logger.error(f"❌ Erreur traitement {video.name}: {e}")
            finally:
                self.processing_queue.task_done()

    def wait_for_completion(self, timeout: int = 300) -> list:
        """
        Attend la fin du traitement des vidéos.
        Retourne la liste des fichiers traités.
        
        Args:
            timeout (int): Timeout en secondes
            
        Returns:
            list: Liste des fichiers traités
        """
        if not self.processing_thread:
            return []

        start_time = time.time()
        while self.processing_thread.is_alive():
            if time.time() - start_time > timeout:
                logger.error("⏰ Timeout pendant le traitement des vidéos")
                return []
            time.sleep(1)

        with self.processing_lock:
            processed = self.processed_files.copy()
        return processed

    def is_already_optimized(self, video_path: Path) -> bool:
        """
        Vérifie si une vidéo est déjà en H.264, ≤ 1080p, ≤ 30 FPS, et en AAC.
        Ajoute un log détaillé expliquant la raison si la normalisation est nécessaire.
        """
        logger.info(f"🔍 Vérification du format de {video_path.name}")

        cmd = [
            "ffprobe", "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "stream=codec_name,width,height,r_frame_rate",
            "-show_entries", "stream=codec_name:stream=sample_rate",
            "-of", "json",
            str(video_path)
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)

        try:
            video_info = json.loads(result.stdout)
            streams = video_info.get("streams", [])

            if not streams:
                logger.warning(f"⚠️ Impossible de lire {video_path}, normalisation forcée.")
                return False

            video_stream = streams[0]
            codec = video_stream.get("codec_name", "").lower()
            width = int(video_stream.get("width", 0))
            height = int(video_stream.get("height", 0))
            framerate = video_stream.get("r_frame_rate", "0/1").split("/")
            fps = round(int(framerate[0]) / int(framerate[1])) if len(framerate) == 2 else 0

            audio_codec = None
            for stream in streams:
                if stream.get("codec_name") and stream.get("codec_name") != codec:
                    audio_codec = stream.get("codec_name").lower()

            logger.info(f"🎥 Codec: {codec}, Résolution: {width}x{height}, FPS: {fps}, Audio: {audio_codec}")

            # Vérification des critères
            needs_transcoding = False
            if codec != "h264":
                logger.info(f"🚨 Codec vidéo non H.264 ({codec}), conversion nécessaire")
                needs_transcoding = True
            if width > 1920 or height > 1080:
                logger.info(f"🚨 Résolution supérieure à 1080p ({width}x{height}), réduction nécessaire")
                needs_transcoding = True
            if fps > 30:
                logger.info(f"🚨 FPS supérieur à 30 ({fps}), réduction nécessaire")
                needs_transcoding = True
            if audio_codec and audio_codec != "aac":
                logger.info(f"🚨 Codec audio non AAC ({audio_codec}), conversion nécessaire")
                needs_transcoding = True

            if needs_transcoding:
                logger.info(f"⚠️ Normalisation nécessaire pour {video_path.name}")
            else:
                logger.info(f"✅ Vidéo déjà optimisée, pas besoin de normalisation pour {video_path.name}")

            return not needs_transcoding

        except json.JSONDecodeError as e:
            logger.error(f"❌ Erreur JSON avec ffprobe: {e}")
            return False

    def process_video(self, video_path: Path) -> Path:
        """
        Normalise une vidéo si nécessaire. Sinon, la copie directement.
        """
        output_path = self.processed_dir / f"{video_path.stem}.mp4"

        # Vérifier si la vidéo est déjà optimisée
        if self.is_already_optimized(video_path):
            logger.info(f"✅ Vidéo déjà optimisée : {video_path.name}, copie directe.")
            shutil.copy2(video_path, output_path)
            return output_path

        # Sinon, normalisation
        logger.info(f"⚙️ Normalisation en cours pour {video_path.name}")

        temp_output = self.processed_dir / f"temp_{video_path.stem}.mp4"
        cmd = ["ffmpeg", "-y", "-i", str(video_path)]

        # Encodage vidéo
        cmd.extend(["-c:v", "libx264", "-preset", "fast", "-crf", "23"])
        
        # Resize si nécessaire
        if self.is_large_resolution(video_path):
            cmd.extend(["-vf", "scale=1920:1080"])

        # Encodage audio
        cmd.extend(["-c:a", "aac", "-b:a", "192k", "-ar", "48000"])

        cmd.append(str(temp_output))

        logger.info(f"📜 Commande FFmpeg: {' '.join(cmd)}")

        process = subprocess.run(cmd, capture_output=True, text=True)
        if process.returncode == 0 and temp_output.exists():
            temp_output.rename(output_path)
            logger.info(f"✅ Normalisation réussie: {video_path.name} -> {output_path.name}")
            return output_path
        else:
            logger.error(f"❌ Erreur FFmpeg: {process.stderr}")
            return None

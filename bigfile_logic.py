import os
from pathlib import Path
import subprocess
import logging
import shutil
import time

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SimpleIPTVChannel:
    def __init__(self, name: str, video_dir: str):
        self.name = name
        self.video_dir = Path(video_dir)
        self.process = None
        
    def prepare_videos(self):
        """Concatène les vidéos en un seul fichier MP4"""
        try:
            videos = sorted([str(f) for f in self.video_dir.glob('*.mp4')])
            if not videos:
                logger.error(f"Aucune vidéo MP4 trouvée dans {self.video_dir}")
                return None
                
            # Création du fichier de concat
            concat_file = self.video_dir / "concat.txt"
            with open(concat_file, "w", encoding="utf-8") as f:
                for video in videos:
                    f.write(f"file '{video}'\n")
                    
            # Création du bigfile
            bigfile = self.video_dir / "bigfile.mp4"
            if bigfile.exists():
                bigfile.unlink()
                
            cmd = [
                "ffmpeg", "-y",
                "-f", "concat",
                "-safe", "0",
                "-i", str(concat_file),
                "-c", "copy",
                str(bigfile)
            ]
            
            subprocess.run(cmd, check=True)
            logger.info(f"Bigfile créé avec succès: {bigfile}")
            return bigfile
            
        except Exception as e:
            logger.error(f"Erreur préparation vidéos: {e}")
            return None
            
    def start_stream(self):
        """Démarre le stream HLS en boucle"""
        try:
            # Préparation du bigfile
            bigfile = self.prepare_videos()
            if not bigfile:
                return False
                
            # Création du dossier HLS
            hls_dir = Path("hls") / self.name
            if hls_dir.exists():
                shutil.rmtree(hls_dir)
            hls_dir.mkdir(parents=True)
            
            # Commande FFmpeg modifiée pour une vraie boucle infinie
            cmd = [
                "ffmpeg",
                "-re",  # Lecture en temps réel
                "-stream_loop", "-1",  # Boucle infinie
                "-i", str(bigfile),
                "-c:v", "copy",  # On garde le codec video tel quel
                "-c:a", "copy",  # On garde le codec audio tel quel
                "-f", "hls",
                "-hls_time", "4",  # Segments de 4 secondes
                "-hls_list_size", "3",  # Garde 3 segments
                "-hls_flags", "delete_segments+omit_endlist",  # Crucial pour la boucle infinie
                "-hls_segment_filename", f"{hls_dir}/segment_%03d.ts",
                f"{hls_dir}/playlist.m3u8"
            ]
            
            # On kill l'ancien process si besoin
            if self.process:
                try:
                    self.process.terminate()
                    self.process.wait(timeout=5)
                except:
                    pass
                    
            # Démarrage du nouveau process
            self.process = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            
            logger.info(f"Stream démarré pour {self.name}")
            return True
            
        except Exception as e:
            logger.error(f"Erreur démarrage stream: {e}")
            return False
            
    def stop_stream(self):
        """Arrête le stream"""
        if self.process:
            try:
                self.process.terminate()
                self.process.wait(timeout=5)
            except:
                pass
            self.process = None

class SimpleIPTVManager:
    def __init__(self, content_dir: str):
        self.content_dir = Path(content_dir)
        self.channels = {}
        Path("hls").mkdir(exist_ok=True)
        
    def scan_channels(self):
        """Scanne les dossiers et démarre les chaînes"""
        for channel_dir in self.content_dir.glob("*"):
            if channel_dir.is_dir():
                name = channel_dir.name
                if name not in self.channels:
                    logger.info(f"Nouvelle chaîne détectée: {name}")
                    channel = SimpleIPTVChannel(name, str(channel_dir))
                    self.channels[name] = channel
                    channel.start_stream()
                    
    def run(self):
        """Boucle principale"""
        try:
            while True:
                self.scan_channels()
                # On rescanne toutes les 5 minutes
                time.sleep(300)
        except KeyboardInterrupt:
            self.cleanup()
            
    def cleanup(self):
        """Arrête tous les streams"""
        for channel in self.channels.values():
            channel.stop_stream()

if __name__ == "__main__":
    manager = SimpleIPTVManager("./content")
    manager.run()
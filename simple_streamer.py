import os
import sys
import time
import signal
import logging
import subprocess
from pathlib import Path

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SimpleStreamer:
    def __init__(self, channel_name):
        self.channel_name = channel_name
        self.process = None
        self.is_running = True
        self.hls_dir = Path(f"/app/hls/{channel_name}")
        self.video_dir = Path(f"/mnt/frigate_data/streaming_awesomeness/{channel_name}")
        
        # Création du dossier HLS si nécessaire
        self.hls_dir.mkdir(parents=True, exist_ok=True)
        
        # Gestion du signal d'arrêt
        signal.signal(signal.SIGTERM, self.handle_signal)
        signal.signal(signal.SIGINT, self.handle_signal)

    def handle_signal(self, signum, frame):
        """Gère l'arrêt propre du streamer"""
        logger.info(f"[{self.channel_name}] Signal reçu ({signum}), arrêt en cours...")
        self.is_running = False
        if self.process:
            self.process.terminate()

    def clean_hls_dir(self):
        """Nettoie le dossier HLS avant de commencer"""
        try:
            # Supprime les anciens segments
            for file in self.hls_dir.glob("*.ts"):
                file.unlink()
            # Supprime l'ancienne playlist
            playlist = self.hls_dir / "playlist.m3u8"
            if playlist.exists():
                playlist.unlink()
        except Exception as e:
            logger.error(f"[{self.channel_name}] Erreur nettoyage HLS: {e}")

    def read_playlist(self):
        """Lit la playlist.txt et retourne la liste des fichiers"""
        playlist_file = self.video_dir / "_playlist.txt"
        if not playlist_file.exists():
            raise FileNotFoundError(f"Playlist non trouvée: {playlist_file}")

        files = []
        with open(playlist_file, "r") as f:
            for line in f:
                if line.strip().startswith("file "):
                    # Extrait le chemin entre guillemets simples
                    file_path = line.strip().split("'")[1]
                    if os.path.exists(file_path):
                        files.append(file_path)
                    else:
                        logger.warning(f"[{self.channel_name}] Fichier non trouvé: {file_path}")

        return files

    def build_ffmpeg_command(self, input_file):
        """Construit la commande ffmpeg pour un fichier"""
        return [
            "ffmpeg",
            "-hide_banner",
            "-loglevel", "info",
            "-y",
            # Paramètres d'entrée
            "-thread_queue_size", "8192",
            "-analyzeduration", "20M",
            "-probesize", "20M",
            "-re",  # Lecture en temps réel
            "-i", input_file,
            # Paramètres de copie
            "-c:v", "copy",
            "-c:a", "copy",
            "-sn", "-dn",
            "-map", "0:v:0",
            "-map", "0:a:0?",
            "-max_muxing_queue_size", "4096",
            # Paramètres HLS
            "-f", "hls",
            "-hls_time", "2",
            "-hls_list_size", "15",
            "-hls_delete_threshold", "2",
            "-hls_flags", "delete_segments+append_list+program_date_time+independent_segments",
            "-hls_allow_cache", "1",
            "-start_number", "0",
            "-hls_segment_type", "mpegts",
            "-max_delay", "2000000",
            "-hls_init_time", "2",
            "-hls_segment_filename", f"{self.hls_dir}/segment_%d.ts",
            f"{self.hls_dir}/playlist.m3u8"
        ]

    def stream_file(self, file_path):
        """Stream un fichier unique avec ffmpeg"""
        try:
            command = self.build_ffmpeg_command(file_path)
            logger.info(f"[{self.channel_name}] Démarrage streaming: {os.path.basename(file_path)}")
            
            self.process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )

            # Attendre la fin du processus tout en surveillant sa sortie
            while self.process.poll() is None and self.is_running:
                # Lire la sortie d'erreur pour les logs ffmpeg
                stderr_line = self.process.stderr.readline()
                if stderr_line:
                    if "error" in stderr_line.lower():
                        logger.error(f"[{self.channel_name}] FFmpeg: {stderr_line.strip()}")
                    elif "warning" in stderr_line.lower():
                        logger.warning(f"[{self.channel_name}] FFmpeg: {stderr_line.strip()}")
                
                time.sleep(0.1)  # Évite de surcharger le CPU

            # Vérifier le code de retour
            if self.process.returncode != 0 and self.is_running:
                logger.error(f"[{self.channel_name}] Erreur FFmpeg ({self.process.returncode})")
                return False

            return True

        except Exception as e:
            logger.error(f"[{self.channel_name}] Erreur streaming {file_path}: {e}")
            return False

    def run(self):
        """Démarre le streaming en boucle"""
        logger.info(f"[{self.channel_name}] Démarrage du streamer")
        
        while self.is_running:
            try:
                # Nettoyer le dossier HLS
                self.clean_hls_dir()
                
                # Lire la playlist
                files = self.read_playlist()
                if not files:
                    logger.error(f"[{self.channel_name}] Aucun fichier à streamer")
                    time.sleep(5)
                    continue

                # Streamer chaque fichier séquentiellement
                for file_path in files:
                    if not self.is_running:
                        break
                        
                    success = self.stream_file(file_path)
                    if not success and self.is_running:
                        logger.warning(f"[{self.channel_name}] Échec streaming {file_path}, passage au suivant")
                        time.sleep(1)  # Petit délai avant de passer au suivant
                
                # Si on arrive ici, on a fini la playlist, on recommence
                logger.info(f"[{self.channel_name}] Fin de playlist, redémarrage")
                
            except Exception as e:
                logger.error(f"[{self.channel_name}] Erreur générale: {e}")
                time.sleep(5)  # Délai avant de réessayer

        logger.info(f"[{self.channel_name}] Arrêt du streamer")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python simple_streamer.py <channel_name>")
        sys.exit(1)
        
    channel_name = sys.argv[1]
    streamer = SimpleStreamer(channel_name)
    streamer.run() 
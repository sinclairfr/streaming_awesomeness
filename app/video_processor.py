# video_processor.py
import time
import subprocess
import threading
from pathlib import Path
from typing import Optional
from tqdm import tqdm  # On affiche la barre de progression CLI
from config import NORMALIZATION_PARAMS, logger
import json

class VideoProcessor:
    """
    # On définit un processeur vidéo qui gère la normalisation et l'encodage des vidéos
    """

    def __init__(self, channel_dir: str, use_gpu: bool = False):
        self.channel_dir = Path(channel_dir)
        self.use_gpu = use_gpu
        self.video_extensions = (".mp4", ".avi", ".mkv", ".mov")
        self.processed_dir = self.channel_dir / "processed"
        self.processed_dir.mkdir(exist_ok=True)

        self.processing_thread = None
        self.processing_complete = threading.Event()
        self.processed_videos = []
        self.current_processing = None
        self.processing_errors = {}

    def process_videos(self):
        """# On traite toutes les vidéos sources"""
        try:
            # On récupère la liste des fichiers source
            source_files = []
            for ext in self.video_extensions:
                source_files.extend(self.channel_dir.glob(f"*{ext}"))
            source_files = sorted(source_files)

            if not source_files:
                logger.info(
                    f"Aucun fichier vidéo source trouvé dans {self.channel_dir}"
                )
                return []

            processed_files = []
            self.processing_errors.clear()

            # On normalise chaque fichier vidéo
            for source_file in source_files:
                try:
                    self.current_processing = source_file
                    processed = self._normalize_video(
                        source_file,
                        NORMALIZATION_PARAMS
                    )
                    if processed:
                        processed_files.append(processed)
                        logger.info(f"✅ {source_file.name} traité avec succès")
                    elif self._is_already_normalized(
                        source_file,
                        NORMALIZATION_PARAMS
                    ):
                        already_processed = (
                            self.processed_dir / f"{source_file.stem}.mp4"
                        )
                        processed_files.append(already_processed)
                        logger.info(f"👍 {source_file.name} déjà traité")
                    else:
                        self.processing_errors[source_file.name] = (
                            "Échec de normalisation"
                        )
                        logger.error(f"❌ Échec du traitement de {source_file.name}")
                except Exception as e:
                    self.processing_errors[source_file.name] = str(e)
                    logger.error(
                        f"❌ Erreur lors du traitement de {source_file.name}: {e}"
                    )
                finally:
                    self.current_processing = None

            self.processed_videos = processed_files

            if processed_files:
                logger.info(
                    f"✨ {len(processed_files)}/{len(source_files)} fichiers traités avec succès"
                )
            if self.processing_errors:
                logger.warning(
                    f"⚠️ {len(self.processing_errors)} erreurs de traitement"
                )

            return processed_files

        except Exception as e:
            logger.error(f"Erreur traitement vidéos: {e}")
            return []
        finally:
            self.processing_complete.set()

    def process_videos_async(self) -> None:
        """# On lance le traitement asynchrone des vidéos"""
        self.processing_complete.clear()
        self.processed_videos = []
        self.processing_errors.clear()

        self.processing_thread = threading.Thread(
            target=self.process_videos
        )
        self.processing_thread.start()

    def wait_for_completion(self) -> list:
        """# On attend la fin du traitement et on renvoie la liste des vidéos traitées"""
        if self.processing_thread:
            self.processing_thread.join()
        return self.processed_videos

    def get_processing_status(self) -> dict:
        """# On donne l'état du traitement en cours"""
        return {
            "current_file": (
                str(self.current_processing) if self.current_processing else None
            ),
            "completed": self.processing_complete.is_set(),
            "processed_count": len(self.processed_videos),
            "errors": self.processing_errors,
        }

    def _normalize_video(self, video_path: Path, ref_info: dict) -> Optional[Path]:
        """
        # On normalise la vidéo avec FFmpeg
        # On gère spécialement le cas des .mkv
        """
        max_attempts = 3
        attempt = 0
        duration_ms = self._get_duration(video_path)

        while attempt < max_attempts:
            temp_output = self.processed_dir / f"temp_{video_path.stem}.mp4"
            output_path = self.processed_dir / f"{video_path.stem}.mp4"

            if self._is_already_normalized(video_path, ref_info):
                return output_path

            cmd = ["ffmpeg", "-y"]

            # On gère les paramètres pour MKV
            if video_path.suffix.lower() == ".mkv":
                cmd.extend([
                    "-fflags", "+genpts+igndts",
                    "-analyzeduration", "100M",
                    "-probesize", "100M"
                ])

            cmd.extend(["-i", str(video_path)])

            # On définit la partie encodage vidéo
            if self.use_gpu:
                cmd.extend([
                    "-c:v", "h264_nvenc",
                    "-preset", "p4",
                    "-profile:v", "high",
                    "-rc", "vbr",
                    "-cq", "23"
                ])
            else:
                cmd.extend(["-c:v", "libx264", "-preset", "fast", "-crf", "22"])

            # On complète les paramètres communs
            cmd.extend([
                "-pix_fmt", "yuv420p",
                "-movflags", "+faststart",
                "-max_muxing_queue_size", "1024",
                "-y"
            ])

            # On gère l'audio
            cmd.extend([
                "-c:a", "aac",
                "-b:a", "192k",
                "-ar", "48000",
                "-ac", "2"
            ])

            cmd.append(str(temp_output))

            logger.info(
                f"On lance FFmpeg pour normaliser {video_path.name} (tentative {attempt+1}/{max_attempts})"
            )

            try:
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True
                )

                pbar = tqdm(
                    total=duration_ms,
                    unit="ms",
                    desc=video_path.name,
                    leave=False
                )

                error_output = []
                while True:
                    line = process.stderr.readline()
                    if not line and process.poll() is not None:
                        break
                    if line:
                        error_output.append(line.strip())
                        if "time=" in line:
                            try:
                                time_str = line.split("time=")[1].split()[0]
                                h, m, s = time_str.split(":")
                                ms = int(
                                    (float(h)*3600 + float(m)*60 + float(s))*1000
                                )
                                pbar.update(max(0, ms - pbar.n))
                            except:
                                pass

                pbar.close()
                process.wait()

                if process.returncode == 0 and temp_output.exists():
                    # On vérifie la validité du fichier de sortie
                    if self._verify_output_file(temp_output):
                        temp_output.rename(output_path)
                        logger.info(
                            f"Normalisation réussie pour {video_path.name}"
                        )
                        return output_path
                    else:
                        logger.error(
                            f"Fichier de sortie invalide pour {video_path.name}"
                        )
                else:
                    logger.error(
                        f"Échec de la normalisation de {video_path.name} (tentative {attempt+1})"
                    )
                    logger.error("\n".join(error_output[-5:]))

                if temp_output.exists():
                    temp_output.unlink()

            except Exception as e:
                logger.error(
                    f"Erreur pendant FFmpeg pour {video_path.name} (tentative {attempt+1}): {e}"
                )
                if temp_output.exists():
                    temp_output.unlink()

            attempt += 1
            time.sleep(2)

        return None

    def _verify_output_file(self, file_path: Path) -> bool:
        """# On vérifie que le fichier de sortie est valide"""
        try:
            cmd = [
                "ffprobe", "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=codec_name",
                "-of", "json",
                str(file_path)
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                return False

            # On vérifie la taille minimum
            if file_path.stat().st_size < 1024:
                return False

            return True

        except Exception as e:
            logger.error(f"Erreur lors de la vérification du fichier {file_path}: {e}")
            return False

    def _get_duration(self, video_path: Path) -> float:
        """# On renvoie la durée de la vidéo en millisecondes"""
        try:
            cmd = [
                "ffprobe",
                "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                str(video_path)
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            duration_sec = float(result.stdout.strip())
            return duration_sec * 1000
        except Exception as e:
            logger.error(
                f"Erreur lors de la récupération de la durée de {video_path}: {e}"
            )
            return 0

    def _is_already_normalized(self, video_path: Path, ref_info: dict) -> bool:
        """# On vérifie si la vidéo est déjà normalisée"""
        output_path = self.processed_dir / f"{video_path.stem}.mp4"
        if output_path.exists():
            logger.info(
                f"Le fichier {video_path.name} a déjà été normalisé dans {output_path}"
            )
            return True
        return False

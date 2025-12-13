import json
import shutil
import subprocess
from pathlib import Path
import threading
import os
import logging
import re
from queue import Queue, Empty
import time
from typing import List, Dict, Optional, Set
from config import logger


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("video_processor")


def verify_file_ready(file_path: Path) -> bool:
    """
    Vérifie si un fichier vidéo est prêt et valide pour le traitement.
    """
    try:
        if not file_path.exists():
            logger.warning(f"⚠️ Fichier introuvable: {file_path}")
            return False

        file_size = file_path.stat().st_size
        if file_size == 0:
            logger.warning(f"⚠️ Fichier vide: {file_path}")
            return False

        cmd = [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(file_path),
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)

        if result.returncode != 0:
            logger.warning(f"⚠️ Erreur ffprobe pour {file_path}: {result.stderr}")
            return False

        duration = float(result.stdout.strip())
        if duration <= 0:
            logger.warning(f"⚠️ Durée invalide pour {file_path}: {duration}s")
            return False

        return True
    except Exception as e:
        logger.error(f"❌ Erreur vérification fichier {file_path}: {e}")
        return False

def get_accurate_duration(file_path: Path) -> float:
    """
    Obtient la durée précise d'un fichier vidéo.
    """
    try:
        cmd = [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(file_path),
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)

        if result.returncode == 0:
            return float(result.stdout.strip())
        else:
            logger.warning(f"⚠️ Erreur ffprobe durée {file_path}: {result.stderr}")
            return 0.0
    except Exception as e:
        logger.error(f"❌ Erreur récupération durée {file_path}: {e}")
        return 0.0


class VideoProcessor:
    """Processeur de vidéos qui gère la normalisation et l'optimisation des fichiers"""

    def __init__(self, channel_dir: str):
        self.channel_dir = Path(channel_dir)
        self.channel_name = self.channel_dir.name

        # Création des dossiers nécessaires
        self.ready_to_stream_dir = self.channel_dir / "ready_to_stream"
        self.ready_to_stream_dir.mkdir(exist_ok=True)

        self.processing_dir = self.channel_dir / "processing"
        self.processing_dir.mkdir(exist_ok=True)

        self.ignored_dir = self.channel_dir / "ignored"
        self.ignored_dir.mkdir(exist_ok=True)

        self.already_processed_dir = self.channel_dir / "already_processed"
        self.already_processed_dir.mkdir(exist_ok=True)

        # Nettoyage initial
        self._clean_processing_dir()

        # Queue et threading pour le traitement asynchrone
        self.processing_queue = Queue()
        self.currently_processing = set()
        self.processing_lock = threading.Lock()

        # Configuration GPU
        self.USE_GPU_FOR_ENCODING = os.getenv("USE_GPU_FOR_ENCODING", "").lower() == "vaapi"
        if self.USE_GPU_FOR_ENCODING:
            self.check_gpu_support()

        # Démarrage du thread de traitement
        self.stop_processing = threading.Event()
        self.processing_thread = threading.Thread(target=self._process_queue, daemon=True)
        self.processing_thread.start()

    def add_file_to_process(self, file_path: Path) -> bool:
        """Ajoute un fichier à la queue de traitement"""
        try:
            if not file_path.exists():
                logger.error(f"[{self.channel_name}] ❌ Fichier introuvable: {file_path}")
                return False

            # Vérifier si le fichier est déjà en traitement
            with self.processing_lock:
                if file_path in self.currently_processing:
                    logger.debug(f"[{self.channel_name}] ⏭️ Fichier déjà en traitement: {file_path.name}")
                    return False

            # Ajouter à la queue
            self.processing_queue.put(file_path)
            logger.info(f"[{self.channel_name}] ✅ Fichier ajouté à la queue: {file_path.name}")
            return True

        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur ajout fichier: {e}")
            return False

    def _process_queue(self):
        """Thread qui traite les fichiers dans la queue"""
        while not self.stop_processing.is_set():
            try:
                # Attendre un fichier à traiter
                try:
                    file_path = self.processing_queue.get(timeout=5)
                except Empty:  # Use Empty directly since it's imported
                    continue

                # Vérifier si le fichier est déjà en traitement
                with self.processing_lock:
                    if file_path in self.currently_processing:
                        self.processing_queue.task_done()
                        continue
                    self.currently_processing.add(file_path)

                try:
                    # Traiter le fichier
                    if file_path.exists():
                        self._process_video(file_path)
                    else:
                        logger.warning(f"[{self.channel_name}] ⚠️ Fichier disparu: {file_path}")

                finally:
                    # Marquer comme terminé
                    with self.processing_lock:
                        self.currently_processing.discard(file_path)
                    self.processing_queue.task_done()

            except Exception as e:
                logger.error(f"[{self.channel_name}] ❌ Erreur traitement queue: {e}")
                time.sleep(5)

    def stop(self):
        """Arrête proprement le processeur"""
        self.stop_processing.set()
        if self.processing_thread and self.processing_thread.is_alive():
            self.processing_thread.join(timeout=5)
            logger.info("Thread de traitement vidéo arrêté")

    def _clean_processing_dir(self):
        """Vide le dossier processing au démarrage"""
        try:
            if self.processing_dir.exists():
                logger.info(f"[{self.channel_name}] 🧹 Nettoyage du dossier processing")
                for file in self.processing_dir.glob("*.*"):
                    try:
                        file.unlink()
                        logger.info(
                            f"[{self.channel_name}] 🗑️ Fichier temporaire supprimé: {file.name}"
                        )
                    except Exception as e:
                        logger.error(
                            f"[{self.channel_name}] ❌ Erreur suppression {file.name}: {e}"
                        )
        except Exception as e:
            logger.error(
                f"[{self.channel_name}] ❌ Erreur nettoyage dossier processing: {e}"
            )

    def _migrate_from_processed(self):
        """Migration des fichiers de l'ancien dossier 'processed' vers la nouvelle structure"""
        old_processed = self.channel_dir / "processed"
        if not old_processed.exists():
            return

        logger.info(
            f"Migration des fichiers de 'processed' vers la nouvelle structure pour {self.channel_dir.name}"
        )

        # On déplace tous les fichiers .mp4 vers ready_to_stream
        for video in old_processed.glob("*.mp4"):
            dest = self.ready_to_stream_dir / video.name
            if not dest.exists():
                try:
                    shutil.move(str(video), str(dest))
                    logger.info(f"Fichier migré vers ready_to_stream: {video.name}")
                except Exception as e:
                    logger.error(f"Erreur migration {video.name}: {e}")

    def check_gpu_support(self):
        """Vérifie si le support GPU est disponible"""
        try:
            result = subprocess.run(["vainfo"], capture_output=True, text=True)
            if result.returncode == 0 and "VAEntrypointVLD" in result.stdout:
                logger.debug("✅ Support VAAPI détecté et activé")
                return True
            else:
                logger.warning(
                    "⚠️ VAAPI configuré mais non fonctionnel, retour au mode CPU"
                )
                self.USE_GPU_FOR_ENCODING = False
                return False
        except Exception as e:
            logger.error(f"❌ Erreur vérification VAAPI: {str(e)}")
            self.USE_GPU_FOR_ENCODING = False
            return False

    def _wait_for_file_stability(self, file_path: Path, timeout=60) -> bool:
        """
        Attend que le fichier soit stable (taille constante) avant de le traiter
        Version améliorée avec vérification des fichiers MP4

        Args:
            file_path: Chemin du fichier à vérifier
            timeout: Délai maximum d'attente en secondes

        Returns:
            bool: True si le fichier est stable, False sinon
        """
        # Vérification EXPLICITE pour éviter de monitorer les fichiers dans processing
        if "processing" in str(file_path):
            # On considère les fichiers dans processing comme déjà stables
            logger.info(
                f"Fichier dans processing, skip de la vérification de stabilité: {file_path.name}"
            )
            return True

        if not file_path.exists():
            logger.error(f"Fichier introuvable: {file_path}")
            return False

        start_time = time.time()
        last_size = -1
        stable_count = 0
        logged_stability = False  # Flag pour éviter les logs répétitifs

        logger.info(f"Vérification de la stabilité du fichier: {file_path.name}")

        while time.time() - start_time < timeout:
            try:
                current_size = file_path.stat().st_size

                # Si c'est un petit fichier, on fait une vérification moins stricte
                min_stable_seconds = 3
                if current_size > 100 * 1024 * 1024:  # > 100 MB
                    min_stable_seconds = 5

                # Log l'évolution de la taille
                if last_size != -1:
                    if current_size == last_size:
                        stable_count += 1
                        
                        # Log une seule fois au début de la stabilité
                        if not logged_stability and stable_count == 3:
                            logged_stability = True
                            logger.info(
                                f"Fichier stable: {file_path.name} ({current_size/1024/1024:.1f} MB)"
                            )
                    else:
                        # Réinitialiser le statut de log si la taille change
                        if logged_stability:
                            logged_stability = False
                            
                        stable_count = 0
                        # Log moins fréquent pour les changements de taille
                        if last_size > 0 and abs(current_size - last_size) > 10240:  # 10KB de changement minimum
                            logger.info(
                                f"{file_path.name} Taille en évolution: {current_size/1024/1024:.1f} MB (était {last_size/1024/1024:.1f} MB)"
                            )

                # Si la taille est stable pendant le temps requis
                if current_size == last_size and stable_count >= min_stable_seconds:
                    # Vérification supplémentaire pour les MP4
                    if file_path.suffix.lower() == ".mp4" and stable_count >= 5:
                        if not self._verify_mp4_completeness(file_path):
                            # Continuer à attendre pour les MP4 incomplets
                            if (
                                stable_count < 20
                            ):  # On attend jusqu'à 20 secondes de stabilité
                                last_size = current_size
                                time.sleep(1)
                                continue
                            else:
                                logger.warning(
                                    f"❌ MP4 incomplet après {stable_count}s de stabilité: {file_path.name}"
                                )
                                return False

                    logger.info(
                        f"✅ Fichier {file_path.name} stable depuis {stable_count}s, prêt pour traitement"
                    )
                    return True

                last_size = current_size
                time.sleep(1)

            except Exception as e:
                logger.error(f"Erreur vérification stabilité {file_path.name}: {e}")
                time.sleep(1)

        logger.warning(f"⏰ Timeout en attendant la stabilité de {file_path.name}")
        return False

    def _verify_mp4_completeness(self, file_path: Path) -> bool:
        """
        Vérifie qu'un fichier MP4 est complet en recherchant l'atome MOOV

        Args:
            file_path: Chemin du fichier MP4

        Returns:
            bool: True si le fichier est complet, False sinon
        """
        try:
            # Vérification avec ffprobe
            cmd = [
                "ffprobe",
                "-v",
                "error",
                "-select_streams",
                "v:0",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                str(file_path),
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)

            # Si retourne une durée valide, le fichier est complet
            if result.returncode == 0:
                try:
                    duration = float(result.stdout.strip())
                    if duration > 0:
                        return True
                except ValueError:
                    pass

            # Vérification spécifique de l'erreur "moov atom not found"
            if "moov atom not found" in result.stderr:
                logger.warning(
                    f"⚠️ Atome MOOV manquant dans {file_path.name}, MP4 incomplet"
                )
                return False

            # Autres erreurs
            logger.warning(
                f"⚠️ Erreur vérification MP4 {file_path.name}: {result.stderr}"
            )
            return False

        except subprocess.TimeoutExpired:
            logger.warning(f"⚠️ Timeout vérification MP4 {file_path.name}")
            return False
        except Exception as e:
            logger.error(f"❌ Erreur inattendue vérification MP4 {file_path.name}: {e}")
            return False

    def notify_file_processed(self, file_path):
        """Notifie que le fichier a été traité avec succès et met à jour le statut de la chaîne"""
        try:
            # Obtient le nom de la chaîne à partir du chemin
            channel_name = Path(self.channel_dir).name

            # Trouve l'instance de IPTVManager dans la pile d'appels
            import inspect

            frame = inspect.currentframe()
            manager = None

            # Cherche un objet qui contient 'channels' et 'channel_ready_status'
            while frame:
                if "self" in frame.f_locals:
                    obj = frame.f_locals["self"]
                    if hasattr(obj, "channels") and hasattr(
                        obj, "channel_ready_status"
                    ):
                        manager = obj
                        break
                frame = frame.f_back

            if manager:
                # Met à jour le statut de préparation de la chaîne
                if channel_name in manager.channels:
                    channel = manager.channels[channel_name]

                    # Marque la chaîne comme prête
                    channel.ready_for_streaming = True

                    # Met à jour le statut dans le manager
                    with manager.scan_lock:
                        old_status = manager.channel_ready_status.get(
                            channel_name, False
                        )
                        manager.channel_ready_status[channel_name] = True

                        # Si le statut a changé, on force la mise à jour de la playlist maître
                        if not old_status:
                            logger.info(
                                f"[{channel_name}] ✅ Chaîne nouvellement prête, mise à jour de la playlist maître"
                            )
                            if hasattr(manager, "_update_master_playlist"):
                                threading.Thread(
                                    target=manager._update_master_playlist, daemon=True
                                ).start()

                            # Démarrer automatiquement le stream si nécessaire
                            if hasattr(channel, "start_stream"):
                                logger.info(f"[{channel_name}] 🚀 Tentative de démarrage automatique du stream")
                                threading.Thread(
                                    target=channel.start_stream,
                                    daemon=True
                                ).start()

                    logger.info(
                        f"[{channel_name}] ✅ Chaîne marquée comme prête après traitement de {Path(file_path).name}"
                    )

        except Exception as e:
            logger.error(f"❌ Erreur notification traitement: {e}")

    def _process_video(self, file_path: Path):
        """Traite le fichier vidéo: vérifie, normalise et optimise"""
        # Vérifier si le fichier est déjà traité pour éviter des traitements inutiles
        if self._is_already_processed_file(file_path):
            logger.info(f"[{self.channel_name}] ⏩ Fichier {file_path.name} déjà traité, skip")
            return
    
        # Attendre que le fichier soit complètement copié et stable
        if not self._wait_for_file_stability(file_path):
            logger.error(f"[{self.channel_name}] ❌ Fichier non stable: {file_path}")
            return
            
        original_filename = file_path.name  # On garde le nom d'origine pour les logs
        try:
            # 1. Première étape: Sanitize le nom du fichier source avant tout traitement
            sanitized_name = self.sanitize_filename(original_filename)
            sanitized_path = file_path.parent / sanitized_name

            # Renommage du fichier source si nécessaire
            if original_filename != sanitized_name:
                logger.info(
                    f"[{self.channel_name}] 🔄 Renommage: {original_filename} -> {sanitized_name}"
                )
                if sanitized_path.exists():
                    sanitized_path.unlink()
                file_path.rename(sanitized_path)
                file_path = sanitized_path

            # 2. Création du nom de fichier de sortie (toujours en .mp4)
            output_stem = file_path.stem
            if file_path.suffix.lower() != ".mp4":
                output_name = f"{output_stem}.mp4"
            else:
                output_name = sanitized_name

            # 3. Chemins pour les différentes étapes
            temp_path = self.processing_dir / output_name
            final_output_path = self.ready_to_stream_dir / output_name
            already_processed_path = self.already_processed_dir / file_path.name

            # 4. Vérification si le fichier est déjà traité
            if final_output_path.exists() and self.is_already_optimized(
                final_output_path
            ):
                logger.info(f"[{self.channel_name}] ✅ {output_name} déjà optimisé")

                # Déplacement du fichier source vers already_processed s'il n'y est pas déjà
                if (
                    file_path.exists()
                    and file_path.parent != self.already_processed_dir
                ):
                    if not already_processed_path.exists():
                        shutil.move(str(file_path), str(already_processed_path))
                        logger.info(
                            f"[{self.channel_name}] 📦 Fichier source déplacé vers already_processed"
                        )
                    else:
                        logger.info(
                            f"[{self.channel_name}] ⚠️ Fichier déjà présent dans already_processed, suppression du doublon"
                        )
                        file_path.unlink()

                # Notifier même si déjà traité
                self.notify_file_processed(final_output_path)
                return

            # Check file size and estimate transcoding time
            file_size_gb = file_path.stat().st_size / (1024**3)
            logger.info(
                f"[{self.channel_name}] 📊 Taille du fichier: {file_size_gb:.2f} GB"
            )

            # On vérifie pourquoi ce fichier nécessite une normalisation et on détecte les codecs incompatibles
            cmd_probe = [
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "stream=codec_name,width,height,r_frame_rate,codec_type",
                "-of",
                "json",
                str(file_path),
            ]
            probe_result = subprocess.run(cmd_probe, capture_output=True, text=True)

            # Variable pour décider si on utilise VAAPI ou pas
            use_hardware_accel = self.USE_GPU_FOR_ENCODING
            incompatible_codecs = False

            try:
                video_info = json.loads(probe_result.stdout)
                streams = video_info.get("streams", [])

                # Récupération des infos vidéo
                video_streams = [s for s in streams if s["codec_type"] == "video"]
                audio_streams = [s for s in streams if s["codec_type"] == "audio"]

                reasons = []

                # Liste des codecs incompatibles avec VAAPI
                vaapi_incompatible_video_codecs = [
                    "msmpeg4v3",
                    "msmpeg4",
                    "wmv1",
                    "wmv2",
                    "wmv3",
                    "vc1",
                    "vp6",
                    "svq3",
                ]
                vaapi_incompatible_audio_codecs = [
                    "wmav1",
                    "wmav2",
                    "wmalossless",
                    "wmapro",
                ]

                if video_streams:
                    video = video_streams[0]
                    codec = video.get("codec_name", "").lower()
                    width = int(video.get("width", 0))
                    height = int(video.get("height", 0))
                    framerate = video.get("r_frame_rate", "0/1").split("/")
                    fps = (
                        round(int(framerate[0]) / int(framerate[1]))
                        if len(framerate) == 2
                        else 0
                    )

                    # Vérifier si le codec vidéo est incompatible avec VAAPI
                    if codec in vaapi_incompatible_video_codecs:
                        use_hardware_accel = False
                        incompatible_codecs = True
                        logger.warning(
                            f"[{self.channel_name}] ⚠️ Codec vidéo {codec} incompatible avec VAAPI, utilisation du mode CPU"
                        )

                    if codec not in ["h264", "hevc", "h265"]:
                        reasons.append(f"codec vidéo {codec} non supporté")
                    if width > 1920 or height > 1080:
                        reasons.append(
                            f"résolution {width}x{height} supérieure à 1080p"
                        )
                    if fps > 60:
                        reasons.append(f"FPS de {fps} supérieur à 60")
                else:
                    reasons.append("pas de flux vidéo détecté")

                if not audio_streams:
                    reasons.append("pas de flux audio détecté")
                elif audio_streams:
                    audio = audio_streams[0]
                    audio_codec = audio.get("codec_name", "").lower()

                    # Vérifier si le codec audio est incompatible avec VAAPI
                    if audio_codec in vaapi_incompatible_audio_codecs:
                        use_hardware_accel = False
                        incompatible_codecs = True
                        logger.warning(
                            f"[{self.channel_name}] ⚠️ Codec audio {audio_codec} incompatible avec VAAPI, utilisation du mode CPU"
                        )

                    if audio_codec not in ["aac", "mp3"]:
                        reasons.append(f"codec audio {audio_codec} non supporté")

                # Log détaillé des raisons de normalisation
                if reasons:
                    reasons_str = ", ".join(reasons)
                    logger.info(
                        f"[{self.channel_name}] 🔄 Normalisation de {sanitized_name} nécessaire: {reasons_str}"
                    )
            except Exception as e:
                logger.warning(
                    f"[{self.channel_name}] ⚠️ Impossible d'analyser les raisons de normalisation pour {sanitized_name}: {e}"
                )
                # Par sécurité, on désactive VAAPI si on ne peut pas analyser
                use_hardware_accel = False

            # Déterminer la durée totale de la vidéo source
            total_duration = self.total_duration = get_accurate_duration(file_path)
            if total_duration <= 0:
                total_duration = None
                logger.warning(
                    f"[{self.channel_name}] ⚠️ Impossible de déterminer la durée de {sanitized_name}"
                )
            else:
                logger.info(
                    f"[{self.channel_name}] 📊 Durée de {sanitized_name}: {self._format_time(total_duration)}"
                )

            # Estimation du temps de transcodage (très approximative)
            # Augmenté pour les codecs incompatibles qui sont plus lents
            cpu_factor = 1.5 if incompatible_codecs else 1.0
            estimated_hours = file_size_gb * (0.5 if use_hardware_accel else cpu_factor)
            logger.info(
                f"[{self.channel_name}] ⏱️ Temps estimé: {estimated_hours:.1f} heures"
            )

            # Timeout minimum de 2h, maximum 8h selon la taille du fichier
            timeout_hours = max(2.0, min(8, estimated_hours * 1.5))  # Entre 2h et 8h
            timeout_seconds = int(timeout_hours * 3600)
            logger.info(
                f"[{self.channel_name}] ⏰ Timeout ajusté: {timeout_hours:.1f} heures"
            )

            # Vérifier si le fichier est HEVC 10-bit pour adapter l'approche
            is_hevc_10bit = self._check_hevc_10bit(file_path)

            # On supprime le fichier temporaire s'il existe déjà
            if temp_path.exists():
                temp_path.unlink()

            # Construction de la commande FFmpeg - version adaptée selon compatibilité
            if is_hevc_10bit or incompatible_codecs:
                # Mode CPU pour HEVC 10-bit ou codecs incompatibles avec VAAPI
                command = [
                    "ffmpeg",
                    "-y",
                    "-i",
                    str(file_path),
                    "-c:v",
                    "libx264",
                    "-crf",
                    "23",
                    "-preset",
                    "fast",
                    "-c:a",
                    "aac",
                    "-b:a",
                    "192k",
                    "-ac",
                    "2",
                    "-sn",
                    "-dn",
                    "-map_chapters",
                    "-1",
                    "-map",
                    "0:v:0",
                    "-map",
                    "0:a:0?",
                    "-movflags",
                    "+faststart",
                    "-max_muxing_queue_size",
                    "4096",
                    "-progress",
                    "pipe:1",
                    str(temp_path),
                ]
            elif use_hardware_accel:
                # Mode VAAPI pour accélération matérielle
                command = [
                    "ffmpeg",
                    "-y",
                    # Options hwaccel AVANT l'input
                    "-hwaccel",
                    "vaapi",
                    "-hwaccel_output_format",
                    "vaapi",
                    "-vaapi_device",
                    "/dev/dri/renderD128",
                    "-i",
                    str(file_path),
                    "-c:v",
                    "h264_vaapi",
                    "-profile:v",
                    "main",
                    "-c:a",
                    "aac",
                    "-b:a",
                    "192k",
                    "-ac",
                    "2",
                    "-sn",
                    "-dn",
                    "-map_chapters",
                    "-1",
                    "-map",
                    "0:v:0",
                    "-map",
                    "0:a:0?",
                    "-movflags",
                    "+faststart",
                    "-max_muxing_queue_size",
                    "4096",
                    "-progress",
                    "pipe:1",
                    str(temp_path),
                ]
            else:
                # Mode CPU standard
                command = [
                    "ffmpeg",
                    "-y",
                    "-i",
                    str(file_path),
                    "-c:v",
                    "libx264",
                    "-preset",
                    "fast",
                    "-crf",
                    "23",
                    "-c:a",
                    "aac",
                    "-b:a",
                    "192k",
                    "-ac",
                    "2",
                    "-sn",
                    "-dn",
                    "-map_chapters",
                    "-1",
                    "-map",
                    "0:v:0",
                    "-map",
                    "0:a:0?",
                    "-movflags",
                    "+faststart",
                    "-max_muxing_queue_size",
                    "4096",
                    "-progress",
                    "pipe:1",
                    str(temp_path),
                ]

            # Log de la commande complète
            logger.info(f"[{self.channel_name}] 🎬 Commande pour {sanitized_name}:")
            logger.info(f"$ {' '.join(command)}")

            # Exécution de la commande
            try:
                process = subprocess.Popen(
                    command,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True,
                    bufsize=1,
                )

                # Suivi de la progression
                start_time = time.time()
                last_progress_time = start_time
                progress_data = {}

                while process.poll() is None:
                    # Vérification du timeout global
                    current_time = time.time()
                    if current_time - start_time > timeout_seconds:
                        logger.error(
                            f"[{self.channel_name}] ⏰ Timeout de {timeout_hours:.1f}h pour {sanitized_name}"
                        )
                        process.kill()

                        # On essaie une dernière fois avec un timeout plus long avant d'ignorer
                        logger.info(f"[{self.channel_name}] 🔄 Dernière tentative avec timeout étendu pour {sanitized_name}")
                        # On tente un dernier essai avec CPU avant d'abandonner
                        return self._retry_with_cpu(file_path, temp_path, final_output_path)

                    # Lecture de la progression
                    try:
                        import select

                        ready_to_read, _, _ = select.select(
                            [process.stdout], [], [], 1.0
                        )
                        if ready_to_read:
                            stdout_line = process.stdout.readline().strip()
                            if stdout_line and "=" in stdout_line:
                                key, value = stdout_line.split("=", 1)
                                progress_data[key] = value

                                # Affichage de la progression
                                if (
                                    key == "out_time"
                                    and current_time - last_progress_time >= 10
                                ):
                                    if total_duration:
                                        # Calcul du pourcentage et temps restant
                                        time_parts = value.split(":")
                                        if len(time_parts) == 3:
                                            hours, minutes, seconds = time_parts
                                            seconds_parts = seconds.split(".")
                                            seconds = float(
                                                f"{seconds_parts[0]}.{seconds_parts[1]}"
                                                if len(seconds_parts) > 1
                                                else seconds_parts[0]
                                            )
                                            out_time_seconds = (
                                                int(hours) * 3600
                                                + int(minutes) * 60
                                                + seconds
                                            )

                                            # Pourcentage et vitesse
                                            elapsed = current_time - start_time
                                            if elapsed > 0:
                                                speed = out_time_seconds / elapsed
                                                percent_done = (
                                                    out_time_seconds / total_duration
                                                ) * 100
                                                remaining = (
                                                    total_duration - out_time_seconds
                                                ) / max(0.1, speed)
                                                eta = time.strftime(
                                                    "%H:%M:%S", time.gmtime(remaining)
                                                )

                                                logger.info(
                                                    f"[{self.channel_name}] 🔄 {sanitized_name}: {value} / {self._format_time(total_duration)} "
                                                    f"({percent_done:.1f}%) - ETA: {eta} - Vitesse: {speed:.2f}x"
                                                )
                                    else:
                                        logger.info(
                                            f"[{self.channel_name}] 🔄 {sanitized_name}: {value}"
                                        )

                                    last_progress_time = current_time
                    except Exception as e:
                        logger.debug(
                            f"[{self.channel_name}] Erreur lecture progression: {e}"
                        )

                    time.sleep(0.1)

                # Vérification du résultat
                return_code = process.wait()
                stderr_output = process.stderr.read()

                if return_code != 0:
                    logger.error(
                        f"[{self.channel_name}] ❌ Erreur FFmpeg pour {sanitized_name}: {stderr_output}"
                    )

                    # Si l'erreur est liée à VAAPI, on réessaie en mode CPU
                    if use_hardware_accel and (
                        "vaapi" in stderr_output.lower()
                        or "hwaccel" in stderr_output.lower()
                    ):
                        logger.warning(
                            f"[{self.channel_name}] ⚠️ Échec VAAPI, nouvelle tentative en mode CPU"
                        )

                        # Nettoyer le fichier temporaire s'il existe
                        if temp_path.exists():
                            temp_path.unlink()

                        # Nouvelle commande sans accélération matérielle
                        cpu_command = [
                            "ffmpeg",
                            "-y",
                            "-i",
                            str(file_path),
                            "-c:v",
                            "libx264",
                            "-preset",
                            "fast",
                            "-crf",
                            "23",
                            "-c:a",
                            "aac",
                            "-b:a",
                            "192k",
                            "-ac",
                            "2",
                            "-sn",
                            "-dn",
                            "-map_chapters",
                            "-1",
                            "-map",
                            "0:v:0",
                            "-map",
                            "0:a:0?",
                            "-movflags",
                            "+faststart",
                            "-max_muxing_queue_size",
                            "4096",
                            "-progress",
                            "pipe:1",
                            str(temp_path),
                        ]

                        logger.info(
                            f"[{self.channel_name}] 🎬 Nouvelle commande CPU pour {sanitized_name}:"
                        )
                        logger.info(f"$ {' '.join(cpu_command)}")

                        # Lancer la nouvelle commande
                        fallback_result = subprocess.run(
                            cpu_command, capture_output=True, text=True
                        )

                        if fallback_result.returncode == 0:
                            # Fichier temporaire créé avec succès, on continue le workflow normal
                            logger.info(
                                f"[{self.channel_name}] ✅ Transcodage CPU réussi: {output_name}"
                            )
                        else:
                            logger.error(
                                f"[{self.channel_name}] ❌ Échec transcodage CPU: {fallback_result.stderr}"
                            )
                            # On fait une deuxième tentative avec paramètres simplifiés avant d'abandonner
                            logger.info(f"[{self.channel_name}] 🔄 Tentative simplifiée pour {file_path.name}")
                            return self._final_fallback_attempt(file_path, temp_path, final_output_path)
                    else:
                        # Échec qui n'est pas lié à VAAPI, on abandonne
                        self._move_to_ignored(
                            file_path, f"erreur FFmpeg code {return_code}"
                        )
                        return

                # Vérification que le fichier temporaire existe et est valide
                if not temp_path.exists() or temp_path.stat().st_size == 0:
                    logger.error(
                        f"[{self.channel_name}] ❌ Fichier temporaire invalide ou absent: {temp_path}"
                    )
                    # Tentative de récupération avant d'abandonner
                    logger.info(f"[{self.channel_name}] 🔄 Tentative de récupération pour {file_path.name}")
                    return self._retry_with_cpu(file_path, temp_path, final_output_path)

                # Vérification que le MP4 est complet
                cmd_check = [
                    "ffprobe",
                    "-v",
                    "error",
                    "-select_streams",
                    "v:0",
                    "-show_entries",
                    "stream=codec_name",
                    "-of",
                    "json",
                    str(temp_path),
                ]

                check_result = subprocess.run(cmd_check, capture_output=True, text=True)

                # Vérification simplifiée: On vérifie juste que c'est un MP4 avec codec h264
                try:
                    data = json.loads(check_result.stdout)
                    if "streams" in data and len(data["streams"]) > 0:
                        codec = data["streams"][0].get("codec_name", "").lower()
                        if codec == "h264":
                            logger.info(f"[{self.channel_name}] ✅ Validation MP4 réussie: codec {codec}")
                        else:
                            logger.warning(f"[{self.channel_name}] ⚠️ Codec {codec} détecté, tentative de récupération")
                            return self._final_fallback_attempt(file_path, temp_path, final_output_path)
                    else:
                        logger.warning(f"[{self.channel_name}] ⚠️ Aucun stream vidéo détecté, tentative de récupération")
                        return self._final_fallback_attempt(file_path, temp_path, final_output_path)
                except Exception as e:
                    logger.warning(f"[{self.channel_name}] ⚠️ Erreur analyse JSON: {e}, tentative de récupération")
                    return self._final_fallback_attempt(file_path, temp_path, final_output_path)

                # Déplacer le fichier temporaire vers le dossier ready_to_stream
                if final_output_path.exists():
                    final_output_path.unlink()

                shutil.move(str(temp_path), str(final_output_path))
                logger.info(
                    f"[{self.channel_name}] ✅ Fichier déplacé vers ready_to_stream: {output_name}"
                )

                # Déplacer le fichier source vers already_processed
                if file_path.exists():
                    if already_processed_path.exists():
                        already_processed_path.unlink()
                    shutil.move(str(file_path), str(already_processed_path))
                    logger.info(
                        f"[{self.channel_name}] 📦 Source déplacée vers already_processed: {file_path.name}"
                    )

                # Notifier que le traitement est terminé
                self.notify_file_processed(final_output_path)
                return

            except Exception as e:
                logger.error(
                    f"[{self.channel_name}] ❌ Exception transcodage {sanitized_name}: {e}"
                )
                if process and process.poll() is None:
                    process.kill()

                # On tente une dernière récupération avant d'abandonner
                try:
                    logger.info(f"[{self.channel_name}] 🔄 Tentative de récupération après exception pour {original_filename}")
                    return self._final_fallback_attempt(file_path, temp_path, final_output_path)
                except Exception as final_e:
                    # Récupérer le chemin original (racine) du fichier source si possible
                    source_path = Path(self.channel_dir) / original_filename
                    if source_path.exists():
                        # Si le fichier source est toujours dans la racine, on utilise celui-là
                        self._move_to_ignored(
                            source_path, f"exception transcodage après plusieurs tentatives: {str(e)[:200]}"
                        )
                    else:
                        # Sinon, on utilise le chemin actuel du fichier
                        self._move_to_ignored(
                            file_path, f"exception transcodage après plusieurs tentatives: {str(e)[:200]}"
                        )

        except Exception as e:
            logger.error(
                f"[{self.channel_name}] ❌ Erreur processing {original_filename}: {e}"
            )
            # En cas d'erreur, on déplace vers ignored
            if "file_path" in locals() and file_path.exists():
                self._move_to_ignored(file_path, f"erreur: {str(e)[:200]}")
            return

    def _retry_with_cpu(
        self, file_path: Path, temp_output_path: Path, final_output_path: Path
    ) -> Optional[Path]:
        """Retente le transcodage en mode CPU (sans VAAPI)"""
        filename = file_path.name
        logger.info(
            f"[{self.channel_name}] 🔄 Tentative de transcodage en mode CPU pour {filename}"
        )

        # Nettoyer le fichier temporaire s'il existe
        if temp_output_path.exists():
            temp_output_path.unlink()

        # Commande CPU optimisée
        command = [
            "ffmpeg",
            "-y",
            "-i",
            str(file_path),
            "-c:v",
            "libx264",
            "-preset",
            "fast",
            "-crf",
            "23",
            "-c:a",
            "aac",
            "-b:a",
            "192k",
            "-ac",
            "2",
            "-sn",
            "-dn",
            "-map_chapters",
            "-1",
            "-map",
            "0:v:0",
            "-map",
            "0:a:0?",
            "-movflags",
            "+faststart",
            "-max_muxing_queue_size",
            "4096",
            "-progress",
            "pipe:1",
            str(temp_output_path),
        ]

        logger.info(
            f"[{self.channel_name}] 🎬 Nouvelle commande CPU: {' '.join(command)}"
        )

        try:
            # Exécution du processus
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                bufsize=1,
            )

            # Attente du résultat
            return_code = process.wait()
            stderr_output = process.stderr.read()

            if return_code != 0:
                logger.error(
                    f"[{self.channel_name}] ❌ Échec mode CPU pour {filename}: {stderr_output}"
                )
                self._move_to_ignored(
                    file_path, f"échec CPU: {stderr_output[:200]}..."
                )
                return None

            # Vérification du fichier de sortie
            if not temp_output_path.exists() or temp_output_path.stat().st_size == 0:
                logger.error(
                    f"[{self.channel_name}] ❌ Fichier de sortie CPU invalide: {temp_output_path}"
                )
                self._move_to_ignored(file_path, "fichier de sortie CPU invalide")
                return None

            # Déplacement vers ready_to_stream
            logger.info(
                f"[{self.channel_name}] ✅ Déplacement CPU vers ready_to_stream: {final_output_path.name}"
            )

            # Suppression du fichier final s'il existe déjà
            if final_output_path.exists():
                final_output_path.unlink()

            # Déplacement du fichier temporaire
            shutil.move(str(temp_output_path), str(final_output_path))

            logger.info(
                f"[{self.channel_name}] ✅ Transcodage CPU réussi: {final_output_path.name}"
            )
            self.notify_file_processed(final_output_path)
            return final_output_path

        # En cas d'erreur dans process_video
        except Exception as e:
            logger.error(
                f"[{self.channel_name}] ❌ Exception transcodage {sanitized_name}: {e}"
            )
            if process and process.poll() is None:
                process.kill()

            # Récupérer le chemin original (racine) du fichier source si possible
            source_path = Path(self.channel_dir) / original_filename
            if source_path.exists():
                # Si le fichier source est toujours dans la racine, on utilise celui-là
                self._move_to_ignored(
                    source_path, f"exception transcodage après plusieurs tentatives: {str(e)[:200]}"
                )
            else:
                # Sinon, on utilise le chemin actuel du fichier
                self._move_to_ignored(
                    file_path, f"exception transcodage après plusieurs tentatives: {str(e)[:200]}"
                )

            return None

    def _check_hevc_10bit(self, file_path: Path) -> bool:
        """Vérifie si un fichier est encodé en HEVC 10-bit"""
        try:
            cmd = [
                "ffprobe",
                "-v",
                "error",
                "-select_streams",
                "v:0",
                "-show_entries",
                "stream=codec_name,pix_fmt",
                "-of",
                "json",
                str(file_path),
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                return False

            video_info = json.loads(result.stdout)
            if "streams" not in video_info or not video_info["streams"]:
                return False

            stream = video_info["streams"][0]
            codec = stream.get("codec_name", "").lower()
            pix_fmt = stream.get("pix_fmt", "").lower()

            # Détecte HEVC/H.265 avec format de pixel 10-bit
            return codec in ["hevc", "h265"] and ("10" in pix_fmt or "p10" in pix_fmt)
        except Exception as e:
            logger.error(f"❌ Erreur détection HEVC 10-bit: {e}")
            return False

    def _two_pass_hevc_conversion(self, input_path: Path, output_path: Path) -> Path:
        """Conversion HEVC en deux étapes avec fichier intermédiaire"""
        try:
            # Créer un fichier temporaire
            temp_file = self.processing_dir / f"temp_{input_path.stem}.mp4"

            # Première étape: Extraction simple sans réencodage vidéo
            extract_cmd = [
                "ffmpeg",
                "-y",
                "-i",
                str(input_path),
                "-map",
                "0:v:0",  # Premier flux vidéo
                "-map",
                "0:a:0?",  # Premier flux audio s'il existe
                "-c:v",
                "copy",  # Copie sans réencodage
                "-c:a",
                "aac",  # Conversion audio en AAC
                "-b:a",
                "192k",
                "-ac",
                "2",  # Stéréo
                "-sn",
                "-dn",  # Pas de sous-titres ni données
                "-map_chapters",
                "-1",  # Pas de chapitres
                str(temp_file),
            ]

            logger.info(f"🔄 Étape 1/2: Extraction en {temp_file}")
            extract_result = subprocess.run(extract_cmd, capture_output=True, text=True)

            if extract_result.returncode != 0:
                logger.error(f"❌ Échec extraction: {extract_result.stderr}")
                return None

            # Deuxième étape: Réencodage en H.264
            encode_cmd = [
                "ffmpeg",
                "-y",
                "-i",
                str(temp_file),
                "-c:v",
                "libx264",
                "-preset",
                "fast",
                "-crf",
                "23",
                "-pix_fmt",
                "yuv420p",
                "-profile:v",
                "high",
                "-level",
                "4.1",
                "-c:a",
                "copy",
                str(output_path),
            ]

            logger.info(f"🔄 Étape 2/2: Réencodage en {output_path}")
            encode_result = subprocess.run(encode_cmd, capture_output=True, text=True)

            # Nettoyage du fichier temporaire
            if temp_file.exists():
                try:
                    temp_file.unlink()
                except:
                    pass

            if encode_result.returncode != 0:
                logger.error(f"❌ Échec réencodage: {encode_result.stderr}")
                return None

            logger.info(f"✅ Conversion en deux étapes réussie pour {input_path.name}")
            return output_path

        except Exception as e:
            logger.error(f"❌ Erreur conversion deux étapes: {e}")
            return None

    def get_processed_files(self) -> List[Path]:
        """Renvoie la liste des fichiers traités et prêts pour le streaming"""
        try:
            mp4_files = list(self.ready_to_stream_dir.glob("*.mp4"))
            return sorted(mp4_files) if mp4_files else []
        except Exception as e:
            logger.error(f"❌ Erreur récupération des fichiers traités: {e}")
            return []

    def is_large_resolution(self, video_path: Path) -> bool:
        """
        Vérifie si une vidéo a une résolution significativement supérieure à 1080p.
        On tolère une marge de 10% pour éviter des conversions inutiles.
        """
        try:
            cmd = [
                "ffprobe",
                "-v",
                "error",
                "-select_streams",
                "v:0",
                "-show_entries",
                "stream=width,height",
                "-of",
                "json",
                str(video_path),
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            video_info = json.loads(result.stdout)

            if "streams" not in video_info or not video_info["streams"]:
                return False

            stream = video_info["streams"][0]
            width = int(stream.get("width", 0))
            height = int(stream.get("height", 0))

            # On tolère une marge de 10% au-dessus de 1080p
            max_height = int(1080 * 1.1)  # ~1188p
            max_width = int(1920 * 1.1)  # ~2112px

            return width > max_width or height > max_height

        except (
            subprocess.SubprocessError,
            json.JSONDecodeError,
            ValueError,
            KeyError,
        ) as e:
            logger.error(f"❌ Erreur vérification résolution {video_path.name}: {e}")
            return False

    def is_already_optimized(self, video_path: Path) -> bool:
        """
        Vérifie si une vidéo est déjà optimisée pour le streaming.
        Critères simplifiés: MP4 + H.264 uniquement.
        """
        logger.info(f"🔍 Vérification du format de {video_path.name}")

        # Vérification de l'extension
        if video_path.suffix.lower() != ".mp4":
            logger.info(f"⚠️ {video_path.name} n'est pas un fichier MP4")
            return False

        cmd = [
            "ffprobe",
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=codec_name",
            "-of",
            "json",
            str(video_path),
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)

        try:
            video_info = json.loads(result.stdout)
            streams = video_info.get("streams", [])

            # Pas de flux vidéo
            if not streams:
                logger.warning(f"⚠️ Aucun flux vidéo détecté dans {video_path.name}")
                return False

            # On vérifie uniquement le codec vidéo
            codec = streams[0].get("codec_name", "").lower()
            
            logger.info(f"🎥 Codec: {codec}")

            # Critère unique: H.264
            if codec == "h264":
                logger.info(f"✅ Vidéo déjà optimisée: {video_path.name} (codec H.264)")
                return True
            else:
                logger.info(f"⚠️ Normalisation nécessaire: codec {codec} (besoin de H.264)")
                return False

        except json.JSONDecodeError as e:
            logger.error(f"❌ Erreur JSON avec ffprobe: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Erreur générale vérification optimisation: {str(e)}")
            return False

    def _move_to_ignored(self, file_path: Path, reason: str):
        """
        Déplace un fichier invalide vers le dossier 'ignored' et nettoie les fichiers temporaires

        Args:
            file_path: Chemin du fichier source à déplacer
            reason: Raison de l'invalidité du fichier
        """
        try:
            # S'assurer que le dossier ignored existe
            ignored_dir = Path(self.channel_dir) / "ignored"
            ignored_dir.mkdir(parents=True, exist_ok=True)

            # Obtenir le nom d'origine et le nom sanitizé pour nettoyer tous les fichiers associés
            original_name = file_path.name
            sanitized_name = self.sanitize_filename(original_name)
            stem = Path(sanitized_name).stem  # Nom sans extension

            # 1. Nettoyer les fichiers temporaires dans processing
            processing_files = list(self.processing_dir.glob(f"{stem}*"))
            for temp_file in processing_files:
                try:
                    temp_file.unlink()
                    logger.info(
                        f"[{self.channel_name}] 🗑️ Suppression du fichier temporaire: {temp_file}"
                    )
                except Exception as e:
                    logger.error(
                        f"[{self.channel_name}] ❌ Erreur suppression {temp_file}: {e}"
                    )

            # 2. Créer le chemin de destination pour le fichier source
            dest_path = ignored_dir / original_name

            # Si le fichier de destination existe déjà, le supprimer
            if dest_path.exists():
                dest_path.unlink()
                logger.info(
                    f"[{self.channel_name}] 🗑️ Suppression du fichier existant dans ignored: {dest_path.name}"
                )

            # 3. Déplacer le fichier source vers ignored
            if file_path.exists():
                # Si le fichier source est dans la racine, on le déplace
                if file_path.parent == Path(self.channel_dir):
                    shutil.move(str(file_path), str(dest_path))
                    logger.info(
                        f"[{self.channel_name}] 🚫 Fichier source déplacé vers ignored: {original_name}"
                    )
                else:
                    # Si le fichier n'est pas dans la racine mais existe ailleurs
                    # (par exemple s'il a déjà été déplacé dans processing)
                    # On le déplace aussi vers ignored
                    shutil.move(str(file_path), str(dest_path))
                    logger.info(
                        f"[{self.channel_name}] 🚫 Fichier déplacé de {file_path.parent.name} vers ignored: {original_name}"
                    )

                    # On vérifie si une copie existe encore dans la racine et on la supprime
                    root_copy = Path(self.channel_dir) / original_name
                    if root_copy.exists():
                        root_copy.unlink()
                        logger.info(
                            f"[{self.channel_name}] 🗑️ Suppression du fichier source restant dans la racine: {original_name}"
                        )

                # 4. Créer un fichier de log à côté avec la raison
                log_path = ignored_dir / f"{Path(original_name).stem}_reason.txt"
                with open(log_path, "w") as f:
                    f.write(f"Fichier ignoré le {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write(f"Raison: {reason}\n")

                logger.info(f"[{self.channel_name}] 📝 Raison enregistrée: {reason}")

        except Exception as e:
            logger.error(
                f"[{self.channel_name}] ❌ Erreur déplacement fichier vers ignored: {e}"
            )

    def _final_fallback_attempt(self, file_path: Path, temp_output_path: Path, final_output_path: Path) -> Optional[Path]:
        """
        Dernière tentative de récupération avec des paramètres ultra simplifiés
        Force la conversion en H.264 uniquement
        """
        try:
            logger.info(f"[{self.channel_name}] 🚨 Dernière tentative avec paramètres simplifiés pour {file_path.name}")
            
            # Utilisation de paramètres FFmpeg minimalistes focalisés sur H.264
            cmd = [
                "ffmpeg", "-y",
                "-i", str(file_path),
                # Forcer la conversion en H.264 le plus simple possible
                "-c:v", "libx264", "-preset", "ultrafast", 
                # Audio aac basique (moins important)
                "-c:a", "aac", "-b:a", "128k",
                # On ignore tout le reste (sous-titres, chapitres, etc.)
                "-sn", "-dn", "-map_chapters", "-1",
                # Sélection des pistes principales uniquement
                "-map", "0:v:0", "-map", "0:a:0?",
                # Pour assurer la compatibilité maximale
                "-pix_fmt", "yuv420p",
                "-max_muxing_queue_size", "1024",
                str(temp_output_path)
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0 and temp_output_path.exists() and temp_output_path.stat().st_size > 0:
                # Vérification rapide que le fichier est bien en H.264
                check_cmd = [
                    "ffprobe", "-v", "error",
                    "-select_streams", "v:0",
                    "-show_entries", "stream=codec_name",
                    "-of", "json",
                    str(temp_output_path)
                ]
                
                check_result = subprocess.run(check_cmd, capture_output=True, text=True)
                is_h264 = False
                
                try:
                    data = json.loads(check_result.stdout)
                    if "streams" in data and len(data["streams"]) > 0:
                        codec = data["streams"][0].get("codec_name", "").lower()
                        is_h264 = (codec == "h264")
                except:
                    # En cas d'erreur, on suppose que ce n'est pas du H.264
                    pass
                
                if is_h264:
                    # Déplacement vers ready_to_stream
                    logger.info(f"[{self.channel_name}] ✅ Conversion H.264 réussie pour {final_output_path.name}")
                    
                    # Suppression du fichier final s'il existe déjà
                    if final_output_path.exists():
                        final_output_path.unlink()
                    
                    # Déplacement du fichier temporaire
                    shutil.move(str(temp_output_path), str(final_output_path))
                    self.notify_file_processed(final_output_path)
                    return final_output_path
                else:
                    logger.error(f"[{self.channel_name}] ❌ Le fichier converti n'est pas en H.264")
                    self._move_to_ignored(file_path, "échec conversion en H.264")
                    return None
            else:
                logger.error(f"[{self.channel_name}] ❌ Échec de la dernière tentative: {result.stderr}")
                self._move_to_ignored(file_path, "échec conversion, fichier peut-être corrompu")
                return None
        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Exception lors de la dernière tentative: {e}")
            self._move_to_ignored(file_path, f"exception finale: {str(e)[:200]}")
            return None

    def sanitize_filename(self, filename: str) -> str:
        """Sanitize le nom de fichier en retirant TOUS les caractères problématiques"""
        # On nettoie plus agressivement les caractères problématiques pour FFmpeg
        sanitized = filename.replace("'", "").replace('"', "").replace(",", "_")
        sanitized = sanitized.replace("-", "_").replace(" ", "_")
        # On supprime les caractères spéciaux et on garde uniquement lettres, chiffres, points et underscore
        sanitized = re.sub(r"[^a-zA-Z0-9._]", "", sanitized)
        # Limitation longueur max à 100 caractères pour éviter problèmes de buffer
        if len(sanitized) > 100:
            base, ext = os.path.splitext(sanitized)
            sanitized = base[:96] + ext  # On garde l'extension
        return sanitized

    def _format_time(self, seconds: float) -> str:
        """Formate un temps en secondes au format HH:MM:SS.mmm"""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        seconds_part = seconds % 60
        return f"{hours:02d}:{minutes:02d}:{seconds_part:06.3f}"

    def _cleanup_temp_dir(self, temp_dir: Path):
        """Nettoie un répertoire temporaire"""
        try:
            if temp_dir.exists():
                shutil.rmtree(temp_dir)
                logger.info(f"🧹 Répertoire temporaire nettoyé: {temp_dir}")
        except Exception as e:
            logger.error(f"❌ Erreur nettoyage répertoire temporaire: {e}")

    def _is_already_processed_file(self, file_path: Path) -> bool:
        """
        Vérifie si un fichier est déjà traité et présent dans ready_to_stream
        ou already_processed pour éviter des vérifications inutiles.
        
        Args:
            file_path: Chemin du fichier à vérifier
            
        Returns:
            True si le fichier est déjà traité, False sinon
        """
        try:
            # Vérifier dans ready_to_stream
            for existing_file in self.ready_to_stream_dir.glob("*.*"):
                if existing_file.name == file_path.name:
                    logger.debug(f"[{self.channel_name}] ⏩ Fichier {file_path.name} déjà dans ready_to_stream")
                    return True
                    
            # Vérifier dans already_processed
            for existing_file in self.already_processed_dir.glob("*.*"):
                if existing_file.name == file_path.name:
                    logger.debug(f"[{self.channel_name}] ⏩ Fichier {file_path.name} déjà dans already_processed")
                    return True
            
            # Vérifier si le nom du fichier est dans la playlist.txt
            playlist_path = self.channel_dir / "playlist.txt"
            if playlist_path.exists():
                with open(playlist_path, "r", encoding="utf-8") as f:
                    content = f.read()
                    if file_path.name in content:
                        logger.debug(f"[{self.channel_name}] ⏩ Fichier {file_path.name} déjà présent dans playlist.txt")
                        return True
            
            return False
        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur vérification fichier déjà traité {file_path.name}: {e}")
            return False

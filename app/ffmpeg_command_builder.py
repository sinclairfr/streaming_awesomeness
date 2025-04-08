# ffmpeg_command_builder.py
import os
import json
import subprocess
from pathlib import Path
from config import FFMPEG_LOG_LEVEL, logger
from typing import Optional


class FFmpegCommandBuilder:
    """
    # Générateur de commandes FFmpeg optimisées
    # Adapte les paramètres selon le type de fichier, la présence de GPU, etc.
    """

    def __init__(self, channel_name, use_gpu=False):
        self.channel_name = channel_name
        self.use_gpu = use_gpu

        # Paramètres par défaut
        self.hls_time = 2
        self.hls_list_size = 20
        self.hls_delete_threshold = 1
        self.gop_size = 48
        self.keyint_min = 48
        self.video_bitrate = "5M"
        self.max_bitrate = "5M"
        self.buffer_size = "10M"

    def build_command(
        self,
        input_file,
        output_dir,
        playback_offset=0,
        progress_file=None,
        has_mkv=False,
    ):
        """Construit la commande FFmpeg complète"""
        try:
            logger.info(
                f"[{self.channel_name}] 🛠️ Construction de la commande FFmpeg..."
            )

            # Construction de la commande optimisée
            command = [
                "ffmpeg",
                "-hide_banner",
                "-loglevel", "info",
                "-y",
                # Paramètres d'entrée optimisés
                "-thread_queue_size", "4096",  # Réduit pour plus de stabilité
                "-analyzeduration", "5M",      # Réduit pour un démarrage plus rapide
                "-probesize", "5M",            # Réduit pour un démarrage plus rapide
                "-re",  # Lecture en temps réel
                "-fflags", "+genpts+igndts+discardcorrupt",  # Simplifié
                "-threads", "2",               # Réduit pour plus de stabilité
                "-avoid_negative_ts", "make_zero",
            ]

            # Ajouter le fichier de progression si fourni
            if progress_file:
                command.extend(["-progress", str(progress_file)])

            # Ajouter l'input
            command.extend(["-i", str(input_file)])

            # Paramètres de copie optimisés
            command.extend([
                "-c:v", "copy",
                "-c:a", "copy",
                "-sn", "-dn",
                "-map", "0:v:0",
                "-map", "0:a:0?",
                "-max_muxing_queue_size", "2048",  # Réduit pour plus de stabilité
                "-fps_mode", "passthrough",
            ])

            # Paramètres HLS optimisés
            command.extend([
                "-f", "hls",
                "-hls_time", "2",
                "-hls_list_size", "6",        # Réduit pour plus de stabilité
                "-hls_delete_threshold", "1",  # Réduit pour plus de stabilité
                "-hls_flags", "delete_segments+append_list+independent_segments",  # Simplifié
                "-hls_allow_cache", "1",
                "-start_number", "0",
                "-hls_segment_type", "mpegts",
                "-max_delay", "1000000",       # Réduit pour plus de stabilité
                "-hls_init_time", "1",
                "-hls_segment_filename", f"{output_dir}/segment_%d.ts",
                f"{output_dir}/playlist.m3u8"
            ])

            # Log de la commande complète
            logger.info("=" * 80)
            logger.info(f"[{self.channel_name}] 🚀 Lancement de la commande FFmpeg:")
            logger.info(f"$ {' '.join(command)}")
            logger.info("=" * 80)

            return command
        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur construction commande: {e}")
            # Fallback à une commande minimale en cas d'erreur
            return self.build_fallback_command(input_file, output_dir)

    def _rename_all_videos_simple(self):
        """Renomme tous les fichiers problématiques avec des noms ultra simples"""
        try:
            processed_dir = Path(CONTENT_DIR) / self.name / "processed"
            if not processed_dir.exists():
                processed_dir.mkdir(parents=True, exist_ok=True)

            source_dir = Path(CONTENT_DIR) / self.name

            # D'abord, on traite les fichiers sources
            for i, video in enumerate(source_dir.glob("*.mp4")):
                if any(c in video.name for c in " ,;'\"()[]{}=+^%$#@!&~`|<>?"):
                    simple_name = f"video_{i+1}.mp4"
                    new_path = video.parent / simple_name
                    try:
                        video.rename(new_path)
                        logger.info(
                            f"[{self.name}] Source renommé: {video.name} -> {simple_name}"
                        )
                    except Exception as e:
                        logger.error(
                            f"[{self.name}] Erreur renommage source {video.name}: {e}"
                        )

            # Ensuite, on traite les fichiers du dossier processed
            for i, video in enumerate(processed_dir.glob("*.mp4")):
                if any(c in video.name for c in " ,;'\"()[]{}=+^%$#@!&~`|<>?"):
                    simple_name = f"processed_{i+1}.mp4"
                    new_path = video.parent / simple_name
                    try:
                        video.rename(new_path)
                        logger.info(
                            f"[{self.name}] Processed renommé: {video.name} -> {simple_name}"
                        )
                    except Exception as e:
                        logger.error(
                            f"[{self.name}] Erreur renommage processed {video.name}: {e}"
                        )

        except Exception as e:
            logger.error(f"[{self.name}] Erreur renommage global: {e}")

    def build_input_params(self, input_file, playback_offset=0, progress_file=None):
        """Construit les paramètres d'entrée avec positionnement précis"""
        params = ["ffmpeg", "-hide_banner", "-loglevel", FFMPEG_LOG_LEVEL, "-y"]

        # Paramètres de buffer optimisés pour réduire le buffering
        params.extend(
            [
                "-thread_queue_size",
                "1024",  # Réduit pour moins de latence
                "-analyzeduration",
                "5M",  # Réduit pour un démarrage plus rapide
                "-probesize",
                "5M",  # Réduit pour un démarrage plus rapide
            ]
        )

        params.extend(
            [
                "-re",  # Lecture en temps réel
                "-fflags",
                "+genpts+igndts+discardcorrupt+autobsf",
                "-threads",
                "2",  # Réduit pour plus de stabilité
                "-avoid_negative_ts",
                "make_zero",
            ]
        )

        if progress_file:
            params.extend(["-progress", str(progress_file)])

        params.extend(["-f", "concat", "-safe", "0", "-i", str(input_file)])

        return params

    def build_hls_params(self, output_dir):
        """
        Construit des paramètres HLS optimisés pour une lecture fluide sans boucles
        """
        return [
            "-f",
            "hls",
            "-hls_time",
            "1",  # Réduit pour moins de latence
            "-hls_list_size",
            "5",  # Réduit pour moins de latence
            "-hls_delete_threshold",
            "1",  # Réduit pour moins de latence
            "-hls_flags",
            "delete_segments+append_list+independent_segments+omit_endlist+discont_start+program_date_time",
            "-hls_allow_cache",
            "0",  # Désactivé pour moins de latence
            "-start_number",
            "0",
            "-hls_segment_type",
            "mpegts",
            "-max_delay",
            "500000",  # Réduit pour moins de latence
            "-hls_init_time",
            "0.5",  # Réduit pour générer la playlist plus rapidement
            "-hls_segment_filename",
            f"{output_dir}/segment_%d.ts",
            f"{output_dir}/playlist.m3u8",
        ]

    def build_encoding_params(self, has_mkv=False):
        """Construit les paramètres d'encodage optimisés pour la copie directe"""
        logger.info(
            f"[{self.channel_name}] 📼 Paramètres optimisés pour la copie directe"
        )

        # Par défaut, on privilégie la copie directe avec des paramètres optimisés
        params = [
            "-c:v",
            "copy",
            "-c:a",
            "copy",
            "-sn",
            "-dn",
            "-map",
            "0:v:0",
            "-map",
            "0:a:0?",
            "-max_muxing_queue_size",
            "1024",  # Réduit pour moins de latence
            "-fps_mode",
            "passthrough",
            "-fflags",
            "+genpts+igndts+discardcorrupt+autobsf",
            "-thread_queue_size",
            "1024",  # Réduit pour moins de latence
            "-avoid_negative_ts",
            "make_zero",
        ]

        # Si on détecte un fichier MKV, on ajuste les paramètres
        if has_mkv:
            logger.info(
                f"[{self.channel_name}] ⚠️ Fichier MKV détecté, ajustement des paramètres"
            )
            # Pour les MKV on peut avoir besoin de spécifier explicitement certains paramètres
            params = [
                "-c:v",
                "copy",
                "-c:a",
                "aac",  # Conversion audio en AAC pour compatibilité
                "-b:a",
                "192k",
                "-sn",
                "-dn",
                "-map",
                "0:v:0",
                "-map",
                "0:a:0?",
                "-max_muxing_queue_size",
                "4096",  # Buffer plus grand pour MKV
                "-fps_mode",
                "passthrough",
                "-fflags",
                "+genpts+igndts+discardcorrupt",
                "-thread_queue_size",
                "16384",  # Queue size encore plus grande pour MKV
                "-avoid_negative_ts",
                "make_zero",
            ]

        return params

    def build_fallback_command(self, input_file, output_dir):
        """
        # Construit une commande minimale en cas d'erreur
        """
        logger.warning(f"[{self.channel_name}] ⚠️ Utilisation de la commande de secours")
        return [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            FFMPEG_LOG_LEVEL,
            "-y",
            "-re",
            "-i",
            str(input_file),
            "-c",
            "copy",
            "-f",
            "hls",
            "-hls_time",
            "6",
            "-hls_list_size",
            "5",
            "-hls_flags",
            "delete_segments",
            "-hls_segment_filename",
            f"{output_dir}/segment_%d.ts",
            f"{output_dir}/playlist.m3u8",
        ]

    def detect_mkv_in_playlist(self, playlist_file):
        """
        # Détecte si la playlist contient des fichiers MKV
        """
        try:
            if not Path(playlist_file).exists():
                return False

            with open(playlist_file, "r") as f:
                content = f.read()
                return ".mkv" in content.lower()

        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur détection MKV: {e}")
            return False

    def optimize_for_hardware(self):
        """
        # Optimise les paramètres pour le hardware disponible avec meilleure détection
        """
        try:
            # Détection VAAPI plus robuste
            if self.use_gpu:
                # Essai direct de VAAPI
                test_cmd = [
                    "ffmpeg",
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-hwaccel",
                    "vaapi",
                    "-hwaccel_output_format",
                    "vaapi",
                    "-vaapi_device",
                    "/dev/dri/renderD128",
                    "-f",
                    "lavfi",
                    "-i",
                    "color=black:s=1280x720",
                    "-vf",
                    "format=nv12|vaapi,hwupload",
                    "-c:v",
                    "h264_vaapi",
                    "-t",
                    "0.1",
                    "-f",
                    "null",
                    "-",
                ]
                result = subprocess.run(test_cmd, capture_output=True, text=True)

                if result.returncode == 0:
                    logger.info(f"[{self.channel_name}] ✅ Support VAAPI vérifié")
                    # On peut utiliser VAAPI
                    return True
                else:
                    logger.warning(
                        f"[{self.channel_name}] ⚠️ Test VAAPI échoué: {result.stderr}"
                    )
                    self.use_gpu = False
                    logger.info(f"[{self.channel_name}] 🔄 Basculement en mode CPU")

            # Détection des capacités CPU
            cpu_count = os.cpu_count() or 4
            if cpu_count <= 2:
                # Ajustements pour CPU faible
                logger.info(
                    f"[{self.channel_name}] ⚠️ CPU limité ({cpu_count} cœurs), ajustement des paramètres"
                )
                self.hls_time = 4  # Segments plus longs
                self.video_bitrate = "3M"  # Bitrate plus faible

            return True

        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur optimisation hardware: {e}")
            # En cas d'erreur, on désactive VAAPI
            self.use_gpu = False
            return False

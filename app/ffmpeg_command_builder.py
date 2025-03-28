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
                "-thread_queue_size", "8192",
                "-analyzeduration", "20M",
                "-probesize", "20M",
                "-re",  # Lecture en temps réel
                "-fflags", "+genpts+igndts+discardcorrupt+autobsf",
                "-threads", "4",
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
                "-max_muxing_queue_size", "4096",
                "-fps_mode", "passthrough",
            ])

            # Paramètres HLS optimisés
            command.extend([
                "-f", "hls",
                "-hls_time", "2",
                "-hls_list_size", "15",
                "-hls_delete_threshold", "2",
                "-hls_flags", "delete_segments+append_list+independent_segments+omit_endlist+discont_start+program_date_time",
                "-hls_allow_cache", "1",
                "-start_number", "0",
                "-hls_segment_type", "mpegts",
                "-max_delay", "2000000",
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

        # Paramètres de buffer (AVANT le -ss pour une meilleure analyse)
        params.extend(
            [
                "-thread_queue_size",
                "8192",  # Réduit pour plus de stabilité
                "-analyzeduration",
                "20M",  # Réduit pour un démarrage plus rapide
                "-probesize",
                "20M",  # Réduit pour un démarrage plus rapide
            ]
        )

        # # Code temporaire pour tester les transitions - commenté
        # try:
        #     with open(input_file, "r") as f:
        #         first_line = f.readline().strip()
        #         if first_line.startswith("file "):
        #             first_video = first_line.split("'")[1]
        #             cmd = [
        #                 "ffprobe",
        #                 "-v", "error",
        #                 "-show_entries", "format=duration",
        #                 "-of", "default=noprint_wrappers=1:nokey=1",
        #                 str(first_video)
        #             ]
        #             result = subprocess.run(cmd, capture_output=True, text=True)
        #             if result.returncode == 0 and result.stdout.strip():
        #                 try:
        #                     duration = float(result.stdout.strip())
        #                     if duration > 30:
        #                         # Ajouter l'offset temporaire (durée - 30 secondes)
        #                         params.extend(["-ss", str(duration - 30)])
        #                         logger.info(f"[{self.channel_name}] 🕒 Ajout offset temporaire: {duration - 30}s")
        #                 except ValueError:
        #                     pass
        # except Exception as e:
        #     logger.error(f"[{self.channel_name}] ❌ Erreur calcul offset temporaire: {e}")

        params.extend(
            [
                "-re",  # Lecture en temps réel
                "-fflags",
                "+genpts+igndts+discardcorrupt+autobsf",  # Ajout de autobsf pour meilleure gestion des transitions
                "-threads",
                "4",
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
            "2",  # Réduit pour plus de réactivité
            "-hls_list_size",
            "15",  # Réduit pour plus de stabilité
            "-hls_delete_threshold",
            "2",  # Réduit pour plus de stabilité
            # Flags HLS optimisés
            "-hls_flags",
            "delete_segments+append_list+independent_segments+omit_endlist+discont_start+program_date_time",  # Ajout de discont_start pour gérer les discontinuités et program_date_time pour compatibilité
            # Cache autorisé
            "-hls_allow_cache",
            "1",
            # Numérotation continue des segments
            "-start_number",
            "0",
            "-hls_segment_type",
            "mpegts",
            # Paramètres de latence réduite
            "-max_delay",
            "2000000",  # Réduit pour plus de stabilité
            "-hls_init_time",
            "1",  # Réduit pour générer la playlist plus rapidement
            # Nom des segments
            "-hls_segment_filename",
            f"{output_dir}/segment_%d.ts",
            # Sortie playlist
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
            "4096",  # Buffer augmenté pour plus de stabilité
            "-fps_mode",
            "passthrough",
            "-fflags",
            "+genpts+igndts+discardcorrupt+autobsf",  # Flags pour une meilleure gestion des timestamps
            "-thread_queue_size",
            "8192",  # Queue size augmentée pour plus de stabilité
            "-avoid_negative_ts",
            "make_zero",  # Évite les timestamps négatifs
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

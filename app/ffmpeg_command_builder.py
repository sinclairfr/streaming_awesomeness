# ffmpeg_command_builder.py
import os
import json
import subprocess
from pathlib import Path
from config import (
    FFMPEG_LOG_LEVEL,
    logger
)
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
        self.hls_list_size = 10
        self.hls_delete_threshold = 1
        self.gop_size = 48
        self.keyint_min = 48
        self.video_bitrate = "5M"
        self.max_bitrate = "5M"
        self.buffer_size = "10M"
        
    def build_command(self, input_file, output_dir, playback_offset=0, progress_file=None, has_mkv=False):
        """
        # Construit la commande FFmpeg complète
        
        Args:
            input_file (str): Chemin du fichier d'entrée (concat ou direct)
            output_dir (str): Dossier de sortie pour les segments HLS
            playback_offset (float): Offset de lecture en secondes
            progress_file (str): Fichier pour stocker la progression
            has_mkv (bool): Si la playlist contient des fichiers MKV
            
        Returns:
            list: La commande FFmpeg complète sous forme de liste
        """
        try:
            logger.info(f"[{self.channel_name}] 🛠️ Construction de la commande FFmpeg...")
            
            if playback_offset > 0:
                # Créer un filtre complexe pour le seek
                input_params = [
                    "ffmpeg", "-hide_banner", "-loglevel", FFMPEG_LOG_LEVEL, "-y",
                    "-thread_queue_size", "8192", "-analyzeduration", "20M", "-probesize", "20M",
                    "-f", "concat", "-safe", "0", "-i", str(input_file),
                    "-ss", f"{playback_offset:.2f}",  # Le -ss après l'input pour meilleure précision
                    "-fflags", "+genpts+igndts+discardcorrupt",
                    "-re", "-stream_loop", "-1"
                ]
            else:
                input_params = self.build_input_params(input_file, 0, progress_file)
            
            # Construction des parties de la commande
            input_params = self.build_input_params(input_file, playback_offset, progress_file)
            encoding_params = self.build_encoding_params(has_mkv)
            hls_params = self.build_hls_params(output_dir)
            
            # Assemblage de la commande complète
            command = input_params + encoding_params + hls_params
            
            # Log pour debug
            logger.debug(f"[{self.channel_name}] 📝 Commande: {' '.join(command)}")
            
            return command
            
        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur construction commande: {e}")
            # Fallback à une commande minimale en cas d'erreur
            return self.build_fallback_command(input_file, output_dir)

    def _create_concat_file(self) -> Optional[Path]:
        """Version ultra simplifiée pour débloquer la situation"""
        try:
            # Renomme tous les fichiers problématiques avec des noms basiques
            self._rename_all_videos_simple()
            
            # Crée une playlist simplifiée
            processed_dir = Path(CONTENT_DIR) / self.name / "processed"
            concat_file = Path(CONTENT_DIR) / self.name / "_playlist.txt"
            
            # Cherche tous les fichiers MP4
            processed_files = list(processed_dir.glob("*.mp4"))
            if not processed_files:
                logger.error(f"[{self.name}] Aucune vidéo dans {processed_dir}")
                return None
                
            # Écrit une playlist ultra basique
            with open(concat_file, "w") as f:
                for i, video in enumerate(sorted(processed_files)):
                    f.write(f"file '{video}'\n")
                    logger.debug(f"[{self.name}] Ajout de {video.name}")
                    
            return concat_file
        except Exception as e:
            logger.error(f"[{self.name}] Erreur playlist: {e}")
            return None

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
                        logger.info(f"[{self.name}] Source renommé: {video.name} -> {simple_name}")
                    except Exception as e:
                        logger.error(f"[{self.name}] Erreur renommage source {video.name}: {e}")
                        
            # Ensuite, on traite les fichiers du dossier processed
            for i, video in enumerate(processed_dir.glob("*.mp4")):
                if any(c in video.name for c in " ,;'\"()[]{}=+^%$#@!&~`|<>?"):
                    simple_name = f"processed_{i+1}.mp4"
                    new_path = video.parent / simple_name
                    try:
                        video.rename(new_path)
                        logger.info(f"[{self.name}] Processed renommé: {video.name} -> {simple_name}")
                    except Exception as e:
                        logger.error(f"[{self.name}] Erreur renommage processed {video.name}: {e}")
                        
        except Exception as e:
            logger.error(f"[{self.name}] Erreur renommage global: {e}")    
   
    def build_input_params(self, input_file, playback_offset=0, progress_file=None):
        """Construit les paramètres d'entrée avec meilleure gestion du buffer"""
        params = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel", FFMPEG_LOG_LEVEL,
            "-y"
        ]
        
        # Paramètres de buffer réduits pour diminuer la latence
        params.extend([
            "-thread_queue_size", "1024",  # Réduit de 8192
            "-analyzeduration", "5M",      # Réduit de 20M
            "-probesize", "5M"             # Réduit de 20M
        ])
        
        if playback_offset > 0:
            params.extend([
                "-ss", f"{playback_offset:.2f}"
            ])
        
        params.extend([
            "-re",
            "-stream_loop", "-1",
            "-fflags", "+genpts+igndts+discardcorrupt+nobuffer",  # Ajouté nobuffer pour réduire la latence
            "-threads", "2",  # Réduit pour moins de charge CPU
            "-avoid_negative_ts", "make_zero"
        ])
        
        if progress_file:
            params.extend(["-progress", str(progress_file)])
        
        params.extend([
            "-f", "concat",
            "-safe", "0",
            "-i", str(input_file)
        ])
        
        return params
        
    def build_hls_params(self, output_dir):
        """
        # Construit les paramètres HLS optimisés pour éviter les sauts de segments
        """
        return [
            "-f", "hls",
            "-hls_time", "1",  # Réduit de 2 à 1 seconde pour un démarrage plus rapide
            "-hls_list_size", "8",  # Playlist plus courte pour moins de mémoire
            "-hls_delete_threshold", "2",  # Suppression plus rapide des segments obsolètes
            "-hls_flags", "delete_segments+append_list+program_date_time+independent_segments+split_by_time",  # Retiré round_durations+omit_endlist qui peuvent causer des problèmes
            "-hls_allow_cache", "1",
            "-start_number", "0",
            "-hls_segment_type", "mpegts",
            "-max_delay", "500000",  # Réduit pour une latence plus faible (de 2000000 à 500000)
            "-hls_init_time", "0.5",  # Réduit pour démarrage plus rapide (de 2 à 0.5)
            "-hls_segment_filename", f"{output_dir}/segment_%d.ts",
            f"{output_dir}/playlist.m3u8"
        ]
    
    def build_encoding_params(self, has_mkv=False):
        """Construit les paramètres d'encodage optimisés pour la rapidité"""
        logger.info(f"[{self.channel_name}] 📼 Paramètres d'encodage optimisés")
        
        # Mode copie directe si possible (bien plus rapide)
        use_copy = os.getenv('SKIP_NORMALIZATION', '0') == '1'
        
        if use_copy:
            logger.info(f"[{self.channel_name}] 🚀 Mode copie directe activé")
            return [
                "-c:v", "copy",
                "-c:a", "copy",
                "-sn", "-dn",
                "-map", "0:v:0", 
                "-map", "0:a:0?",
                "-max_muxing_queue_size", "4096"
            ]
        
        # Paramètres CPU optimisés pour la rapidité
        params = [
            "-c:v", "libx264",
            "-profile:v", "baseline",  # Plus simple/rapide que "main"
            "-preset", "ultrafast",
            "-tune", "zerolatency",  # Crucial pour le streaming en direct
            "-x264opts", "no-scenecut", # Évite les sauts de keyframes
            "-b:v", "3M",  # Bitrate plus faible mais suffisant
            "-maxrate", "4M",
            "-bufsize", "4M",  # Buffer plus petit pour moins de latence
            "-g", "30",  # GOP plus court pour meilleure réactivité
            "-force_key_frames", "expr:gte(t,n_forced*1)",  # Force keyframe chaque seconde
            "-max_muxing_queue_size", "1024"  # Plus petit pour moins de mise en cache
        ]
        

        # Paramètres GPU si activé
        if use_gpu_actual:
            logger.info(f"[{self.channel_name}] 🖥️ Utilisation de l'accélération GPU (VAAPI)")
            params.extend([
                "-vf", "format=nv12|vaapi,hwupload",
                "-low_power", "1"  # Mode basse consommation
            ])
        else:
            logger.info(f"[{self.channel_name}] 💻 Utilisation du CPU pour l'encodage")
   
        # Paramètres audio
        params.extend([
            "-c:a", "aac",
            "-b:a", "192k",
            "-ar", "48000",
            "-ac", "2"  # Force stereo
        ])
        
        # Désactivation explicite des sous-titres et pistes de données
        params.extend([
            "-sn",  # Pas de sous-titres
            "-dn",  # Pas de flux de données
            "-map", "0:v:0",  # Map uniquement le premier flux vidéo
            "-map", "0:a:0"   # Map uniquement le premier flux audio
        ])
        
        return params

    def build_fallback_command(self, input_file, output_dir):
        """
        # Construit une commande minimale en cas d'erreur
        """
        logger.warning(f"[{self.channel_name}] ⚠️ Utilisation de la commande de secours")
        return [
            "ffmpeg", "-hide_banner", "-loglevel", FFMPEG_LOG_LEVEL,
            "-y", "-re",
            "-i", str(input_file),
            "-c", "copy",
            "-f", "hls",
            "-hls_time", "6",
            "-hls_list_size", "5",
            "-hls_flags", "delete_segments+append_list",
            "-hls_segment_filename", f"{output_dir}/segment_%d.ts",
            f"{output_dir}/playlist.m3u8"
        ]
    
    def detect_mkv_in_playlist(self, playlist_file):
        """
        # Détecte si la playlist contient des fichiers MKV
        """
        try:
            if not Path(playlist_file).exists():
                return False
                
            with open(playlist_file, 'r') as f:
                content = f.read()
                return '.mkv' in content.lower()
                
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
                    'ffmpeg', '-hide_banner', '-loglevel', 'error',
                    '-hwaccel', 'vaapi', '-hwaccel_output_format', 'vaapi',
                    '-vaapi_device', '/dev/dri/renderD128',
                    '-f', 'lavfi', '-i', 'color=black:s=1280x720', 
                    '-vf', 'format=nv12|vaapi,hwupload', 
                    '-c:v', 'h264_vaapi', '-t', '0.1',
                    '-f', 'null', '-'
                ]
                result = subprocess.run(test_cmd, capture_output=True, text=True)
                
                if result.returncode == 0:
                    logger.info(f"[{self.channel_name}] ✅ Support VAAPI vérifié")
                    # On peut utiliser VAAPI
                    return True
                else:
                    logger.warning(f"[{self.channel_name}] ⚠️ Test VAAPI échoué: {result.stderr}")
                    self.use_gpu = False
                    logger.info(f"[{self.channel_name}] 🔄 Basculement en mode CPU")
            
            # Détection des capacités CPU
            cpu_count = os.cpu_count() or 4
            if cpu_count <= 2:
                # Ajustements pour CPU faible
                logger.info(f"[{self.channel_name}] ⚠️ CPU limité ({cpu_count} cœurs), ajustement des paramètres")
                self.hls_time = 4  # Segments plus longs
                self.video_bitrate = "3M"  # Bitrate plus faible
            
            return True
            
        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur optimisation hardware: {e}")
            # En cas d'erreur, on désactive VAAPI
            self.use_gpu = False
            return False
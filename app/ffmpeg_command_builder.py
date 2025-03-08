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
        """Construit la commande FFmpeg complète"""
        try:
            logger.info(f"[{self.channel_name}] 🛠️ Construction de la commande FFmpeg...")
            
            # Construction des parties de la commande
            input_params = self.build_input_params(input_file, playback_offset, progress_file)
            encoding_params = self.build_encoding_params(has_mkv)
            hls_params = self.build_hls_params(output_dir)
            
            # Assemblage de la commande complète
            command = input_params + encoding_params + hls_params
            
            # Vérification et correction du chemin de sortie
            output_file = command[-1]
            if not output_file.startswith('/app/hls/'):
                # On corrige le chemin de sortie
                corrected_path = f"/app/hls/{self.channel_name}/playlist.m3u8"
                command[-1] = corrected_path
                logger.info(f"[{self.channel_name}] 🛠️ Correction du chemin de sortie: {output_file} -> {corrected_path}")
            
            # Log pour debug
            logger.info(f"[{self.channel_name}] 📝 Commande: {' '.join(command)}")
            
            # Juste avant le return command
            output_path = command[-1]
            if '/app/hls' in output_path and not '/app/hls/' in output_path:
                fixed_path = output_path.replace('/app/hls', '/app/hls/')
                command[-1] = fixed_path
                logger.info(f"[{self.channel_name}] 🔧 Correction chemin: {output_path} → {fixed_path}")
            
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
        """Construit les paramètres d'entrée avec meilleur positionnement initial"""
        params = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel", FFMPEG_LOG_LEVEL,
            "-y"
        ]
        
        # IMPORTANT: Le placement du -ss AVANT l'input permet un positionnement précis
        if playback_offset > 0:
            params.extend([
                "-ss", f"{playback_offset:.2f}"
            ])
        
        # Paramètres de buffer pour stabilité
        params.extend([
            "-thread_queue_size", "16384",
            "-analyzeduration", "20M",
            "-probesize", "20M"
        ])
        
        # Après le seek, on applique les autres options
        params.extend([
            "-re",
            "-stream_loop", "-1",
            "-fflags", "+genpts+igndts+discardcorrupt",
            "-threads", "4",
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
        """Construit les paramètres HLS optimisés pour la stabilité et les changements d'offset"""
        return [
            "-f", "hls",
            "-hls_time", "6",                 # Augmenté de 4 à 6 secondes pour plus de stabilité
            "-hls_list_size", "30",           # Augmenté à 30 pour garder plus de segments
            "-hls_delete_threshold", "10",    # Augmenté pour éviter suppressions trop agressives
            "-hls_flags", "delete_segments+append_list+program_date_time+independent_segments+split_by_time",
            "-hls_allow_cache", "1",
            "-start_number", "0",
            "-hls_segment_type", "mpegts",
            "-max_delay", "5000000",          # Augmenté pour plus de tolérance
            "-hls_init_time", "6",            # Ajusté pour correspondre à hls_time
            "-force_key_frames", "expr:gte(t,n_forced*6)",  # Ajusté pour correspondre à hls_time
            "-sc_threshold", "0",             # Désactive la détection de changement de scène
            "-hls_segment_filename", f"{output_dir}/segment_%d.ts",
            f"{output_dir}/playlist.m3u8"
        ]
        
    def build_encoding_params(self, has_mkv=False):
        """Construit les paramètres d'encodage optimisés pour la copie directe"""
        logger.info(f"[{self.channel_name}] 📼 Paramètres optimisés pour la copie directe")
        
        # Par défaut, on privilégie la copie directe
        params = [
            "-c:v", "copy",                # Copie directe du flux vidéo
            "-c:a", "copy",                # Copie directe du flux audio
            "-sn", "-dn",                  # Pas de sous-titres/données
            "-map", "0:v:0",               # Premier flux vidéo uniquement
            "-map", "0:a:0?",              # Premier flux audio s'il existe
            "-max_muxing_queue_size", "4096"  # Buffer augmenté
        ]
        
        # Ajustements pour MKV si nécessaire
        if has_mkv:
            logger.info(f"[{self.channel_name}] ⚠️ Fichier MKV détecté, ajustement des paramètres")
            # Pour les MKV on peut avoir besoin de spécifier explicitement certains paramètres
            params = [
                "-c:v", "copy",
                "-c:a", "aac",             # Conversion audio en AAC pour compatibilité
                "-b:a", "192k",
                "-sn", "-dn",
                "-map", "0:v:0",
                "-map", "0:a:0?",
                "-max_muxing_queue_size", "8192"  # Buffer encore plus grand pour MKV
            ]
        
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
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
    def build_command_without_offset(self, input_file, output_dir, progress_file=None, has_mkv=False):
        """Construit une commande sans offset en cas d'échec de la première tentative"""
        logger.warning(f"[{self.channel_name}] ⚠️ Construction d'une commande sans offset")
        # Reste de la logique similaire à build_command mais sans le -ss
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
                    logger.info(f"[{self.name}] Ajout de {video.name}")
                    
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
        
        # Paramètres de buffer augmentés
        params.extend([
            "-thread_queue_size", "8192",
            "-analyzeduration", "20M", 
            "-probesize", "20M"
        ])
        
        # Ajout de l'offset AVANT l'input
        if playback_offset > 0:
            params.extend([
                "-ss", f"{playback_offset:.2f}"
            ])
        
        # Suite des paramètres
        params.extend([
            "-re",
            "-stream_loop", "-1",
            "-fflags", "+genpts+igndts+discardcorrupt+fastseek",
            "-threads", "4",
            "-avoid_negative_ts", "make_zero"
        ])
        
        # Ajout du fichier de progression
        if progress_file:
            params.extend(["-progress", str(progress_file)])
        
        # Ajout du fichier d'entrée
        params.extend([
            "-f", "concat",
            "-safe", "0",
            "-segment_time_metadata", "1",
            "-i", str(input_file)
        ])
        
        return params 
    
    # Mise à jour de la méthode build_hls_params dans ffmpeg_command_builder.py
    def build_hls_params(self, output_dir):
        """
        # Construit les paramètres HLS optimisés pour éviter les sauts de segments
        """
        return [
            "-f", "hls",
            "-hls_time", str(self.hls_time),  # Durée des segments
            "-hls_list_size", str(max(15, self.hls_list_size)),  # Plus de segments dans la playlist
            "-hls_delete_threshold", str(max(5, self.hls_delete_threshold)),  # Attendre plus longtemps avant suppression
            "-hls_flags", "delete_segments+append_list+program_date_time+independent_segments+split_by_time+round_durations",
            "-hls_allow_cache", "1",  # Autorise mise en cache des segments
            "-start_number", "0",
            "-hls_segment_type", "mpegts",
            "-max_delay", "2000000",  # Délai max réduit
            "-avoid_negative_ts", "make_zero",
            "-hls_init_time", "2",  # Durée initiale
            "-hls_segment_filename", f"{output_dir}/segment_%d.ts",
            f"{output_dir}/playlist.m3u8"
        ]

    def build_encoding_params(self, has_mkv=False):
        """
        # Construit les paramètres d'encodage adaptés
        """
        # Si on a des MKV, on utilise des paramètres optimisés
        if has_mkv:
            logger.info(f"[{self.channel_name}] 📼 Paramètres optimisés pour MKV")
            
            # Paramètres de base
            params = [
                "-c:v", "h264_vaapi" if self.use_gpu else "libx264",
                "-profile:v", "main",
                "-preset", "fast",
                "-level", "4.1",
                "-b:v", self.video_bitrate,
                "-maxrate", self.max_bitrate,
                "-bufsize", self.buffer_size,
                "-g", str(self.gop_size),
                "-keyint_min", str(self.keyint_min),
                "-sc_threshold", "0",  # Désactive la détection de changement de scène
                "-force_key_frames", "expr:gte(t,n_forced*2)"  # Force des keyframes toutes les 2s
            ]
            
            # Paramètres GPU si activé
            if self.use_gpu:
                params.extend([
                    "-vf", "format=nv12|vaapi,hwupload",
                    "-low_power", "1"  # Mode basse consommation
                ])
            
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
            
        # Mode copie pour fichiers déjà normalisés
        else:
            logger.info(f"[{self.channel_name}] 📄 Mode copie optimisé")
            params = [
                "-c:v", "copy",      # Copie du flux vidéo
                "-c:a", "aac",       # Force conversion audio en AAC
                "-b:a", "192k",      # Bitrate audio constant
                "-ac", "2",          # Force stereo
                "-sn",               # Pas de sous-titres
                "-dn",               # Pas de flux de données
                "-map", "0:v:0",     # Map uniquement le premier flux vidéo
                "-map", "0:a:0?",    # Map uniquement le premier flux audio s'il existe
                "-max_muxing_queue_size", "4096"  # Augmenté de 2048 à 4096
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
        # Optimise les paramètres pour le hardware disponible
        """
        try:
            # Détection des capacités GPU si activé
            if self.use_gpu:
                # Vérification VAAPI
                result = subprocess.run(['vainfo'], capture_output=True, text=True)
                if result.returncode == 0 and 'VAEntrypointVLD' in result.stdout:
                    logger.info(f"[{self.channel_name}] ✅ Support VAAPI détecté")
                    # On peut ajuster les paramètres pour VAAPI
                    self.hls_time = 2  # Segments plus courts pour le hardware
                    return True
                else:
                    logger.warning(f"[{self.channel_name}] ⚠️ VAAPI configuré mais non fonctionnel")
                    self.use_gpu = False
            
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
            return False
    
    def set_hls_params(self, hls_time=None, hls_list_size=None, hls_delete_threshold=None):
        """
        # Personnalise les paramètres HLS
        """
        if hls_time is not None:
            self.hls_time = hls_time
        if hls_list_size is not None:
            self.hls_list_size = hls_list_size
        if hls_delete_threshold is not None:
            self.hls_delete_threshold = hls_delete_threshold
    
    def set_video_params(self, video_bitrate=None, max_bitrate=None, buffer_size=None, gop_size=None, keyint_min=None):
        """
        # Personnalise les paramètres vidéo
        """
        if video_bitrate is not None:
            self.video_bitrate = video_bitrate
        if max_bitrate is not None:
            self.max_bitrate = max_bitrate
        if buffer_size is not None:
            self.buffer_size = buffer_size
        if gop_size is not None:
            self.gop_size = gop_size
        if keyint_min is not None:
            self.keyint_min = keyint_min
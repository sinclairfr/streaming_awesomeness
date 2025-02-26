from config import (
    logger,
    CONTENT_DIR,
    USE_GPU,
)
class MKVHandler:
    """
    # On ajoute une classe dédiée pour gérer les fichiers MKV avec des paramètres optimisés
    """
    def __init__(self, name: str, logger):
        self.name = name
        self.logger = logger
        
        # On définit des paramètres d'encodage optimisés pour MKV
        self.encoding_params = {
            'video': {
                'codec': 'h264_vaapi',  # Codec matériel si disponible
                'fallback_codec': 'libx264',  # Codec logiciel en fallback
                'profile': 'main',
                'preset': 'fast',  # Équilibre entre qualité et vitesse
                'crf': 23,  # Qualité constante, bon compromis
                'maxrate': '5M',
                'bufsize': '10M',
                'g': 48,  # GOP size
                'keyint_min': 48,
            },
            'audio': {
                'codec': 'aac',
                'bitrate': '192k',
                'sampling': '48000',
                'channels': '2',  # Stereo par défaut
            }
        }

    def build_mkv_input_params(self) -> list:
        """
        # On construit les paramètres d'entrée spécifiques pour MKV
        """
        return [
            '-hwaccel', 'vaapi',  # Activation du hardware encoding si disponible
            '-hwaccel_output_format', 'vaapi',
            '-hwaccel_device', '/dev/dri/renderD128',
            '-thread_queue_size', '4096',  # Buffer plus grand pour MKV
            '-strict', 'experimental',
            '-fflags', '+genpts+igndts+discardcorrupt',  # Gestion robuste des timestamps
            '-vsync', 'cfr',  # Force constant framerate
            '-analyzeduration', '100M',  # Augmente le temps d'analyse
            '-probesize', '100M'
        ]

    def build_mkv_video_params(self, use_gpu: bool) -> list:
        """
        # Paramètres vidéo optimisés pour MKV
        """
        codec = self.encoding_params['video']['codec'] if use_gpu else self.encoding_params['video']['fallback_codec']
        
        params = [
            '-c:v', codec,
            '-profile:v', self.encoding_params['video']['profile'],
            '-preset', self.encoding_params['video']['preset'],
            '-crf', str(self.encoding_params['video']['crf']),
            '-maxrate', self.encoding_params['video']['maxrate'],
            '-bufsize', self.encoding_params['video']['bufsize'],
            '-g', str(self.encoding_params['video']['g']),
            '-keyint_min', str(self.encoding_params['video']['keyint_min']),
            '-force_key_frames', 'expr:gte(t,n_forced*2)',  # Force keyframes toutes les 2 secondes
            '-x264opts', 'no-scenecut',  # Désactive scenecut pour stabilité
            '-movflags', '+faststart'  # Optimise pour streaming
        ]
        
        if use_gpu:
            params.extend([
                '-qp', '23',  # Qualité constante pour GPU
                '-low_power', '1'  # Mode basse consommation
            ])
            
        return params

    def build_mkv_audio_params(self) -> list:
        """
        # Paramètres audio optimisés
        """
        return [
            '-c:a', self.encoding_params['audio']['codec'],
            '-b:a', self.encoding_params['audio']['bitrate'],
            '-ar', self.encoding_params['audio']['sampling'],
            '-ac', self.encoding_params['audio']['channels'],
            '-filter:a', 'aresample=async=1000'  # Resynchronisation audio
        ]

    def verify_mkv(self, file_path: str) -> bool:
        """
        # On vérifie que le fichier MKV est valide et peut être traité
        """
        try:
            import subprocess
            
            # On vérifie d'abord la validité du conteneur
            probe_cmd = [
                'ffprobe',
                '-v', 'error',
                '-show_entries', 'format=duration',
                '-of', 'default=noprint_wrappers=1:nokey=1',
                file_path
            ]
            
            result = subprocess.run(probe_cmd, capture_output=True, text=True)
            
            if result.returncode != 0:
                self.logger.error(f"[{self.name}] MKV invalide: {file_path}")
                return False
                
            # On vérifie les streams vidéo et audio
            streams_cmd = [
                'ffprobe',
                '-v', 'error',
                '-select_streams', 'v:0',  # Premier stream vidéo
                '-show_entries', 'stream=codec_name,width,height,r_frame_rate',
                '-of', 'json',
                file_path
            ]
            
            streams = subprocess.run(streams_cmd, capture_output=True, text=True)
            
            if streams.returncode != 0:
                self.logger.error(f"[{self.name}] Streams MKV invalides: {file_path}")
                return False
                
            self.logger.info(f"[{self.name}] ✅ MKV validé: {file_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"[{self.name}] Erreur vérification MKV: {e}")
            return False

    def get_mkv_metadata(self, file_path: str) -> dict:
        """
        # On récupère les métadonnées importantes du MKV
        """
        try:
            import json
            import subprocess
            
            cmd = [
                'ffprobe',
                '-v', 'quiet',
                '-print_format', 'json',
                '-show_format',
                '-show_streams',
                file_path
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            data = json.loads(result.stdout)
            
            metadata = {
                'duration': float(data['format']['duration']),
                'size': int(data['format']['size']),
                'bitrate': int(data['format']['bit_rate']),
                'streams': []
            }
            
            for stream in data['streams']:
                stream_info = {
                    'codec_type': stream['codec_type'],
                    'codec_name': stream['codec_name']
                }
                
                if stream['codec_type'] == 'video':
                    stream_info.update({
                        'width': int(stream['width']),
                        'height': int(stream['height']),
                        'fps': eval(stream['r_frame_rate'])  # Calcule fps depuis fraction
                    })
                
                metadata['streams'].append(stream_info)
            
            return metadata
            
        except Exception as e:
            self.logger.error(f"[{self.name}] Erreur extraction métadonnées: {e}")
            return {}

    def optimize_encoding_params(self, metadata: dict) -> None:
        """
        # On ajuste les paramètres d'encodage selon les métadonnées
        """
        try:
            if not metadata:
                return
                
            # Ajustement bitrate selon résolution
            video_stream = next((s for s in metadata['streams'] if s['codec_type'] == 'video'), None)
            if video_stream:
                height = video_stream.get('height', 0)
                
                if height <= 720:
                    self.encoding_params['video'].update({
                        'maxrate': '3M',
                        'bufsize': '6M',
                        'crf': 21
                    })
                elif height <= 1080:
                    self.encoding_params['video'].update({
                        'maxrate': '5M',
                        'bufsize': '10M',
                        'crf': 23
                    })
                else:
                    self.encoding_params['video'].update({
                        'maxrate': '8M',
                        'bufsize': '16M',
                        'crf': 24
                    })
                    
                # Ajustement GOP size selon FPS
                fps = video_stream.get('fps', 30)
                self.encoding_params['video']['g'] = int(fps * 2)  # GOP de 2 secondes
                self.encoding_params['video']['keyint_min'] = int(fps)
                
            # Optimisation audio selon canaux disponibles
            audio_stream = next((s for s in metadata['streams'] if s['codec_type'] == 'audio'), None)
            if audio_stream:
                # On conserve les canaux d'origine si > 2
                if 'channels' in audio_stream and audio_stream['channels'] > 2:
                    self.encoding_params['audio']['channels'] = str(audio_stream['channels'])
                    # On augmente le bitrate pour multi-canaux
                    self.encoding_params['audio']['bitrate'] = '384k'
                
        except Exception as e:
            self.logger.error(f"[{self.name}] Erreur optimisation paramètres: {e}")
import os
import json
import subprocess
from pathlib import Path
from config import FFMPEG_LOG_LEVEL, logger, HLS_SEGMENT_DURATION
from typing import Optional


class FFmpegCommandBuilder:
    """
    # Optimized FFmpeg command generator
    # Adapts parameters based on file type, GPU presence, etc.
    """
    def __init__(self, channel_name, use_gpu=False):
        self.channel_name = channel_name
        self.use_gpu = use_gpu

        # Default parameters - adjusted for stability
        self.hls_time = HLS_SEGMENT_DURATION
        self.hls_list_size = 20
        self.hls_delete_threshold = 7
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
        """Builds the complete FFmpeg command with optimized parameters"""
        try:
            logger.info(
                f"[{self.channel_name}] 🛠️ Building FFmpeg command..."
            )

            # Base command parameters
            command = [
                "ffmpeg",
                "-hide_banner",
                "-loglevel", "warning",
                "-y",
                "-thread_queue_size", "32768",
                "-analyzeduration", "20M",
                "-probesize", "20M",
                "-re",
                "-fflags", "+genpts+discardcorrupt+nobuffer+flush_packets",
                "-err_detect", "aggressive",
                "-threads", "0",
                "-thread_type", "slice",
                "-avoid_negative_ts", "make_zero",
            ]

            # Add progress file if provided
            if progress_file:
                command.extend(["-progress", str(progress_file)])

            # Add input
            command.extend(["-i", str(input_file)])

            # Copy parameters
            command.extend([
                "-c:v", "copy",
                "-c:a", "copy",
                "-copyts",
                "-start_at_zero",
                "-sn", "-dn",
                "-map", "0:v:0",
                "-map", "0:a",
                "-max_muxing_queue_size", "16384",
                # IMPORTANT: Force flush des buffers pour éviter la perte de fin de vidéo
                "-flush_packets", "1",
                "-max_interleave_delta", "0",
            ])

            # HLS parameters
            hls_params = self.build_hls_params(output_dir)
            command.extend(hls_params)

            # Log the complete command
            logger.info("=" * 80)
            logger.info(f"[{self.channel_name}] 🚀 Launching FFmpeg command:")
            logger.info(f"$ {' '.join(command)}")
            logger.info("=" * 80)

            return command
        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Command build error: {e}")
            # Retry logic is implicitly handled by potentially adding flags below
            # We'll construct a potentially modified command if needed
            logger.warning(f"[{self.channel_name}] ⚠️ Error during command build: {e}. Will proceed but stability might be affected.")
            # If the command list exists but failed later, return it as is for now
            # Or potentially add more robust retry flags here if needed
            # For now, just return the partially built command if it exists
            if 'command' in locals() and command:
                 # Add more aggressive flags ONLY on error *during build* (less common)
                 # or consider moving retry logic to the process execution phase
                 command.extend([
                    "-max_error_rate", "0.5",  # More tolerant to errors for retry
                    "-err_detect", "ignore_err",  # More aggressive error detection for retry
                 ])
                 logger.warning(f"[{self.channel_name}] ⚠️ Added fallback error flags due to build exception.")
                 return command
            else:
                 # If command doesn't even exist, we can't proceed
                 logger.critical(f"[{self.channel_name}] 💥 Cannot build even a base command.")
                 return None # Or raise exception

    def _rename_all_videos_simple(self):
        """Renames all problematic files with ultra simple names"""
        try:
            processed_dir = Path(CONTENT_DIR) / self.name / "processed"
            if not processed_dir.exists():
                processed_dir.mkdir(parents=True, exist_ok=True)

            source_dir = Path(CONTENT_DIR) / self.name

            # First, process source files
            for i, video in enumerate(source_dir.glob("*.mp4")):
                if any(c in video.name for c in " ,;'\"()[]{}=+^%$#@!&~`|<>?"):
                    simple_name = f"video_{i+1}.mp4"
                    new_path = video.parent / simple_name
                    try:
                        video.rename(new_path)
                        logger.info(
                            f"[{self.name}] Source renamed: {video.name} -> {simple_name}"
                        )
                    except Exception as e:
                        logger.error(
                            f"[{self.name}] Error renaming source {video.name}: {e}"
                        )

            # Then, process files in the processed folder
            for i, video in enumerate(processed_dir.glob("*.mp4")):
                if any(c in video.name for c in " ,;'\"()[]{}=+^%$#@!&~`|<>?"):
                    simple_name = f"processed_{i+1}.mp4"
                    new_path = video.parent / simple_name
                    try:
                        video.rename(new_path)
                        logger.info(
                            f"[{self.name}] Processed renamed: {video.name} -> {simple_name}"
                        )
                    except Exception as e:
                        logger.error(
                            f"[{self.name}] Error renaming processed {video.name}: {e}"
                        )

        except Exception as e:
            logger.error(f"[{self.name}] Global renaming error: {e}")

    def build_input_params(self, input_file, playback_offset=0, progress_file=None):
        """Builds input parameters with precise positioning"""
        params = ["ffmpeg", "-hide_banner", "-loglevel", FFMPEG_LOG_LEVEL, "-y"]

        # Optimized buffer parameters to improve buffering
        params.extend(
            [
                "-thread_queue_size",
                "16384",  # Increased for better stability
                "-analyzeduration",
                "15M",  # Increased for more accurate analysis
                "-probesize",
                "15M",  # Increased for better detection
            ]
        )

        params.extend(
            [
                "-re",  # Real-time reading
                # Removed nobuffer flag for better buffering
                "-fflags",
                "+genpts+igndts+discardcorrupt+autobsf",
                "-threads",
                "8",  # Increased for better performance
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
        Builds optimized HLS parameters for smooth playback using defaults from __init__
        """
        hls_time_val = getattr(self, 'hls_time', 5)
        hls_list_size_val = 30
        hls_delete_threshold_val = 15

        return [
            "-force_key_frames", f"expr:gte(t,n_forced*{hls_time_val})",
            "-f",
            "hls",
            "-hls_time", str(hls_time_val),
            "-hls_list_size", str(hls_list_size_val),
            "-hls_delete_threshold", str(hls_delete_threshold_val),
            # IMPORTANT: round_durations pour s'assurer que les segments sont correctement terminés
            "-hls_flags", "delete_segments+append_list+independent_segments+omit_endlist+program_date_time+round_durations",
            "-use_wallclock_as_timestamps", "1",
            "-flags", "low_delay",
            "-avioflags", "direct",
            "-hls_allow_cache", "1",
            "-start_number", "0",
            "-hls_segment_type", "mpegts",
            "-max_delay", "2000000",
            "-hls_init_time", "2",
            "-hls_segment_filename", f"{output_dir}/segment_%d.ts",
            f"{output_dir}/playlist.m3u8",
        ]

    def build_encoding_params(self, has_mkv=False):
        """Builds optimized encoding parameters for direct copy"""
        logger.info(
            f"[{self.channel_name}] 📼 Optimized parameters for direct copy"
        )

        # By default, we prioritize direct copy with optimized parameters
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
            "8192",  # Increased to avoid blocking
            "-fps_mode",
            "passthrough",
            # Removed nobuffer flag for better buffering
            "-fflags",
            "+genpts+igndts+discardcorrupt+autobsf",
            "-thread_queue_size",
            "16384",  # Increased for better stability
            "-avoid_negative_ts",
            "make_zero",
        ]

        # If an MKV file is detected, adjust parameters
        if has_mkv:
            logger.info(
                f"[{self.channel_name}] ⚠️ MKV file detected, adjusting parameters"
            )
            # For MKV we may need to explicitly specify certain parameters
            params = [
                "-c:v",
                "copy",
                "-c:a",
                "aac",  # Audio conversion to AAC for compatibility
                "-b:a",
                "192k",
                "-sn",
                "-dn",
                "-map",
                "0:v:0",
                "-map",
                "0:a:0?",
                "-max_muxing_queue_size",
                "16384",  # Increased even more for MKV
                "-fps_mode",
                "passthrough",
                # Removed nobuffer flag for better buffering
                "-fflags",
                "+genpts+igndts+discardcorrupt",
                "-thread_queue_size",
                "32768",  # Even larger queue size for MKV
                "-avoid_negative_ts",
                "make_zero",
            ]

        return params

    def detect_mkv_in_playlist(self, playlist_file):
        """
        # Detects if the playlist contains MKV files
        """
        try:
            if not Path(playlist_file).exists():
                logger.debug(f"[{self.channel_name}] ℹ️ Playlist file not found: {playlist_file}")
                return False

            # Try different encodings if UTF-8 fails
            encodings = ['utf-8', 'latin-1', 'cp1252']
            content = None
            
            for encoding in encodings:
                try:
                    with open(playlist_file, "r", encoding=encoding) as f:
                        content = f.read()
                        break
                except UnicodeDecodeError:
                    continue
                    
            if content is None:
                logger.warning(f"[{self.channel_name}] ⚠️ Could not read playlist file with any encoding")
                return False

            # Check for MKV extension in a case-insensitive way
            return ".mkv" in content.lower()

        except Exception as e:
            logger.warning(f"[{self.channel_name}] ⚠️ MKV detection warning: {e}")
            # Return False on error to use default parameters
            return False

    def optimize_for_hardware(self):
        """
        # Optimizes parameters for available hardware with better detection
        """
        try:
            # More robust VAAPI detection
            if self.use_gpu:
                # Direct VAAPI test
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
                    logger.info(f"[{self.channel_name}] ✅ VAAPI support verified")
                    # We can use VAAPI
                    return True
                else:
                    logger.warning(
                        f"[{self.channel_name}] ⚠️ VAAPI test failed: {result.stderr}"
                    )
                    self.use_gpu = False
                    logger.info(f"[{self.channel_name}] 🔄 Switching to CPU mode")

            # CPU capabilities detection
            cpu_count = os.cpu_count() or 4
            if cpu_count <= 2:
                # Adjustments for low CPU
                logger.info(
                    f"[{self.channel_name}] ⚠️ Limited CPU ({cpu_count} cores), adjusting parameters"
                )
                self.hls_time = 6  # Longer segments
                self.video_bitrate = "3M"  # Lower bitrate
            elif cpu_count >= 8:
                # High-end CPU optimizations
                logger.info(
                    f"[{self.channel_name}] 💪 Powerful CPU ({cpu_count} cores), optimizing parameters"
                )
                self.threads = min(cpu_count - 2, 16)  # Keep some cores free for system

            return True

        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Hardware optimization error: {e}")
            # In case of error, disable VAAPI
            self.use_gpu = False
            return False

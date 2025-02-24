    def _monitor_ffmpeg(self, hls_dir: str):
        """Surveille le processus FFmpeg et gère les erreurs"""
        if not self.ffmpeg_process or not self.ffmpeg_process.stderr:
            logger.error(f"[{self.name}] ❌ ffmpeg_process n'est pas initialisé correctement")
            return
    
        last_segment_log = 0
        SEGMENT_LOG_INTERVAL = 30  # Log des segments toutes les 30 secondes
        
        try:
            while self.ffmpeg_process:
                
                current_time = time.time()
                # On met à jour la position de lecture
                if self.logger and self.logger.get_progress_file():
                    self._update_playback_position(self.logger.get_progress_file())
                else:
                    logging.warning("{self.name} - No progress file found for monitoring.")
                      
                # Log périodique des segments
                if current_time - last_segment_log > SEGMENT_LOG_INTERVAL:
                    self._check_segments(hls_dir)
                    last_segment_log = current_time
                
                # Vérifier si le processus est toujours en vie
                if self.ffmpeg_process.poll() is not None:
                    logger.error(f"[{self.name}] ❌ FFmpeg s'est arrêté avec code: {self.ffmpeg_process.returncode}")
                    self.error_handler.add_error("PROCESS_DIED")
                    break

                # Lire la sortie FFmpeg
                for line in iter(self.ffmpeg_process.stderr.readline, b''):
                    if line:
                        line = line.decode('utf-8').strip()
                        if "error" in line.lower():
                            error_type = self._categorize_ffmpeg_error(line)
                            if self.error_handler.add_error(error_type):
                                logger.error(f"[{self.name}] Erreur FFmpeg critique: {line}")
                                self._restart_stream()
                        elif "warning" in line.lower():
                            logger.warning(f"[{self.name}] Warning FFmpeg: {line}")
                        else:
                            logger.debug(f"[{self.name}] FFmpeg: {line}")

                # Vérifier les segments HLS
                self._check_segments(hls_dir)

                # Vérifier les timeouts
                current_time = time.time()
                if self._handle_timeouts(current_time, 300):  # 5 minutes sans nouveaux segments
                    logger.error(f"[{self.name}] ⏱️ Timeout détecté")
                    self._restart_stream()

                # Vérifier l'inactivité des viewers
                if self._check_viewer_inactivity(current_time, 3600):  # 1 heure sans viewer
                    logger.info(f"[{self.name}] 💤 Stream arrêté pour inactivité")
                    self.stop_stream()
                    break

            # Lecture de la sortie FFmpeg
            for line in iter(self.ffmpeg_process.stderr.readline, b''):
                if line:
                    line = line.decode('utf-8').strip()
                    if "error" in line.lower():
                        error_type = self._categorize_ffmpeg_error(line)
                        if self.error_handler.add_error(error_type):
                            logger.error(f"[{self.name}] Erreur FFmpeg critique: {line}")
                            self._restart_stream()
                    elif "warning" in line.lower():
                        logger.warning(f"[{self.name}] Warning FFmpeg: {line}")
                    else:
                        logger.debug(f"[{self.name}] FFmpeg: {line}")

                # Vérifications périodiques
                if self._handle_timeouts(current_time, 300):
                    logger.error(f"[{self.name}] ⏱️ Timeout détecté")
                    self._restart_stream()

                if self._check_viewer_inactivity(current_time, 3600):
                    logger.info(f"[{self.name}] 💤 Stream arrêté pour inactivité")
                    self.stop_stream()
                    break

                time.sleep(1)  # Éviter une utilisation CPU excessive

        except Exception as e:
            logger.error(f"[{self.name}] Erreur monitoring FFmpeg: {e}")
            self.error_handler.add_error(f"MONITOR_ERROR: {str(e)}")
        finally:
            self._clean_processes()

    def _update_playback_position(self, progress_file):
        """Met à jour et log la position de lecture actuelle"""
        if progress_file.exists():
            try:
                with open(progress_file, 'r') as f:
                    content = f.read()
                    if 'out_time_ms=' in content:
                        position_lines = [l for l in content.split('\n') if 'out_time_ms=' in l]
                        if position_lines:
                            ms_value = int(position_lines[-1].split('=')[1])
                            
                            # Correction pour valeurs négatives
                            if ms_value < 0:
                                # Utilise la durée totale calculée (en microsecondes)
                                total_duration_us = self._calculate_total_duration() * 1_000_000
                                # Convertit la position négative en une position valide positive
                                ms_value = total_duration_us + ms_value
                            
                            # Convertit en secondes
                            self.current_position = ms_value / 1_000_000
                            
                            # Log chaque 10 secondes
                            if time.time() % 10 < 1:
                                logger.info(f"⏱️ {self.name} - Position: {self.current_position:.2f}s")
            except Exception as e:
                logger.error(f"Erreur mise à jour position lecture: {e}")

    def log_ffmpeg_processes(self):
        """On vérifie et log le nombre de processus FFmpeg uniquement s'il y a un changement"""
        ffmpeg_count = 0
        for proc in psutil.process_iter(attrs=["name", "cmdline"]):
            try:
                if ("ffmpeg" in proc.info["name"].lower() and
                    proc.info.get("cmdline") and  # On vérifie que cmdline existe
                    any(self.name in str(arg) for arg in proc.info["cmdline"])):
                    ffmpeg_count += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        # On stocke le dernier count connu
        if not hasattr(self, '_last_ffmpeg_count'):
            self._last_ffmpeg_count = -1

        # On log uniquement si le nombre a changé
        if ffmpeg_count != self._last_ffmpeg_count:
            logger.warning(f"📊 {self.name}: {ffmpeg_count} processus FFmpeg actifs")
            self._last_ffmpeg_count = ffmpeg_count
    
    def _check_inactivity(self, current_time: float) -> bool:
        """Vérifie si le flux est réellement inactif"""
        
        # Temps depuis la dernière requête client
        time_since_last_request = current_time - self.last_watcher_time
        
        # Temps depuis le dernier segment demandé
        time_since_last_segment = current_time - getattr(self, 'last_segment_time', 0)
        
        # Si l'un des deux est actif récemment, le flux n'est pas inactif
        if time_since_last_request < TIMEOUT_NO_VIEWERS or time_since_last_segment < TIMEOUT_NO_VIEWERS:
            return False
            
        logger.warning(
            f"⚠️ {self.name} - Inactivité détectée:"
            f"\n- Dernière requête: il y a {time_since_last_request:.1f}s"
            f"\n- Dernier segment: il y a {time_since_last_segment:.1f}s"
        )
        return True

    def _verify_file_streams(self, file_path: str) -> dict:
        """Analyse les streams présents dans le fichier"""
        try:
            cmd = [
                "ffprobe", "-v", "quiet",
                "-print_format", "json",
                "-show_streams",
                str(file_path)
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            data = json.loads(result.stdout)
            
            streams = {
                'video': 0,
                'audio': 0,
                'subtitle': 0,
                'data': 0
            }
            
            for stream in data.get('streams', []):
                stream_type = stream.get('codec_type')
                if stream_type in streams:
                    streams[stream_type] += 1
                    
            logger.info(f"[{self.name}] 📊 Streams détectés dans {file_path}:")
            for type_, count in streams.items():
                logger.info(f"  - {type_}: {count}")
                
            return streams
            
        except Exception as e:
            logger.error(f"[{self.name}] ❌ Erreur analyse streams: {e}")
            return {}
      
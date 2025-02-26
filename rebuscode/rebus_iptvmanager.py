
    def _start_ready_channel(self, channel: IPTVChannel):
        """# On démarre rapidement une chaîne déjà prête"""
        try:
            logger.info(f"Démarrage rapide de la chaîne {channel.name}")
            if channel._scan_videos():
                if channel.start_stream():
                    logger.info(f"✅ {channel.name} démarrée")
                    #self.generate_master_playlist()
                else:
                    logger.error(f"❌ Échec démarrage {channel.name}")
            else:
                logger.error(f"❌ Échec scan vidéos {channel.name}")
        except Exception as e:
            logger.error(f"Erreur démarrage {channel.name}: {e}")

    def _start_channel(self, channel: IPTVChannel) -> bool:
        """# On tente de démarrer une chaîne"""
        try:
            if psutil.cpu_percent() > self.CPU_THRESHOLD:
                logger.warning(f"CPU trop chargé pour {channel.name}")
                return False

            if not channel._scan_videos():
                logger.error(f"Aucune vidéo valide pour {channel.name}")
                return False

            start_time = time.time()
            if not channel.start_stream():
                return False

            # On attend l'apparition des segments
            while time.time() - start_time < 10:
                if list(Path(f"/app/hls/{channel.name}").glob("*.ts")):
                    logger.info(f"✅ Chaîne {channel.name} démarrée avec succès")
                    return True
                time.sleep(0.5)

            logger.error(f"❌ Timeout démarrage {channel.name}")
            return False
        except Exception as e:
            logger.error(f"Erreur démarrage {channel.name}: {e}")
            return False

    def _is_channel_ready(self, channel_name: str) -> bool:
        """# On vérifie si une chaîne a des vidéos traitées"""
        try:
            channel_dir = Path(f"{CONTENT_DIR}/{channel_name}")
            processed_dir = channel_dir / "processed"
            if not processed_dir.exists():
                return False

            processed_videos = [
                f for f in processed_dir.glob("*.mp4") if not f.name.startswith("temp_")
            ]
            if not processed_videos:
                return False

            source_videos = [
                f for f in channel_dir.glob("*.*")
                if f.suffix.lower() in self.VIDEO_EXTENSIONS
                and not f.name.startswith("temp_")
                and f.parent == channel_dir
            ]
            processed_names = {v.stem for v in processed_videos}
            source_names = {v.stem for v in source_videos}
            return processed_names >= source_names and len(processed_videos) > 0
        except Exception as e:
            logger.error(f"Erreur is_channel_ready {channel_name}: {e}")
            return False

    def _update_channel_playlist(self, channel: IPTVChannel, channel_dir: Path):
        """# On met à jour la playlist d'une chaîne existante"""
        try:
            new_videos = self._scan_new_videos(channel_dir)
            if not new_videos:
                logger.debug(f"Pas de nouveau contenu pour {channel.name}")
                return

            from video_processor import VideoProcessor
            processor = VideoProcessor(str(channel_dir), use_gpu=self.use_gpu)
            processor.process_videos_async()
            new_processed = processor.wait_for_completion()
            if not new_processed:
                logger.error(f"Échec traitement nouveaux fichiers: {channel.name}")
                return

            channel.processed_videos.extend(new_processed)
            channel.processed_videos.sort()

            concat_file = channel._create_concat_file()
            if not concat_file:
                logger.error(f"Échec création concat file: {channel.name}")
                return

        except Exception as e:
            logger.error(f"Erreur mise à jour {channel.name}: {e}")
            logger.error(traceback.format_exc())


    def _clean_channel(self, channel_name: str):
        """Nettoie une chaîne"""
        try:
            if channel_name in self.channels:
                channel = self.channels[channel_name]

                # Vérifier la playlist avant nettoyage
                channel._verify_playlist()

                # Arrêter le stream si actif
                if channel.ffmpeg_process:
                    channel.ffmpeg_process.terminate()
                    channel.ffmpeg_process = None

                # Nettoyer les fichiers HLS
                if self.hls_cleaner:
                    self.hls_cleaner.cleanup_channel(channel_name)

                # Supprimer la chaîne
                del self.channels[channel_name]
                logger.info(f"🧹 Chaîne nettoyée: {channel_name}")

        except Exception as e:
            logger.error(f"Erreur nettoyage {channel_name}: {e}")

    def _signal_handler(self, signum, frame):
        """# On gère les signaux système"""
        logger.info(f"Signal {signum} reçu, nettoyage...")
        self.cleanup()
        sys.exit(0)

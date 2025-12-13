from watchdog.events import FileSystemEventHandler
import threading
import time
import logging
from pathlib import Path
from base_file_event_handler import BaseFileEventHandler
import os
import json

logger = logging.getLogger(__name__)

class ReadyContentHandler(BaseFileEventHandler):
    """Surveille les modifications dans les dossiers ready_to_stream"""

    def __init__(self, manager):
        """Initialise le gestionnaire d'événements"""
        self.manager = manager
        super().__init__(manager)  # Call the parent class's __init__ method

    def _handle_event(self, event):
        """Gère tous les événements détectés dans ready_to_stream"""
        try:
            if event.is_directory:
                return

            path = Path(event.src_path)
            if not path.exists():
                return

            # Le format attendu est */content/{channel_name}/ready_to_stream/*.mp4
            parts = path.parts

            # Trouve l'index du dossier ready_to_stream
            ready_idx = None
            for i, part in enumerate(parts):
                if part == "ready_to_stream":
                    ready_idx = i
                    break

            if ready_idx is None or ready_idx == 0:
                return

            # Le nom de la chaîne est juste avant "ready_to_stream"
            channel_name = parts[ready_idx - 1]

            if not path.suffix.lower() in [".mp4", ".mkv", ".avi", ".mov"]:
                return

            logger.info(
                f"🔔 Modification détectée dans ready_to_stream pour {channel_name}: {path.name}"
            )

            # Mettre à jour la chaîne
            self._update_channel(channel_name)

        except Exception as e:
            logger.error(f"❌ Erreur traitement événement ready_to_stream: {e}")

    def _update_channel(self, channel_name):
        """Met à jour les playlists et l'état de la chaîne"""
        try:
            logger.info(f"[{channel_name}] 🔄 Mise à jour suite à modification dans ready_to_stream")

            # Vérifier si la chaîne existe déjà dans le gestionnaire
            channel = None
            if channel_name in self.manager.channels:
                channel = self.manager.channels[channel_name]
                logger.info(f"[{channel_name}] ✅ Chaîne existante, mise à jour...")

                # ----------------- Refresh videos -----------------
                # Récupérer les fichiers actuels
                if hasattr(channel, "refresh_videos"):
                    channel.refresh_videos()
                else:
                    logger.warning(f"[{channel_name}] ⚠️ La chaîne n'a pas de méthode refresh_videos.")
                
                # Add _restart_stream method if it doesn't exist
                if not hasattr(channel, "_restart_stream"):
                    logger.warning(f"[{channel_name}] ⚠️ Adding missing _restart_stream method to channel.")
                    # Add a basic implementation that will restart using start_stream
                    def restart_stream_impl(diagnostic=None):
                        """Basic implementation for channels missing _restart_stream method"""
                        try:
                            logger.info(f"[{channel_name}] 🔄 Basic restart implementation called. Reason: {diagnostic or 'Unknown'}")
                            # Stop current stream if it's running
                            if hasattr(channel, "process_manager") and channel.process_manager.is_running():
                                channel.process_manager.stop_process()
                            
                            # Clean HLS segments if possible
                            if hasattr(channel, "hls_cleaner"):
                                channel.hls_cleaner.cleanup_channel(channel_name)
                            
                            # Try to start the stream again
                            if hasattr(channel, "start_stream"):
                                success = channel.start_stream()
                                logger.info(f"[{channel_name}] {'✅ Stream restarted successfully' if success else '❌ Failed to restart stream'}")
                                return success
                            else:
                                logger.error(f"[{channel_name}] ❌ Channel doesn't have start_stream method")
                                return False
                        except Exception as e:
                            logger.error(f"[{channel_name}] ❌ Error in basic restart implementation: {e}")
                            return False
                    
                    # Add the method to the channel
                    setattr(channel, "_restart_stream", restart_stream_impl)
                    logger.info(f"[{channel_name}] ✅ Added basic _restart_stream method to channel")

                # Vérifier l'état de la playlist
                playlist_path = Path(channel.video_dir) / "_playlist.txt"
                old_content = ""
                if playlist_path.exists():
                    with open(playlist_path, "r", encoding="utf-8") as f:
                        old_content = f.read()

                # Forcer la création du fichier de playlist
                if hasattr(channel, "_create_concat_file"):
                    channel._create_concat_file()
                    logger.info(f"[{channel_name}] ✅ Playlist mise à jour")
                else:
                    logger.warning(f"[{channel_name}] ⚠️ La chaîne n'a pas de méthode _create_concat_file.")

                # Vérifier si la playlist a changé
                new_content = ""
                if playlist_path.exists():
                    with open(playlist_path, "r", encoding="utf-8") as f:
                        new_content = f.read()

                # Redémarrer le stream seulement si la playlist a changé
                if old_content != new_content:
                    logger.info(f"[{channel_name}] 🔄 Playlist modifiée, redémarrage nécessaire")
                    if hasattr(channel, "_restart_stream"):
                        logger.info(f"[{channel_name}] 🔄 Redémarrage du stream pour appliquer la nouvelle playlist")
                        # Redémarrer dans un thread séparé pour ne pas bloquer
                        threading.Thread(
                            target=channel._restart_stream,
                            daemon=True
                        ).start()
                else:
                    logger.info(f"[{channel_name}] ✓ Playlist inchangée, vérification du démarrage")
                    # Même si la playlist n'a pas changé, on vérifie si le stream doit être démarré
                    if not channel.is_running() and channel.is_ready_for_streaming():
                        threading.Thread(
                            target=channel.start_stream,
                            daemon=True
                        ).start()

            logger.info(
                f"✅ Mises à jour initiées pour {channel_name} suite à changement dans ready_to_stream"
            )

        except Exception as e:
            logger.error(f"❌ Erreur mise à jour chaîne {channel_name}: {e}") 
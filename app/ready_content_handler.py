from watchdog.events import FileSystemEventHandler
import threading
import time
import logging
from pathlib import Path
from base_file_event_handler import BaseFileEventHandler

logger = logging.getLogger(__name__)

class ReadyContentHandler(BaseFileEventHandler):
    """Surveille les modifications dans les dossiers ready_to_stream"""

    def __init__(self, manager):
        super().__init__(manager)

    def _handle_event(self, event):
        """Gère tous les événements détectés dans ready_to_stream"""
        try:
            # Vérifie que c'est un fichier MP4
            path = Path(event.src_path)
            if not self._should_process_event(event):
                return

            # Extrait le nom de la chaîne du chemin
            # Le format attendu est */content/{channel_name}/ready_to_stream/*.mp4
            path_parts = path.parts

            # Trouve l'index du dossier ready_to_stream
            ready_index = -1
            for i, part in enumerate(path_parts):
                if part == "ready_to_stream":
                    ready_index = i
                    break

            if ready_index > 0 and ready_index < len(path_parts):
                # Le nom de la chaîne est juste avant "ready_to_stream"
                channel_name = path_parts[ready_index - 1]

                # Vérifie le cooldown pour éviter les mises à jour trop fréquentes
                if not self._check_cooldown(channel_name):
                    return

                # Log l'événement
                logger.info(
                    f"🔔 Modification détectée dans ready_to_stream pour {channel_name}: {path.name}"
                )

                # Effectue les mises à jour nécessaires
                self._update_channel(channel_name)

        except Exception as e:
            logger.error(f"❌ Erreur traitement événement ready_to_stream: {e}")

    def _update_channel(self, channel_name):
        """Met à jour les playlists et l'état de la chaîne"""
        try:
            # Vérifie que la chaîne existe
            if channel_name not in self.manager.channels:
                logger.warning(f"⚠️ Chaîne {channel_name} non trouvée dans le manager")
                return

            channel = self.manager.channels[channel_name]

            # 1. Demande à la chaîne de rafraîchir ses vidéos
            threading.Thread(target=channel.refresh_videos, daemon=True).start()

            # 2. Met à jour le statut de la chaîne dans le manager
            with self.manager.scan_lock:
                self.manager.channel_ready_status[channel_name] = True

            # 3. Met à jour la playlist maître
            threading.Thread(
                target=self.manager._update_master_playlist, daemon=True
            ).start()

            # 4. Calcul et mise à jour de la durée totale
            if hasattr(channel, "position_manager") and hasattr(
                channel, "_calculate_total_duration"
            ):

                def update_duration():
                    try:
                        total_duration = channel._calculate_total_duration()
                        channel.position_manager.set_total_duration(total_duration)
                        channel.process_manager.set_total_duration(total_duration)
                        logger.info(
                            f"[{channel_name}] ✅ Durée totale mise à jour: {total_duration:.2f}s"
                        )
                    except Exception as e:
                        logger.error(
                            f"[{channel_name}] ❌ Erreur mise à jour durée: {e}"
                        )

                threading.Thread(target=update_duration, daemon=True).start()

            # 5. Mise à jour des offsets si stream en cours d'exécution
            if (
                hasattr(channel, "process_manager")
                and channel.process_manager.is_running()
            ):
                # On ne fait plus de mise à jour d'offset, on recrée juste la playlist si nécessaire
                if hasattr(channel, "_create_concat_file"):
                    # Vérifier l'état actuel de la playlist avant modification
                    playlist_path = Path(channel.video_dir) / "_playlist.txt"
                    old_content = ""
                    if playlist_path.exists():
                        with open(playlist_path, "r", encoding="utf-8") as f:
                            old_content = f.read()
                    
                    # Mettre à jour la playlist
                    channel._create_concat_file()
                    logger.info(f"[{channel_name}] 🔄 Playlist mise à jour suite aux changements")
                    
                    # Vérifier si la playlist a réellement changé
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
                        if hasattr(channel, "start_stream_if_needed"):
                            threading.Thread(
                                target=channel.start_stream_if_needed,
                                daemon=True
                            ).start()

            logger.info(
                f"✅ Mises à jour initiées pour {channel_name} suite à changement dans ready_to_stream"
            )

        except Exception as e:
            logger.error(f"❌ Erreur mise à jour chaîne {channel_name}: {e}") 
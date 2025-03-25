# file_event_handler.py
import time
import threading
from watchdog.events import FileSystemEventHandler
from pathlib import Path
import os
from config import logger
import subprocess


class FileEventHandler(FileSystemEventHandler):
    """Gestionnaire d'événements pour les changements de fichiers dans les dossiers des chaînes"""

    def __init__(self, manager):
        self.manager = manager
        self.copying_files = {}  # Dict pour tracker les fichiers en cours de copie
        self.lock = threading.Lock()  # Ajout de l'attribut lock manquant
        self.channel_changes = set()  # Pour suivre les chaînes modifiées
        self.last_scan_time = 0
        self.scan_cooldown = 10  # 10 secondes minimum entre les scans
        self.changed_channels = set()
        self.scan_timer = None
        self.channel_file_counters = {}  # Pour compter les fichiers copiés par chaîne
        self.channel_all_files = {}  # Pour compter tous les fichiers détectés par chaîne
        super().__init__()

    def is_file_ready(self, file_path: str, timeout: int = 300) -> bool:
        """
        Vérifie si un fichier a fini d'être copié en surveillant sa taille.
        Version améliorée avec meilleure détection des MP4 incomplets.

        Args:
            file_path: Chemin du fichier
            timeout: Timeout en secondes

        Returns:
            bool: True si le fichier est prêt, False sinon
        """
        path = Path(file_path)
        if not path.exists():
            return False

        # Ignorer explicitement les fichiers dans le dossier processing
        if "processing" in str(path):
            logger.info(
                f"Fichier dans dossier processing, on le considère comme stable: {path.name}"
            )
            return True

        start_time = time.time()
        last_size = -1
        stable_count = 0

        # Vérification initiale de la taille pour les gros fichiers
        try:
            initial_size = path.stat().st_size
            # On attend au moins 5 secondes pour les fichiers > 100 MB
            if initial_size > 100 * 1024 * 1024:  # > 100 MB
                logger.info(
                    f"Gros fichier détecté ({initial_size/1024/1024:.1f} MB), attente minimum 5s"
                )
                time.sleep(5)
        except Exception:
            pass

        while time.time() - start_time < timeout:
            try:
                # On vérifie si le fichier est accessible
                if not os.access(file_path, os.R_OK):
                    time.sleep(1)
                    continue

                current_size = path.stat().st_size

                # Si la taille n'a pas changé
                if current_size == last_size:
                    stable_count += 1
                    logger.info(
                        f"Fichier stable depuis {stable_count}s: {file_path} ({current_size/1024/1024:.1f} MB)"
                    )

                    # Si le fichier est stable depuis assez longtemps et qu'il s'agit d'un MP4,
                    # on vérifie la présence de l'atom MOOV qui est crucial pour les MP4
                    if stable_count >= 5 and path.suffix.lower() == ".mp4":
                        # Vérification avancée pour MP4
                        if self._verify_mp4_completeness(file_path):
                            return True
                        else:
                            # Si l'atome MOOV est manquant mais que le fichier est stable,
                            # on attend un peu plus pour les gros fichiers
                            if stable_count >= 15:
                                logger.warning(
                                    f"❌ Fichier MP4 incomplet même après stabilité prolongée: {file_path}"
                                )
                                return False
                    elif stable_count >= 5:
                        # Pour les autres types de fichiers, la stabilité suffit
                        return True
                else:
                    stable_count = 0
                    logger.info(
                        f"{file_path} Taille en évolution: {current_size/1024/1024:.1f} MB (était {last_size/1024/1024:.1f} MB)"
                    )

                last_size = current_size
                time.sleep(1)

            except (OSError, PermissionError) as e:
                # Le fichier est probablement encore verrouillé
                logger.debug(f"Fichier {file_path} pas encore accessible: {e}")
                time.sleep(1)
                continue

        logger.warning(f"⏰ Timeout en attendant {file_path}")
        return False

    def _verify_mp4_completeness(self, file_path: str) -> bool:
        """
        Vérifie si un fichier MP4 est complet en cherchant l'atome MOOV.

        Args:
            file_path: Chemin du fichier MP4 à vérifier

        Returns:
            bool: True si le fichier semble complet, False sinon
        """
        try:
            # Utilisation de ffprobe pour vérifier la validité du fichier
            cmd = [
                "ffprobe",
                "-v",
                "error",
                "-select_streams",
                "v:0",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                str(file_path),
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)

            # Si ffprobe réussit et retourne une durée, le fichier est probablement valide
            if result.returncode == 0 and result.stdout.strip():
                try:
                    duration = float(result.stdout.strip())
                    if duration > 0:
                        logger.info(
                            f"✅ Fichier MP4 valide avec durée de {duration:.2f}s: {Path(file_path).name}"
                        )
                        return True
                except ValueError:
                    pass

            # Si on est ici, c'est que ffprobe a échoué ou n'a pas retourné de durée
            error_text = result.stderr.lower()
            if "moov atom not found" in error_text:
                logger.warning(
                    f"⚠️ Atome MOOV manquant dans {Path(file_path).name}, fichier MP4 incomplet"
                )
                return False

            logger.warning(
                f"⚠️ Vérification MP4 échouée pour {Path(file_path).name}: {result.stderr}"
            )
            return False

        except subprocess.TimeoutExpired:
            logger.warning(
                f"⚠️ Timeout lors de la vérification MP4 pour {Path(file_path).name}"
            )
            return False
        except Exception as e:
            logger.error(f"❌ Erreur vérification MP4 {Path(file_path).name}: {e}")
            return False

    def _wait_for_copy_completion(self, file_path: str, channel_name: str = ""):
        """Surveille un fichier en cours de copie et attend qu'il soit stable"""
        try:
            # Vérification du fichier
            if not self.is_file_ready(file_path):
                logger.info(f"❌ Fichier {file_path} non stable après le délai")
                with self.lock:
                    if file_path in self.copying_files:
                        del self.copying_files[file_path]
                    
                    # Si c'est un fichier ready_to_stream, le retirer du comptage
                    if channel_name and "ready_to_stream" in file_path:
                        if channel_name in self.channel_all_files and file_path in self.channel_all_files[channel_name]:
                            self.channel_all_files[channel_name].remove(file_path)
                return

            logger.info(f"✅ Copie terminée: {file_path}")

            # Planifier un scan si on a changé le channel_name
            if channel_name:
                # Tracker le fichier complété
                if "ready_to_stream" in file_path:
                    with self.lock:
                        if channel_name not in self.channel_file_counters:
                            self.channel_file_counters[channel_name] = set()
                        self.channel_file_counters[channel_name].add(file_path)
                        
                        all_files = len(self.channel_all_files.get(channel_name, set()))
                        completed_files = len(self.channel_file_counters.get(channel_name, set()))
                        
                        logger.info(f"[{channel_name}] ⏳ {completed_files}/{all_files} fichiers stables")
                        
                        # Vérifier si tous les fichiers de cette chaîne sont maintenant stables
                        if all_files > 0 and completed_files == all_files:
                            logger.info(f"[{channel_name}] 🎉 Tous les fichiers sont stables! Démarrage du scan et du stream")
                            # Forcer un scan immédiat
                            threading.Thread(
                                target=self.manager.scan_channels, 
                                args=(True,),  # Force=True pour assurer l'initialisation
                                daemon=True
                            ).start()
                            
                            # Démarrer le stream directement si la chaîne existe déjà
                            if channel_name in self.manager.channels:
                                channel = self.manager.channels[channel_name]
                                if hasattr(channel, "ready_for_streaming") and channel.ready_for_streaming:
                                    logger.info(f"[{channel_name}] 🚀 Démarrage du stream après stabilisation de tous les fichiers")
                                    threading.Thread(
                                        target=channel.start_stream,
                                        daemon=True
                                    ).start()
                        else:
                            # Si ce n'est pas le dernier fichier, on lance juste un scan
                            logger.info(f"🔄 Déclenchement d'un scan pour la chaîne: {channel_name}")
                            threading.Thread(
                                target=self.manager.scan_channels, 
                                args=(True,),
                                daemon=True
                            ).start()

        except Exception as e:
            logger.error(f"❌ Erreur surveillance copie: {e}")

        finally:
            with self.lock:
                if file_path in self.copying_files:
                    del self.copying_files[file_path]

    def get_channel_from_path(self, path: str) -> str:
        """Extrait le nom de la chaîne à partir du chemin"""
        try:
            # Le chemin est généralement content/<channel>/...
            path_parts = Path(path).parts
            content_index = -1

            # Cherche l'indice du dossier "content"
            for i, part in enumerate(path_parts):
                if part.lower() == "content":
                    content_index = i
                    break

            # Si content trouvé et il y a un élément après
            if content_index >= 0 and content_index + 1 < len(path_parts):
                return path_parts[content_index + 1]

            # Plan B: on prend juste le dossier parent
            return Path(path).parent.name

        except Exception as e:
            logger.error(f"❌ Erreur extraction nom de chaîne: {e}")
            return ""

    def force_scan_now(self):
        """Force un scan immédiat des chaînes"""
        logger.info("🔄 Forçage d'un scan immédiat des chaînes...")
        self.scan_channels(force=True)
        logger.info("✅ Scan forcé terminé")

    def on_created(self, event):
        """Gère la création d'un fichier"""
        if event.is_directory:
            logger.info(f"📂 Nouveau dossier détecté: {event.src_path}")
            # On vérifie si c'est un dossier de chaîne (direct dans content_dir)
            content_dir = Path(self.manager.content_dir)
            path = Path(event.src_path)

            if path.parent == content_dir:
                channel_name = path.name
                logger.info(f"🆕 Nouvelle chaîne potentielle détectée: {channel_name}")
                # Force un scan immédiat plutôt que d'attendre
                threading.Thread(
                    target=self.manager.scan_channels, args=(True,), daemon=True
                ).start()
            return

        # Pour les fichiers, vérification du type de fichier et ignorer les tmp_
        path = Path(event.src_path)

        # Ignorer les fichiers temporaires de transcodage
        if "tmp_" in path.name:
            logger.debug(f"Fichier temporaire ignoré: {path.name}")
            return

        # Correction:
        if path.suffix.lower() not in [".mp4", ".mkv", ".avi", ".mov", ".m4v"]:
            return  # On ignore les fichiers non vidéo

        # On vérifie si les fichiers sont en cours de copie
        with self.lock:
            if event.src_path in self.copying_files:
                return

            self.copying_files[event.src_path] = time.time()

            # On note la chaîne concernée
            channel_name = self.get_channel_from_path(event.src_path)
            if channel_name:
                self.changed_channels.add(channel_name)
                
                # Ajouter à notre compteur de fichiers
                if "ready_to_stream" in event.src_path:
                    if channel_name not in self.channel_all_files:
                        self.channel_all_files[channel_name] = set()
                    self.channel_all_files[channel_name].add(event.src_path)
                    logger.info(f"[{channel_name}] ➕ Nouveau fichier détecté: {Path(event.src_path).name} (total: {len(self.channel_all_files[channel_name])})")

        # On lance un thread dédié pour surveiller la copie
        threading.Thread(
            target=self._wait_for_copy_completion,
            args=(event.src_path, channel_name),
            daemon=True,
        ).start()

    def on_modified(self, event):
        """Gère la modification d'un fichier"""
        if event.is_directory:
            return
        self._handle_change(event.src_path)

    def on_deleted(self, event):
        """Gère la suppression d'un fichier"""
        if event.is_directory:
            return
        self._handle_change(event.src_path)

    def _handle_change(self, file_path):
        """Gère un changement de fichier en planifiant un scan si nécessaire"""
        try:
            # Ignorer les fichiers temporaires et cachés
            filename = Path(file_path).name
            if filename.startswith('.') or filename.startswith('~'):
                return

            # Extraire le nom de la chaîne du chemin
            channel_name = Path(file_path).parent.name
            if channel_name in self.manager.channels:
                self.changed_channels.add(channel_name)
                self._schedule_scan()

        except Exception as e:
            logger.error(f"❌ Erreur traitement changement fichier: {e}")

    def _schedule_scan(self):
        """Planifie un scan avec un cooldown pour éviter les scans trop fréquents"""
        try:
            current_time = time.time()

            # Annuler le timer existant s'il y en a un
            if self.scan_timer:
                self.scan_timer.cancel()

            # Si des chaînes ont changé, planifier un nouveau scan
            if self.changed_channels:
                logger.info(f"🔄 Scan programmé pour les chaînes: {', '.join(self.changed_channels)}")
                # Demander un scan au manager
                self.manager.request_scan(force=True)
                # Lancer directement un scan forcé pour être sûr
                threading.Thread(
                    target=self.manager.scan_channels,
                    args=(True,),  # Force=True pour garantir l'initialisation complète
                    daemon=True
                ).start()
                self.changed_channels.clear()

        except Exception as e:
            logger.error(f"❌ Erreur planification scan: {e}")


class ReadyContentHandler(FileSystemEventHandler):
    """Surveille les modifications dans les dossiers ready_to_stream"""

    def __init__(self, manager):
        self.manager = manager
        self.lock = threading.Lock()
        self.last_update_time = {}  # Pour éviter les mises à jour trop fréquentes
        self.update_cooldown = 5  # 5 secondes entre mises à jour
        super().__init__()

    def on_deleted(self, event):
        self._handle_event(event)

    def on_moved(self, event):
        self._handle_event(event)

    def on_modified(self, event):
        self._handle_event(event)

    def _handle_event(self, event):
        """Gère tous les événements détectés dans ready_to_stream"""
        try:
            # Vérifie que c'est un fichier MP4
            path = Path(event.src_path)
            if event.is_directory or path.suffix.lower() != ".mp4":
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
                current_time = time.time()
                with self.lock:
                    last_time = self.last_update_time.get(channel_name, 0)
                    if current_time - last_time < self.update_cooldown:
                        return

                    # Met à jour le timestamp
                    self.last_update_time[channel_name] = current_time

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
                    
                    # NOUVEAU: Redémarrer le stream seulement si la playlist a changé
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

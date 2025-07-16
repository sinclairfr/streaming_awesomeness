# file_event_handler.py
import time
import threading
from watchdog.events import FileSystemEventHandler
from pathlib import Path
import os
from config import logger, HLS_SEGMENT_DURATION, SERVER_URL, HLS_DIR
import subprocess
import traceback
import shutil
from base_file_event_handler import BaseFileEventHandler


class FileEventHandler(BaseFileEventHandler):
    """Gestionnaire d'événements pour les changements de fichiers dans les dossiers des chaînes"""

    def __init__(self, manager):
        super().__init__(manager)
        self.copying_files = {}
        self.changed_channels = set()
        self.channel_file_counters = {}
        self.channel_all_files = {}

    def is_file_ready(self, file_path: str, timeout: int = 600) -> bool:
        """
        Attend qu'un fichier soit stable (taille ne change plus) avant de procéder.
        Utiliser un timeout plus long pour éviter les faux négatifs.
        
        Args:
            file_path: Chemin du fichier à vérifier
            timeout: Timeout en secondes
            
        Returns:
            True si le fichier est stable, False sinon
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
        logged_stability = False  # Flag pour éviter les logs répétitifs

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
                    
                    # Log une seule fois au début de la stabilité
                    if not logged_stability and stable_count == 3:
                        logged_stability = True
                        logger.info(
                            f"Fichier stable: {path.name} ({current_size/1024/1024:.1f} MB)"
                        )

                    # Si le fichier est stable depuis assez longtemps et qu'il s'agit d'un MP4,
                    # on vérifie la présence de l'atom MOOV qui est crucial pour les MP4
                    if stable_count >= 5 and path.suffix.lower() == ".mp4":
                        # Vérification avancée pour MP4
                        if self._verify_mp4_completeness(file_path):
                            logger.info(f"✅ MP4 validé comme stable après {stable_count}s: {path.name}")
                            return True
                        else:
                            # Si l'atome MOOV est manquant mais que le fichier est stable,
                            # on attend un peu plus pour les gros fichiers
                            if stable_count >= 15:
                                logger.warning(
                                    f"❌ Fichier MP4 incomplet même après stabilité prolongée: {path.name}"
                                )
                                return False
                    elif stable_count >= 5:
                        # Pour les autres types de fichiers, la stabilité suffit
                        logger.info(f"✅ Fichier validé comme stable après {stable_count}s: {path.name}")
                        return True
                else:
                    # Réinitialiser le statut de log si la taille change
                    if logged_stability:
                        logged_stability = False
                    
                    stable_count = 0
                    # Log moins fréquent pour les changements de taille
                    if last_size > 0 and abs(current_size - last_size) > 10240:  # 10KB de changement minimum
                        logger.info(
                            f"{path.name} Taille en évolution: {current_size/1024/1024:.1f} MB (était {last_size/1024/1024:.1f} MB)"
                        )

                last_size = current_size
                time.sleep(1)

            except (OSError, PermissionError) as e:
                # Le fichier est probablement encore verrouillé
                logger.debug(f"Fichier {path.name} pas encore accessible: {e}")
                time.sleep(1)
                continue

        logger.warning(f"⏰ Timeout en attendant {path.name}")
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

    def _handle_event(self, event):
        """Gère un événement de fichier"""
        if not self._should_process_event(event):
            return

        path = Path(event.src_path)
        channel_name = self._get_channel_name(str(path))

        if not channel_name:
            return

        # Vérifier le cooldown
        if not self._check_cooldown(channel_name):
            return

        # Gérer la création de dossier
        if event.is_directory and path.parent == Path(self.manager.content_dir):
            self._handle_new_channel_directory(path)
            return

        # Gérer les fichiers
        with self.lock:
            if event.src_path in self.copying_files:
                return

            self.copying_files[event.src_path] = time.time()
            self.changed_channels.add(channel_name)

            # Ajouter à notre compteur de fichiers
            if "ready_to_stream" in str(path):
                if channel_name not in self.channel_all_files:
                    self.channel_all_files[channel_name] = set()
                self.channel_all_files[channel_name].add(str(path))
                logger.info(f"[{channel_name}] ➕ Nouveau fichier détecté: {path.name} (total: {len(self.channel_all_files[channel_name])})")

        # Lancer un thread pour surveiller la copie
        threading.Thread(
            target=self._wait_for_copy_completion,
            args=(event.src_path, channel_name),
            daemon=True,
        ).start()

    def _handle_new_channel_directory(self, path: Path):
        """Gère la création d'un nouveau dossier de chaîne"""
        channel_name = path.name
        logger.info(f"🆕 Nouvelle chaîne potentielle détectée: {channel_name}")
        
        # Créer le dossier ready_to_stream s'il n'existe pas
        ready_dir = path / "ready_to_stream"
        ready_dir.mkdir(exist_ok=True)
        
        # Vérifier si la chaîne existe déjà
        if channel_name in self.manager.channels:
            logger.info(f"[{channel_name}] ℹ️ La chaîne existe déjà")
            return
        
        # Force un scan immédiat
        self.manager.scan_channels(force=True)
        
        # Attendre un peu pour laisser le temps au scan de s'exécuter
        time.sleep(2)
        
        # Si la chaîne n'a pas été créée par le scan, la créer manuellement
        if channel_name not in self.manager.channels:
            logger.info(f"[{channel_name}] 🔄 Forçage de la création de la chaîne")
            self._force_channel_creation(channel_name)

    def _is_already_indexed(self, file_path: str) -> bool:
        """
        Vérifie si un fichier est déjà indexé dans un playlist.txt
        pour éviter des vérifications inutiles de stabilité.
        
        Args:
            file_path: Chemin du fichier à vérifier
            
        Returns:
            True si le fichier est déjà dans une playlist, False sinon
        """
        try:
            # Vérifier si le fichier existe
            if not os.path.exists(file_path):
                return False
                
            # Obtenir le nom du canal (dossier parent)
            path = Path(file_path)
            channel_name = self._get_channel_name(str(path))
            if not channel_name:
                return False
                
            # Chemin vers le fichier playlist.txt
            playlist_path = Path(self.manager.content_dir) / channel_name / "playlist.txt"
            if not playlist_path.exists():
                return False
                
            # Lire le contenu de playlist.txt
            with open(playlist_path, "r", encoding="utf-8") as f:
                content = f.read()
                
            # Vérifier si le nom du fichier est dans le contenu
            if path.name in content:
                logger.debug(f"[{channel_name}] Fichier {path.name} déjà dans la playlist, skip de la vérification")
                return True
                
            return False
        except Exception as e:
            logger.error(f"Erreur vérification fichier déjà indexé {file_path}: {e}")
            return False
            
    def _wait_for_copy_completion(self, file_path: str, channel_name: str = ""):
        """Surveille un fichier en cours de copie et attend qu'il soit stable"""
        try:
            # Vérifier si le fichier est déjà indexé
            if self._is_already_indexed(file_path):
                logger.info(f"✅ Fichier déjà indexé, skip d'attente: {Path(file_path).name}")
                with self.lock:
                    if file_path in self.copying_files:
                        del self.copying_files[file_path]
                return
                
            # Vérification du fichier
            if not self.is_file_ready(file_path):
                logger.info(f"❌ Fichier {Path(file_path).name} non stable après le délai")
                with self.lock:
                    if file_path in self.copying_files:
                        del self.copying_files[file_path]
                    
                    # Si c'est un fichier ready_to_stream, le retirer du comptage
                    if channel_name and "ready_to_stream" in file_path:
                        if channel_name in self.channel_all_files and file_path in self.channel_all_files[channel_name]:
                            self.channel_all_files[channel_name].remove(file_path)
                return

            logger.info(f"✅ Copie terminée: {Path(file_path).name}")

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
                            self._handle_all_files_stable(channel_name)

        except Exception as e:
            logger.error(f"❌ Erreur surveillance copie: {e}")
            import traceback
            logger.error(traceback.format_exc())

        finally:
            with self.lock:
                if file_path in self.copying_files:
                    del self.copying_files[file_path]

    def _handle_all_files_stable(self, channel_name: str):
        """Gère le cas où tous les fichiers d'une chaîne sont stables"""
        logger.info(f"[{channel_name}] 🎉 Tous les fichiers sont stables!")
        
        # Vérifier si on a déjà fait une vérification de stabilité pour cette chaîne
        stability_check_key = f"{channel_name}_stability_check"
        if hasattr(self, stability_check_key) and getattr(self, stability_check_key):
            logger.info(f"[{channel_name}] ℹ️ Vérification de stabilité déjà effectuée, on ignore")
            return
        
        # Attendre un peu plus longtemps pour s'assurer que tout est bien stable
        time.sleep(2)
        
        # Vérifier une dernière fois que tous les fichiers sont toujours stables
        ready_dir = os.path.join(self.manager.content_dir, channel_name, "ready_to_stream")
        if os.path.exists(ready_dir):
            mp4_files = [f for f in os.listdir(ready_dir) if f.endswith('.mp4')]
            all_stable = True
            for mp4 in mp4_files:
                file_path = os.path.join(ready_dir, mp4)
                if not self.is_file_ready(file_path):
                    all_stable = False
                    break
            
            if not all_stable:
                logger.warning(f"[{channel_name}] ⚠️ Certains fichiers ne sont plus stables, attente...")
                return
        
        # Marquer que la vérification de stabilité a été faite
        setattr(self, stability_check_key, True)

        # Attendre un peu plus longtemps pour s'assurer que tout est bien initialisé
        time.sleep(5)

        # Si la chaîne existe déjà, forcer un redémarrage direct
        if channel_name in self.manager.channels:
            channel = self.manager.channels[channel_name]
            # Vérifier que la chaîne n'est pas None
            if channel is None:
                logger.error(f"[{channel_name}] ❌ La chaîne existe dans le manager mais est None")
                return
                
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
                
            # Now call the _restart_stream method (either original or our added implementation)
            logger.info(f"[{channel_name}] 🔄 Forçage du redémarrage du stream après détection de stabilité des fichiers")
            channel._restart_stream()
            # Attendre que le stream démarre
            time.sleep(2)
            # Vérifier que le stream est bien démarré
            if hasattr(channel, "process_manager") and channel.process_manager.is_running():
                logger.info(f"[{channel_name}] ✅ Stream redémarré avec succès")
                # Réinitialiser le flag de stabilité après un redémarrage réussi
                if hasattr(self, stability_check_key):
                     delattr(self, stability_check_key)
            else:
                # Si le redémarrage a échoué, tenter le démarrage direct
                logger.warning(f"[{channel_name}] ⚠️ Échec du redémarrage, tentative de démarrage direct FFmpeg")
                # On ne force pas le démarrage ffmpeg ici, cela pourrait causer des problèmes si la chaîne n'est pas prête
                # self._force_ffmpeg_start(channel_name)

        # Mise à jour de la playlist principale - Only if channel exists and restarted?
        # Let's move this inside the if block to avoid updating if channel didn't exist
        if channel_name in self.manager.channels:
            self._force_master_playlist_update()

        # Nettoyer les compteurs pour cette chaîne
        with self.lock:
            if channel_name in self.channel_file_counters:
                del self.channel_file_counters[channel_name]
            if channel_name in self.channel_all_files:
                del self.channel_all_files[channel_name]

    def _force_channel_creation(self, channel_name: str) -> bool:
        """Force la création d'une chaîne et son ajout au manager"""
        try:
            # Vérifier si la chaîne existe déjà
            if channel_name in self.manager.channels:
                logger.info(f"[{channel_name}] ℹ️ La chaîne existe déjà, pas besoin de la recréer")
                return True

            from iptv_channel import IPTVChannel
            import subprocess
            
            # Chemin du dossier de la chaîne
            channel_dir = os.path.join(self.manager.content_dir, channel_name)
            ready_dir = os.path.join(channel_dir, "ready_to_stream")
            
            if not os.path.exists(channel_dir):
                logger.error(f"[{channel_name}] ❌ Le dossier de la chaîne n'existe pas: {channel_dir}")
                return False
            
            # Attendre que tous les fichiers soient copiés et stables
            logger.info(f"[{channel_name}] ⏳ Attente de la stabilisation des fichiers...")
            max_wait = 60  # Maximum 60 secondes d'attente
            start_time = time.time()
            
            # Attendre que le dossier ready_to_stream existe
            while time.time() - start_time < max_wait:
                if not os.path.exists(ready_dir):
                    time.sleep(1)
                    continue
                
                mp4_files = [f for f in os.listdir(ready_dir) if f.endswith('.mp4')]
                if not mp4_files:
                    time.sleep(1)
                    continue
                
                # Vérifier que tous les fichiers sont stables
                all_stable = True
                for mp4 in mp4_files:
                    file_path = os.path.join(ready_dir, mp4)
                    if not self.is_file_ready(file_path):
                        all_stable = False
                        break
                
                if all_stable:
                    logger.info(f"[{channel_name}] ✅ Tous les fichiers sont stables")
                    break
                
                time.sleep(1)
            
            # Si on n'a pas réussi à avoir des fichiers stables, on abandonne
            if not all_stable:
                logger.error(f"[{channel_name}] ❌ Impossible d'avoir des fichiers stables après {max_wait}s")
                return False
            
            logger.info(f"[{channel_name}] 🔄 Création forcée de la chaîne")
            
            # Créer l'objet chaîne directement
            channel = IPTVChannel(
                channel_name,
                str(channel_dir),
                hls_cleaner=self.manager.hls_cleaner,
                use_gpu=self.manager.use_gpu,
                stats_collector=self.manager.stats_collector
            )
            
            # S'assurer que la chaîne est marquée comme prête
            channel.ready_for_streaming = True
            
            # Ajouter la référence au manager
            channel.manager = self.manager
            
            # Ajouter la chaîne au manager
            self.manager.channels[channel_name] = channel
            
            logger.info(f"[{channel_name}] ✅ Chaîne créée avec succès")
            return True
            
        except Exception as e:
            logger.error(f"[{channel_name}] ❌ Erreur lors de la création forcée de la chaîne: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def _force_master_playlist_update(self):
        """Force la mise à jour de la playlist maître"""
        try:
            # Chemin de la playlist
            playlist_path = f"{HLS_DIR}/playlist.m3u"
            
            # Créer le contenu de base
            content = "#EXTM3U\n"
            
            # Récupérer toutes les chaînes actives
            active_channels = []
            
            # Vérification directe des dossiers HLS
            hls_dir = Path(HLS_DIR)
            for channel_dir in hls_dir.iterdir():
                if channel_dir.is_dir() and (channel_dir / "playlist.m3u8").exists():
                    channel_name = channel_dir.name
                    # Vérifier qu'il y a au moins un segment
                    segments = list(channel_dir.glob("segment_*.ts"))
                    if segments:
                        active_channels.append(channel_name)
                        logger.info(f"✅ Chaîne active détectée: {channel_name} avec {len(segments)} segments")
            
            # Si aucune chaîne n'est active selon la méthode directe, essayer la méthode basée sur l'attribut
            if not active_channels:
                for channel_name, channel in self.manager.channels.items():
                    if hasattr(channel, "ready_for_streaming") and channel.ready_for_streaming:
                        active_channels.append(channel_name)
                        logger.info(f"✅ Chaîne active selon l'attribut: {channel_name}")
            
            # Trier les chaînes par ordre alphabétique
            active_channels.sort()
            
            # Ajouter chaque chaîne à la playlist
            for channel_name in active_channels:
                content += f'#EXTINF:-1 tvg-id="{channel_name}" tvg-name="{channel_name}",{channel_name}\n'
                content += f"http://{SERVER_URL}/hls/{channel_name}/playlist.m3u8\n"
            
            # Écrire le contenu
            with open(playlist_path, "w", encoding="utf-8") as f:
                f.write(content)
            
            
            logger.info(f"✅ Playlist mise à jour avec {len(active_channels)} chaînes: {', '.join(active_channels)}")
            
        except Exception as e:
            logger.error(f"❌ Erreur mise à jour playlist: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def _force_ffmpeg_start(self, channel_name: str):
        """Force le démarrage de FFmpeg pour une chaîne"""
        try:
            channel_dir = os.path.join(self.manager.content_dir, channel_name)
            playlist_file = os.path.join(channel_dir, "_playlist.txt")
            
            if not os.path.exists(playlist_file):
                logger.info(f"[{channel_name}] 🔄 Création de la playlist de concaténation")
                # Trouver tous les fichiers MP4 dans ready_to_stream
                ready_dir = os.path.join(channel_dir, "ready_to_stream")
                mp4_files = []
                if os.path.exists(ready_dir):
                    mp4_files = [f for f in os.listdir(ready_dir) if f.endswith('.mp4')]
                
                if not mp4_files:
                    logger.error(f"[{channel_name}] ❌ Aucun fichier MP4 trouvé dans ready_to_stream")
                    return False
                
                # Créer la playlist en mode aléatoire
                import random
                random.shuffle(mp4_files)
                
                with open(playlist_file, "w", encoding="utf-8") as f:
                    for mp4 in mp4_files:
                        mp4_path = os.path.join(ready_dir, mp4)
                        f.write(f"file '{mp4_path}'\n")
                
                logger.info(f"[{channel_name}] ✅ Playlist créée avec {len(mp4_files)} fichiers")
            
            # Vérifier qu'il n'y a pas déjà un processus FFmpeg en cours pour cette chaîne
            import psutil
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    if proc.info['name'] == 'ffmpeg' and channel_name in str(proc.info['cmdline']):
                        logger.warning(f"[{channel_name}] ⚠️ Un processus FFmpeg existe déjà, tentative d'arrêt")
                        proc.terminate()
                        proc.wait(timeout=5)
                except (psutil.NoSuchProcess, psutil.TimeoutExpired):
                    pass
            
            # Démarrer FFmpeg avec des paramètres optimisés
            logger.info(f"[{channel_name}] 🚀 Démarrage direct de FFmpeg")
            
            # Construire la commande FFmpeg
            cmd = [
                "ffmpeg",
                "-f", "concat",
                "-safe", "0",
                "-i", playlist_file,
                "-c", "copy",
                "-f", "hls",
                "-hls_time", str(HLS_SEGMENT_DURATION),
                "-hls_list_size", "5",
                "-hls_flags", "delete_segments+append_list",
                "-hls_segment_filename", f"{HLS_DIR}/{channel_name}/segment_%d.ts",
                f"{HLS_DIR}/{channel_name}/playlist.m3u8"
            ]
            
            # Ajouter les options GPU si nécessaire
            if self.manager.use_gpu:
                cmd.insert(1, "-hwaccel")
                cmd.insert(2, "cuda")
            
            # Démarrer le processus
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # Attendre un peu pour voir si le processus démarre correctement
            time.sleep(2)
            
            # Vérifier si le processus est toujours en cours
            if process.poll() is None:
                logger.info(f"[{channel_name}] ✅ FFmpeg démarré avec succès")
                return True
            else:
                logger.error(f"[{channel_name}] ❌ FFmpeg a échoué à démarrer")
                return False
                
        except Exception as e:
            logger.error(f"[{channel_name}] ❌ Erreur démarrage FFmpeg: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False


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

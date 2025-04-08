# file_event_handler.py
import time
import threading
from watchdog.events import FileSystemEventHandler
from pathlib import Path
import os
from config import logger
import subprocess
import traceback
import shutil


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
                                if hasattr(channel, "_restart_stream"):
                                    logger.info(f"[{channel_name}] 🔄 Forçage du redémarrage du stream")
                                    channel._restart_stream()
                                    # Attendre que le stream démarre
                                    time.sleep(2)
                                    # Vérifier que le stream est bien démarré
                                    if hasattr(channel, "process_manager") and channel.process_manager.is_running():
                                        logger.info(f"[{channel_name}] ✅ Stream redémarré avec succès")
                                        # Réinitialiser le flag de stabilité après un redémarrage réussi
                                        delattr(self, stability_check_key)
                                    else:
                                        # Si le redémarrage a échoué, essayer le démarrage direct
                                        logger.warning(f"[{channel_name}] ⚠️ Échec du redémarrage, tentative de démarrage direct")
                                        self._force_ffmpeg_start(channel_name)
                            else:
                                # La chaîne n'existe pas, la créer et démarrer directement
                                logger.info(f"[{channel_name}] 🔄 Création et démarrage forcé de la chaîne")
                                if not self._force_channel_creation(channel_name):
                                    # Si la création échoue, tenter le démarrage direct FFmpeg
                                    logger.warning(f"[{channel_name}] ⚠️ Échec de la création, tentative de démarrage direct FFmpeg")
                                    self._force_ffmpeg_start(channel_name)

                            # Mise à jour de la playlist principale
                            self._force_master_playlist_update()

                            # Nettoyer les compteurs pour cette chaîne
                            with self.lock:
                                if channel_name in self.channel_file_counters:
                                    del self.channel_file_counters[channel_name]
                                if channel_name in self.channel_all_files:
                                    del self.channel_all_files[channel_name]

                        else:
                            # Ne pas déclencher de scan pour chaque fichier
                            logger.info(f"[{channel_name}] ⏳ En attente des autres fichiers...")

        except Exception as e:
            logger.error(f"❌ Erreur surveillance copie: {e}")
            import traceback
            logger.error(traceback.format_exc())

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

            self.copying_files[event.src_path] = time.sleep(1)

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
                
                # Notifier le manager des changements
                with self.manager.scan_lock:
                    # 1. Ajouter nos chaînes modifiées à celles du manager
                    if not hasattr(self.manager, "pending_changes"):
                        self.manager.pending_changes = set()
                    self.manager.pending_changes.update(self.changed_channels)
                    
                    # 2. Forcer un scan immédiat des chaînes modifiées
                    logger.info(f"🔍 Scan forcé des chaînes: {', '.join(self.changed_channels)}")
                    self.manager.scan_channels(force=True)
                    
                    # 3. Attendre un peu que le scan se termine
                    time.sleep(2)
                    
                    # 4. Resynchroniser les playlists
                    if hasattr(self.manager, "_resync_all_playlists"):
                        logger.info("🔄 Resynchronisation des playlists...")
                        self.manager._resync_all_playlists()
                    
                    # 5. Réinitialiser notre liste locale
                    self.changed_channels.clear()
                    
                    logger.info("✅ Scan et resynchronisation terminés")

        except Exception as e:
            logger.error(f"❌ Erreur planification scan: {e}")
            logger.error(traceback.format_exc())

    def _force_channel_creation(self, channel_name: str):
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
            
            # Ajouter la chaîne au dictionnaire du manager
            with self.manager.scan_lock:
                if channel_name not in self.manager.channels:  # Double vérification
                    self.manager.channels[channel_name] = channel
                    self.manager.channel_ready_status[channel_name] = True
                    logger.info(f"[{channel_name}] ✅ Chaîne créée et ajoutée au manager")
                else:
                    logger.info(f"[{channel_name}] ℹ️ La chaîne a été créée entre temps")
                    return True
            
            # Créer le dossier HLS
            hls_dir = f"/app/hls/{channel_name}"
            os.makedirs(hls_dir, exist_ok=True)
            os.chmod(hls_dir, 0o777)
            
            # Forcer la création de la playlist de concaténation
            if hasattr(channel, "_create_concat_file"):
                channel._create_concat_file()
            
            # Mise à jour de la playlist principale
            logger.info(f"[{channel_name}] 🔄 Mise à jour des playlists")
            self._force_master_playlist_update()
            
            # Attendre un peu plus longtemps pour s'assurer que tout est bien initialisé
            time.sleep(5)
            
            # Démarrer le stream une seule fois
            if hasattr(channel, "start_stream"):
                logger.info(f"[{channel_name}] 🚀 Démarrage du stream")
                success = channel.start_stream()
                
                if success:
                    logger.info(f"[{channel_name}] ✅ Stream démarré avec succès!")
                    return True
                else:
                    logger.error(f"[{channel_name}] ❌ Échec du démarrage du stream")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"[{channel_name}] ❌ Erreur lors de la création forcée de la chaîne: {e}")
            logger.error(traceback.format_exc())
            return False

    def _force_master_playlist_update(self):
        """Force la mise à jour de la playlist maître avec une approche simple et directe"""
        try:
            # Chemin de la playlist
            playlist_path = "/app/hls/playlist.m3u"
            
            # Créer le contenu de base
            content = "#EXTM3U\n"
            
            # Récupérer toutes les chaînes actives
            active_channels = []
            
            # Vérification directe des dossiers HLS au lieu d'utiliser ready_for_streaming
            hls_dir = Path("/app/hls")
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
            server_url = os.getenv("SERVER_URL", "192.168.10.183")
            for channel_name in active_channels:
                content += f'#EXTINF:-1 tvg-id="{channel_name}" tvg-name="{channel_name}",{channel_name}\n'
                content += f"http://{server_url}/hls/{channel_name}/playlist.m3u8\n"
            
            # Log du contenu qui sera écrit
            logger.info(f"📝 Contenu de la playlist à écrire:\n{content}")
            
            # Écrire le contenu directement sans fichier temporaire pour débugger
            with open(playlist_path, "w", encoding="utf-8") as f:
                f.write(content)
            
            # S'assurer que le fichier a les bonnes permissions
            os.chmod(playlist_path, 0o777)  # Permissions plus larges pour le débogage
            
            # Vérifier que le fichier a bien été écrit
            if os.path.exists(playlist_path):
                size = os.path.getsize(playlist_path)
                logger.info(f"✅ Playlist écrite: {playlist_path}, taille: {size} octets")
                
                # Lire le contenu pour vérification
                with open(playlist_path, "r", encoding="utf-8") as f:
                    read_content = f.read()
                    logger.info(f"📄 Contenu lu:\n{read_content}")
            else:
                logger.error(f"❌ Fichier non trouvé après écriture: {playlist_path}")
            
            logger.info(f"✅ Playlist mise à jour avec {len(active_channels)} chaînes: {', '.join(active_channels)}")
            
        except Exception as e:
            logger.error(f"❌ Erreur mise à jour playlist: {e}")
            logger.error(traceback.format_exc())
            # En cas d'erreur, créer une playlist minimale
            try:
                with open(playlist_path, "w", encoding="utf-8") as f:
                    f.write("#EXTM3U\n")
                logger.info(f"✅ Playlist minimale créée en fallback")
            except Exception as inner_e:
                logger.error(f"❌ Échec création playlist minimale: {inner_e}")
                logger.error(traceback.format_exc())

    def _force_ffmpeg_start(self, channel_name: str):
        """Force directement le démarrage d'un stream FFmpeg sans attendre l'initialisation complète"""
        try:
            # Chemin du dossier de la chaîne
            channel_dir = os.path.join(self.manager.content_dir, channel_name)
            
            if not os.path.exists(channel_dir):
                logger.error(f"[{channel_name}] ❌ Le dossier de la chaîne n'existe pas: {channel_dir}")
                return False
                
            # Créer le dossier HLS si nécessaire
            hls_output_dir = f"/app/hls/{channel_name}"
            os.makedirs(hls_output_dir, exist_ok=True)
            os.chmod(hls_output_dir, 0o777)
            
            # Vérifier si la playlist de concaténation existe, sinon la créer
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
            
            ffmpeg_cmd = [
                "ffmpeg",
                "-hide_banner",
                "-loglevel", "warning",
                "-f", "concat",
                "-safe", "0",
                "-stream_loop", "-1",  # Boucle infinie
                "-i", playlist_file,
                "-c:v", "copy",      # Copier le codec vidéo
                "-c:a", "copy",      # Copier le codec audio
                "-f", "hls",         # Format HLS
                "-hls_time", "10",   # Durée des segments
                "-hls_list_size", "6",  # Nombre de segments dans la playlist
                "-hls_flags", "delete_segments+append_list+discont_start",
                "-hls_segment_type", "mpegts",
                "-hls_allow_cache", "1",  # Permettre la mise en cache
                "-hls_segment_filename", f"{hls_output_dir}/segment_%03d.ts",
                f"{hls_output_dir}/playlist.m3u8"
            ]
            
            # Créer les fichiers de log
            log_dir = "/app/logs/ffmpeg"
            os.makedirs(log_dir, exist_ok=True)
            ffmpeg_log = os.path.join(log_dir, f"{channel_name}_ffmpeg.log")
            
            # Démarrer FFmpeg avec redirection des logs
            with open(ffmpeg_log, "a") as log_file:
                process = subprocess.Popen(
                    ffmpeg_cmd,
                    stdout=log_file,
                    stderr=log_file,
                    stdin=subprocess.PIPE,
                    start_new_session=True
                )
            
            # Vérifier que le processus a bien démarré
            if process.poll() is None:
                logger.info(f"[{channel_name}] ✅ Stream FFmpeg démarré avec PID {process.pid}")
                
                # Attendre un peu et vérifier que des segments sont générés
                time.sleep(5)
                segments = list(Path(hls_output_dir).glob("*.ts"))
                if segments:
                    logger.info(f"[{channel_name}] ✅ Segments HLS générés avec succès")
                    return True
                else:
                    logger.warning(f"[{channel_name}] ⚠️ Aucun segment généré après 5s")
                    return False
            else:
                logger.error(f"[{channel_name}] ❌ Échec du démarrage FFmpeg")
                return False
            
        except Exception as e:
            logger.error(f"[{channel_name}] ❌ Erreur démarrage direct FFmpeg: {e}")
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

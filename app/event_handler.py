# event_handler.py
import time
import threading
from watchdog.events import FileSystemEventHandler
from pathlib import Path
import os
from config import logger
import subprocess

class ChannelEventHandler(FileSystemEventHandler):

    def __init__(self, manager):
        self.manager = manager
        self.copying_files = {}  # Dict pour tracker les fichiers en cours de copie
        self.lock = threading.Lock()  # Ajout de l'attribut lock manquant
        self.channel_changes = set()  # Pour suivre les chaînes modifiées
        self.last_scan_time = 0  # Horodatage du dernier scan
        self.scan_cooldown = 5  # Temps minimal entre deux scans en secondes
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
            
        start_time = time.time()
        last_size = -1
        stable_count = 0
        
        # Vérification initiale de la taille pour les gros fichiers
        try:
            initial_size = path.stat().st_size
            # On attend au moins 5 secondes pour les fichiers > 100 MB
            if initial_size > 100 * 1024 * 1024:  # > 100 MB
                logger.info(f"Gros fichier détecté ({initial_size/1024/1024:.1f} MB), attente minimum 5s")
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
                    logger.info(f"Fichier stable depuis {stable_count}s: {file_path} ({current_size/1024/1024:.1f} MB)")
                    
                    # Si le fichier est stable depuis assez longtemps et qu'il s'agit d'un MP4,
                    # on vérifie la présence de l'atom MOOV qui est crucial pour les MP4
                    if stable_count >= 5 and path.suffix.lower() == '.mp4':
                        # Vérification avancée pour MP4
                        if self._verify_mp4_completeness(file_path):
                            return True
                        else:
                            # Si l'atome MOOV est manquant mais que le fichier est stable,
                            # on attend un peu plus pour les gros fichiers
                            if stable_count >= 15:
                                logger.warning(f"❌ Fichier MP4 incomplet même après stabilité prolongée: {file_path}")
                                return False
                    elif stable_count >= 5:
                        # Pour les autres types de fichiers, la stabilité suffit
                        return True
                else:
                    stable_count = 0
                    logger.info(f"{file_path} Taille en évolution: {current_size/1024/1024:.1f} MB (était {last_size/1024/1024:.1f} MB)")
                    
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
                "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                str(file_path)
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            
            # Si ffprobe réussit et retourne une durée, le fichier est probablement valide
            if result.returncode == 0 and result.stdout.strip():
                try:
                    duration = float(result.stdout.strip())
                    if duration > 0:
                        logger.info(f"✅ Fichier MP4 valide avec durée de {duration:.2f}s: {Path(file_path).name}")
                        return True
                except ValueError:
                    pass
            
            # Si on est ici, c'est que ffprobe a échoué ou n'a pas retourné de durée
            error_text = result.stderr.lower()
            if "moov atom not found" in error_text:
                logger.warning(f"⚠️ Atome MOOV manquant dans {Path(file_path).name}, fichier MP4 incomplet")
                return False
                
            logger.warning(f"⚠️ Vérification MP4 échouée pour {Path(file_path).name}: {result.stderr}")
            return False
            
        except subprocess.TimeoutExpired:
            logger.warning(f"⚠️ Timeout lors de la vérification MP4 pour {Path(file_path).name}")
            return False
        except Exception as e:
            logger.error(f"❌ Erreur vérification MP4 {Path(file_path).name}: {e}")
            return False

    def _wait_for_copy_completion(self, file_path: str, channel_name: str = ""):
        """Attend la fin de la copie et déclenche le scan pour une chaîne spécifique.
        Version améliorée avec gestion des fichiers MP4 et retentatives."""
        try:
            # Ignorer les fichiers temporaires de transcodage
            if "tmp_" in Path(file_path).name:
                logger.info(f"Fichier temporaire de transcodage détecté, suivi via FFmpeg: {Path(file_path).name}")
                return
                
            file_size = Path(file_path).stat().st_size if Path(file_path).exists() else 0
            logger.info(f"Surveillance de la copie de {Path(file_path).name} ({file_size/1024/1024:.1f} MB)")
            
            # Pour les fichiers volumineux, attente plus longue
            timeout = 300  # 5 minutes par défaut
            if file_size > 1024 * 1024 * 1024:  # > 1 GB
                timeout = 600  # 10 minutes pour les fichiers > 1 GB
                logger.info(f"Fichier volumineux détecté (> 1 GB), timeout étendu à {timeout}s")
            
            # Ajout de tentatives pour les fichiers MP4
            max_retries = 3
            for attempt in range(max_retries):
                if self.is_file_ready(file_path, timeout=timeout):
                    logger.info(f"✅ Copie terminée: {file_path}")
                    
                    # On attend un peu pour s'assurer que le système de fichiers a fini
                    time.sleep(2)
                    
                    # Si on a le nom de la chaîne, on peut demander juste un refresh de celle-ci
                    if channel_name and channel_name in self.manager.channels:
                        channel = self.manager.channels[channel_name]
                        if hasattr(channel, 'refresh_videos'):
                            logger.info(f"🔄 Rafraîchissement de la chaîne {channel_name}")
                            channel.refresh_videos()
                        else:
                            self._schedule_scan()
                    else:
                        # Sinon, on scanne tout
                        self._schedule_scan()
                        
                    return  # Sortie réussie
                    
                if Path(file_path).suffix.lower() == '.mp4':
                    logger.warning(f"⚠️ Tentative {attempt+1}/{max_retries} échouée pour {Path(file_path).name}")
                    
                    # Pour les MP4, on attend plus longtemps entre les tentatives
                    if attempt < max_retries - 1:
                        wait_time = 30 * (attempt + 1)  # 30s, puis 60s, puis 90s
                        logger.info(f"Attente de {wait_time}s avant nouvelle tentative pour {Path(file_path).name}")
                        time.sleep(wait_time)
                else:
                    # Pour les autres types, on sort directement
                    break
                    
            logger.warning(f"❌ Échec suivi copie après {max_retries} tentatives: {file_path}")
            
        finally:
            with self.lock:
                self.copying_files.pop(file_path, None)
    
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

    def on_created(self, event):
        if event.is_directory:
            logger.info(f"📂 Nouveau dossier détecté: {event.src_path}")
            # On vérifie si c'est un dossier de chaîne
            if Path(event.src_path).parent == Path(self.manager.content_dir):
                logger.info(f"🆕 Nouvelle chaîne potentielle détectée: {Path(event.src_path).name}")
                self.manager.scan_channels()
            return

        # Pour les fichiers, vérification du type de fichier et ignorer les tmp_
        path = Path(event.src_path)
        
        # Ignorer les fichiers temporaires de transcodage
        if "tmp_" in path.name:
            logger.debug(f"Fichier temporaire ignoré: {path.name}")
            return
            
        if path.suffix.lower() not in ['.mp4', '.mkv', '.avi', '.mov','m4v']:
            return  # On ignore les fichiers non vidéo

        # On vérifie si les fichiers sont en cours de copie
        with self.lock:
            if event.src_path in self.copying_files:
                return

            self.copying_files[event.src_path] = time.time()
            
            # On note la chaîne concernée
            channel_name = self.get_channel_from_path(event.src_path)
            if channel_name:
                self.channel_changes.add(channel_name)
            
        # On lance un thread dédié pour surveiller la copie
        threading.Thread(
            target=self._wait_for_copy_completion,
            args=(event.src_path, channel_name),
            daemon=True
        ).start()

    def on_modified(self, event):
        if not event.is_directory:
            # On ignore les modifications si le fichier est en cours de copie
            with self.lock:
                if event.src_path in self.copying_files:
                    return
                    
            # Vérification du type de fichier
            path = Path(event.src_path)
            if path.suffix.lower() not in ['.mp4', '.mkv', '.avi', '.mov', 'm4v', '.txt']:
                return  # On ignore les fichiers non vidéo/non playlist
                
            self._handle_event(event)

    def on_moved(self, event):
        """Gère les déplacements de fichiers (souvent utilisés pour les renommages)"""
        if not event.is_directory:
            # Vérification du type de fichier
            dest_path = Path(event.dest_path)
            if dest_path.suffix.lower() not in ['.mp4', '.mkv', '.avi', '.mov', '.m4v', '.txt']:
                return  # On ignore les fichiers non vidéo/non playlist
                
            # On note la chaîne concernée
            channel_name = self.get_channel_from_path(event.dest_path)
            if channel_name:
                with self.lock:
                    self.channel_changes.add(channel_name)
                    
            self._handle_event(event)

    def _handle_event(self, event):
        logger.debug(f"🔄 Modification détectée: {event.src_path}")
        self._schedule_scan()

    def _schedule_scan(self):
        """Planifie un scan avec un cooldown pour éviter les scans trop fréquents"""
        current_time = time.time()
        
        # Augmentation du cooldown pour éviter les scans trop fréquents
        # 5s -> 30s minimum entre deux scans
        if current_time - self.last_scan_time < self.scan_cooldown:
            logger.debug(f"Scan ignoré: dernier scan il y a {current_time - self.last_scan_time:.1f}s (cooldown: {self.scan_cooldown}s)")
            return
            
        self.last_scan_time = current_time
        
        # Si on a des chaînes spécifiques à scanner
        with self.lock:
            changed_channels = list(self.channel_changes)
            self.channel_changes.clear()
            
        if changed_channels:
            logger.info(f"🔄 Scan programmé pour les chaînes: {', '.join(changed_channels)}")
            for channel_name in changed_channels:
                if channel_name in self.manager.channels:
                    channel = self.manager.channels[channel_name]
                    if hasattr(channel, 'refresh_videos'):
                        threading.Thread(
                            target=channel.refresh_videos,
                            daemon=True
                        ).start()
        else:
            # On ne fait pas de scan complet si aucune chaîne n'est à scanner
            # self.manager.scan_channels()
            logger.debug("Scan complet ignoré: aucune chaîne spécifique à scanner")


        """Planifie un scan avec un cooldown pour éviter les scans trop fréquents"""
        current_time = time.time()
        
        # Si le dernier scan est trop récent, on ne fait rien
        if current_time - self.last_scan_time < self.scan_cooldown:
            return
            
        self.last_scan_time = current_time
        
        # Si on a des chaînes spécifiques à scanner
        with self.lock:
            changed_channels = list(self.channel_changes)
            self.channel_changes.clear()
            
        if changed_channels:
            logger.info(f"🔄 Scan programmé pour les chaînes: {', '.join(changed_channels)}")
            for channel_name in changed_channels:
                if channel_name in self.manager.channels:
                    channel = self.manager.channels[channel_name]
                    if hasattr(channel, 'refresh_videos'):
                        threading.Thread(
                            target=channel.refresh_videos,
                            daemon=True
                        ).start()
        else:
            # Scan complet en dernier recours
            self.manager.scan_channels()
# event_handler.py
import time
import threading
from watchdog.events import FileSystemEventHandler
from pathlib import Path
import os
from config import logger

class ChannelEventHandler(FileSystemEventHandler):
    def __init__(self, manager):
        self.manager = manager
        self.copying_files = {}  # Dict pour tracker les fichiers en cours de copie
        self.lock = threading.Lock()
        self.channel_changes = set()  # Pour suivre les chaînes modifiées
        self.last_scan_time = 0  # Horodatage du dernier scan
        self.scan_cooldown = 5  # Temps minimal entre deux scans en secondes
        super().__init__()

    def is_file_ready(self, file_path: str, timeout: int = 300) -> bool:
        """
        Vérifie si un fichier a fini d'être copié en surveillant sa taille.
        
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
                    # Si stable pendant 5 secondes pour les gros fichiers, on considère que c'est fini
                    if stable_count >= 5:
                        return True
                else:
                    stable_count = 0
                    logger.info(f"Taille en évolution: {current_size/1024/1024:.1f} MB (était {last_size/1024/1024:.1f} MB)")
                    
                last_size = current_size
                time.sleep(1)
                
            except (OSError, PermissionError) as e:
                # Le fichier est probablement encore verrouillé
                logger.debug(f"Fichier {file_path} pas encore accessible: {e}")
                time.sleep(1)
                continue
                
        logger.warning(f"⏰ Timeout en attendant {file_path}")
        return False
    
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
            
        if path.suffix.lower() not in ['.mp4', '.mkv', '.avi', '.mov']:
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
        
    def _wait_for_copy_completion(self, file_path: str, channel_name: str = ""):
        """Attend la fin de la copie et déclenche le scan pour une chaîne spécifique"""
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
            else:
                logger.warning(f"❌ Échec suivi copie: {file_path}")
        finally:
            with self.lock:
                self.copying_files.pop(file_path, None)
    
    def on_modified(self, event):
        if not event.is_directory:
            # On ignore les modifications si le fichier est en cours de copie
            with self.lock:
                if event.src_path in self.copying_files:
                    return
                    
            # Vérification du type de fichier
            path = Path(event.src_path)
            if path.suffix.lower() not in ['.mp4', '.mkv', '.avi', '.mov', '.txt']:
                return  # On ignore les fichiers non vidéo/non playlist
                
            self._handle_event(event)

    def on_moved(self, event):
        """Gère les déplacements de fichiers (souvent utilisés pour les renommages)"""
        if not event.is_directory:
            # Vérification du type de fichier
            dest_path = Path(event.dest_path)
            if dest_path.suffix.lower() not in ['.mp4', '.mkv', '.avi', '.mov', '.txt']:
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
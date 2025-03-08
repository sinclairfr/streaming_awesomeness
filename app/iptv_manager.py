# iptv_manager.py
import os
import sys
import time
import glob
import shutil
import signal
import random
import psutil
import traceback
import subprocess 
from queue import Queue, Empty
from pathlib import Path
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
import threading
from event_handler import (
    ChannelEventHandler, 
    ReadyContentHandler
)
from hls_cleaner import HLSCleaner
from client_monitor import ClientMonitor
from resource_monitor import ResourceMonitor
from iptv_channel import IPTVChannel
import signal
from ffmpeg_monitor import FFmpegMonitor
from config import (
    CONTENT_DIR,
    NGINX_ACCESS_LOG,
    SERVER_URL,
    TIMEOUT_NO_VIEWERS,
    logger,
    VIDEO_EXTENSIONS,
    CPU_THRESHOLD,
    SEGMENT_AGE_THRESHOLD
)
#TODO mkv stream quand meme et sans gpu si condition ok, offset bien géré en premier pour éviter annulation avec lancement stream, 
# maj watcher et playlist périodique ok mais à la demande en cas d'update chaine
# verifier kill process
# statistiques par user dans un json

class IPTVManager:
    """
    Gestionnaire principal du service IPTV - version améliorée avec:
    - Meilleure gestion des dossiers
    - Lancement non bloquant des chaînes
    - Meilleure détection et gestion des fichiers
    """
    def __init__(self, content_dir: str, use_gpu: bool = False):
        # Assurons-nous que la valeur de USE_GPU est bien prise de l'environnement
        use_gpu_env = os.getenv('USE_GPU', 'false').lower() == 'true'
        if use_gpu != use_gpu_env:
            logger.warning(f"⚠️ Valeur USE_GPU en paramètre ({use_gpu}) différente de l'environnement ({use_gpu_env}), on utilise celle de l'environnement")
            use_gpu = use_gpu_env
        
        self.use_gpu = use_gpu
        logger.info(f"✅ Accélération GPU: {'ACTIVÉE' if self.use_gpu else 'DÉSACTIVÉE'}")
        
        self.ensure_hls_directory()  # Sans argument pour le dossier principal

        self.content_dir = content_dir
        self.use_gpu = use_gpu
        self.channels = {}
        self.channel_ready_status = {}  # Pour suivre l'état de préparation des chaînes
        
        # Queue pour les chaînes à initialiser en parallèle
        self.channel_init_queue = Queue()
        self.max_parallel_inits = 3  # Nombre max d'initialisations parallèles
        self.active_init_threads = 0
        self.init_threads_lock = threading.Lock()

        # Moniteur FFmpeg
        self.ffmpeg_monitor = FFmpegMonitor(self.channels)
        self.ffmpeg_monitor.start()

        # On initialise le nettoyeur HLS avec le bon chemin
        self.hls_cleaner = HLSCleaner("/app/hls")
        self.hls_cleaner.initial_cleanup()
        self.hls_cleaner.start()

        self.scan_lock = threading.Lock()
        self.failing_channels = set()

        logger.info("Initialisation du gestionnaire IPTV amélioré")
        self._clean_startup()
        
        # Observer
        self.observer = Observer()
        event_handler = ChannelEventHandler(self)
        self.observer.schedule(event_handler, self.content_dir, recursive=True)

        # NOUVEAU: Observer pour les dossiers ready_to_stream
        self.ready_observer = Observer()
        self.ready_event_handler = ReadyContentHandler(self)

        # Démarrage du thread d'initialisation des chaînes
        self.stop_init_thread = threading.Event()
        self.channel_init_thread = threading.Thread(target=self._process_channel_init_queue, daemon=True)
        self.channel_init_thread.start()

        # Moniteur clients
        logger.info(f"🚀 Démarrage du client_monitor avec {NGINX_ACCESS_LOG}")
        self.client_monitor = ClientMonitor(NGINX_ACCESS_LOG, self.update_watchers, self)

        # Vérification explicite qu'on a bien accès au fichier
        if os.path.exists(NGINX_ACCESS_LOG):
            try:
                with open(NGINX_ACCESS_LOG, 'r') as f:
                    # On lit juste les dernières lignes pour voir si ça fonctionne
                    f.seek(max(0, os.path.getsize(NGINX_ACCESS_LOG) - 1000))
                    last_lines = f.readlines()
                    logger.info(f"✅ Lecture réussie du fichier de log, {len(last_lines)} dernières lignes trouvées")
            except Exception as e:
                logger.error(f"❌ Erreur lors de la lecture du fichier de log: {e}")

        # Démarrage du client_monitor une seule fois
        self.client_monitor.start()

        # Thread de surveillance du log
        self._log_monitor_thread = threading.Thread(
            target=self._check_client_monitor, 
            daemon=True
        )
        self._log_monitor_thread.start()

        # Moniteur ressources
        self.resource_monitor = ResourceMonitor()
        self.resource_monitor.start()

        # Thread de mise à jour de la playlist maître
        self.master_playlist_updater = threading.Thread(
            target=self._manage_master_playlist,
            daemon=True
        )
        self.master_playlist_updater.start()

        # Thread qui vérifie les watchers
        self.watchers_thread = threading.Thread(
            target=self._watchers_loop,
            daemon=True
        )
        self.running = True

    def _check_client_monitor(self):
        """Vérifie périodiquement l'état du client_monitor"""
        while True:
            try:
                if not hasattr(self, 'client_monitor') or not self.client_monitor.is_alive():
                    logger.critical("🚨 client_monitor n'est plus actif!")
                    # Tentative de redémarrage
                    logger.info("🔄 Tentative de redémarrage du client_monitor...")
                    self.client_monitor = ClientMonitor(NGINX_ACCESS_LOG, self.update_watchers, self)
                    self.client_monitor.start()
            except Exception as e:
                logger.error(f"❌ Erreur vérification client_monitor: {e}")
            time.sleep(60)  # Vérification toutes les minutes
            
    def _process_channel_init_queue(self):
        """Traite la queue d'initialisation des chaînes en parallèle"""
        while not self.stop_init_thread.is_set():
            try:
                # Limite le nombre d'initialisations parallèles
                with self.init_threads_lock:
                    if self.active_init_threads >= self.max_parallel_inits:
                        time.sleep(0.5)
                        continue
                
                # Essaie de récupérer une chaîne de la queue
                try:
                    channel_data = self.channel_init_queue.get(block=False)
                except Empty:
                    time.sleep(0.5)
                    continue
                
                # Incrémente le compteur de threads actifs
                with self.init_threads_lock:
                    self.active_init_threads += 1
                
                # Lance un thread pour initialiser cette chaîne
                threading.Thread(
                    target=self._init_channel_async,
                    args=(channel_data,),
                    daemon=True
                ).start()
                
            except Exception as e:
                logger.error(f"Erreur dans le thread d'initialisation: {e}")
                time.sleep(1)
    
    def _init_channel_async(self, channel_data):
        """Initialise une chaîne de manière asynchrone"""
        try:
            channel_name = channel_data["name"]
            channel_dir = channel_data["dir"]
            
            logger.info(f"[{channel_name}] - Initialisation asynchrone de la chaîne: ")
            
            # Crée l'objet chaîne
            channel = IPTVChannel(
                channel_name,
                str(channel_dir),
                hls_cleaner=self.hls_cleaner,
                use_gpu=self.use_gpu
            )
            
            # Ajoute la chaîne au dictionnaire
            with self.scan_lock:
                self.channels[channel_name] = channel
                self.channel_ready_status[channel_name] = False  # Pas encore prête
            
            # Attente que la chaîne soit prête (max 30 secondes)
            for _ in range(30):
                if hasattr(channel, 'ready_for_streaming') and channel.ready_for_streaming:
                    with self.scan_lock:
                        self.channel_ready_status[channel_name] = True
                    logger.info(f"✅ Chaîne {channel_name} prête pour le streaming")
                    break
                time.sleep(1)
            
        except Exception as e:
            logger.error(f"Erreur initialisation de la chaîne {channel_data.get('name')}: {e}")
        finally:
            # Décrémente le compteur de threads actifs
            with self.init_threads_lock:
                self.active_init_threads -= 1
            
            # Marque la tâche comme terminée
            self.channel_init_queue.task_done()

    def auto_start_ready_channels(self):
        """Démarre automatiquement toutes les chaînes prêtes avec un délai entre chaque démarrage"""
        logger.info("🚀 Démarrage automatique des chaînes prêtes...")
        
        # Attendre que plus de chaînes soient prêtes
        for attempt in range(3):
            ready_channels = []
            with self.scan_lock:
                for name, is_ready in self.channel_ready_status.items():
                    if is_ready and name in self.channels:
                        channel = self.channels[name]
                        if channel.ready_for_streaming:
                            ready_channels.append(name)
            
            if len(ready_channels) >= len(self.channels) * 0.5:  # Au moins 50% des chaînes sont prêtes
                break
                
            logger.info(f"⏳ Seulement {len(ready_channels)}/{len(self.channels)} chaînes prêtes, attente supplémentaire ({attempt+1}/3)...")
            time.sleep(10)  # 10 secondes d'attente par tentative
        
        # Trier pour prévisibilité
        ready_channels.sort()
        
        # Limiter le CPU pour éviter saturation
        max_parallel = 4
        groups = [ready_channels[i:i+max_parallel] for i in range(0, len(ready_channels), max_parallel)]
        
        for group_idx, group in enumerate(groups):
            logger.info(f"🚀 Démarrage du groupe {group_idx+1}/{len(groups)} ({len(group)} chaînes)")
            
            # Démarrer chaque chaîne du groupe avec un petit délai entre elles
            for i, channel_name in enumerate(group):
                delay = i * 3  # 3 secondes entre chaque chaîne du même groupe
                threading.Timer(delay, self._start_channel, args=[channel_name]).start()
                logger.info(f"[{channel_name}] ⏱️ Démarrage programmé dans {delay} secondes")
            
            # Attendre avant le prochain groupe
            if group_idx < len(groups) - 1:
                time.sleep(max_parallel * 5)  # 5 secondes par chaîne entre les groupes
        
        if ready_channels:
            logger.info(f"✅ {len(ready_channels)} chaînes programmées pour démarrage automatique")
        else:
            logger.warning("⚠️ Aucune chaîne prête à démarrer")
    
    def _start_channel(self, channel_name):
        """Démarre une chaîne spécifique"""
        try:
            if channel_name in self.channels:
                channel = self.channels[channel_name]
                if channel.ready_for_streaming:
                    logger.info(f"[{channel_name}] 🚀 Démarrage automatique...")
                    success = channel.start_stream()
                    if success:
                        logger.info(f"[{channel_name}] ✅ Démarrage automatique réussi")
                    else:
                        logger.error(f"[{channel_name}] ❌ Échec du démarrage automatique")
                else:
                    logger.warning(f"[{channel_name}] ⚠️ Non prête pour le streaming, démarrage ignoré")
        except Exception as e:
            logger.error(f"[{channel_name}] ❌ Erreur lors du démarrage automatique: {e}") 
            
    def _watchers_loop(self):
        """Surveille l'activité des watchers et arrête les streams inutilisés"""
        last_log_time = 0
        log_cycle = int(os.getenv('WATCHERS_LOG_CYCLE', '10'))
        
        while True:
            try:
                current_time = time.time()
                channels_checked = set()

                # Pour chaque chaîne, on vérifie l'activité
                for channel_name, channel in self.channels.items():
                    if not hasattr(channel, 'last_watcher_time'):
                        continue

                    # On calcule l'inactivité
                    inactivity_duration = current_time - channel.last_watcher_time

                    # Si inactif depuis plus de TIMEOUT_NO_VIEWERS (120s par défaut)
                    if inactivity_duration > TIMEOUT_NO_VIEWERS:
                        if channel.process_manager.is_running():
                            logger.warning(
                                f"[{channel_name}] ⚠️ Stream inactif depuis {inactivity_duration:.1f}s, on arrête FFmpeg"
                            )
                            channel.stop_stream_if_needed()

                    channels_checked.add(channel_name)
                
                # Log périodique de tous les watchers actifs
                if current_time - last_log_time > log_cycle:
                    active_watchers = []
                    for name, channel in sorted(self.channels.items()):
                        if hasattr(channel, 'watchers_count'):
                            count = channel.watchers_count
                            watcher_text = f"{count} watcher{'' if count <= 1 else 's'}"
                            active_watchers.append(f"{name}: {watcher_text}")
                    
                    if active_watchers:
                        logger.info(f"👥 Watchers actifs: {', '.join(active_watchers)}")
                    last_log_time = current_time

                # On vérifie les processus FFmpeg orphelins
                for proc in psutil.process_iter(attrs=["pid", "name", "cmdline"]):
                    try:
                        if "ffmpeg" in proc.info["name"].lower():
                            cmd_str = " ".join(str(arg) for arg in proc.info.get("cmdline", []))
                            
                            # Pour chaque chaîne, on vérifie si le process lui appartient
                            for channel_name in self.channels:
                                if f"/hls/{channel_name}/" in cmd_str:
                                    if channel_name not in channels_checked:
                                        logger.warning(f"🔥 Process FFmpeg orphelin détecté pour {channel_name}, PID {proc.pid}")
                                        try:
                                            os.kill(proc.pid, signal.SIGKILL)
                                            logger.info(f"✅ Process orphelin {proc.pid} nettoyé")
                                        except:
                                            pass
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        continue

                time.sleep(10)  # Vérification toutes les 10s

            except Exception as e:
                logger.error(f"❌ Erreur watchers_loop: {e}")
                time.sleep(10)   

    def update_watchers(self, channel_name: str, count: int, request_path: str):
        """Met à jour les watchers en fonction des requêtes m3u8 et ts"""
        try:
            # Log explicite pour débug
            logger.info(f"🔍 Requête détectée: {channel_name} - {request_path} - count: {count}")
            
            # Si la chaîne n'existe pas, on vérifie si on peut la créer
            if channel_name not in self.channels:
                # [code existant inchangé]
                        
                # Si la chaîne n'est toujours pas disponible
                if channel_name not in self.channels:
                    logger.warning(f"❌ Chaîne inconnue: {channel_name}")
                    return

            channel = self.channels[channel_name]

            # Mise à jour SYSTÉMATIQUE du last_watcher_time à chaque requête
            channel.last_watcher_time = time.time()

            # Si c'est une requête de segment, on met aussi à jour last_segment_time
            if ".ts" in request_path:
                channel.last_segment_time = time.time()

            old_count = getattr(channel, 'watchers_count', 0)
            
            # Pas de vérification conditionnelle, on applique toujours la valeur exacte fournie
            channel.watchers_count = count

            # Log même quand le compte ne change pas, pour débug
            logger.info(f"[{channel_name}] 👁️ Watchers: {count} (était: {old_count})")

            if old_count != count:
                logger.info(f"📊 Mise à jour {channel_name}: {count} watchers")

            # Vérifier et démarrer le stream si nécessaire
            if count > 0:
                # Vérification si la chaîne est prête
                if channel_name in self.channel_ready_status and self.channel_ready_status[channel_name]:
                    if not channel.process_manager.is_running():
                        logger.info(f"[{channel_name}] 🔥 Watchers actifs mais stream arrêté, redémarrage")
                        if not channel.start_stream():
                            logger.error(f"[{channel_name}] ❌ Échec démarrage stream")
                        else:
                            logger.info(f"[{channel_name}] ✅ Stream redémarré avec succès")
                else:
                    logger.warning(f"[{channel_name}] ⚠️ Chaîne pas encore prête, impossible de démarrer le stream")

        except Exception as e:
            logger.error(f"❌ Erreur update_watchers: {e}")
            import traceback
            logger.error(f"Stack trace: {traceback.format_exc()}")
                        
    def _clean_startup(self):
        """Nettoie avant de démarrer"""
        try:
            logger.info("🧹 Nettoyage initial...")
            patterns_to_clean = [
                ("/app/hls/**/*", "Fichiers HLS"),
                ("/app/content/**/_playlist.txt", "Playlists"),
                ("/app/content/**/*.vtt", "Fichiers VTT"),
                ("/app/content/**/temp_*", "Fichiers temporaires"),
            ]
            for pattern, desc in patterns_to_clean:
                count = 0
                for f in glob.glob(pattern, recursive=True):
                    try:
                        if os.path.isfile(f):
                            os.remove(f)
                        elif os.path.isdir(f):
                            shutil.rmtree(f)
                        count += 1
                    except Exception as e:
                        logger.error(f"Erreur nettoyage {f}: {e}")
                logger.info(f"✨ {count} {desc} supprimés")
            os.makedirs("/app/hls", exist_ok=True)
        except Exception as e:
            logger.error(f"Erreur nettoyage initial: {e}")

    def scan_channels(self, force: bool = False, initial: bool = False):
        """
        Scanne le contenu pour détecter les nouveaux dossiers (chaînes).
        Version améliorée avec limitation de fréquence
        """
        # Limiter la fréquence des scans
        current_time = time.time()
        scan_cooldown = 30  # 30s entre scans complets (sauf si force=True)
        
        if not force and not initial and hasattr(self, 'last_scan_time') and current_time - self.last_scan_time < scan_cooldown:
            logger.debug(f"Scan ignoré: dernier scan il y a {current_time - self.last_scan_time:.1f}s")
            return
            
        setattr(self, 'last_scan_time', current_time)
        
        with self.scan_lock:
            try:
                content_path = Path(self.content_dir)
                if not content_path.exists():
                    logger.error(f"Le dossier {content_path} n'existe pas!")
                    return

                channel_dirs = [d for d in content_path.iterdir() if d.is_dir()]

                logger.info(f"📡 Scan des chaînes disponibles...")
                for channel_dir in channel_dirs:
                    channel_name = channel_dir.name

                    if channel_name in self.channels:
                        # Si la chaîne existe déjà, on vérifie son état
                        if force:
                            logger.info(f"🔄 Rafraîchissement de la chaîne {channel_name}")
                            channel = self.channels[channel_name]
                            if hasattr(channel, 'refresh_videos'):
                                channel.refresh_videos()
                        else:
                            logger.info(f"✅ Chaîne existante: {channel_name}")
                        continue

                    logger.info(f"✅ Nouvelle chaîne trouvée: {channel_name}")
                    
                    # Ajoute la chaîne à la queue d'initialisation
                    self.channel_init_queue.put({
                        "name": channel_name,
                        "dir": channel_dir
                    })

                logger.info(f"📡 Scan terminé, {len(channel_dirs)} chaînes identifiées")

            except Exception as e:
                logger.error(f"Erreur scan des chaînes: {e}")

    def ensure_hls_directory(self, channel_name: str = None):
        """Crée et configure les dossiers HLS avec les bonnes permissions"""
        try:
            # Dossier HLS principal
            base_hls = Path("/app/hls")
            if not base_hls.exists():
                logger.info("📂 Création du dossier HLS principal...")
                base_hls.mkdir(parents=True, exist_ok=True)
                os.chmod(base_hls, 0o777)

            # Dossier spécifique à une chaîne si demandé
            if channel_name:
                channel_hls = base_hls / channel_name
                if not channel_hls.exists():
                    logger.info(f"📂 Création du dossier HLS pour {channel_name}")
                    channel_hls.mkdir(parents=True, exist_ok=True)
                    os.chmod(channel_hls, 0o777)
        except Exception as e:
            logger.error(f"❌ Erreur création dossiers HLS: {e}")

    def _manage_master_playlist(self):
        """
        Gère la création et mise à jour de la playlist principale.
        Cette méthode tourne en boucle et regénère la playlist toutes les 60s,
        ou peut être appelée explicitement après un changement.
        """
        # Si c'est un appel direct (après mise à jour d'une chaîne), faire une mise à jour unique
        if threading.current_thread() != self.master_playlist_updater:
            try:
                self._update_master_playlist()
                return
            except Exception as e:
                logger.error(f"Erreur mise à jour ponctuelle de la playlist: {e}")
                return
                
        # Sinon, c'est la boucle normale
        while True:
            try:
                self._update_master_playlist()
                time.sleep(60)  # On attend 60s avant la prochaine mise à jour
            except Exception as e:
                logger.error(f"Erreur maj master playlist: {e}")
                logger.error(traceback.format_exc())
                time.sleep(60)  # On attend même en cas d'erreur

    def _update_master_playlist(self):
        """Effectue la mise à jour de la playlist principale"""
        playlist_path = os.path.abspath("/app/hls/playlist.m3u")
        logger.info(f"🔄 Master playlist maj.: {playlist_path}")

        with open(playlist_path, "w", encoding="utf-8") as f:
            f.write("#EXTM3U\n")

            # Re-vérifie chaque chaîne pour confirmer qu'elle est prête
            with self.scan_lock:
                for name, channel in self.channels.items():
                    # Vérification directe des fichiers
                    ready_dir = Path(channel.video_dir) / "ready_to_stream"
                    has_videos = list(ready_dir.glob("*.mp4")) if ready_dir.exists() else []
                    
                    # Mise à jour du statut si nécessaire
                    if has_videos and not self.channel_ready_status.get(name, False):
                        logger.info(f"[{name}] 🔄 Mise à jour auto du statut: chaîne prête (vidéos trouvées)")
                        self.channel_ready_status[name] = True
                        channel.ready_for_streaming = True
                    elif not has_videos and self.channel_ready_status.get(name, False):
                        logger.info(f"[{name}] ⚠️ Mise à jour auto du statut: chaîne non prête (aucune vidéo)")
                        self.channel_ready_status[name] = False
                        channel.ready_for_streaming = False
            
            # Ne référence que les chaînes prêtes
            ready_channels = []
            for name, channel in sorted(self.channels.items()):
                if name in self.channel_ready_status and self.channel_ready_status[name]:
                    ready_channels.append((name, channel))
            
            # Écriture des chaînes prêtes
            for name, channel in ready_channels:
                f.write(f'#EXTINF:-1 tvg-id="{name}" tvg-name="{name}",{name}\n')
                f.write(f"http://{SERVER_URL}/hls/{name}/playlist.m3u8\n")

        logger.info(f"Playlist mise à jour ({len(ready_channels)} chaînes prêtes sur {len(self.channels)} totales)")
    
    def cleanup(self):
        logger.info("Début du nettoyage...")
        
        # Arrêt du thread d'initialisation
        self.stop_init_thread.set()
        if hasattr(self, "channel_init_thread") and self.channel_init_thread.is_alive():
            self.channel_init_thread.join(timeout=5)
        
        if hasattr(self, "hls_cleaner"):
            self.hls_cleaner.stop()

        if hasattr(self, "observer"):
            self.observer.stop()
            self.observer.join()
        
        if hasattr(self, "ready_observer"):
            self.ready_observer.stop()
            self.ready_observer.join()

        for name, channel in self.channels.items():
            channel._clean_processes()

        logger.info("Nettoyage terminé")
 
    def _setup_ready_observer(self):
        """Configure l'observateur pour les dossiers ready_to_stream de chaque chaîne"""
        try:
            # Pour chaque chaîne existante
            for name, channel in self.channels.items():
                ready_dir = Path(channel.video_dir) / "ready_to_stream"
                if ready_dir.exists():
                    self.ready_observer.schedule(
                        self.ready_event_handler, 
                        str(ready_dir), 
                        recursive=False
                    )
                    logger.info(f"👁️ Surveillance ready_to_stream configurée pour {name}")
            
            # Démarrage de l'observateur s'il n'est pas déjà en cours
            if not self.ready_observer.is_alive():
                self.ready_observer.start()
                logger.info("🚀 Démarrage de l'observateur ready_to_stream")
        
        except Exception as e:
            logger.error(f"❌ Erreur configuration surveillance ready_to_stream: {e}")
   
    def run(self):
        try:
            # Démarrer la boucle de surveillance des watchers
            if not self.watchers_thread.is_alive():
                self.watchers_thread.start()
                logger.info("🔄 Boucle de surveillance des watchers démarrée")
            
            logger.debug("📥 Scan initial des chaînes...")
            self.scan_channels(initial=True)  # Marquer comme scan initial
            
            logger.debug("🕵️ Démarrage de l'observer...")
            if not self.observer.is_alive():
                self.observer.start()
            
            # Configurer l'observateur pour ready_to_stream
            self._setup_ready_observer()
            
            # Attente suffisamment longue pour l'initialisation des chaînes
            logger.info("⏳ Attente de 30 secondes pour l'initialisation des chaînes...")
            time.sleep(30)
            
            # Démarrage automatique des chaînes prêtes
            self.auto_start_ready_channels()
            
            while True:
                time.sleep(1)

        except KeyboardInterrupt:
            self.cleanup()
        except Exception as e:
            logger.error(f"🔥 Erreur manager : {e}")
            self.cleanup()
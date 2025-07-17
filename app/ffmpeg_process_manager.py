# ffmpeg_process_manager.py
import os
import time
import signal
import psutil
import threading
import subprocess
from pathlib import Path
from config import logger, HLS_DIR
import traceback
from typing import Callable, Optional
from datetime import datetime
from error_handler import ErrorHandler  # Import the ErrorHandler class

# Constante pour le seuil de détection des problèmes
# Plus élevé = plus tolérant aux délais entre segments
CRASH_THRESHOLD = 30  # 30 secondes sans nouveau segment avant d'être considéré comme problématique

class FFmpegProcessManager:
    """
    # Gestion centralisée des processus FFmpeg
    # Démarrage, arrêt, surveillance
    """

    # Registre global des instances
    all_channels = {}

    def __init__(self, channel_name, logger_instance=None):
        self.channel_name = channel_name
        self.process = None
        self.lock = threading.Lock()
        self.monitor_thread = None
        self.stop_monitoring = threading.Event()
        self.last_playback_time = 0
        self.playback_offset = 0
        self.total_duration = 0
        self.logger_instance = logger_instance
        
        # État de santé et compteurs
        self.last_segment_time = time.time()  # Initialisation
        self.crash_count = 0
        self.last_crash_time = 0
        self.health_warnings = 0  # Compteur d'avertissements de santé

        # Callbacks
        self.on_process_died = None
        self.on_position_update = None
        self.on_segment_created = None

        # Enregistrement global
        FFmpegProcessManager.all_channels[channel_name] = self

    def is_running(self) -> bool:
        """Vérifie si le processus est en cours d'exécution"""
        return self.process is not None and self.process.poll() is None

    def get_pid(self) -> Optional[int]:
        """Retourne le PID du processus actuel s'il existe"""
        if self.process:
            return self.process.pid
        return None

    def start_process(self, command, hls_dir) -> bool:
        """Démarre un processus FFmpeg avec la commande spécifiée"""
        with self.lock:
            # --- Modified: Stop existing process if running --- 
            if self.is_running():
                logger.warning(f"[{self.channel_name}] Un processus FFmpeg est déjà en cours, tentative d'arrêt...")
                if not self.stop_process(timeout=5): # Try to stop it gracefully
                    logger.error(f"[{self.channel_name}] ❌ Impossible d'arrêter le processus existant, abandon du démarrage.")
                    return False
                # Short delay to allow resources to release
                time.sleep(1.0)
                if self.is_running(): # Double-check if stop actually worked
                    logger.error(f"[{self.channel_name}] ❌ Le processus existant est toujours en cours après tentative d'arrêt, abandon.")
                    return False
                logger.info(f"[{self.channel_name}] ✅ Ancien processus arrêté.")
            # --- End Modification ---

            # Nettoyer les processus existants (zombies)
            self._clean_zombie_processes()

            # Créer le dossier HLS si nécessaire
            os.makedirs(hls_dir, exist_ok=True)

            try:
                # Vérifier que le fichier d'entrée existe
                input_file = None
                for i, arg in enumerate(command):
                    if arg == "-i" and i + 1 < len(command):
                        input_file = command[i + 1]
                        break
                
                if input_file and not os.path.exists(input_file):
                    logger.error(f"[{self.channel_name}] ❌ Fichier d'entrée introuvable: {input_file}")
                    
                    # Gérer l'erreur de fichier introuvable avec notre nouveau gestionnaire d'erreurs
                    error_handler = ErrorHandler(self.channel_name)
                    if error_handler.handle_ffmpeg_file_error(self.channel_name, input_file):
                        logger.info(f"[{self.channel_name}] ✅ Récupération automatique après erreur de fichier")
                        # On retourne False car ce processus ne démarrera pas,
                        # mais le système a déjà lancé une récupération
                    return False

                # Démarrer le processus
                logger.info(f"[{self.channel_name}] 🚀 Démarrage FFmpeg: {' '.join(command[:5])}...")
                
                # Définir le chemin du fichier log FFmpeg
                ffmpeg_log_path = Path(f"/app/logs/ffmpeg/{self.channel_name}_ffmpeg.log")
                # S'assurer que le dossier de logs existe
                ffmpeg_log_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Ouvrir le fichier log en mode append ('a') pour stderr
                stderr_log_file = open(ffmpeg_log_path, 'a')
                
                self.process = subprocess.Popen(
                    command,
                    stdout=subprocess.PIPE,
                    stderr=stderr_log_file,
                    text=True,
                    preexec_fn=os.setsid  # Run in a new process group
                )
                # Enregistrer le temps de démarrage pour la période de grâce
                self.start_time = time.time()
                
                # Le descripteur de fichier peut être fermé par le script Python après le démarrage de Popen,
                # le sous-processus conserve son propre descripteur ouvert.
                stderr_log_file.close() 
                
                # Attendre un peu pour voir si le processus démarre correctement
                time.sleep(2)
                
                # Vérifier si le processus est toujours en cours
                if self.process.poll() is not None:
                    error_output = self.process.stderr.read() if self.process.stderr else "Pas de sortie d'erreur"
                    logger.error(f"[{self.channel_name}] ❌ FFmpeg a échoué à démarrer: {error_output}")
                    return False
                
                # Réinitialiser les compteurs d'état
                self.last_segment_time = time.time()
                self.health_warnings = 0
                
                # Démarrer la surveillance
                self._start_monitoring(hls_dir)
                
                # Vérifier la création du premier segment
                segment_check_start = time.time()
                while time.time() - segment_check_start < 20:  # Attendre jusqu'à 20 secondes
                    if any(Path(hls_dir).glob("segment_*.ts")):
                        logger.info(f"[{self.channel_name}] ✅ Premier segment créé avec succès")
                        return True
                    time.sleep(1)
                
                logger.warning(f"[{self.channel_name}] ⚠️ Aucun segment créé après 20 secondes")
                return True  # On retourne True quand même car le processus est en cours
                
            except Exception as e:
                logger.error(f"[{self.channel_name}] ❌ Erreur démarrage FFmpeg: {e}")
                if self.process:
                    self.process.terminate()
                    self.process = None
                return False

    def stop_process(self, timeout: int = 5) -> bool:
        """Arrête proprement le processus FFmpeg"""
        with self.lock:
            if not self.is_running():
                return True

            try:
                # Arrêter la surveillance
                if self.monitor_thread and self.monitor_thread.is_alive():
                    self.stop_monitoring.set()
                
                pid_to_stop = self.process.pid if self.process else None
                logger.info(f"[{self.channel_name}] 🛑 Arrêt du processus FFmpeg PID {pid_to_stop}")

                if pid_to_stop:
                    try:
                        p = psutil.Process(pid_to_stop)
                        # Terminate children first
                        for child in p.children(recursive=True):
                            try:
                                child.terminate()
                            except psutil.NoSuchProcess:
                                pass
                        # Terminate the parent process
                        p.terminate()
                        
                        # Wait for the process to terminate
                        try:
                            p.wait(timeout=timeout)
                            logger.info(f"[{self.channel_name}] ✅ Processus {pid_to_stop} arrêté proprement.")
                        except psutil.TimeoutExpired:
                            logger.warning(f"[{self.channel_name}] ⚠️ Kill forcé du processus FFmpeg PID {pid_to_stop}")
                            # Kill children first
                            for child in p.children(recursive=True):
                                try:
                                    child.kill()
                                except psutil.NoSuchProcess:
                                    pass
                            # Kill the parent process
                            p.kill()
                            time.sleep(1.0) # Allow time for kill
                            if p.is_running():
                                logger.error(f"[{self.channel_name}] ❌ Échec du kill forcé pour PID {pid_to_stop}")
                            else:
                                logger.info(f"[{self.channel_name}] ✅ Processus {pid_to_stop} tué avec succès.")

                    except psutil.NoSuchProcess:
                        logger.info(f"[{self.channel_name}] ✅ Processus {pid_to_stop} déjà arrêté.")
                    except Exception as e:
                         logger.error(f"[{self.channel_name}] ❌ Erreur lors de l'arrêt du processus {pid_to_stop}: {e}")

                # Nettoyage final
                self.process = None 
                return True

            except Exception as e:
                logger.error(f"[{self.channel_name}] ❌ Erreur arrêt FFmpeg: {e}")
                return False

    def _clean_zombie_processes(self):
        """Nettoie les processus FFmpeg orphelins pour cette chaîne"""
        try:
            pattern = f"/{HLS_DIR.strip('/')}/{self.channel_name}/"
            current_pid = self.get_pid()
            processes_killed = 0

            for proc in psutil.process_iter(["pid", "name", "cmdline"]):
                try:
                    # Vérifier si c'est ffmpeg
                    if "ffmpeg" not in proc.info["name"].lower():
                        continue

                    # Vérifier si c'est pour notre chaîne
                    cmdline = " ".join(proc.info["cmdline"] or [])
                    if pattern not in cmdline:
                        continue

                    # Sauter notre propre processus géré
                    if current_pid and proc.info["pid"] == current_pid:
                        continue
                        
                    # Vérifier si le processus existe toujours
                    if not psutil.pid_exists(proc.info['pid']):
                        continue

                    # Tuer le processus orphelin
                    logger.warning(f"[{self.channel_name}] 🧹 Nettoyage du processus FFmpeg orphelin PID {proc.info['pid']}")
                    try:
                        proc.kill()
                        processes_killed += 1
                        # Attente courte pour laisser le système traiter le kill
                        time.sleep(0.1)
                    except psutil.NoSuchProcess:
                        logger.info(f"[{self.channel_name}] 💨 Processus {proc.info['pid']} déjà terminé.")
                        pass # Process already gone

                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
                except Exception as e:
                    logger.warning(f"[{self.channel_name}] ⚠️ Erreur nettoyage: {e}")

            if processes_killed > 0:
                time.sleep(1)  # Attendre que les processus se terminent

        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur nettoyage global: {e}")

    def _start_monitoring(self, hls_dir):
        """Démarre le thread de surveillance"""
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.stop_monitoring.set()
            if self.monitor_thread != threading.current_thread():
                try:
                    self.monitor_thread.join(timeout=3)
                except RuntimeError:
                    pass
            self.stop_monitoring.clear()

        self.monitor_thread = threading.Thread(
            target=self._monitor_process,
            daemon=True
        )
        self.monitor_thread.start()

    def _monitor_process(self):
        """Surveillance du processus FFmpeg"""
        try:
            # Intervalles de vérification
            health_check_interval = 60  # Vérification de santé toutes les 60 secondes
            startup_grace_period = 15  # Période de grâce au démarrage
            
            last_health_check = time.time()
            startup_time = time.time()
            
            while not self.stop_monitoring.is_set():
                # 1. Vérification de base: processus toujours en vie?
                if not self.is_running():
                    return_code = self.process.poll() if self.process else -999
                    logger.error(f"[{self.channel_name}] ❌ Processus FFmpeg arrêté (code: {return_code})")
                    if self.on_process_died:
                        self.on_process_died(return_code, None)
                    break

                # 2. Vérification périodique de la santé
                current_time = time.time()
                if current_time - last_health_check >= health_check_interval:
                    # Période de grâce au démarrage
                    if current_time - startup_time > startup_grace_period:
                        self.check_stream_health()
                    last_health_check = current_time
                
                # Pause courte pour économiser des ressources
                time.sleep(1)
                
        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur surveillance: {e}")
            if self.on_process_died:
                self.on_process_died(-1, str(e))

    def check_stream_health(self) -> bool:
        """Vérifie la santé du stream HLS"""
        try:
            # Vérification de base du processus
            if not self.is_running():
                return False

            # Récupération du répertoire HLS
            hls_dir = f"{HLS_DIR}/{self.channel_name}"
            if not hls_dir or not Path(hls_dir).exists():
                logger.error(f"[{self.channel_name}] ❌ Répertoire HLS introuvable")
                return False

            # Vérification des segments TS
            ts_files = list(Path(hls_dir).glob("*.ts"))
            if not ts_files:
                logger.warning(f"[{self.channel_name}] ⚠️ Aucun segment TS trouvé")
                return True  # On continue à surveiller

            # --- FIX: Prevent race condition on stat() call ---
            # Safely get stats for existing files to avoid race conditions with the cleaner
            file_stats = []
            for f in ts_files:
                try:
                    file_stats.append((f, f.stat()))
                except FileNotFoundError:
                    # This is expected if the cleaner removes a file while we are checking
                    logger.debug(f"[{self.channel_name}] Segment {f.name} a été supprimé pendant la vérification de santé, ignoré.")
                    continue

            if not file_stats:
                logger.warning(f"[{self.channel_name}] ⚠️ Aucun segment valide trouvé après vérification de l'existence.")
                return True # Continue monitoring

            # Log détaillé des segments pour diagnostic
            logger.debug(f"[{self.channel_name}] 📊 {len(file_stats)} segments trouvés et validés")

            # Trier par date de modification en utilisant les stats pré-chargées
            file_stats.sort(key=lambda x: x[1].st_mtime)
            
            # Vérification de la boucle infinie
            if len(file_stats) > 5:  # Réduit de 10 à 5 segments minimum
                # On vérifie les 3 derniers segments (réduit de 5 à 3)
                last_segments_stats = file_stats[-3:]
                segment_sizes = [s.st_size for _, s in last_segments_stats]
                
                # Log des tailles pour diagnostic
                logger.debug(f"[{self.channel_name}] 📏 Tailles des 3 derniers segments: {segment_sizes}")
                
                # Si les 3 derniers segments ont exactement la même taille
                if len(set(segment_sizes)) == 1 and segment_sizes[0] > 0: # Ignore empty files
                    logger.warning(f"[{self.channel_name}] ⚠️ Détection possible de boucle infinie - segments identiques de taille {segment_sizes[0]}")
                    self.health_warnings += 1
                    
                    if self.health_warnings >= 2:  # Réduit de 3 à 2 avertissements
                        logger.error(f"[{self.channel_name}] ❌ BOUCLE INFINIE CONFIRMÉE - redémarrage du stream")
                        if self.on_process_died:
                            self.on_process_died(-3, "Boucle infinie détectée")
                        return False

            # Vérification du temps écoulé depuis le dernier segment
            # Utiliser le dernier élément de la liste triée
            latest_segment_time = file_stats[-1][1].st_mtime
            current_time = time.time()
            elapsed = current_time - latest_segment_time

            # Seuil de tolérance pour les segments (en secondes)
            segment_threshold = 45  # Augmenté de 30 à 45 secondes

            # Collecter des métriques système
            cpu_usage = psutil.cpu_percent(interval=1)
            memory_usage = psutil.virtual_memory()
            memory_usage_mb = memory_usage.used / (1024 * 1024)

            # Vérification de la taille moyenne des segments
            total_size = sum(s.st_size for _, s in file_stats)
            average_segment_size = total_size / len(file_stats) if file_stats else 0

            # Collecter des métriques pour les problèmes
            if elapsed > segment_threshold:
                # AJOUT: Vérifier si le processus vient de démarrer
                grace_period = 60 # secondes de grâce après le démarrage
                process_uptime = current_time - getattr(self, 'start_time', 0)
                if process_uptime < grace_period:
                    logger.info(f"[{self.channel_name}] ✅ Health check deferred: process recently started ({process_uptime:.1f}s ago).")
                    return True # Continuer la surveillance sans avertissement pendant la période de grâce

                # Log détaillé des métriques système (si hors période de grâce)
                logger.warning(f"[{self.channel_name}] ⚠️ Pas de segment traité depuis {elapsed:.1f}s | CPU: {cpu_usage:.1f}% | RAM: {memory_usage_mb:.1f}MB")
                
                # Accumulation d'avertissements plutôt que décision immédiate
                self.health_warnings += 1
                
                # S'assurer que le compteur ne dépasse pas 5 pour la logique de traitement
                health_warnings_for_display = min(self.health_warnings, 5)
                
                logger.warning(f"[{self.channel_name}] 🚨 Avertissement santé {health_warnings_for_display}/5 - Dernier segment il y a {elapsed:.1f}s")
                
                # Vérification des logs FFmpeg pour indices supplémentaires
                try:
                    ffmpeg_log = Path(f"/app/logs/ffmpeg/{self.channel_name}_ffmpeg.log")
                    if ffmpeg_log.exists():
                        # Lire les 5 dernières lignes du log
                        with open(ffmpeg_log, 'r') as f:
                            lines = f.readlines()
                            last_lines = lines[-10:] if len(lines) >= 10 else lines
                            log_excerpt = "".join(last_lines).strip()
                            
                            # Vérifier des problèmes connus dans les logs
                            issues = []
                            if "error" in log_excerpt.lower():
                                issues.append("Erreur FFmpeg détectée")
                            if "dropping" in log_excerpt.lower():
                                issues.append("FFmpeg abandonne des frames")
                            if "delay" in log_excerpt.lower() and "too large" in log_excerpt.lower():
                                issues.append("Délai trop important")
                            if "stalled" in log_excerpt.lower():
                                issues.append("Lecture bloquée")
                            
                            if issues:
                                logger.warning(f"[{self.channel_name}] 📝 Problèmes détectés dans les logs FFmpeg: {', '.join(issues)}")
                                logger.warning(f"[{self.channel_name}] 📝 Extrait des logs FFmpeg:\n{log_excerpt}")
                except Exception as e:
                    logger.debug(f"[{self.channel_name}] Impossible de lire les logs FFmpeg: {e}")
                
                # Décider d'un problème seulement après 5 avertissements
                if self.health_warnings >= 5:
                    # Diagnostic détaillé
                    diagnosis = "Aucun segment traité (génération ou téléchargement)"
                    if len(file_stats) > 0 and (current_time - latest_segment_time) < 30:
                        diagnosis = "Segments générés mais non téléchargés par les clients"
                    elif cpu_usage > 90:
                        diagnosis = f"CPU surchargé ({cpu_usage:.1f}%), FFmpeg ne peut pas générer les segments assez rapidement"
                    elif len(file_stats) == 0:
                        diagnosis = "Aucun segment généré, FFmpeg a peut-être des problèmes avec le fichier source"
                    
                    logger.error(f"[{self.channel_name}] ❌ PROBLÈME DE SANTÉ CONFIRMÉ après {self.health_warnings} avertissements")
                    logger.error(f"[{self.channel_name}] 📊 Résumé: {elapsed:.1f}s sans segment | CPU: {cpu_usage:.1f}% | {len(file_stats)} segments TS")
                    logger.error(f"[{self.channel_name}] 🔍 DIAGNOSTIC: {diagnosis}")
                    
                    # Notifier seulement après confirmation du problème
                    if self.on_process_died:
                        # Structurer les informations de diagnostic pour le callback
                        error_details = {
                            "type": "health_check_failed",
                            "elapsed": elapsed,
                            "cpu_usage": cpu_usage,
                            "segments_count": len(file_stats),
                            "diagnosis": diagnosis,
                            "average_segment_size": average_segment_size
                        }
                        
                        self.on_process_died(-2, str(error_details))
                    return False
                
                # Continuer à surveiller si pas assez d'avertissements
                return True
            else:
                # Santé normale - log périodique pour surveillance
                if (int(current_time) % 300) < 1:  # Log toutes les ~5 minutes 
                    logger.debug(f"[{self.channel_name}] ✅ Santé OK: dernier segment il y a {elapsed:.1f}s | CPU: {cpu_usage:.1f}%")
                
                # Réinitialiser les avertissements si tout va bien
                if self.health_warnings > 0:
                    logger.info(f"[{self.channel_name}] ✅ Stream de nouveau en bonne santé (dernier segment il y a {elapsed:.1f}s)")
                    self.health_warnings = 0
        
            # Processus en vie et santé OK
            return True
        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur vérification santé: {e}")
            return False

    def on_new_segment(self, segment_path, size):
        """Appelé quand un nouveau segment est créé"""
        # Mise à jour du timestamp de dernier segment
        self.last_segment_time = time.time()
        
        # Réinitialiser le compteur d'avertissements
        self.health_warnings = 0
        
        # Appeler le callback externe si disponible
        if self.on_segment_created:
            self.on_segment_created(segment_path, size)

    def set_playback_offset(self, offset):
        """Définit l'offset de lecture"""
        self.playback_offset = offset

    def get_playback_offset(self):
        """Récupère l'offset de lecture actuel"""
        return self.playback_offset

    def set_total_duration(self, duration):
        """Définit la durée totale de la playlist"""
        self.total_duration = duration
        logger.info(f"[{self.channel_name}] Durée totale définie: {duration:.2f}s")

    def restart_process(self):
        """Redémarre le processus FFmpeg de façon simplifiée"""
        try:
            # Mémoriser l'état et la commande actuelle
            was_running = self.is_running()
            current_offset = self.get_playback_offset() if was_running else 0
            command = self.process.args if self.process else None
            
            if not was_running or not command:
                logger.warning(f"[{self.channel_name}] ⚠️ Impossible de redémarrer: processus inactif ou commande inconnue")
                return False
                
            # Extraction du dossier HLS
            hls_dir = command[-1].rsplit('/', 1)[0]
            
            # Arrêter proprement
            self.stop_process()
            time.sleep(2)
            
            # Redémarrer
            success = self.start_process(command, hls_dir)
            
            if success:
                # Restaurer la position
                self.set_playback_offset(current_offset)
                logger.info(f"[{self.channel_name}] ✅ Redémarrage réussi")
                return True
            else:
                logger.error(f"[{self.channel_name}] ❌ Échec du redémarrage")
                return False
                
        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur lors du redémarrage: {e}")
            return False

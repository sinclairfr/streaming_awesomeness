# ffmpeg_process_manager.py
import os
import time
import signal
import psutil
import threading
import subprocess
from pathlib import Path
from config import logger
import traceback


class FFmpegProcessManager:
    """
    # On centralise toute la gestion des processus FFmpeg
    # Démarrage, arrêt, surveillance, nettoyage, etc.
    """

    def __init__(self, channel_name, logger_instance=None):
        self.channel_name = channel_name
        self.process = None
        self.active_pids = set()  # Tous les PIDs FFmpeg associés à cette chaîne
        self.lock = threading.Lock()
        self.monitor_thread = None
        self.stop_monitoring = threading.Event()
        self.last_playback_time = time.time()
        self.playback_offset = 0
        self.total_duration = 0
        self.logger_instance = logger_instance  # Instance FFmpegLogger, optionnelle

        # Callbacks qu'on peut remplacer depuis l'extérieur
        self.on_process_died = None  # Callback quand le processus meurt
        self.on_position_update = None  # Callback quand la position est mise à jour
        self.on_segment_created = None  # Callback quand un segment est créé

        # Registre global des channels pour retrouver le parent depuis n'importe où
        if not hasattr(FFmpegProcessManager, "all_channels"):
            FFmpegProcessManager.all_channels = {}

    def start_process(self, command, hls_dir):
        """
        # Démarre un processus FFmpeg avec la commande fournie
        # Renvoie True si démarré avec succès, False sinon
        """
        with self.lock:
            logger.debug(f"[{self.channel_name}] 🚀 Démarrage start_process")
            # On nettoie d'abord les processus existants
            self._clean_existing_processes()

            try:
                # On s'assure que le dossier HLS existe
                Path(hls_dir).mkdir(parents=True, exist_ok=True)

                # Préparation du log si disponible
                log_file = None
                log_path = None  # Initialisation de log_path
                if self.logger_instance:
                    log_path = self.logger_instance.get_main_log_file()
                    os.makedirs(os.path.dirname(log_path), exist_ok=True)
                    log_file = open(log_path, "a", buffering=1)
                    logger.info(f"[{self.channel_name}] 📝 Logs FFmpeg -> {log_path}")

                # Configuration de l'environnement
                env = os.environ.copy()
                env["AV_LOG_FORCE_NOCOLOR"] = (
                    "1"  # Évite les problèmes de couleur dans les logs
                )
                if log_path:  # Vérification que log_path est défini
                    env["FFREPORT"] = f"file={log_path}:level=32"  # Log détaillé
                env["TMPDIR"] = (
                    "/tmp"  # S'assure que le dossier temporaire est accessible
                )

                # Lancement du processus
                logger.info(
                    f"[{self.channel_name}] 🚀 Lancement FFmpeg: {' '.join(command)}"
                )

                if log_file:
                    # Avec redirection des logs
                    process = subprocess.Popen(
                        command,
                        stdout=log_file if log_file else subprocess.DEVNULL,
                        stderr=subprocess.STDOUT if log_file else subprocess.DEVNULL,
                        bufsize=1,
                        universal_newlines=True,
                        env=env,  # Ajoute l'environnement
                    )
                else:
                    # Sans redirection (stdout/stderr ignorés)
                    process = subprocess.Popen(
                        command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
                    )

                self.process = process
                self.active_pids.add(process.pid)

                # Vérification rapide du démarrage
                time.sleep(1)
                if self.process.poll() is not None:
                    logger.error(
                        f"[{self.channel_name}] ❌ FFmpeg s'est arrêté immédiatement"
                    )
                    if log_file:
                        log_file.close()
                    return False

                # Démarrage du thread de surveillance
                self._start_monitoring(hls_dir)

                logger.info(
                    f"[{self.channel_name}] ✅ FFmpeg démarré avec PID: {self.process.pid}"
                )
                return True

            except Exception as e:
                logger.error(f"[{self.channel_name}] ❌ Erreur démarrage FFmpeg: {e}")
                logger.error(f"[{self.channel_name}] Détail: {traceback.format_exc()}")
                if "log_file" in locals() and log_file:
                    log_file.close()
                return False

    def stop_process(self):
        """Arrête proprement le processus FFmpeg en cours, sans sauvegarder la position"""
        with self.lock:
            if not self.process:
                return

            try:
                # On arrête la surveillance
                if self.monitor_thread and self.monitor_thread.is_alive():
                    self.stop_monitoring.set()
                    # Évite le join() si c'est le thread courant
                    if self.monitor_thread != threading.current_thread():
                        self.monitor_thread.join(timeout=3)

                pid = self.process.pid
                logger.info(f"[{self.channel_name}] 🛑 Arrêt du processus FFmpeg {pid}")

                # Tentative d'arrêt propre avec SIGTERM
                self.process.terminate()

                # On attend un peu que ça se termine
                for _ in range(5):  # 5 secondes max
                    if self.process.poll() is not None:
                        logger.info(
                            f"[{self.channel_name}] ✅ Processus {pid} terminé proprement"
                        )
                        break
                    time.sleep(1)

                # Si toujours en vie, on force avec SIGKILL
                if self.process.poll() is None:
                    logger.warning(
                        f"[{self.channel_name}] ⚠️ Processus {pid} résistant, envoi de SIGKILL"
                    )
                    self.process.kill()
                    time.sleep(1)

                # On retire ce PID des actifs
                self.active_pids.discard(pid)

                # Nettoyage
                self.process = None

                # On nettoie aussi les autres processus au cas où
                self._clean_orphan_processes()

                logger.info(f"[{self.channel_name}] 🧹 Processus FFmpeg nettoyé")
                return True

            except Exception as e:
                logger.error(f"[{self.channel_name}] ❌ Erreur arrêt FFmpeg: {e}")
                self.process = None
                return False

    def _clean_existing_processes(self):
        """
        # Nettoie tous les processus FFmpeg existants pour cette chaîne
        """
        # Si on a un processus actif, on l'arrête proprement
        if self.process and self.process.poll() is None:
            self.stop_process()

        # On nettoie tous les autres processus qui pourraient traîner
        self._clean_orphan_processes()

    def _clean_orphan_processes(self, force_cleanup=True):
        """Nettoie brutalement les processus FFmpeg orphelins"""
        try:
            # Recherche tous les processus FFmpeg liés à cette chaîne
            pattern = f"/hls/{self.channel_name}/"
            processes_killed = 0

            # Utilise directement ps et grep pour trouver les PID, puis kill -9
            try:
                cmd = f"ps aux | grep ffmpeg | grep '{pattern}' | grep -v grep | awk '{{print $2}}'"
                pids = (
                    subprocess.check_output(cmd, shell=True)
                    .decode()
                    .strip()
                    .split("\n")
                )

                for pid in pids:
                    if pid.strip():
                        try:
                            # Kill -9 direct, sans états d'âme
                            kill_cmd = f"kill -9 {pid}"
                            subprocess.run(kill_cmd, shell=True)
                            logger.info(
                                f"[{self.channel_name}] 🔪 Processus {pid} tué avec kill -9"
                            )
                            processes_killed += 1

                            # Petit délai entre les kills pour éviter les cascades
                            if processes_killed > 0:
                                time.sleep(0.5)
                        except Exception as e:
                            logger.error(
                                f"[{self.channel_name}] Erreur kill -9 {pid}: {e}"
                            )

                # Si on a tué des processus, attendre un peu avant de continuer
                if processes_killed > 0:
                    time.sleep(1)

            except Exception as e:
                logger.error(f"[{self.channel_name}] Erreur recherche processus: {e}")

            # Approche alternative avec psutil comme backup
            for proc in psutil.process_iter(["pid", "name", "cmdline"]):
                try:
                    if "ffmpeg" in proc.info["name"]:
                        cmdline = " ".join(proc.info["cmdline"] or [])
                        if pattern in cmdline:
                            proc.kill()  # SIGKILL directement
                            logger.info(
                                f"[{self.channel_name}] 🔪 Processus {proc.info['pid']} tué via psutil"
                            )
                            processes_killed += 1
                            time.sleep(0.5)  # Petit délai
                except Exception:
                    pass

        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur nettoyage global: {e}")

    def _start_monitoring(self, hls_dir):
        """Démarre le thread de surveillance du processus FFmpeg"""
        if self.monitor_thread and self.monitor_thread.is_alive():
            # On arrête d'abord le thread existant
            self.stop_monitoring.set()
            # Évite le join sur le thread courant
            if self.monitor_thread != threading.current_thread():
                try:
                    self.monitor_thread.join(timeout=3)
                except RuntimeError:
                    logger.warning(
                        f"[{self.channel_name}] Impossible de joindre le thread (thread courant)"
                    )
            self.stop_monitoring.clear()

        # On crée et démarre le nouveau thread
        self.monitor_thread = threading.Thread(
            target=self._monitor_process, args=(hls_dir,), daemon=True
        )
        self.monitor_thread.start()
        logger.debug(f"[{self.channel_name}] 👀 Surveillance FFmpeg démarrée")

    def _monitor_process(self, hls_dir):
        """Surveille le processus FFmpeg en continu"""
        try:
            while not self.stop_monitoring.is_set():
                if not self.process or not self.process.poll() is None:
                    # Le processus est mort
                    return_code = self.process.poll() if self.process else -999
                    if self.on_process_died:
                        # On passe le code de retour comme argument
                        self.on_process_died(return_code)
                    break

                # Mise à jour de la position de lecture
                if self.on_position_update:
                    self.on_position_update(self.playback_offset)

                time.sleep(1)

        except Exception as e:
            logger.error(f"[{self.channel_name}] ❌ Erreur surveillance processus: {e}")
            if self.on_process_died:
                # On passe un code d'erreur -1 en cas d'exception
                self.on_process_died(-1)

    def set_total_duration(self, duration):
        """
        # Définit la durée totale de la playlist
        """
        self.total_duration = duration

    def set_playback_offset(self, offset):
        """
        # Définit l'offset de lecture avec validation
        """
        # S'assurer que l'offset est valide et dans les limites
        if self.total_duration > 0:
            # Appliquer le modulo pour rester dans les limites
            self.playback_offset = offset % self.total_duration
            logger.debug(
                f"[{self.channel_name}] 🔄 Process manager: offset ajusté à {self.playback_offset:.2f}s (modulo {self.total_duration:.2f}s)"
            )
        else:
            self.playback_offset = offset

        self.last_playback_time = time.time()

    def get_playback_offset(self):
        """
        # Renvoie l'offset de lecture actuel
        """
        if self.total_duration <= 0:
            return 0

        # Calcul de l'offset actuel
        current_time = time.time()
        elapsed = current_time - self.last_playback_time
        current_offset = (self.playback_offset + elapsed) % self.total_duration

        return current_offset

    def is_running(self):
        """
        # Vérifie si le processus FFmpeg est en cours d'exécution
        """
        return self.process is not None and self.process.poll() is None

# ffmpeg_monitor.py
import psutil
import threading
import time
from pathlib import Path
from config import logger
import os
import signal
from config import TIMEOUT_NO_VIEWERS, FFMPEG_LOG_LEVEL, logger, WATCHERS_LOG_CYCLE
import random


class FFmpegMonitor(threading.Thread):
    """
    # On centralise toute la surveillance des processus FFmpeg ici
    """

    def __init__(self, channels_dict):
        super().__init__(daemon=True)
        self.channels = channels_dict  # Référence au dict des chaînes
        self.stop_event = threading.Event()
        self.ffmpeg_log_dir = Path("/app/logs/ffmpeg")
        self.ffmpeg_log_dir.mkdir(parents=True, exist_ok=True)

    def _check_all_ffmpeg_processes(self):
        """
        Parcourt tous les processus pour voir lesquels sont liés à FFmpeg,
        groupés par nom de chaîne. Puis gère le nettoyage des processus
        multiples (zombies) avec throttling.
        """
        # Limiter la fréquence d'exécution
        current_time = time.time()
        if hasattr(self, "last_check_time") and current_time - self.last_check_time < 30:
            # Au maximum toutes les 30 secondes
            return

        setattr(self, "last_check_time", current_time)

        ffmpeg_processes = {}

        # Scanne tous les processus système
        for proc in psutil.process_iter(attrs=["pid", "name", "cmdline"]):
            try:
                # Vérifie si c'est un processus ffmpeg
                if "ffmpeg" in proc.info["name"].lower():
                    cmd_args = proc.info["cmdline"] or []
                    cmd_str = " ".join(cmd_args)

                    # Détecte le nom de la chaîne si "/hls/<channel_name>/" est présent
                    for channel_name in self.channels:
                        if f"/hls/{channel_name}/" in cmd_str:
                            ffmpeg_processes.setdefault(channel_name, []).append(
                                proc.info["pid"]
                            )
                            break  # On s'arrête après la première correspondance
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        # Pour chaque chaîne, on vérifie s'il y a plusieurs PIDs ou s'ils sont inactifs
        for channel_name, pids in ffmpeg_processes.items():
            # On récupère la chaîne et calcule le temps depuis le dernier watcher
            channel = self.channels.get(channel_name)
            if not channel:
                continue

            time_since_last_watcher = current_time - channel.last_watcher_time

            # On log des infos de monitoring mais seulement une fois toutes les 5 minutes
            if not hasattr(self, "last_inspect_time"):
                self.last_inspect_time = {}

            if channel_name not in self.last_inspect_time or current_time - self.last_inspect_time[channel_name] > 300:
                logger.info(
                    f"[{channel_name}] - nombre de process : {len(pids)} - temps depuis dernier watcher : {time_since_last_watcher:.1f}s"
                )
                self.last_inspect_time[channel_name] = current_time

            # Si on a plusieurs processus OU qu'on a dépassé le temps d'inactivité, on nettoie
            if len(pids) > 1 or time_since_last_watcher > TIMEOUT_NO_VIEWERS:
                if len(pids) > 1:
                    logger.warning(
                        f"⚠️ {channel_name}: {len(pids)} processus FFmpeg actifs détectés"
                    )
                elif time_since_last_watcher > TIMEOUT_NO_VIEWERS:
                    logger.warning(
                        f"⚠️ {channel_name}: Processus FFmpeg inactif depuis {time_since_last_watcher:.1f}s, arrêt programmé"
                    )
                    # Ajout critique - on arrête le stream directement, sans passer par la fonction  
                    channel.stop_stream_if_needed()  # AJOUT IMPORTANT

                # Ajouter un délai aléatoire avant le nettoyage pour éviter les cascades
                jitter = random.uniform(0.5, 3.0)
                time.sleep(jitter)

                # Nettoyer les processus
                channel.process_manager._clean_orphan_processes(force_cleanup=True)

                # Attendre un peu après le nettoyage
                time.sleep(1)

    def _watchers_loop(self):
        """Surveille l'activité des watchers et arrête les streams inutilisés"""
        last_log_time = 0
        last_health_check = 0
        last_summary_time = 0  # Nouveau compteur pour le récapitulatif
        log_cycle = WATCHERS_LOG_CYCLE  # Augmenter à 60s au lieu de 10s
        summary_cycle = SUMMARY_CYCLE  # 5 minutes par défaut

        while True:
            try:
                current_time = time.time()
                channels_to_stop = []

                # Ajout du health check toutes les 5 minutes
                if current_time - last_health_check > 300:  # 5 minutes
                    for channel_name, channel in self.channels.items():
                        if hasattr(channel, "channel_health_check"):
                            try:
                                channel.channel_health_check()
                            except Exception as e:
                                logger.error(
                                    f"[{channel_name}] ❌ Erreur health check: {e}"
                                )
                    last_health_check = current_time

                # Générer le récapitulatif des chaînes
                if current_time - last_summary_time > summary_cycle:
                    self._log_channels_summary()
                    last_summary_time = current_time

                # Pour chaque chaîne, on vérifie l'inactivité
                for channel_name, channel in self.channels.items():
                    if not hasattr(channel, "last_watcher_time"):
                        continue

                    # On calcule l'inactivité
                    inactivity_duration = current_time - channel.last_watcher_time

                    # Si inactif depuis plus de TIMEOUT_NO_VIEWERS
                    if inactivity_duration > TIMEOUT_NO_VIEWERS:
                        if channel.process_manager.is_running():
                            logger.warning(
                                f"[{channel_name}] ⚠️ Stream inactif depuis {inactivity_duration:.1f}s, arrêt programmé"
                            )
                            channels_to_stop.append(channel)
                            
                            # CORRECTION: Forcer l'arrêt immédiat pour les streams très inactifs
                            if inactivity_duration > TIMEOUT_NO_VIEWERS * 2:  # Double timeout = forcer l'arrêt
                                logger.error(
                                    f"[{channel_name}] 🔥 Inactivité CRITIQUE ({inactivity_duration:.1f}s), arrêt forcé"
                                )
                                channel.stop_stream_if_needed()

                # Arrêt des chaînes sans watchers (avec un délai pour éviter les cascades)
                for i, channel in enumerate(channels_to_stop):
                    # Ajout d'un petit délai entre les arrêts (0.5s entre chaque)
                    time.sleep(i * 0.5)
                    channel.stop_stream_if_needed()

                # Log périodique des watchers actifs (moins fréquent)
                if current_time - last_log_time > log_cycle:
                    active_channels = []
                    for name, channel in sorted(self.channels.items()):
                        if (
                            hasattr(channel, "watchers_count")
                            and channel.watchers_count > 0
                        ):
                            active_channels.append(f"{name}: {channel.watchers_count}")

                    if active_channels:
                        logger.info(
                            f"👥 Chaînes avec viewers: {', '.join(active_channels)}"
                        )
                    last_log_time = current_time

                time.sleep(10)  # Vérification toutes les 10s

            except Exception as e:
                logger.error(f"❌ Erreur watchers_loop: {e}")
                time.sleep(10)
    def _save_stats_periodically(self):
        """Sauvegarde périodiquement les statistiques"""
        if hasattr(self.channels, "stats_collector") and self.channels.stats_collector:
            self.channels.stats_collector.save_stats()

    def run(self):
        while not self.stop_event.is_set():
            try:
                self._check_all_ffmpeg_processes()
                time.sleep(10)  # On vérifie toutes les 10s
            except Exception as e:
                logger.error(f"❌ Erreur monitoring FFmpeg: {e}")
                time.sleep(10)

#!/usr/bin/env python3
# stats_summary.py - Script utilitaire pour visualiser les statistiques IPTV

import json
import os
import sys
import time
from pathlib import Path


def format_time(seconds):
    """Formate un temps en secondes en h:m:s"""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    return f"{hours}h {minutes}m {secs}s"


def load_stats_file(file_path):
    """Charge un fichier de statistiques"""
    try:
        with open(file_path, "r") as f:
            return json.load(f)
    except json.JSONDecodeError:
        print(f"Erreur: Fichier {file_path} corrompu ou mal formaté")
        return None
    except FileNotFoundError:
        print(f"Erreur: Fichier {file_path} introuvable")
        return None


def show_stats_summary(stats_file):
    """Affiche un résumé des statistiques"""
    stats = load_stats_file(stats_file)
    if not stats:
        return

    print("\n" + "=" * 50)
    print(f"📊 RÉSUMÉ DES STATISTIQUES: {Path(stats_file).name}")
    print("=" * 50)

    # Statistiques globales
    global_stats = stats.get("global", {})
    print("\n📈 STATISTIQUES GLOBALES:")
    print(f"- Pic de spectateurs: {global_stats.get('peak_watchers', 0)}")
    if global_stats.get("peak_time", 0) > 0:
        peak_time = time.strftime(
            "%Y-%m-%d %H:%M:%S", time.localtime(global_stats.get("peak_time", 0))
        )
        print(f"- Pic atteint le: {peak_time}")
    print(
        f"- Temps de visionnage total: {format_time(global_stats.get('total_watch_time', 0))}"
    )
    print(
        f"- Dernière mise à jour: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(global_stats.get('last_updated', 0)))}"
    )

    # Statistiques par chaîne
    channels = stats.get("channels", {})
    if channels:
        print("\n📺 STATISTIQUES PAR CHAÎNE:")
        # Trier les chaînes par nombre de spectateurs actuels puis par pic
        sorted_channels = sorted(
            [(name, data) for name, data in channels.items()],
            key=lambda x: (
                x[1].get("current_watchers", 0),
                x[1].get("peak_watchers", 0),
            ),
            reverse=True,
        )

        for name, data in sorted_channels:
            current = data.get("current_watchers", 0)
            peak = data.get("peak_watchers", 0)
            total_time = data.get("total_watch_time", 0)
            sessions = data.get("session_count", 0)
            segments = data.get("total_segments", 0)

            # Ne montrer que les chaînes avec activité
            if peak > 0 or total_time > 0 or segments > 0:
                print(f"\n🎬 {name}:")
                print(f"  - Spectateurs actuels: {current}")
                print(f"  - Pic de spectateurs: {peak}")
                print(f"  - Temps de visionnage: {format_time(total_time)}")
                print(f"  - Sessions: {sessions}")
                print(f"  - Segments générés: {segments}")

                # Afficher les IPs connectées si disponibles
                watchlist = data.get("watchlist", {})
                if watchlist:
                    active_ips = [
                        ip
                        for ip, ip_data in watchlist.items()
                        if time.time() - ip_data.get("last_seen", 0) < 3600
                    ]  # IPs actives dans la dernière heure
                    if active_ips:
                        print(f"  - IPs actives récemment: {len(active_ips)}")

                    # Top 3 des IPs par temps de visionnage
                    top_ips = sorted(
                        [(ip, ip_data) for ip, ip_data in watchlist.items()],
                        key=lambda x: x[1].get("total_time", 0),
                        reverse=True,
                    )[:3]

                    if top_ips:
                        print("  - Top IPs par visionnage:")
                        for ip, ip_data in top_ips:
                            view_time = format_time(ip_data.get("total_time", 0))
                            last_seen = time.strftime(
                                "%Y-%m-%d %H:%M:%S",
                                time.localtime(ip_data.get("last_seen", 0)),
                            )
                            print(
                                f"    * {ip}: {view_time} (dernière vue: {last_seen})"
                            )

    # Statistiques par jour
    daily_stats = stats.get("daily", {})
    if daily_stats:
        print("\n📅 STATISTIQUES QUOTIDIENNES:")
        # Trier les jours par date décroissante (plus récent d'abord)
        sorted_days = sorted(daily_stats.keys(), reverse=True)

        for day in sorted_days:
            day_data = daily_stats[day]
            print(f"\n📆 {day}:")
            print(f"  - Pic de spectateurs: {day_data.get('peak_watchers', 0)}")
            print(
                f"  - Temps de visionnage: {format_time(day_data.get('total_watch_time', 0))}"
            )

            # Top 3 des chaînes par jour
            day_channels = day_data.get("channels", {})
            if day_channels:
                top_channels = sorted(
                    [(ch_name, ch_data) for ch_name, ch_data in day_channels.items()],
                    key=lambda x: x[1].get("total_watch_time", 0),
                    reverse=True,
                )[:3]

                if top_channels:
                    print("  - Top chaînes:")
                    for ch_name, ch_data in top_channels:
                        view_time = format_time(ch_data.get("total_watch_time", 0))
                        peak = ch_data.get("peak_watchers", 0)
                        print(f"    * {ch_name}: {view_time} (pic: {peak} spectateurs)")

    # Statistiques utilisateurs
    user_stats_file = str(Path(stats_file).parent / "user_stats.json")
    if os.path.exists(user_stats_file):
        user_stats = load_stats_file(user_stats_file)
        if user_stats and "users" in user_stats:
            print("\n👥 STATISTIQUES UTILISATEURS:")

            # Trier les utilisateurs par temps de visionnage total
            sorted_users = sorted(
                [(ip, data) for ip, data in user_stats["users"].items()],
                key=lambda x: x[1].get("total_watch_time", 0),
                reverse=True,
            )[
                :10
            ]  # Top 10 utilisateurs

            for ip, data in sorted_users:
                total_time = format_time(data.get("total_watch_time", 0))
                first_seen = time.strftime(
                    "%Y-%m-%d", time.localtime(data.get("first_seen", 0))
                )
                last_seen = time.strftime(
                    "%Y-%m-%d %H:%M:%S", time.localtime(data.get("last_seen", 0))
                )

                # Déterminer la chaîne favorite
                channels = data.get("channels", {})
                favorite = "aucune"
                for ch_name, ch_data in channels.items():
                    if ch_data.get("favorite", False):
                        favorite = ch_name
                        break

                print(f"\n👤 Utilisateur: {ip}")
                print(f"  - Temps total: {total_time}")
                print(f"  - Premier accès: {first_seen}")
                print(f"  - Dernier accès: {last_seen}")
                print(f"  - Chaîne favorite: {favorite}")
                print(f"  - Chaînes consultées: {len(channels)}")

                # Top 3 des chaînes de cet utilisateur
                if channels:
                    top_user_channels = sorted(
                        [(ch_name, ch_data) for ch_name, ch_data in channels.items()],
                        key=lambda x: x[1].get("total_watch_time", 0),
                        reverse=True,
                    )[:3]

                    if top_user_channels:
                        print("  - Top chaînes:")
                        for ch_name, ch_data in top_user_channels:
                            ch_time = format_time(ch_data.get("total_watch_time", 0))
                            print(f"    * {ch_name}: {ch_time}")


def main():
    """Fonction principale"""
    # Chemin par défaut
    stats_dir = "/app/stats"

    # Utiliser le premier argument comme chemin si fourni
    if len(sys.argv) > 1:
        stats_dir = sys.argv[1]

    stats_file = os.path.join(stats_dir, "channel_stats.json")

    # Vérifier si le fichier existe
    if not os.path.exists(stats_file):
        print(f"Erreur: Fichier de statistiques introuvable à {stats_file}")
        print("Vous pouvez spécifier un autre chemin en argument.")
        sys.exit(1)

    show_stats_summary(stats_file)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# stats_maintenance.py - Utilitaire de maintenance des statistiques

import json
import os
import sys
import time
from pathlib import Path


def fix_stats_file(stats_file):
    """Corrige les problèmes courants dans le fichier de statistiques"""
    try:
        # Chargement du fichier
        with open(stats_file, "r") as f:
            stats = json.load(f)

        # Sauvegarde du fichier original
        backup_path = f"{stats_file}.bak"
        with open(backup_path, "w") as f:
            json.dump(stats, f, indent=2)
            print(f"✅ Sauvegarde créée: {backup_path}")

        # Corrections
        changes_made = 0

        # Correction 1: S'assurer que tous les temps de visionnage sont des entiers
        for channel_name, channel_data in stats.get("channels", {}).items():
            if not isinstance(channel_data.get("total_watch_time"), (int, float)):
                channel_data["total_watch_time"] = 0
                changes_made += 1

            # Vérifier les watchlists
            for ip, ip_data in channel_data.get("watchlist", {}).items():
                if not isinstance(ip_data.get("total_time"), (int, float)):
                    ip_data["total_time"] = 0
                    changes_made += 1

        # Correction 2: S'assurer que les statistiques globales sont correctes
        if "global" in stats:
            if not isinstance(stats["global"].get("total_watch_time"), (int, float)):
                stats["global"]["total_watch_time"] = 0
                changes_made += 1

            # Recalculer le total global si nécessaire
            channel_total = sum(
                ch.get("total_watch_time", 0)
                for ch in stats.get("channels", {}).values()
            )
            if stats["global"].get("total_watch_time", 0) != channel_total:
                stats["global"]["total_watch_time"] = channel_total
                changes_made += 1
                print(f"📊 Total global recalculé: {channel_total}")

        # Correction 3: Vérifier les statistiques quotidiennes
        for day, day_data in stats.get("daily", {}).items():
            if not isinstance(day_data.get("total_watch_time"), (int, float)):
                day_data["total_watch_time"] = 0
                changes_made += 1

            # Vérifier les chaînes quotidiennes
            for ch_name, ch_data in day_data.get("channels", {}).items():
                if not isinstance(ch_data.get("total_watch_time"), (int, float)):
                    ch_data["total_watch_time"] = 0
                    changes_made += 1

        # Sauvegarder les modifications
        if changes_made > 0:
            with open(stats_file, "w") as f:
                json.dump(stats, f, indent=2)
            print(f"✅ Corrections appliquées ({changes_made} modifications)")
        else:
            print("✅ Aucune correction nécessaire")

        return True

    except Exception as e:
        print(f"❌ Erreur: {e}")
        return False


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

    # Corriger le fichier
    fix_stats_file(stats_file)


if __name__ == "__main__":
    main()

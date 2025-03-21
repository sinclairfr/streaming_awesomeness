#!/usr/bin/env python3
import os
import sys
import time

# Vérifier si les fichiers de statistiques existent déjà
STATS_DIR = "/app/stats"
USER_STATS_FILE = os.path.join(STATS_DIR, "user_stats.json")
CHANNEL_STATS_FILE = os.path.join(STATS_DIR, "channel_stats.json")

def main():
    # Créer le dossier stats s'il n'existe pas
    os.makedirs(STATS_DIR, exist_ok=True)
    
    # Si les fichiers n'existent pas, générer des données
    if not os.path.exists(USER_STATS_FILE) or not os.path.exists(CHANNEL_STATS_FILE):
        print("Fichiers de statistiques non trouvés, génération de données de test...")
        try:
            os.system("python generate_mock_data.py")
        except Exception as e:
            print(f"Erreur lors de la génération des données: {e}")
    else:
        print("Fichiers de statistiques trouvés, utilisation des données existantes")
    
    # Démarrer le dashboard
    print("Démarrage du dashboard...")
    os.system("python stats_dashboard.py")

if __name__ == "__main__":
    main()
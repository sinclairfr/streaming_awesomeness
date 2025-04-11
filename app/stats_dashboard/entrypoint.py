#!/usr/bin/env python3
import os
import sys
import time

# # Vérifier si les fichiers de statistiques existent déjà (LOGIQUE SUPPRIMÉE)
# STATS_DIR = "/app/stats"
# USER_STATS_FILE = os.path.join(STATS_DIR, "user_stats.json") # Ancien nom
# CHANNEL_STATS_FILE = os.path.join(STATS_DIR, "channel_stats.json")

def main():
    # Créer le dossier stats s'il n'existe pas (toujours utile)
    STATS_DIR = "/app/stats"
    os.makedirs(STATS_DIR, exist_ok=True)
    
    # # Si les fichiers n'existent pas, générer des données (LOGIQUE SUPPRIMÉE)
    # if not os.path.exists(USER_STATS_FILE) or not os.path.exists(CHANNEL_STATS_FILE):
    #     print("Fichiers de statistiques non trouvés, génération de données de test...")
    #     try:
    #         os.system("python generate_mock_data.py")
    #     except Exception as e:
    #         print(f"Erreur lors de la génération des données: {e}")
    # else:
    #     print("Fichiers de statistiques trouvés, utilisation des données existantes")
    
    # Démarrer le dashboard directement
    print("Démarrage du dashboard...")
    # Utiliser exec pour remplacer le processus actuel par le dashboard
    # Cela évite de garder le script entrypoint en mémoire inutilement
    try:
        os.execvp("python", ["python", "stats_dashboard.py"])
    except FileNotFoundError:
        print("Erreur: Commande 'python' non trouvée. Assurez-vous que Python est dans le PATH.")
        sys.exit(1)
    except Exception as e:
        print(f"Erreur lors du démarrage du dashboard: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
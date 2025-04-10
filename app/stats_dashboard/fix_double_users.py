#!/usr/bin/env python3
"""
Script pour corriger le problème de double imbrication 'users' dans le fichier user_stats.json
et ajouter des utilisateurs de test si nécessaire
"""
import json
import os
import time
import sys
import random
from pathlib import Path
from datetime import datetime, timedelta

def generate_test_users(count=5):
    """Génère des données de test pour les utilisateurs"""
    test_users = {}
    current_time = int(time.time())
    
    # Quelques exemples de chaînes
    channels = ["france24", "bfmtv", "cnn", "euronews", "m6", "tf1", "c8", "nrj12"]
    
    # Générer des utilisateurs aléatoires
    for i in range(1, count+1):
        ip = f"192.168.1.{i+10}"
        
        # Nombre aléatoire de chaînes vues par cet utilisateur (1-3)
        user_channels = random.sample(channels, random.randint(1, 3))
        
        # Créer structure pour cet utilisateur
        user_data = {
            "total_watch_time": random.randint(600, 7200),  # 10 min - 2h
            "last_seen": current_time - random.randint(0, 86400),  # 0-24h
            "channels_watched": user_channels,
            "user_agent": "Mozilla/5.0 Test Browser",
            "channels": {}
        }
        
        # Ajouter les données par chaîne
        total_time = user_data["total_watch_time"]
        channel_times = []
        
        # Répartir le temps total entre les chaînes
        for _ in range(len(user_channels) - 1):
            time_slice = random.uniform(0.1, 0.4) * total_time
            channel_times.append(time_slice)
            total_time -= time_slice
        
        channel_times.append(total_time)  # Ajouter le temps restant
        
        # Créer les entrées pour chaque chaîne
        for idx, channel in enumerate(user_channels):
            channel_time = channel_times[idx]
            user_data["channels"][channel] = {
                "first_seen": current_time - random.randint(86400, 604800),  # 1-7 jours
                "last_seen": user_data["last_seen"],
                "total_watch_time": channel_time,
                "favorite": idx == 0  # La première chaîne est la favorite
            }
        
        test_users[ip] = user_data
    
    print(f"Généré {count} utilisateurs de test")
    return test_users

def fix_double_users(file_path, backup=True, add_test_users=False):
    """Corrige le problème de double imbrication 'users' dans le fichier user_stats.json"""
    
    print(f"Correction du fichier: {file_path}")
    
    # Vérifier si le fichier existe
    if not os.path.exists(file_path):
        print(f"Erreur: Fichier {file_path} non trouvé")
        return False
        
    # Créer une sauvegarde
    if backup:
        backup_file = f"{file_path}.bak.{int(time.time())}"
        try:
            import shutil
            shutil.copy2(file_path, backup_file)
            print(f"Sauvegarde créée: {backup_file}")
        except Exception as e:
            print(f"Attention: Impossible de créer une sauvegarde: {e}")
    
    try:
        # Charger le fichier
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        # Vérifier la structure
        if not isinstance(data, dict):
            print(f"Erreur: Format JSON invalide dans {file_path}")
            return False
        
        # Structure corrigée
        fixed_data = {
            "users": {},
            "last_updated": data.get("last_updated", int(time.time()))
        }
        
        # Vérifier si nous avons un problème de double imbrication
        if "users" in data and isinstance(data["users"], dict):
            users_data = data["users"]
            
            # Cas de double imbrication: {"users": {"users": {...}}}
            if "users" in users_data and isinstance(users_data["users"], dict):
                print("Double imbrication 'users' détectée, correction en cours...")
                fixed_data["users"] = users_data["users"]
            else:
                # Structure correcte ou simple imbrication
                fixed_data["users"] = users_data
        
        # Ajouter des utilisateurs de test si nécessaire
        if add_test_users and (not fixed_data["users"] or len(fixed_data["users"]) == 0):
            print("Aucun utilisateur trouvé, ajout d'utilisateurs de test...")
            fixed_data["users"] = generate_test_users(5)
        
        # Sauvegarder le fichier corrigé
        with open(file_path, 'w') as f:
            json.dump(fixed_data, f, indent=2)
        
        print(f"Fichier corrigé sauvegardé: {file_path}")
        print(f"Nombre d'utilisateurs: {len(fixed_data['users'])}")
        print(f"Structure finale: {json.dumps(fixed_data, indent=2)[:200]}...")
        return True
        
    except Exception as e:
        print(f"Erreur lors de la correction du fichier: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    # Chemin par défaut
    default_path = "/app/stats/user_stats.json"
    
    # Obtenir le chemin à partir des arguments si fourni
    file_path = sys.argv[1] if len(sys.argv) > 1 else default_path
    
    # Déterminer si on doit ajouter des utilisateurs de test
    add_test_users = "--add-test-users" in sys.argv
    
    # Corriger le fichier
    success = fix_double_users(file_path, backup=True, add_test_users=add_test_users)
    
    # Sortir avec le code approprié
    sys.exit(0 if success else 1) 
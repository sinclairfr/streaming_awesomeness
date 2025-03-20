#!/bin/bash
# update_stats_system.sh - Script pour appliquer toutes les corrections

# Couleurs pour une meilleure lisibilité
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== Script de mise à jour du système de statistiques ===${NC}"

# Vérifier qu'on est dans le bon dossier
if [ ! -d "/app" ]; then
  echo -e "${RED}Erreur: Ce script doit être exécuté dans le conteneur iptv-manager${NC}"
  exit 1
fi

# Fonction pour sauvegarder un fichier
backup_file() {
  local file=$1
  if [ -f "$file" ]; then
    local backup="${file}.bak.$(date +%Y%m%d%H%M%S)"
    cp "$file" "$backup"
    echo -e "${GREEN}✓ Sauvegarde créée: ${backup}${NC}"
  else
    echo -e "${RED}✗ Fichier $file introuvable${NC}"
    return 1
  fi
}

# Fonction pour remplacer du code dans un fichier
replace_in_file() {
  local file=$1
  local search=$2
  local replace=$3
  
  # Vérifier que le fichier existe
  if [ ! -f "$file" ]; then
    echo -e "${RED}✗ Fichier $file introuvable${NC}"
    return 1
  fi
  
  # Vérifier que le pattern existe
  if ! grep -q "$search" "$file"; then
    echo -e "${YELLOW}⚠ Pattern non trouvé dans $file${NC}"
    echo -e "${YELLOW}⚠ Veuillez appliquer manuellement:${NC}"
    echo -e "$replace"
    return 1
  fi
  
  # Faire le remplacement
  sed -i "s/$search/$replace/g" "$file"
  echo -e "${GREEN}✓ Modification appliquée dans $file${NC}"
}

# Fonction pour ajouter du code après un match dans un fichier
add_after_match() {
  local file=$1
  local match=$2
  local code=$3
  
  # Vérifier que le fichier existe
  if [ ! -f "$file" ]; then
    echo -e "${RED}✗ Fichier $file introuvable${NC}"
    return 1
  fi
  
  # Vérifier que le pattern existe
  if ! grep -q "$match" "$file"; then
    echo -e "${YELLOW}⚠ Pattern non trouvé dans $file${NC}"
    echo -e "${YELLOW}⚠ Veuillez appliquer manuellement:${NC}"
    echo -e "$code"
    return 1
  fi
  
  # Faire l'ajout
  sed -i "/$match/a $code" "$file"
  echo -e "${GREEN}✓ Code ajouté dans $file${NC}"
}

echo -e "${BLUE}1. Sauvegarde des fichiers originaux...${NC}"
backup_file "/app/ffmpeg_process_manager.py"
backup_file "/app/client_monitor.py"
backup_file "/app/stats_collector.py"
backup_file "/app/iptv_channel.py"

echo -e "${BLUE}2. Modification de _handle_process_died dans iptv_channel.py...${NC}"
# Cette modification est plus complexe et nécessiterait un patch complet
# On recommande ici d'utiliser l'éditeur directement

echo -e "${YELLOW}Pour modifier _handle_process_died dans iptv_channel.py:${NC}"
echo -e "${YELLOW}1. Ajoutez un paramètre 'return_code' à la fonction${NC}"
echo -e "${YELLOW}2. Utilisez return_code dans le log d'erreur${NC}"
echo -e "${YELLOW}3. Ne changez pas le reste du code${NC}"

echo -e "${BLUE}3. Modification de _process_log_line dans client_monitor.py...${NC}"
# Ajouter le code pour mettre à jour les statistiques
echo -e "${YELLOW}Pour modifier _process_log_line dans client_monitor.py:${NC}"
echo -e "${YELLOW}Après l'extraction de segment_id, ajoutez ce code:${NC}"
echo "            # AJOUT: Mise à jour explicite des stats ici
            # Déterminer la durée à ajouter selon le type de requête
            if request_type == \"segment\":
                # Les segments sont typiquement de 2-4 secondes
                duration = 4.0
            elif request_type == \"playlist\":
                # Les playlists sont des heartbeats
                duration = 0.5
            else:
                # Pour les autres types, durée minimale
                duration = 0.1
                
            # Mise à jour des statistiques via le StatsCollector
            if hasattr(self.manager, \"stats_collector\") and self.manager.stats_collector:
                self.manager.stats_collector.add_watch_time(channel, ip, duration)
                # Mise à jour des stats utilisateur également
                self.manager.stats_collector.update_user_stats(ip, channel, duration, user_agent)"

echo -e "${BLUE}4. Création du script de diagnostic des statistiques...${NC}"
# Créer le script de diagnostic
cat > /app/debug_stats.py << 'EOF'
#!/usr/bin/env python3
# debug_stats.py - Script de diagnostic des statistiques
# Exécuter avec: python debug_stats.py

import json
import os
import sys
import time
from pathlib import Path

STATS_DIR = "/app/stats"

def format_time(seconds):
    """Formate un temps en secondes en h:m:s"""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    return f"{hours}h {minutes}m {secs}s"

def load_json_file(file_path):
    """Charge un fichier JSON avec gestion d'erreur"""
    try:
        if not os.path.exists(file_path):
            print(f"⚠️ Fichier inexistant: {file_path}")
            return None
            
        with open(file_path, "r") as f:
            return json.load(f)
    except json.JSONDecodeError:
        print(f"❌ Erreur: Fichier {file_path} corrompu")
        return None
    except Exception as e:
        print(f"❌ Erreur lors du chargement de {file_path}: {e}")
        return None

def check_stats_files():
    """Vérifie l'existence et la validité des fichiers de statistiques"""
    print("\n--- VÉRIFICATION DES FICHIERS DE STATISTIQUES ---")
    
    # Vérifier le dossier
    stats_dir = Path(STATS_DIR)
    if not stats_dir.exists():
        print(f"❌ Le dossier {STATS_DIR} n'existe pas!")
        try:
            os.makedirs(STATS_DIR, exist_ok=True)
            print(f"✅ Dossier {STATS_DIR} créé")
        except Exception as e:
            print(f"❌ Impossible de créer le dossier: {e}")
        return False
    
    # Vérifier les fichiers
    channel_stats_file = stats_dir / "channel_stats.json"
    user_stats_file = stats_dir / "user_stats.json"
    
    files_ok = True
    
    # Vérifier channel_stats.json
    if not channel_stats_file.exists():
        print(f"❌ Le fichier {channel_stats_file} n'existe pas!")
        files_ok = False
    else:
        stats = load_json_file(channel_stats_file)
        if stats:
            print(f"✅ Fichier {channel_stats_file} valide")
            
            # Vérifier les temps de visionnage
            global_time = stats.get("global", {}).get("total_watch_time", 0)
            print(f"   - Temps de visionnage global: {format_time(global_time)}")
            
            # Vérifier si les temps sont à zéro
            if global_time == 0:
                print("⚠️ Le temps de visionnage global est à zéro!")
        else:
            print(f"❌ Fichier {channel_stats_file} invalide ou corrompu")
            files_ok = False
    
    # Vérifier user_stats.json
    if not user_stats_file.exists():
        print(f"❌ Le fichier {user_stats_file} n'existe pas!")
        files_ok = False
    else:
        user_stats = load_json_file(user_stats_file)
        if user_stats:
            print(f"✅ Fichier {user_stats_file} valide")
            user_count = len(user_stats.get("users", {}))
            print(f"   - Nombre d'utilisateurs: {user_count}")
        else:
            print(f"❌ Fichier {user_stats_file} invalide ou corrompu")
            files_ok = False
    
    return files_ok

def fix_empty_stats_files():
    """Crée des fichiers de statistiques vides si nécessaire"""
    print("\n--- CRÉATION/RÉPARATION DES FICHIERS DE STATISTIQUES ---")
    
    stats_dir = Path(STATS_DIR)
    if not stats_dir.exists():
        try:
            os.makedirs(stats_dir, exist_ok=True)
            print(f"✅ Dossier {stats_dir} créé")
        except Exception as e:
            print(f"❌ Impossible de créer le dossier: {e}")
            return False
    
    # Fichier channel_stats.json
    channel_stats_file = stats_dir / "channel_stats.json"
    if not channel_stats_file.exists():
        try:
            # Structure initiale des stats
            initial_stats = {
                "channels": {},
                "global": {
                    "total_watchers": 0,
                    "peak_watchers": 0,
                    "peak_time": int(time.time()),
                    "total_watch_time": 0,
                    "last_updated": int(time.time()),
                },
                "daily": {},
                "last_daily_save": int(time.time()),
            }
            
            with open(channel_stats_file, "w") as f:
                json.dump(initial_stats, f, indent=2)
            
            print(f"✅ Fichier {channel_stats_file} créé avec structure initiale")
        except Exception as e:
            print(f"❌ Impossible de créer {channel_stats_file}: {e}")
            return False
    
    # Fichier user_stats.json
    user_stats_file = stats_dir / "user_stats.json"
    if not user_stats_file.exists():
        try:
            initial_user_stats = {
                "users": {},
                "last_updated": int(time.time()),
            }
            
            with open(user_stats_file, "w") as f:
                json.dump(initial_user_stats, f, indent=2)
            
            print(f"✅ Fichier {user_stats_file} créé avec structure initiale")
        except Exception as e:
            print(f"❌ Impossible de créer {user_stats_file}: {e}")
            return False
    
    return True

def simulate_watch_time():
    """Simule l'ajout de temps de visionnage pour tester le système"""
    print("\n--- SIMULATION D'AJOUT DE TEMPS DE VISIONNAGE ---")
    
    # Charger les fichiers
    stats_dir = Path(STATS_DIR)
    channel_stats_file = stats_dir / "channel_stats.json"
    user_stats_file = stats_dir / "user_stats.json"
    
    if not channel_stats_file.exists() or not user_stats_file.exists():
        print("❌ Les fichiers de statistiques n'existent pas")
        fix_empty_stats_files()
    
    # Charger les statistiques
    channel_stats = load_json_file(channel_stats_file)
    user_stats = load_json_file(user_stats_file)
    
    if not channel_stats or not user_stats:
        print("❌ Les fichiers de statistiques sont invalides")
        return False
    
    # Demander confirmation
    choice = input("Voulez-vous ajouter du temps de visionnage simulé? (o/n): ").lower()
    if choice != 'o':
        print("Simulation annulée")
        return False
    
    # Liste des chaînes disponibles
    available_channels = list(channel_stats.get("channels", {}).keys())
    if not available_channels:
        # Créer quelques chaînes par défaut
        available_channels = ["test_channel_1", "test_channel_2"]
        for channel in available_channels:
            if "channels" not in channel_stats:
                channel_stats["channels"] = {}
            
            channel_stats["channels"][channel] = {
                "current_watchers": 0,
                "peak_watchers": 0,
                "peak_time": int(time.time()),
                "total_watch_time": 0,
                "session_count": 0,
                "total_segments": 0,
                "watchlist": {},
            }
    
    print(f"Chaînes disponibles: {', '.join(available_channels)}")
    
    # Choisir une chaîne
    channel_name = input(f"Entrez le nom de la chaîne (défaut: {available_channels[0]}): ")
    if not channel_name.strip():
        channel_name = available_channels[0]
    
    # S'assurer que la chaîne existe
    if channel_name not in channel_stats["channels"]:
        print(f"Création de la chaîne {channel_name}")
        channel_stats["channels"][channel_name] = {
            "current_watchers": 0,
            "peak_watchers": 0,
            "peak_time": int(time.time()),
            "total_watch_time": 0,
            "session_count": 0,
            "total_segments": 0,
            "watchlist": {},
        }
    
    # Adresse IP simulée
    ip = "192.168.1.100"
    
    # Durée à ajouter
    try:
        duration = float(input("Entrez la durée à ajouter en secondes (défaut: 60): ") or 60)
    except ValueError:
        duration = 60
    
    print(f"Ajout de {duration} secondes pour {ip} sur {channel_name}")
    
    # Mise à jour des statistiques principales
    channel_stats["channels"][channel_name]["total_watch_time"] += duration
    channel_stats["global"]["total_watch_time"] += duration
    
    # Mise à jour des statistiques quotidiennes
    today = time.strftime("%Y-%m-%d")
    if "daily" not in channel_stats:
        channel_stats["daily"] = {}
    
    if today not in channel_stats["daily"]:
        channel_stats["daily"][today] = {
            "peak_watchers": 0,
            "total_watch_time": 0,
            "channels": {},
        }
    
    if "channels" not in channel_stats["daily"][today]:
        channel_stats["daily"][today]["channels"] = {}
    
    if channel_name not in channel_stats["daily"][today]["channels"]:
        channel_stats["daily"][today]["channels"][channel_name] = {
            "peak_watchers": 0,
            "total_watch_time": 0,
        }
    
    channel_stats["daily"][today]["channels"][channel_name]["total_watch_time"] += duration
    channel_stats["daily"][today]["total_watch_time"] += duration
    
    # Mise à jour des statistiques utilisateur
    if "users" not in user_stats:
        user_stats["users"] = {}
    
    if ip not in user_stats["users"]:
        user_stats["users"][ip] = {
            "first_seen": int(time.time()),
            "last_seen": int(time.time()),
            "total_watch_time": 0,
            "channels": {},
        }
    
    user_stats["users"][ip]["total_watch_time"] += duration
    user_stats["users"][ip]["last_seen"] = int(time.time())
    
    if "channels" not in user_stats["users"][ip]:
        user_stats["users"][ip]["channels"] = {}
    
    if channel_name not in user_stats["users"][ip]["channels"]:
        user_stats["users"][ip]["channels"][channel_name] = {
            "first_seen": int(time.time()),
            "last_seen": int(time.time()),
            "total_watch_time": 0,
            "favorite": False,
        }
    
    user_stats["users"][ip]["channels"][channel_name]["total_watch_time"] += duration
    user_stats["users"][ip]["channels"][channel_name]["last_seen"] = int(time.time())
    
    # Sauvegarder les modifications
    try:
        with open(channel_stats_file, "w") as f:
            json.dump(channel_stats, f, indent=2)
        with open(user_stats_file, "w") as f:
            json.dump(user_stats, f, indent=2)
        print("✅ Statistiques mises à jour avec succès")
        
        # Afficher les valeurs actuelles
        print(f"\nNouvelles valeurs:")
        print(f"- Temps global: {format_time(channel_stats['global']['total_watch_time'])}")
        print(f"- Temps pour {channel_name}: {format_time(channel_stats['channels
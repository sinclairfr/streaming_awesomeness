import json
import random
import time
from datetime import datetime, timedelta
import os

# Configuration
STATS_DIR = "/app/stats"
USER_STATS_FILE = os.path.join(STATS_DIR, "user_stats.json")
CHANNEL_STATS_FILE = os.path.join(STATS_DIR, "channel_stats.json")

# Assurez-vous que le répertoire existe
os.makedirs(STATS_DIR, exist_ok=True)

# Données de test
channels = ["bronzes", "pagnol", "jbond", "tarantino", "kubrick"]
ips = [f"192.168.10.{i}" for i in range(100, 110)]
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
    "TiviMate/4.6.0",
    "VLC/3.0.16"
]

now = time.time()
two_days_ago = now - (2 * 24 * 60 * 60)

def generate_random_date(start, end):
    """Génère une date aléatoire entre start et end"""
    delta = end - start
    random_second = random.randrange(int(delta))
    return start + random_second

def generate_user_stats():
    """Génère des statistiques d'utilisateurs factices"""
    users = {}
    
    for ip in ips:
        # Pour chaque IP, générons des activités sur plusieurs chaînes
        channels_watched = random.sample(channels, k=random.randint(1, len(channels)))
        total_watch_time = random.uniform(600, 10800)  # Entre 10 minutes et 3 heures
        
        # Distribution du temps entre les chaînes (pas équitable)
        watch_times = []
        remaining_time = total_watch_time
        
        for _ in range(len(channels_watched) - 1):
            time_for_channel = random.uniform(300, remaining_time * 0.7)
            watch_times.append(time_for_channel)
            remaining_time -= time_for_channel
        
        watch_times.append(remaining_time)
        
        # Déterminer la chaîne favorite (celle avec le plus de temps)
        favorite_index = watch_times.index(max(watch_times))
        
        # Créer les données utilisateur
        user_data = {
            "total_watch_time": total_watch_time,
            "channels_watched": channels_watched,
            "last_seen": generate_random_date(two_days_ago, now),
            "user_agent": random.choice(user_agents),
            "channels": {}
        }
        
        # Ajouter les détails par chaîne
        for i, channel in enumerate(channels_watched):
            channel_time = watch_times[i]
            is_favorite = (i == favorite_index)
            
            # Date de première et dernière vue
            first_seen = generate_random_date(two_days_ago, now - 3600)
            last_seen = generate_random_date(first_seen, now)
            
            user_data["channels"][channel] = {
                "first_seen": first_seen,
                "last_seen": last_seen,
                "total_watch_time": channel_time,
                "favorite": is_favorite
            }
        
        users[ip] = user_data
    
    return {"users": users, "last_updated": int(now)}

def generate_channel_stats(user_stats):
    """Génère des statistiques de chaînes basées sur les stats utilisateurs"""
    channels_data = {}
    
    # Initialiser les données pour chaque chaîne
    for channel in channels:
        channels_data[channel] = {
            "total_watch_time": 0,
            "unique_viewers": [],
            "watchlist": {},
            "last_update": int(now)
        }
    
    # Agréger les données des utilisateurs
    for ip, user_data in user_stats["users"].items():
        for channel, channel_data in user_data.get("channels", {}).items():
            if channel in channels_data:
                # Ajouter l'IP aux spectateurs uniques
                if ip not in channels_data[channel]["unique_viewers"]:
                    channels_data[channel]["unique_viewers"].append(ip)
                
                # Ajouter le temps de visionnage
                channels_data[channel]["total_watch_time"] += channel_data["total_watch_time"]
                
                # Ajouter à la watchlist
                channels_data[channel]["watchlist"][ip] = channel_data["total_watch_time"]
    
    # Calculer les statistiques globales
    all_viewers = set()
    total_time = 0
    
    for channel_data in channels_data.values():
        all_viewers.update(channel_data["unique_viewers"])
        total_time += channel_data["total_watch_time"]
    
    global_stats = {
        "total_watch_time": total_time,
        "unique_viewers": list(all_viewers),
        "last_update": int(now)
    }
    
    return {"channels": channels_data, "global": global_stats}

# Générer et sauvegarder les données
user_stats = generate_user_stats()
channel_stats = generate_channel_stats(user_stats)

with open(USER_STATS_FILE, 'w') as f:
    json.dump(user_stats, f, indent=2)

with open(CHANNEL_STATS_FILE, 'w') as f:
    json.dump(channel_stats, f, indent=2)

print(f"Données générées avec succès!")
print(f"- {len(user_stats['users'])} utilisateurs")
print(f"- {len(channel_stats['channels'])} chaînes")
print(f"- {channel_stats['global']['total_watch_time']:.2f} secondes de visionnage total")
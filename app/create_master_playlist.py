#!/usr/bin/env python3
import os
import sys
from pathlib import Path
import time
import traceback

# Import from config
try:
    from config import SERVER_URL, HLS_DIR, CONTENT_DIR
except ImportError:
    # Fallback if config.py is not available
    SERVER_URL = os.getenv("SERVER_URL", "192.168.10.183")
    HLS_DIR = os.getenv("HLS_DIR", "/mnt/iptv")
    CONTENT_DIR = os.getenv("CONTENT_DIR", "/mnt/videos")

PLAYLIST_PATH = f"{HLS_DIR}/playlist.m3u"

def create_master_playlist():
    """
    Crée ou met à jour la playlist master avec les chaînes disponibles.
    Si aucune chaîne n'est disponible, crée une playlist minimale.
    """
    try:
        print(f"Création/mise à jour de la playlist master: {PLAYLIST_PATH}")
        
        # Vérifier que le dossier HLS existe
        hls_dir = Path(HLS_DIR)
        if not hls_dir.exists():
            os.makedirs(str(hls_dir), exist_ok=True)
            print(f"Dossier HLS créé: {hls_dir}")
        
        # Conteneur pour les chaînes actives
        available_channels = []
        
        # Vérifier les chaînes disponibles dans le dossier de contenu
        content_dir = Path(CONTENT_DIR)
        if content_dir.exists() and content_dir.is_dir():
            for channel_dir in content_dir.iterdir():
                if channel_dir.is_dir():
                    # On considère chaque sous-dossier comme une chaîne potentielle
                    available_channels.append(channel_dir.name)
        
        # Trier alphabétiquement
        available_channels.sort()
        
        # Créer le contenu de la playlist
        content = "#EXTM3U\n"
        
        # Ajouter les chaînes disponibles, ou une notice si aucune n'est trouvée
        if available_channels:
            for channel_name in available_channels:
                content += f'#EXTINF:-1 tvg-id="{channel_name}" tvg-name="{channel_name}",{channel_name}\n'
                content += f"http://{SERVER_URL}/hls/{channel_name}/playlist.m3u8\n"
            
            print(f"Ajout de {len(available_channels)} chaînes disponibles: {', '.join(available_channels)}")
        else:
            # Playlist minimale avec commentaire
            content += "# Aucune chaîne disponible dans le dossier de contenu\n"
            print("Aucune chaîne trouvée, création d'une playlist minimale")
        
        # Écrire le contenu dans la playlist
        with open(PLAYLIST_PATH, "w", encoding="utf-8") as f:
            f.write(content)
        
        
        # Vérification
        if os.path.exists(PLAYLIST_PATH):
            size = os.path.getsize(PLAYLIST_PATH)
            print(f"Playlist écrite: {PLAYLIST_PATH}, taille: {size} octets")
            
            # Lire le contenu pour vérification
            with open(PLAYLIST_PATH, "r", encoding="utf-8") as f:
                read_content = f.read()
                if read_content == content:
                    print("Contenu vérifié et validé")
                else:
                    print("ERREUR: Contenu lu différent du contenu écrit!")
                    print(f"Contenu lu:\n{read_content}")
        else:
            print(f"ERREUR: Fichier non trouvé après écriture: {PLAYLIST_PATH}")
            
        return True
    
    except Exception as e:
        print(f"ERREUR lors de la création de la playlist: {e}")
        traceback.print_exc()
        
        # En cas d'erreur, essayer de créer une playlist minimale
        try:
            with open(PLAYLIST_PATH, "w", encoding="utf-8") as f:
                f.write("#EXTM3U\n# Erreur de génération - playlist minimale\n")
            print(f"Playlist minimale de secours créée: {PLAYLIST_PATH}")
            return True
        except Exception as inner_e:
            print(f"ERREUR critique lors de la création de la playlist de secours: {inner_e}")
            return False

if __name__ == "__main__":
    # Nombre de tentatives
    MAX_RETRIES = 3
    retry_count = 0
    
    while retry_count < MAX_RETRIES:
        success = create_master_playlist()
        if success:
            print("Playlist créée avec succès!")
            sys.exit(0)
        
        retry_count += 1
        print(f"Echec, nouvelle tentative {retry_count}/{MAX_RETRIES}...")
        time.sleep(2)
    
    print("Échec après plusieurs tentatives")
    sys.exit(1) 
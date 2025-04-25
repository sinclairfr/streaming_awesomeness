#!/usr/bin/env python3
import os
import sys
from pathlib import Path
import time
import traceback

# Import from config
try:
    from config import SERVER_URL
except ImportError:
    # Fallback if config.py is not available
    SERVER_URL = os.getenv("SERVER_URL", "192.168.10.183")
    
PLAYLIST_PATH = "/app/hls/playlist.m3u"

def create_master_playlist():
    """
    Crée ou met à jour la playlist master avec les chaînes disponibles.
    Si aucune chaîne n'est disponible, crée une playlist minimale.
    """
    try:
        print(f"Création/mise à jour de la playlist master: {PLAYLIST_PATH}")
        
        # Vérifier que le dossier HLS existe
        hls_dir = Path("/app/hls")
        if not hls_dir.exists():
            os.makedirs(str(hls_dir), exist_ok=True)
            os.chmod(str(hls_dir), 0o777)
            print(f"Dossier HLS créé: {hls_dir}")
        
        # Conteneur pour les chaînes actives
        active_channels = []
        
        # Vérifier les chaînes actives (qui ont une playlist.m3u8)
        for channel_dir in hls_dir.iterdir():
            if channel_dir.is_dir() and (channel_dir / "playlist.m3u8").exists():
                channel_name = channel_dir.name
                segments = list(channel_dir.glob("segment_*.ts"))
                if segments:
                    active_channels.append(channel_name)
                    print(f"Chaîne active: {channel_name} avec {len(segments)} segments")
        
        # Créer le contenu de la playlist
        content = "#EXTM3U\n"
        
        # Ajouter les chaînes actives, ou une notice si aucune chaîne n'est active
        if active_channels:
            # Trier alphabétiquement
            active_channels.sort()
            
            for channel_name in active_channels:
                content += f'#EXTINF:-1 tvg-id="{channel_name}" tvg-name="{channel_name}",{channel_name}\n'
                content += f"http://{SERVER_URL}/hls/{channel_name}/playlist.m3u8\n"
                
            print(f"Ajout de {len(active_channels)} chaînes actives: {', '.join(active_channels)}")
        else:
            # Playlist minimale avec commentaire
            content += "# Aucune chaîne active pour le moment\n"
            print("Aucune chaîne active, création d'une playlist minimale")
        
        # Écrire le contenu dans la playlist
        with open(PLAYLIST_PATH, "w", encoding="utf-8") as f:
            f.write(content)
        
        # Assurer les permissions
        os.chmod(PLAYLIST_PATH, 0o777)
        
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
            os.chmod(PLAYLIST_PATH, 0o777)
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
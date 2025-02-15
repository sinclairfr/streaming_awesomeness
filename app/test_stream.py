#!/usr/bin/env python3
# test_stream.py

import os
import time
import subprocess
from pathlib import Path

CHANNEL = "medo_3"  # On teste avec medo_3 puisqu'il vient d'avoir un watcher
HLS_DIR = f"/app/hls/{CHANNEL}"
CONTENT_DIR = f"/app/content/{CHANNEL}"

def test_directories():
    """On vérifie la structure des dossiers"""
    print("\n=== Test des dossiers ===")
    for d in [HLS_DIR, CONTENT_DIR]:
        print(f"\nContenu de {d}:")
        try:
            if not Path(d).exists():
                Path(d).mkdir(parents=True)
            for item in Path(d).glob("*"):
                print(f"- {item.name} ({item.stat().st_size} bytes)")
        except Exception as e:
            print(f"Erreur: {e}")

def test_ffmpeg():
    """On teste si ffmpeg fonctionne"""
    print("\n=== Test FFmpeg ===")
    try:
        result = subprocess.run(["ffmpeg", "-version"], 
                              capture_output=True, text=True)
        print(f"FFmpeg version: {result.stdout.splitlines()[0]}")
    except Exception as e:
        print(f"Erreur FFmpeg: {e}")

def create_playlist():
    """On crée un _playlist.txt de test"""
    print("\n=== Création playlist ===")
    processed_dir = Path(CONTENT_DIR) / "processed"
    playlist_path = Path(CONTENT_DIR) / "_playlist.txt"
    try:
        videos = list(processed_dir.glob("*.mp4"))
        if not videos:
            print("⚠️ Aucune vidéo trouvée!")
            return
        with open(playlist_path, "w") as f:
            for video in videos:
                f.write(f"file 'processed/{video.name}'\n")
        print(f"✅ Playlist créée: {playlist_path}")
    except Exception as e:
        print(f"Erreur création playlist: {e}")

def test_direct_stream():
    """On teste un streaming direct avec FFmpeg"""
    print("\n=== Test streaming direct ===")
    try:
        playlist = Path(CONTENT_DIR) / "_playlist.txt"
        if not playlist.exists():
            print("❌ Pas de _playlist.txt!")
            return

        cmd = [
            "ffmpeg", "-y",
            "-fflags", "+genpts+igndts",
            "-f", "concat",
            "-safe", "0",
            "-i", str(playlist),
            "-c", "copy",
            "-f", "hls",
            "-hls_time", "6",
            "-hls_list_size", "20",
            "-hls_flags", "delete_segments+append_list",
            f"{HLS_DIR}/playlist.m3u8"
        ]
        
        print(f"Commande: {' '.join(cmd)}")
        
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        print(f"Process lancé (PID: {process.pid})")
        
        # On attend 10s pour voir si des segments apparaissent
        start = time.time()
        segments_found = False
        
        while time.time() - start < 10:
            segments = list(Path(HLS_DIR).glob("*.ts"))
            if segments:
                print(f"✅ {len(segments)} segments créés!")
                segments_found = True
                break
            time.sleep(0.5)
            
        if not segments_found:
            print("❌ Aucun segment créé après 10s")
            # On récupère l'erreur si disponible
            err = process.stderr.read().decode()
            if err:
                print(f"Erreur FFmpeg:\n{err}")
                
    except Exception as e:
        print(f"Erreur test stream: {e}")

if __name__ == "__main__":
    test_directories()
    test_ffmpeg()
    create_playlist()
    test_direct_stream()

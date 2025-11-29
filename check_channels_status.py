#!/usr/bin/env python3
"""Check channel readiness status"""
import sys
import os
from pathlib import Path

# Add app to path
sys.path.insert(0, '/app')

# Import config
from config import HLS_DIR

# Get all channel dirs with HLS playlists
hls_path = Path(HLS_DIR)
channels_with_hls = []
for channel_dir in hls_path.iterdir():
    if channel_dir.is_dir() and (channel_dir / "playlist.m3u8").exists():
        channels_with_hls.append(channel_dir.name)

print(f"Channels with active HLS playlists: {len(channels_with_hls)}")
print("Channels:", sorted(channels_with_hls))

# Read master playlist
master_playlist = hls_path / "playlist.m3u"
if master_playlist.exists():
    with open(master_playlist, 'r') as f:
        content = f.read()
        playlist_channels = [line.split('"')[3] for line in content.split('\n') if 'tvg-name=' in line]

    print(f"\nChannels in master playlist: {len(playlist_channels)}")
    print("In master:", sorted(playlist_channels))

    missing = set(channels_with_hls) - set(playlist_channels)
    print(f"\nMissing from master playlist: {len(missing)}")
    print("Missing:", sorted(missing))
else:
    print("Master playlist not found!")

#!/usr/bin/env python3
# fix_now.py - Immediate fix for channel status issues
import os
import json
import sys
import time

# Configuration
CHANNELS_STATUS_FILE = "/app/stats/channels_status.json"

def reset_all_channels():
    """Reset all channels to zero viewers immediately"""
    print(f"Resetting all channels in {CHANNELS_STATUS_FILE}")
    
    # Load existing file
    try:
        with open(CHANNELS_STATUS_FILE, 'r') as f:
            data = json.load(f)
            # Make sure we're accessing the right data structure
            print(f"Loaded file with keys: {list(data.keys())}")
            channels = data.get('channels', {})
            print(f"Found {len(channels)} channels in status file")
    except Exception as e:
        print(f"Error loading file: {e}")
        return False
    
    # Reset all viewers to zero
    for channel_name, channel_data in channels.items():
        print(f"Resetting channel: {channel_name}")
        channel_data['viewers'] = 0
        channel_data['watchers'] = []
        channel_data['last_updated'] = time.strftime("%Y-%m-%dT%H:%M:%S.%f")
    
    # Update total active viewers
    data['active_viewers'] = 0
    data['last_updated'] = int(time.time())
    
    # Save file
    try:
        with open(CHANNELS_STATUS_FILE, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"✅ Successfully reset {len(channels)} channels to zero viewers")
        return True
    except Exception as e:
        print(f"Error saving file: {e}")
        return False

def set_active_channel(channel_name, ip="192.168.10.104"):
    """Set a specific channel as having a viewer"""
    if not channel_name:
        print("Error: Channel name is required")
        return False
    
    print(f"Setting {channel_name} as active with viewer {ip}")
    
    # Load existing file
    try:
        with open(CHANNELS_STATUS_FILE, 'r') as f:
            data = json.load(f)
            channels = data.get('channels', {})
    except Exception as e:
        print(f"Error loading file: {e}")
        return False
    
    # Check if channel exists
    if channel_name not in channels:
        print(f"Error: Channel {channel_name} not found in file")
        return False
    
    # Reset all viewers to zero first
    for name, channel_data in channels.items():
        if name == channel_name:
            # Set this channel as active
            channel_data['viewers'] = 1
            channel_data['watchers'] = [ip]
        else:
            # Set all other channels to inactive
            channel_data['viewers'] = 0
            channel_data['watchers'] = []
        channel_data['last_updated'] = time.strftime("%Y-%m-%dT%H:%M:%S.%f")
    
    # Update total active viewers
    data['active_viewers'] = 1
    data['last_updated'] = int(time.time())
    
    # Save file
    try:
        with open(CHANNELS_STATUS_FILE, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"✅ Successfully set {channel_name} as active (1 viewer)")
        return True
    except Exception as e:
        print(f"Error saving file: {e}")
        return False

def list_channels():
    """List all available channels"""
    try:
        with open(CHANNELS_STATUS_FILE, 'r') as f:
            data = json.load(f)
            channels = data.get('channels', {})
        
        print(f"Available channels ({len(channels)}):")
        for name, info in channels.items():
            viewers = info.get('viewers', 0)
            status = "🟢 ACTIVE" if viewers > 0 else "⚪ inactive"
            print(f"- {name}: {status} ({viewers} viewers)")
        
        return True
    except Exception as e:
        print(f"Error listing channels: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python fix_now.py reset        # Reset all channels to zero viewers")
        print("  python fix_now.py set CHANNEL  # Set CHANNEL as active with one viewer")
        print("  python fix_now.py list         # List all available channels")
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    if command == "reset":
        reset_all_channels()
    elif command == "set" and len(sys.argv) >= 3:
        set_active_channel(sys.argv[2])
    elif command == "list":
        list_channels()
    else:
        print(f"Unknown command: {command}")
        print("Use 'reset', 'set CHANNEL', or 'list'") 
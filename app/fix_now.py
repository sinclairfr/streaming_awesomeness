#!/usr/bin/env python3
# fix_now.py - Immediate fix for channel status issues
import os
import json
import sys
import time
import importlib.util
import logging

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
CHANNELS_STATUS_FILE = "/app/stats/channels_status.json"

def reset_all_channels():
    """Reset all channels to zero viewers immediately"""
    logger.info(f"Resetting all channels in {CHANNELS_STATUS_FILE}")
    
    # Load existing file
    try:
        with open(CHANNELS_STATUS_FILE, 'r') as f:
            data = json.load(f)
            # Make sure we're accessing the right data structure
            logger.info(f"Loaded file with keys: {list(data.keys())}")
            channels = data.get('channels', {})
            logger.info(f"Found {len(channels)} channels in status file")
    except Exception as e:
        logger.error(f"Error loading file: {e}")
        return False
    
    # Reset all viewers to zero
    for channel_name, channel_data in channels.items():
        logger.info(f"Resetting channel: {channel_name}")
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
        logger.info(f"✅ Successfully reset {len(channels)} channels to zero viewers")
        return True
    except Exception as e:
        logger.error(f"Error saving file: {e}")
        return False

def reset_channels_via_manager():
    """Reset all channels to zero viewers by directly updating the IPTV manager"""
    try:
        # Try to import from current environment first
        logger.info("Attempting to import IPTVManager...")
        from iptv_manager import IPTVManager
        
        # Look for existing manager instances in the process
        logger.info("Looking for existing IPTVManager instances...")
        found_manager = False
        
        # Method 1: Try to find it in current modules
        import sys
        manager = None
        for module_name, module in sys.modules.items():
            if hasattr(module, 'manager') and isinstance(module.manager, IPTVManager):
                manager = module.manager
                logger.info(f"Found manager instance in module: {module_name}")
                found_manager = True
                break
        
        # Method 2: If not found, try to get it from process attributes
        if not found_manager:
            logger.info("No manager found in modules, trying direct reset method")
            
            # Use the _reset_channel_statuses function directly on any IPTVManager instance
            logger.info("Resetting all channel viewers via status file...")
            
            # First make sure the status file exists
            if not os.path.exists(CHANNELS_STATUS_FILE):
                logger.error(f"Status file not found: {CHANNELS_STATUS_FILE}")
                # Fall back to reset_all_channels method
                return reset_all_channels()
                
            # Reset all channels to zero directly in the status file
            reset_success = reset_all_channels()
            
            if reset_success:
                logger.info("✅ Successfully reset channel statuses via file")
                return True
            else:
                logger.error("❌ Failed to reset channel statuses via file")
                return False
                
        # If we found a manager instance, use it to reset channels
        logger.info("Resetting all channel viewers via manager...")
        
        # Use the manager's internal reset function if it exists
        if hasattr(manager, '_reset_channel_statuses'):
            logger.info("Using manager's _reset_channel_statuses method")
            manager._reset_channel_statuses()
            logger.info("✅ Successfully reset all channels via manager's reset method")
            return True
            
        # Otherwise manually update each channel
        count = 0
        for channel_name, channel in manager.channels.items():
            if channel:
                logger.info(f"Resetting channel: {channel_name}")
                # Update viewers for this channel to 0
                manager.update_watchers(channel_name, 0, [], "/hls/", source='reset_script')
                count += 1
                
        logger.info(f"✅ Successfully reset {count} channels via direct manager update")
        return True
        
    except ImportError as e:
        logger.error(f"Error importing IPTVManager: {e}")
        # Fall back to reset_all_channels method
        logger.info("Falling back to file-based reset...")
        return reset_all_channels()
    except Exception as e:
        logger.error(f"Error while resetting channels via manager: {e}")
        import traceback
        logger.error(traceback.format_exc())
        # Fall back to reset_all_channels method
        logger.info("Falling back to file-based reset due to error...")
        return reset_all_channels()

def set_active_channel(channel_name, ip="192.168.10.104"):
    """Set a specific channel as having a viewer"""
    if not channel_name:
        logger.error("Error: Channel name is required")
        return False
    
    logger.info(f"Setting {channel_name} as active with viewer {ip}")
    
    # Load existing file
    try:
        with open(CHANNELS_STATUS_FILE, 'r') as f:
            data = json.load(f)
            channels = data.get('channels', {})
    except Exception as e:
        logger.error(f"Error loading file: {e}")
        return False
    
    # Check if channel exists
    if channel_name not in channels:
        logger.error(f"Error: Channel {channel_name} not found in file")
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
        logger.info(f"✅ Successfully set {channel_name} as active (1 viewer)")
        return True
    except Exception as e:
        logger.error(f"Error saving file: {e}")
        return False

def list_channels():
    """List all available channels"""
    try:
        with open(CHANNELS_STATUS_FILE, 'r') as f:
            data = json.load(f)
            channels = data.get('channels', {})
        
        logger.info(f"Available channels ({len(channels)}):")
        for name, info in channels.items():
            viewers = info.get('viewers', 0)
            status = "🟢 ACTIVE" if viewers > 0 else "⚪ inactive"
            logger.info(f"- {name}: {status} ({viewers} viewers)")
        
        return True
    except Exception as e:
        logger.error(f"Error listing channels: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python fix_now.py reset        # Reset all channels to zero viewers in status file")
        print("  python fix_now.py reset_manager # Reset all channels via IPTV manager (more thorough)")
        print("  python fix_now.py set CHANNEL  # Set CHANNEL as active with one viewer")
        print("  python fix_now.py list         # List all available channels")
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    if command == "reset":
        reset_all_channels()
    elif command == "reset_manager":
        reset_channels_via_manager()
    elif command == "set" and len(sys.argv) >= 3:
        set_active_channel(sys.argv[2])
    elif command == "list":
        list_channels()
    else:
        print(f"Unknown command: {command}")
        print("Use 'reset', 'reset_manager', 'set CHANNEL', or 'list'") 
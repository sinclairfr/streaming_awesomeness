#!/usr/bin/env python3
# channel_status_manager.py - Manages real-time channel status for dashboard

import os
import json
import time
import threading
import shutil
from pathlib import Path
from config import logger, CHANNELS_STATUS_FILE

class ChannelStatusManager:
    """
    Manages a lightweight JSON file with real-time channel status 
    for dashboard consumption
    """
    
    def __init__(self, status_file=CHANNELS_STATUS_FILE):
        """Initialize the status manager"""
        self.status_file = status_file
        self.channels = {}
        self.last_updated = 0
        self.active_viewers = 0
        self._lock = threading.Lock()
        self.update_interval = 2  # Update every 2 seconds
        
        # Create stats dir if needed
        os.makedirs(os.path.dirname(self.status_file), exist_ok=True)
        
        # Load existing data if available
        self._load_status()
        
        # Start update thread
        self.stop_thread = threading.Event()
        self.update_thread = threading.Thread(target=self._update_loop, daemon=True)
        self.update_thread.start()
        
        logger.info(f"ChannelStatusManager initialized, status file: {self.status_file}")
    
    def _load_status(self):
        """Load channel status from file if it exists"""
        try:
            if os.path.exists(self.status_file):
                with open(self.status_file, 'r') as f:
                    data = json.load(f)
                    self.channels = data.get('channels', {})
                    self.last_updated = data.get('last_updated', 0)
                    self.active_viewers = data.get('active_viewers', 0)
                logger.info(f"Loaded existing channel status data with {len(self.channels)} channels")
            else:
                # Create empty status file
                self._save_status()
                logger.info("Created new channel status file")
        except Exception as e:
            logger.error(f"Error loading status file: {e}")
    
    def _save_status(self):
        """Save current status to file"""
        try:
            # Create a temporary file
            temp_file = f"{self.status_file}.tmp"
            
            # Write to temporary file
            with open(temp_file, 'w') as f:
                json.dump({
                    'channels': self.channels,
                    'last_updated': self.last_updated,
                    'active_viewers': self.active_viewers
                }, f, indent=2)
            
            # Give permissions
            os.chmod(temp_file, 0o666)
            
            # Rename atomically
            os.replace(temp_file, self.status_file)
            
            # Ensure final file has correct permissions
            os.chmod(self.status_file, 0o666)
            
            return True
        except Exception as e:
            logger.error(f"Error saving status file: {e}")
            return False
    
    def update_channel(self, channel_name: str, is_active: bool = False, viewers: int = 0, streaming: bool = False, watchers: list = None):
        """
        Update status for a single channel
        
        Args:
            channel_name: Name of the channel
            is_active: Whether the channel has content available
            viewers: Number of current viewers
            streaming: Whether the channel is currently streaming
            watchers: List of current watchers
        """
        with self._lock:
            if channel_name not in self.channels:
                self.channels[channel_name] = {}
            
            self.channels[channel_name].update({
                'active': is_active,
                'streaming': streaming,
                'viewers': viewers,
                'watchers': watchers or []
            })
            
            self.last_updated = int(time.time())
            
            # Update total active viewers
            self.active_viewers = sum(ch.get('viewers', 0) for ch in self.channels.values())
            
            # Don't save here - let the update loop handle it
            # This prevents constant file writes
            return True
    
    def update_all_channels(self, channels_dict):
        """
        Update all channels at once
        
        Args:
            channels_dict: Dictionary mapping channel names to status dicts
                           with 'active', 'viewers', 'streaming' keys
        """
        try:
            with self._lock:
                current_time = int(time.time())
                total_viewers = 0
                
                logger.debug(f"📊 Mise à jour des statuts pour {len(channels_dict)} chaînes")
                
                # Clear existing channels data
                self.channels = {}
                
                for channel_name, status in channels_dict.items():
                    viewers = status.get("viewers", 0)
                    total_viewers += viewers
                    
                    logger.debug(f"📡 Mise à jour de {channel_name}: viewers={viewers}, active={status.get('active')}, streaming={status.get('streaming')}")
                    
                    # Add to dictionary
                    self.channels[channel_name] = {
                        "active": status.get("active", True),
                        "viewers": viewers,
                        "streaming": status.get("streaming", False),
                        "watchers": status.get("watchers", []),
                        "last_update": current_time,
                        "peak_viewers": viewers
                    }
                
                # Update total active viewers and timestamp
                self.active_viewers = total_viewers
                self.last_updated = current_time
                
                # Force save immediately
                success = self._save_status()
                if success:
                    logger.debug(f"✅ Statuts sauvegardés avec succès: {total_viewers} viewers au total")
                else:
                    logger.error("❌ Échec de la sauvegarde des statuts")
                
                return success
                
        except Exception as e:
            logger.error(f"❌ Erreur lors de la mise à jour des statuts: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def get_status_data(self):
        """Get a copy of the current status data"""
        with self._lock:
            return dict(self.channels)
    
    def _update_loop(self):
        """Background thread to periodically update status file"""
        while not self.stop_thread.is_set():
            try:
                # Save current status
                self._save_status()
                
                # Calculate total active viewers
                total_viewers = sum(
                    ch.get("viewers", 0) 
                    for ch in self.channels.values()
                )
                self.active_viewers = total_viewers
                
                # Sleep until next update
                time.sleep(self.update_interval)
                
            except Exception as e:
                logger.error(f"Error in status update loop: {e}")
                time.sleep(5)  # Shorter sleep on error
    
    def stop(self):
        """Stop the update thread and save final status"""
        self.stop_thread.set()
        if self.update_thread.is_alive():
            self.update_thread.join(timeout=5)
        
        # Final save
        self._save_status()
        logger.info("Channel status manager stopped")
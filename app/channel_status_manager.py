#!/usr/bin/env python3
# channel_status_manager.py - Manages real-time channel status for dashboard

import os
import json
import time
import threading
import shutil
from pathlib import Path
from config import logger

class ChannelStatusManager:
    """
    Manages a lightweight JSON file with real-time channel status 
    for dashboard consumption
    """
    
    def __init__(self, stats_dir="/app/stats"):
        """Initialize the status manager"""
        self.stats_dir = Path(stats_dir)
        self.status_file = self.stats_dir / "channels_status.json"
        self.lock = threading.Lock()
        self.update_interval = 10  # Update every 10 seconds
        
        # Create stats dir if needed
        self.stats_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize empty status data
        self.status_data = {
            "channels": {},
            "last_updated": int(time.time()),
            "active_viewers": 0
        }
        
        # Clean status file at startup
        self._clean_status_file()
        
        # Load existing data if available
        self._load_status()
        
        # Start update thread
        self.stop_thread = threading.Event()
        self.update_thread = threading.Thread(target=self._update_loop, daemon=True)
        self.update_thread.start()
        
        logger.info(f"ChannelStatusManager initialized, status file: {self.status_file}")
    
    def _clean_status_file(self):
        """Clean the status file at startup"""
        try:
            if self.status_file.exists():
                # Backup the old file with timestamp
                backup_file = self.status_file.with_suffix(f'.json.bak.{int(time.time())}')
                shutil.copy2(self.status_file, backup_file)
                logger.info(f"✅ Backup of old status file created: {backup_file}")
                
                # Create new empty status file
                with open(self.status_file, 'w') as f:
                    json.dump(self.status_data, f, indent=2)
                logger.info("✅ Status file cleaned at startup")
        except Exception as e:
            logger.error(f"❌ Error cleaning status file: {e}")
    
    def _update_channel_status(self):
        """Update the channel status file with current channel information"""
        if not hasattr(self, "channel_status") or not self.channel_status:
            return False
        
        try:
            # Debug the current channels list
            logger.debug(f"Updating channel status with {len(self.channels)} channels: {list(self.channels.keys())}")
            
            channels_dict = {}
            
            for name, channel in self.channels.items():
                # Get channel information with safer attribute access
                is_active = bool(getattr(channel, "ready_for_streaming", False))
                is_streaming = bool(channel.process_manager.is_running() if hasattr(channel, "process_manager") else False)
                viewers = getattr(channel, "watchers_count", 0)
                
                # Add to dictionary
                channels_dict[name] = {
                    "active": is_active,
                    "streaming": is_streaming,
                    "viewers": viewers
                }
            
            # Update all channels at once
            self.channel_status.update_all_channels(channels_dict)
            return True
        
        except Exception as e:
            logger.error(f"❌ Error updating channel status: {e}")
            return False    

    def _load_status(self):
        """Load channel status from file if it exists"""
        if self.status_file.exists():
            try:
                with open(self.status_file, 'r') as f:
                    self.status_data = json.load(f)
                logger.info(f"Loaded existing channel status data with {len(self.status_data.get('channels', {}))} channels")
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON in status file, using empty data")
            except Exception as e:
                logger.error(f"Error loading status file: {e}")
        else:
            # Create empty status file
            self._save_status()
            logger.info("Created new channel status file")
    
    def _save_status(self):
        """Save current status to file"""
        with self.lock:
            try:
                # Update timestamp
                self.status_data["last_updated"] = int(time.time())
                
                # Write to file
                with open(self.status_file, 'w') as f:
                    json.dump(self.status_data, f, indent=2)
                
                return True
            except Exception as e:
                logger.error(f"Error saving status file: {e}")
                return False
    
    def update_channel(self, channel_name, is_active=True, viewers=0, streaming=False):
        """
        Update status for a single channel
        
        Args:
            channel_name: Name of the channel
            is_active: Whether the channel has content available
            viewers: Number of current viewers
            streaming: Whether the channel is currently streaming
        """
        with self.lock:
            # Get current time
            current_time = int(time.time())
            
            # Get or create channel entry
            if channel_name not in self.status_data["channels"]:
                self.status_data["channels"][channel_name] = {
                    "active": is_active,
                    "viewers": viewers,
                    "streaming": streaming,
                    "last_update": current_time,
                    "peak_viewers": viewers
                }
            else:
                channel_data = self.status_data["channels"][channel_name]
                
                # Update fields
                channel_data["active"] = is_active
                channel_data["viewers"] = viewers
                channel_data["streaming"] = streaming
                channel_data["last_update"] = current_time
                
                # Update peak viewers if current count is higher
                if viewers > channel_data.get("peak_viewers", 0):
                    channel_data["peak_viewers"] = viewers
            
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
        with self.lock:
            current_time = int(time.time())
            total_viewers = 0
            
            for channel_name, status in channels_dict.items():
                viewers = status.get("viewers", 0)
                total_viewers += viewers
                
                self.update_channel(
                    channel_name,
                    is_active=status.get("active", True),
                    viewers=viewers,
                    streaming=status.get("streaming", False)
                )
            
            # Update total active viewers
            self.status_data["active_viewers"] = total_viewers
            
            # Force save when updating all channels
            self._save_status()
            return True
    
    def get_status_data(self):
        """Get a copy of the current status data"""
        with self.lock:
            return dict(self.status_data)
    
    def _update_loop(self):
        """Background thread to periodically update status file"""
        while not self.stop_thread.is_set():
            try:
                # Save current status
                self._save_status()
                
                # Calculate total active viewers
                total_viewers = sum(
                    ch.get("viewers", 0) 
                    for ch in self.status_data["channels"].values()
                )
                self.status_data["active_viewers"] = total_viewers
                
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
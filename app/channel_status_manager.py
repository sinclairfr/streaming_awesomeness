#!/usr/bin/env python3
# channel_status_manager.py - Manages real-time channel status for dashboard

import os
import json
import time
import threading
import shutil
from pathlib import Path
from config import logger, CHANNELS_STATUS_FILE
from datetime import datetime
from typing import List, Dict, Any

class ChannelStatusManager:
    """
    Manages a lightweight JSON file with real-time channel status 
    for dashboard consumption
    """
    
    def __init__(self, status_file: str):
        """Initialize the channel status manager"""
        self.status_file = status_file
        self.channels = {}
        self.last_updated = int(time.time())
        self.active_viewers = 0
        self._lock = threading.Lock()
        self._save_lock = threading.Lock()
        self.update_interval = 10
        
        # Ensure directory exists and has proper permissions
        stats_dir = os.path.dirname(status_file)
        os.makedirs(stats_dir, exist_ok=True)
        os.chmod(stats_dir, 0o777)
        
        # Load existing status if file exists
        if os.path.exists(status_file):
            try:
                with open(status_file, 'r') as f:
                    data = json.load(f)
                    self.channels = data.get('channels', {})
                    self.last_updated = data.get('last_updated', int(time.time()))
                    self.active_viewers = data.get('active_viewers', 0)
                logger.info(f"Loaded existing channel status data with {len(self.channels)} channels")
            except Exception as e:
                logger.error(f"Error loading channel status: {e}")
                # Reset to default values
                self.channels = {}
                self.last_updated = int(time.time())
                self.active_viewers = 0
        else:
            # Create initial status file
            try:
                with open(status_file, 'w') as f:
                    json.dump({
                        'channels': {},
                        'last_updated': int(time.time()),
                        'active_viewers': 0
                    }, f, indent=2)
                os.chmod(status_file, 0o666)
                logger.info("Created initial channel status file")
            except Exception as e:
                logger.error(f"Error creating initial channel status file: {e}")
        
        logger.info(f"ChannelStatusManager initialized, status file: {status_file}")
        
        # Start update thread
        self.stop_thread = threading.Event()
        self.update_thread = threading.Thread(target=self._update_loop, daemon=True)
        self.update_thread.start()
    
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
        """Save current status to file using atomic write"""
        with self._save_lock:
            try:
                logger.info(f"🔄 Saving status to {self.status_file}")
                
                # Ensure directory exists and has proper permissions
                stats_dir = os.path.dirname(self.status_file)
                os.makedirs(stats_dir, exist_ok=True)
                try:
                    os.chmod(stats_dir, 0o777)
                except Exception as chmod_err:
                    logger.warning(f"Could not chmod stats dir {stats_dir}: {chmod_err}")
                
                # Format the data to ensure consistency
                formatted_channels = {}
                for channel_id, channel_data in self.channels.items():
                    # Convert old format to new format if needed
                    is_live = channel_data.get('is_live', channel_data.get('active', False))
                    viewers = channel_data.get('viewers', 0)
                    watchers = channel_data.get('watchers', [])
                    
                    formatted_channels[channel_id] = {
                        'is_live': is_live,
                        'viewers': viewers,
                        'watchers': watchers,
                        'last_updated': channel_data.get('last_updated', datetime.now().isoformat())
                    }
                
                # Calculate total active viewers
                total_viewers = sum(ch.get('viewers', 0) for ch in formatted_channels.values())
                
                # Prepare the complete content
                content_to_save = {
                    'channels': formatted_channels,
                    'last_updated': int(time.time()),
                    'active_viewers': total_viewers
                }
                
                # Log active channels
                active_channels = [ch_id for ch_id, ch_data in formatted_channels.items() if ch_data.get('viewers', 0) > 0]
                if active_channels:
                    logger.info(f"🔄 Status update with {len(active_channels)} active channels: {', '.join(active_channels)}")
                    
                # Use a temporary file for atomic write
                temp_file = f"{self.status_file}.tmp"
                try:
                    with open(temp_file, 'w') as f:
                        json.dump(content_to_save, f, indent=2)
                        f.flush()
                        os.fsync(f.fileno())
                    
                    # Check if temp file was written correctly
                    if not os.path.exists(temp_file):
                        logger.error("❌ Temp file not created during save")
                        return False
                        
                    if os.path.getsize(temp_file) == 0:
                        logger.error("❌ Temp file is empty after write")
                        return False
                    
                    # Atomic rename (more reliable than direct write)
                    os.replace(temp_file, self.status_file)
                    
                    # Set file permissions
                    try:
                        os.chmod(self.status_file, 0o666)
                    except Exception as chmod_err:
                        logger.warning(f"Could not chmod status file {self.status_file}: {chmod_err}")
                    
                    logger.info(f"✅ Status saved successfully to {self.status_file}")
                    return True
                    
                except Exception as write_err:
                    logger.error(f"❌ Error writing to temp file {temp_file}: {write_err}")
                    # Try direct write as fallback
                    try:
                        with open(self.status_file, 'w') as f:
                            json.dump(content_to_save, f, indent=2)
                        logger.info("✅ Status saved directly (fallback method)")
                        return True
                    except Exception as direct_err:
                        logger.error(f"❌ Error in fallback direct write: {direct_err}")
                        return False
                    
            except Exception as e:
                logger.error(f"❌ Error saving status: {e}")
                import traceback
                logger.error(traceback.format_exc())
                return False
    
    def update_channel(self, channel_id: str, data: Dict[str, Any]) -> bool:
        """Update a single channel's status"""
        try:
            with self._lock:
                # Get current data
                current_data = self.channels.get(channel_id, {})
                
                # Log current and new watchers
                current_watchers = current_data.get('watchers', [])
                new_watchers = data.get('watchers', [])
                
                # Identifier les viewers retirés
                removed_viewers = set(current_watchers) - set(new_watchers)
                if removed_viewers:
                    logger.info(f"🧹 Viewers retirés du fichier status pour {channel_id}: {list(removed_viewers)}")
                
                # Identifier les viewers ajoutés (pour le debug)
                added_viewers = set(new_watchers) - set(current_watchers)
                if added_viewers:
                    logger.debug(f"➕ Nouveaux viewers pour {channel_id}: {list(added_viewers)}")
                
                logger.debug(f"Channel {channel_id} - Current watchers: {current_watchers}, New watchers: {new_watchers}")
                
                # Forcer une mise à jour si viewers ont changé, même si d'autres champs sont identiques
                viewers_changed = len(removed_viewers) > 0 or len(added_viewers) > 0
                
                # Check if we need to update
                needs_update = viewers_changed
                if not needs_update:
                    for key, value in data.items():
                        if current_data.get(key) != value:
                            needs_update = True
                            logger.debug(f"Channel {channel_id} - Field {key} changed from {current_data.get(key)} to {value}")
                            break
                
                if needs_update or viewers_changed:
                    logger.debug(f"Channel {channel_id} - Updating status (viewers changed: {viewers_changed})")
                    # Update the data explicitly, prioritizing keys from 'data'
                    # Get the existing data or an empty dict
                    updated_channel_data = self.channels.get(channel_id, {}).copy()
                    
                    # Update with keys from the new 'data' dict
                    for key, value in data.items():
                        updated_channel_data[key] = value
                        
                    # Always update the timestamp
                    updated_channel_data['last_updated'] = datetime.now().isoformat()
                    
                    # Assign the updated dictionary back
                    self.channels[channel_id] = updated_channel_data
                    
                    # Call save status
                    return self._save_status()
                else:
                    logger.debug(f"No changes needed for channel {channel_id}")
                    return True
                    
        except Exception as e:
            logger.error(f"Error updating channel {channel_id}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def update_all_channels(self, channels_dict):
        """Update status for all channels at once"""
        try:
            with self._lock:
                logger.debug("🔄 Mise à jour du statut de toutes les chaînes")
                # Clear existing data
                self.channels.clear()
                
                # Update with new data
                for channel_id, channel_data in channels_dict.items():
                    # Convert old format to new format if needed
                    is_live = channel_data.get('is_live', channel_data.get('active', False))
                    viewers = channel_data.get('viewers', 0)
                    watchers = channel_data.get('watchers', [])
                    
                    self.channels[channel_id] = {
                        'is_live': is_live,
                        'viewers': viewers,
                        'watchers': watchers,
                        'last_updated': datetime.now().isoformat()
                    }
                
                # Save the updated status
                if self._save_status():
                    logger.debug("✅ Statut de toutes les chaînes mis à jour avec succès")
                    return True
                else:
                    logger.error("❌ Échec de la sauvegarde du statut des chaînes")
                    return False
                    
        except Exception as e:
            logger.error(f"❌ Erreur lors de la mise à jour de toutes les chaînes: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def _update_loop(self):
        """Background thread to periodically check status file integrity"""
        while not self.stop_thread.is_set():
            try:
                # Vérifier que le fichier de statut existe toujours
                if not os.path.exists(self.status_file):
                    logger.warning(f"⚠️ Fichier de statut disparu: {self.status_file}, recréation...")
                    self._save_status()
                
                # Pause pour éviter une boucle occupée
                self.stop_thread.wait(self.update_interval)
            except Exception as e:
                logger.error(f"Error in status update loop: {e}")
                # Pause plus courte en cas d'erreur
                self.stop_thread.wait(min(self.update_interval, 30))
    
    def stop(self):
        """Stop the update thread and save final status"""
        self.stop_thread.set()
        if self.update_thread.is_alive():
            self.update_thread.join(timeout=5)
        
        # Final save
        self._save_status()
        logger.info("Channel status manager stopped")

    def flush_all_viewers(self) -> bool:
        """Vide tous les viewers de tous les canaux"""
        with self._lock:
            try:
                logger.info("🧹 Vidage de tous les viewers pour arrêt du script...")
                
                # Garder une trace des canaux modifiés pour le log
                modified_channels = []
                viewer_counts = {}
                
                # Mettre à jour chaque canal
                for channel_id, channel_data in self.channels.items():
                    current_watchers = channel_data.get('watchers', [])
                    if current_watchers:
                        viewer_counts[channel_id] = len(current_watchers)
                        # Copier les données actuelles et modifier les watchers
                        updated_data = channel_data.copy()
                        updated_data['watchers'] = []
                        updated_data['viewers'] = 0
                        updated_data['last_updated'] = datetime.now().isoformat()
                        
                        # Mettre à jour les données
                        self.channels[channel_id] = updated_data
                        modified_channels.append(channel_id)
                
                # Si des canaux ont été modifiés, sauvegarder
                if modified_channels:
                    logger.info(f"🧹 Vidage des viewers pour {len(modified_channels)} canaux: {', '.join(modified_channels)}")
                    for channel, count in viewer_counts.items():
                        logger.info(f"[{channel}] 🧹 {count} viewers supprimés")
                    
                    # Sauvegarder les changements
                    return self._save_status()
                else:
                    logger.info("✅ Aucun canal n'avait de viewers à vider")
                    return True
                    
            except Exception as e:
                logger.error(f"❌ Erreur lors du vidage de tous les viewers: {e}")
                import traceback
                logger.error(traceback.format_exc())
                return False
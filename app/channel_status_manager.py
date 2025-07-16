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
        self.last_updated = 0
        self.active_viewers = 0
        self._lock = threading.Lock()
        self._save_lock = threading.Lock()
        self.update_interval = 10
        
        # Debounce mechanism for rapid channel switching
        self._save_timer = None
        self._pending_save = False
        self._debounce_interval = 0.8  # Wait 800ms before writing file
        self._last_update_time = 0
        self._rapid_switches_count = 0
        self._last_forced_save_time = 0
        
        # Ensure directory exists and has proper permissions
        stats_dir = os.path.dirname(status_file)
        try:
            os.makedirs(stats_dir, exist_ok=True)
        except Exception as e:
            logger.error(f"❌ Error creating stats directory: {e}")
        
        # Create initial status file
        try:
            with open(status_file, 'w') as f:
                json.dump({
                    'channels': {},
                    'last_updated': 0,
                    'active_viewers': 0
                }, f, indent=2)
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
            # Create empty status file
            self._save_status()
            logger.info("Created new channel status file")
        except Exception as e:
            logger.error(f"Error loading status file: {e}")
    
    def _save_status(self):
        """Save current status to file using atomic write"""
        with self._save_lock:
            # Cancel any pending timer
            if self._save_timer:
                self._save_timer.cancel()
                self._save_timer = None
            
            try:
                # Create a temporary file
                temp_file = f"{self.status_file}.tmp"
                
                # Calculate total active viewers from all channels
                total_active_viewers = sum(
                    channel_data.get('viewers', 0)
                    for channel_data in self.channels.values()
                )
                
                # Write to temporary file
                with open(temp_file, 'w') as f:
                    json.dump({
                        'channels': self.channels,
                        'last_updated': int(time.time()),
                        'active_viewers': total_active_viewers
                    }, f, indent=2)
                
                # Atomic replace
                os.replace(temp_file, self.status_file)
                
                
                logger.info(f"✅ Status saved: {len(self.channels)} channels, {total_active_viewers} active viewers")
                return True
                
            except Exception as e:
                logger.error(f"❌ Error saving status file: {e}")
                import traceback
                logger.error(traceback.format_exc())
                return False
    
    def _debounced_save(self):
        """Schedule a save operation with debouncing for rapid updates"""
        if hasattr(self, '_last_forced_save_time') and time.time() - self._last_forced_save_time < 2.0:
            logger.debug("Skipping debounced save, a forced save happened recently.")
            self._pending_save = False
            if self._save_timer:
                self._save_timer.cancel()
                self._save_timer = None
            return

        # Cancel any pending timer
        if self._save_timer:
            self._save_timer.cancel()
            self._save_timer = None
            
        # Mark that we have a pending save
        self._pending_save = True
        
        # Check if we're in rapid switching mode
        current_time = time.time()
        time_since_last_update = current_time - self._last_update_time
        self._last_update_time = current_time
        
        # If updates are happening rapidly (less than 1.5 seconds apart), adjust the debounce interval
        if time_since_last_update < 1.5:
            self._rapid_switches_count += 1
            # Increase debounce time for rapid switching (up to 2 seconds max)
            debounce_time = min(2.0, self._debounce_interval + (self._rapid_switches_count * 0.2))
            logger.debug(f"🚀 Rapid channel switching detected ({self._rapid_switches_count}x) - Increasing debounce to {debounce_time:.1f}s")
        else:
            # Reset to normal debounce when switching slows down
            self._rapid_switches_count = 0
            debounce_time = self._debounce_interval
        
        # Schedule a new timer with the appropriate delay
        self._save_timer = threading.Timer(debounce_time, self._execute_save)
        self._save_timer.daemon = True
        self._save_timer.start()
        
    def _execute_save(self):
        """Execute the actual save operation after debounce delay"""
        try:
            # Don't acquire self._save_lock here - _save_status does it internally
            if self._pending_save:
                success = self._save_status()
                if not success:
                    logger.warning("❌ Failed to save status file after debounce - will retry later")
                    # Try again after a short delay
                    self._save_timer = threading.Timer(2.0, self._execute_save)
                    self._save_timer.daemon = True
                    self._save_timer.start()
        except Exception as e:
            logger.error(f"❌ Error in _execute_save: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    def update_channel(self, channel_id: str, data: Dict[str, Any], force_save: bool = False) -> bool:
        """
        Update a single channel's status
        
        Args:
            channel_id: Channel identifier
            data: Channel data to update
            force_save: If True, save immediately instead of using debounce
        
        Returns:
            bool: Success status
        """
        try:
            # Check if we should update the channel
            with self._lock:
                # Get the existing data or create it
                if channel_id not in self.channels:
                    self.channels[channel_id] = {}
                
                # Check if this is a meaningful update (viewers changed, etc)
                current_data = self.channels[channel_id]
                viewers_changed = ("viewers" in data and data["viewers"] != current_data.get("viewers", 0))
                watchers_changed = ("watchers" in data and set(data["watchers"]) != set(current_data.get("watchers", [])))
                
                # TOUJOURS mettre à jour le statut du canal, même si les viewers n'ont pas changé
                # C'est essentiel pour maintenir la synchronisation avec les logs Nginx
                # Set needs_update to True to always update
                needs_update = True
                
                logger.debug(f"Channel {channel_id} - Updating status (viewers changed: {viewers_changed}, watchers changed: {watchers_changed})")
                # Update the data explicitly, prioritizing keys from 'data'
                # Get the existing data or an empty dict
                updated_channel_data = self.channels.get(channel_id, {}).copy()
                
                # Update with keys from the new 'data' dict
                for key, value in data.items():
                    updated_channel_data[key] = value
                    
                # Always update the timestamp
                updated_channel_data['last_updated'] = datetime.now().isoformat()
                
                # Corriger les valeurs incohérentes
                if 'watchers' in updated_channel_data and 'viewers' in updated_channel_data:
                    # S'assurer que le nombre de spectateurs correspond à la liste des watchers
                    watchers_count = len(updated_channel_data['watchers'])
                    if updated_channel_data['viewers'] != watchers_count:
                        logger.debug(f"[{channel_id}] Correction du nombre de viewers: {updated_channel_data['viewers']} → {watchers_count}")
                        updated_channel_data['viewers'] = watchers_count
                
                # Assign the updated dictionary back
                self.channels[channel_id] = updated_channel_data
                
                # Use debounced save or immediate save based on force_save parameter
                if force_save:
                    # Save immediately without debounce
                    self._last_forced_save_time = time.time()
                    success = self._save_status()
                    if success:
                        logger.info(f"[{channel_id}] ✅ Sauvegarde immédiate forcée réussie")
                    else:
                        logger.warning(f"[{channel_id}] ⚠️ Échec de la sauvegarde immédiate forcée")
                else:
                    # Use normal debounced save
                    self._debounced_save()
                return True
                    
        except Exception as e:
            logger.error(f"Error updating channel {channel_id}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def switch_to_channel(self, new_active_channel: str, viewer_ip: str):
        """
        Optimized method for channel zapping - marks one channel as active and all others as inactive.
        This is faster than calling update_channel multiple times.
        """
        try:
            with self._lock:
                channels_updated = False
                logger.info(f"🔀 Switching to channel: {new_active_channel} for viewer: {viewer_ip}")
                
                # First, check if this viewer is already watching this channel
                current_watchers = self.channels.get(new_active_channel, {}).get('watchers', [])
                if viewer_ip in current_watchers:
                    logger.debug(f"Viewer {viewer_ip} already watching {new_active_channel} - no change needed")
                    return True
                
                # Need to find which channel this viewer was watching before (if any)
                previous_channel = None
                for channel_id, channel_data in self.channels.items():
                    if channel_id == new_active_channel:
                        continue
                        
                    watchers = channel_data.get('watchers', [])
                    if viewer_ip in watchers:
                        previous_channel = channel_id
                        # Remove viewer from previous channel
                        watchers.remove(viewer_ip)
                        channel_data['watchers'] = watchers
                        channel_data['viewers'] = len(watchers)
                        channel_data['last_updated'] = datetime.now().isoformat()
                        channels_updated = True
                        logger.debug(f"Removed viewer {viewer_ip} from previous channel {previous_channel}")
                
                # Now add viewer to the new channel
                if new_active_channel not in self.channels:
                    self.channels[new_active_channel] = {
                        'is_live': True,
                        'viewers': 1,
                        'watchers': [viewer_ip],
                        'last_updated': datetime.now().isoformat()
                    }
                    channels_updated = True
                else:
                    channel_data = self.channels[new_active_channel]
                    watchers = channel_data.get('watchers', [])
                    if viewer_ip not in watchers:
                        watchers.append(viewer_ip)
                        channel_data['watchers'] = watchers
                        channel_data['viewers'] = len(watchers)
                        channel_data['is_live'] = True
                        channel_data['last_updated'] = datetime.now().isoformat()
                        channels_updated = True
                
                if channels_updated:
                    # Schedule a save with appropriate debouncing
                    self._debounced_save()
                    
                    if previous_channel:
                        logger.info(f"✅ Viewer {viewer_ip} switched from {previous_channel} to {new_active_channel}")
                    else:
                        logger.info(f"✅ Viewer {viewer_ip} started watching {new_active_channel}")
                    
                return True
                
        except Exception as e:
            logger.error(f"❌ Error in switch_to_channel: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def update_all_channels(self, channels_dict: Dict[str, Any]) -> bool:
        """
        Met à jour le statut de toutes les chaînes en fusionnant les nouvelles données
        avec les données existantes pour préserver les informations sur les viewers.
        """
        try:
            with self._lock:
                logger.debug(f"🔄 Fusion du statut pour {len(channels_dict)} chaînes.")
                
                # Fusionner les nouvelles données, en préservant les clés existantes comme viewers/watchers
                for channel_id, new_data in channels_dict.items():
                    # Obtenir les données existantes ou créer un nouveau dictionnaire
                    existing_data = self.channels.get(channel_id, {})
                    
                    # Mettre à jour avec les nouvelles données (ex: is_live, streaming)
                    existing_data.update(new_data)

                    # Assurer la structure de base pour les nouvelles chaînes
                    if 'is_live' not in existing_data:
                        existing_data['is_live'] = False
                    if 'viewers' not in existing_data:
                        existing_data['viewers'] = 0
                    if 'watchers' not in existing_data:
                        existing_data['watchers'] = []
                    
                    # Toujours mettre à jour le timestamp
                    existing_data['last_updated'] = datetime.now().isoformat()
                    
                    # Remettre dans le dictionnaire principal
                    self.channels[channel_id] = existing_data
                
                # Utiliser la sauvegarde débauncée
                self._debounced_save()
                logger.debug("✅ Fusion du statut de toutes les chaînes terminée.")
                return True
                
        except Exception as e:
            logger.error(f"❌ Erreur lors de la fusion de toutes les chaînes: {e}")
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
        
        # Cancel any pending save timer
        if self._save_timer:
            self._save_timer.cancel()
            self._save_timer = None
            
        # Final save - use direct save to ensure data is written
        if self._pending_save:
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

    def remove_channel(self, channel_id: str) -> bool:
        """Remove a channel from the status file"""
        with self._lock:
            if channel_id in self.channels:
                del self.channels[channel_id]
                logger.info(f"🗑️ Canal '{channel_id}' supprimé du gestionnaire de statut.")
                # Save immediately after removal
                return self._save_status()
        return False
#!/usr/bin/env python3
"""Force a playlist update by triggering the manager"""
import sys
import os

# Add app directory to path
sys.path.insert(0, '/app')

# Import after path is set
from config import HLS_DIR, VIDEO_DIR
import logging

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Import manager
from iptv_manager import IPTVManager

# Create manager instance
logger.info("Creating IPTVManager instance...")
manager = IPTVManager(content_dir=VIDEO_DIR)

# Check how many channels are loaded
logger.info(f"Channels in manager: {len(manager.channels)}")
for name, channel in sorted(manager.channels.items()):
    if channel:
        is_ready = channel.is_ready_for_streaming() if hasattr(channel, 'is_ready_for_streaming') else False
        logger.info(f"  [{name}] Ready: {is_ready}")
    else:
        logger.info(f"  [{name}] Channel object is None")

# Force playlist update
logger.info("Forcing playlist update...")
manager._update_master_playlist()

logger.info("Done!")

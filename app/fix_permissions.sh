#!/bin/bash
# Script to fix permissions for IPTV streaming application

# Create stats directory if it doesn't exist
mkdir -p /app/stats

# Set proper ownership and permissions for the stats directory
chown -R streamer:streamer /app/stats
chmod 777 /app/stats

# Create the files with appropriate permissions if they don't exist
touch /app/stats/user_stats_v2.json
touch /app/stats/channel_stats.json
touch /app/stats/channels_status.json
touch /app/stats/channel_stats_bytes.json
touch /app/stats/user_stats_bytes.json

# Set permissions for all stats files
chmod 666 /app/stats/user_stats_v2.json
chmod 666 /app/stats/channel_stats.json
chmod 666 /app/stats/channels_status.json
chmod 666 /app/stats/channel_stats_bytes.json
chmod 666 /app/stats/user_stats_bytes.json

# Make sure the ownership is correct for all files
chown streamer:streamer /app/stats/*.json

echo "Permissions fixed for stats files" 
#!/bin/bash
# Script to set up host directories with correct permissions before starting containers

echo "Setting up host directories with correct permissions..."

# Create necessary directories
mkdir -p app/logs/ffmpeg app/logs/nginx app/hls app/stats

# Set very permissive permissions
chmod -R 777 app/logs app/stats
chmod -R 777 app/hls

# Create log files
touch app/logs/app.log
touch app/logs/playlist_watchdog.log
chmod 666 app/logs/app.log app/logs/playlist_watchdog.log

# Create initial HLS placeholder files
touch app/hls/placeholder.txt
chmod 666 app/hls/placeholder.txt

# Check that directories are properly set up
echo "Checking directory permissions:"
ls -la app/logs
ls -la app/hls

echo "Host directories prepared with correct permissions."
echo "You can now start the containers with: docker-compose up -d" 
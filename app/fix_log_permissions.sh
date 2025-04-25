#!/bin/bash
# Script to fix log permissions for IPTV streaming application

echo "🔧 Fixing log permissions..."

# Create logs directories if they don't exist
mkdir -p /app/logs
mkdir -p /app/logs/ffmpeg
mkdir -p /app/logs/nginx

# Create log files if they don't exist
touch /app/logs/app.log
touch /app/logs/playlist_watchdog.log

# Set directory permissions
chmod -R 777 /app/logs
chmod -R 777 /app/logs/ffmpeg
chmod -R 777 /app/logs/nginx

# Set file permissions
chmod 666 /app/logs/app.log
chmod 666 /app/logs/playlist_watchdog.log

# Set ownership
chown -R streamer:streamer /app/logs

echo "✅ Log permissions fixed successfully"
echo "   - Directory: /app/logs (777)"
echo "   - Files: app.log, playlist_watchdog.log (666)"

exit 0 
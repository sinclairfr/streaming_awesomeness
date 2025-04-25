#!/bin/bash
# Script to fix log permissions without restarting container

echo "Fixing log permissions on iptv-manager container..."

# Create the app.log file and fix permissions
docker exec -it iptv-manager mkdir -p /app/logs /app/logs/ffmpeg /app/logs/nginx
docker exec -it iptv-manager touch /app/logs/app.log
docker exec -it iptv-manager chmod -R 777 /app/logs
docker exec -it iptv-manager chmod 666 /app/logs/app.log
docker exec -it iptv-manager chown -R streamer:streamer /app/logs

echo "Log permissions fixed! Try restarting the application now." 
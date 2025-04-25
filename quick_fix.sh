#!/bin/bash
# Script to quickly fix permissions for a running container

echo "====== IPTV Manager Quick Fix ======"

# Create host directories with correct permissions
echo "Setting up host directories..."
mkdir -p app/logs/ffmpeg app/logs/nginx app/hls app/stats

# Set proper permissions as root to ensure they are set correctly
echo "Setting permissions on host directories..."
sudo chmod -R 777 app/logs app/hls app/stats
sudo chown -R $USER:$USER app/logs app/hls app/stats

# Create necessary files
echo "Creating log files..."
touch app/logs/app.log
touch app/logs/playlist_watchdog.log
chmod 666 app/logs/app.log app/logs/playlist_watchdog.log

# Create initial HLS placeholder
echo "Creating HLS placeholder..."
touch app/hls/placeholder.txt
chmod 666 app/hls/placeholder.txt

# Create initial stats files
echo "Creating stats files..."
echo "{}" > app/stats/user_stats_v2.json
echo "{}" > app/stats/channel_stats.json
echo "{}" > app/stats/channels_status.json
echo "{\"global\":{\"total_watch_time\":0,\"total_bytes_transferred\":0,\"unique_viewers\":[],\"last_update\":0}}" > app/stats/channel_stats_bytes.json
echo "{\"users\":{},\"last_updated\":$(date +%s)}" > app/stats/user_stats_bytes.json
chmod 666 app/stats/*.json

echo "Verifying directory permissions:"
ls -la app/
ls -la app/logs/
ls -la app/hls/
ls -la app/stats/

echo "Host directories fixed. Now fixing container..."

# Fix permissions inside container
docker exec -i iptv-manager bash << 'EOF'
mkdir -p /app/logs /app/logs/ffmpeg /app/logs/nginx /app/hls /app/stats
chmod -R 777 /app/logs /app/hls /app/stats 2>/dev/null || true
touch /app/logs/app.log 2>/dev/null || true
chmod 666 /app/logs/app.log 2>/dev/null || true

# Create stats files
echo "{}" > /app/stats/user_stats_v2.json 2>/dev/null || true
echo "{}" > /app/stats/channel_stats.json 2>/dev/null || true
echo "{}" > /app/stats/channels_status.json 2>/dev/null || true
echo '{"global":{"total_watch_time":0,"total_bytes_transferred":0,"unique_viewers":[],"last_update":0}}' > /app/stats/channel_stats_bytes.json 2>/dev/null || true
echo '{"users":{},"last_updated":'$(date +%s)'}' > /app/stats/user_stats_bytes.json 2>/dev/null || true
chmod 666 /app/stats/*.json 2>/dev/null || true

echo "Container directories fixed"
EOF

echo "Restarting container..."
docker restart iptv-manager

echo "====== Quick Fix Complete ======"
echo "Check logs with: docker-compose logs -f iptv-manager" 
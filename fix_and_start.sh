#!/bin/bash
# Wrapper script to properly set up the environment and start the containers

echo "====== IPTV Manager Setup ======"
echo "Setting up host directories..."

# Stop containers if running
docker-compose down

# Create necessary directories with correct permissions
mkdir -p app/logs/ffmpeg app/logs/nginx app/hls app/stats

# Set proper permissions with sudo to ensure they are set correctly
sudo chmod -R 777 app/logs app/hls app/stats
sudo chown -R $USER:$USER app/logs app/hls app/stats

# Create log files with correct permissions
touch app/logs/app.log
touch app/logs/playlist_watchdog.log
chmod 666 app/logs/app.log app/logs/playlist_watchdog.log

# Create initial HLS placeholder
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

# Verify permissions
echo "Verifying directory permissions:"
ls -la app/
ls -la app/logs/
ls -la app/hls/
ls -la app/stats/

echo "Host directories prepared with correct permissions."

# Ensure volume mounts will work in docker-compose.yml
echo "Checking docker-compose.yml configuration..."
if grep -q "./app:/app:ro" docker-compose.yml; then
  echo "Docker Compose file has the correct volume mounts."
else
  echo "Warning: Updating docker-compose.yml with correct volume mounts..."
  # Replace app volume mount
  sed -i 's|./app:/app:rw|./app:/app:ro|g' docker-compose.yml
  echo "✅ docker-compose.yml updated to use read-only app volume with specific writeable mounts."
fi

# Rebuild containers to ensure they have the latest code
echo "Building containers..."
docker-compose build

# Start containers
echo "Starting containers..."
docker-compose up -d

echo "====== Setup Complete ======"
echo "You can view logs with: docker-compose logs -f" 
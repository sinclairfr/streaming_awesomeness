#!/bin/bash
echo "🛠️ Correction des permissions sur /app/hls..."
mkdir -p /app/hls
chown -R streamer:streamer /app/hls
chmod -R 777 /app/hls
echo "✅ Permissions corrigées."

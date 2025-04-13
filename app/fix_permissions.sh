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

# Chemin des répertoires à vérifier
CONTENT_DIR="/mnt/frigate_data/streaming_awesomeness/content"
HLS_DIR="/app/hls"

echo "💡 Démarrage du script de correction des permissions"

# Vérifier si le répertoire content existe
if [ ! -d "$CONTENT_DIR" ]; then
    echo "❌ ERREUR: Le répertoire $CONTENT_DIR n'existe pas!"
    exit 1
fi

# Corriger les permissions du répertoire principal
echo "📂 Application de permissions 777 sur $CONTENT_DIR"
chmod -R 777 "$CONTENT_DIR" || echo "⚠️ Impossible d'appliquer les permissions sur $CONTENT_DIR"

# Parcourir tous les sous-répertoires (chaînes)
echo "🔍 Parcours des répertoires de chaînes..."
for channel_dir in "$CONTENT_DIR"/*; do
    if [ -d "$channel_dir" ]; then
        channel_name=$(basename "$channel_dir")
        echo "📁 Correction des permissions pour la chaîne: $channel_name"
        
        # Permissions sur le répertoire de chaîne
        chmod 777 "$channel_dir"
        
        # Permissions pour ready_to_stream
        if [ -d "$channel_dir/ready_to_stream" ]; then
            echo "  ↳ Correction de ready_to_stream pour $channel_name"
            chmod 777 "$channel_dir/ready_to_stream"
            
            # Permissions pour tous les fichiers vidéo
            find "$channel_dir/ready_to_stream" -type f -name "*.mp4" -o -name "*.mkv" -o -name "*.avi" | while read -r video_file; do
                chmod 666 "$video_file"
                echo "  ↳ ✅ Permissions 666 sur $(basename "$video_file")"
            done
        fi
        
        # Permissions pour processed
        if [ -d "$channel_dir/processed" ]; then
            echo "  ↳ Correction de processed pour $channel_name"
            chmod 777 "$channel_dir/processed"
        fi
        
        # Création du répertoire HLS s'il n'existe pas
        hls_channel_dir="$HLS_DIR/$channel_name"
        if [ ! -d "$hls_channel_dir" ]; then
            echo "  ↳ Création du répertoire HLS pour $channel_name"
            mkdir -p "$hls_channel_dir"
        fi
        chmod 777 "$hls_channel_dir"
    fi
done

echo "✅ Correction de permissions terminée"
exit 0 
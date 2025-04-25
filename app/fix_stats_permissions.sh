#!/bin/bash
# Script pour corriger les permissions du dossier stats

echo "🔧 Correction des permissions du dossier stats..."

# Créer le dossier stats s'il n'existe pas
mkdir -p /app/stats

# Créer les fichiers stats
echo "📄 Création des fichiers stats..."
touch /app/stats/user_stats_v2.json
touch /app/stats/channel_stats.json
touch /app/stats/channels_status.json
touch /app/stats/channel_stats_bytes.json
touch /app/stats/user_stats_bytes.json

# Initialiser les fichiers avec des structures JSON valides s'ils sont vides
for f in /app/stats/user_stats_v2.json /app/stats/channel_stats.json /app/stats/channels_status.json; do
  if [ ! -s "$f" ]; then
    echo "{}" > "$f"
    echo "📋 Initialisation de $f avec {}"
  fi
done

if [ ! -s "/app/stats/channel_stats_bytes.json" ]; then
  echo '{"global":{"total_watch_time":0,"total_bytes_transferred":0,"unique_viewers":[],"last_update":0}}' > /app/stats/channel_stats_bytes.json
  echo "📋 Initialisation de channel_stats_bytes.json"
fi

if [ ! -s "/app/stats/user_stats_bytes.json" ]; then
  echo '{"users":{},"last_updated":'$(date +%s)'}' > /app/stats/user_stats_bytes.json
  echo "📋 Initialisation de user_stats_bytes.json"
fi

# Définir les permissions
chmod -R 777 /app/stats
chmod 666 /app/stats/*.json

# Vérifier les permissions
echo "✅ Permissions appliquées:"
ls -la /app/stats/

echo "🎉 Correction des permissions du dossier stats terminée!"
echo "Pour exécuter ce script dans le conteneur: docker exec -it iptv-manager /app/fix_stats_permissions.sh"

exit 0 
#!/bin/bash
# Script pour corriger les permissions du dossier stats sans redémarrer le conteneur

echo "====== Correction des permissions du dossier stats ======"

# Vérifier si le conteneur est en cours d'exécution
if ! docker ps | grep -q iptv-manager; then
  echo "❌ Le conteneur iptv-manager n'est pas en cours d'exécution."
  echo "Démarrez-le d'abord avec: docker-compose up -d"
  exit 1
fi

# Créer les dossiers nécessaires sur l'hôte
echo "📁 Création des dossiers sur l'hôte..."
mkdir -p app/stats

# Définir les permissions sur l'hôte
echo "🔒 Définition des permissions sur l'hôte..."
sudo chmod -R 777 app/stats
sudo chown -R $USER:$USER app/stats

# Créer les fichiers de stats initiaux
echo "📄 Création des fichiers de stats initiaux..."
echo "{}" > app/stats/user_stats_v2.json
echo "{}" > app/stats/channel_stats.json
echo "{}" > app/stats/channels_status.json
echo '{"global":{"total_watch_time":0,"total_bytes_transferred":0,"unique_viewers":[],"last_update":0}}' > app/stats/channel_stats_bytes.json
echo '{"users":{},"last_updated":'$(date +%s)'}' > app/stats/user_stats_bytes.json
chmod 666 app/stats/*.json

# Vérifier les permissions sur l'hôte
echo "✅ Permissions appliquées sur l'hôte:"
ls -la app/stats

# Exécuter le script dans le conteneur
echo "🔧 Exécution du script de correction dans le conteneur..."
docker exec -it iptv-manager /app/fix_stats_permissions.sh

echo "====== Correction terminée ======"
echo "Pour voir les logs, exécutez: docker-compose logs -f iptv-manager" 
#!/bin/bash
# Script de démarrage de l'application

echo "Démarrage du service IPTV..."

# Test critical directories before starting
echo "Vérification des répertoires critiques..."
mkdir -p /app/hls /app/logs /app/logs/ffmpeg /app/logs/nginx /app/stats 2>/dev/null || true

# Test if we can write to critical directories
for d in "/app/hls" "/app/logs" "/app/stats"; do
  if touch "$d/test_write.tmp" 2>/dev/null; then
    echo "✅ Le répertoire $d est accessible en écriture"
    rm "$d/test_write.tmp"
  else
    echo "⚠️ ALERTE: Le répertoire $d n'est PAS accessible en écriture!"
    echo "   Problèmes de permissions docker détectés. Vérifiez les montages de volumes."
  fi
done

# Configuration des logs sans exiger les permissions
echo "Configuration des logs..."
mkdir -p /app/logs /app/logs/ffmpeg /app/logs/nginx 2>/dev/null || true
touch /app/logs/app.log 2>/dev/null || true
chmod -R 777 /app/logs 2>/dev/null || true
chmod 666 /app/logs/app.log 2>/dev/null || true

# Configuration du dossier stats
echo "Configuration des stats..."
mkdir -p /app/stats 2>/dev/null || true
touch /app/stats/user_stats_v2.json 2>/dev/null || true
touch /app/stats/channel_stats.json 2>/dev/null || true
touch /app/stats/channels_status.json 2>/dev/null || true
touch /app/stats/channel_stats_bytes.json 2>/dev/null || true
touch /app/stats/user_stats_bytes.json 2>/dev/null || true

# Initialisation des fichiers stats avec des structures JSON valides si vides
for f in /app/stats/user_stats_v2.json /app/stats/channel_stats.json /app/stats/channels_status.json; do
  if [ ! -s "$f" ]; then
    echo "{}" > "$f" 2>/dev/null || true
  fi
done

if [ ! -s "/app/stats/channel_stats_bytes.json" ]; then
  echo '{"global":{"total_watch_time":0,"total_bytes_transferred":0,"unique_viewers":[],"last_update":0}}' > /app/stats/channel_stats_bytes.json 2>/dev/null || true
fi

if [ ! -s "/app/stats/user_stats_bytes.json" ]; then
  echo '{"users":{},"last_updated":'$(date +%s)'}' > /app/stats/user_stats_bytes.json 2>/dev/null || true
fi

chmod -R 777 /app/stats 2>/dev/null || true
chmod 666 /app/stats/*.json 2>/dev/null || true

# Vérification de l'accès au fichier log
if [ -w "/app/logs/app.log" ]; then
    echo "✅ Fichier de log accessible en écriture."
else
    echo "⚠️ ALERTE: Fichier de log non accessible en écriture."
    echo "   Le logging se fera uniquement sur la console."
fi

# Vérification de l'environnement
echo "Utilisateur actuel: $(id)"
echo "Permissions du répertoire logs:"
ls -la /app/logs || true
echo "Permissions du répertoire hls:"
ls -la /app/hls || true
echo "Permissions du répertoire stats:"
ls -la /app/stats || true

# Lancement du service cron
echo "Démarrage du service cron..."
sudo service cron start || true

# Attendre 1 seconde avant de démarrer pour s'assurer que tout est prêt
sleep 1

# Lancement de l'application principale
echo "Lancement de l'application principale..."
exec python3 main.py 
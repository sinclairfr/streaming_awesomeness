#!/bin/bash
# Script de démarrage de l'application

echo "Démarrage du service IPTV..."

# S'assurer que les permissions sont correctes
if [ "$(id -u)" -eq 0 ]; then
    echo "Configuration des permissions..."
    chown -R streamer:streamer /app
    chmod -R 777 /app
fi

# Nettoyer les logs trop longs avant de démarrer
echo "Nettoyage des logs trop longs..."
python3 /app/cleanup_logs.py

# Lancement du service cron
echo "Démarrage du service cron..."
sudo service cron start

# Lancement de l'application principale
echo "Lancement de l'application principale..."
exec python3 main.py 
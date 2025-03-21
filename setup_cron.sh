#!/bin/bash

# Créer le fichier de log pour cron s'il n'existe pas
touch /var/log/cron.log

# Ajouter la tâche cron pour exécuter stuck_stream_monitor.py toutes les 5 minutes
echo "*/5 * * * * /usr/bin/python3 /app/stuck_stream_monitor.py >> /var/log/cron.log 2>&1" > /etc/cron.d/stuck_stream_monitor

# Donner les permissions appropriées
chmod 0644 /etc/cron.d/stuck_stream_monitor

# Appliquer les changements de cron
crontab /etc/cron.d/stuck_stream_monitor

# Démarrer le service cron
service cron start

# Garder le conteneur en vie
tail -f /var/log/cron.log 
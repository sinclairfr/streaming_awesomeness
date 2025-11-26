    #!/bin/bash

# Créer le fichier de log pour cron
touch /var/log/cron.log

# Ajouter la tâche cron pour exécuter client_monitor.py toutes les minutes
echo "*/1 * * * * /usr/bin/python3 /app/client_monitor.py >> /var/log/cron.log 2>&1" > /etc/cron.d/client_monitor

# S'assurer que le fichier a les bonnes permissions
chmod 0644 /etc/cron.d/client_monitor

# Ajouter la tâche à crontab
crontab /etc/cron.d/client_monitor

# Démarrer le service cron
service cron start

# Garder le conteneur en vie
tail -f /var/log/cron.log 
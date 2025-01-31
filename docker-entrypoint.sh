#!/bin/bash
set -e

# Configuration des permissions par défaut si non spécifiées
PUID=${PUID:-1000}
PGID=${PGID:-1000}

# Création de l'utilisateur avec les UID/GID spécifiés
groupadd -g $PGID appgroup
useradd -u $PUID -g $PGID -m appuser

# S'assure que les dossiers existent
mkdir -p /hls /cache /content

# Donne les permissions à l'utilisateur
chown -R appuser:appgroup /hls /cache
chmod -R 755 /hls /cache

# Exécute la commande en tant que l'utilisateur appuser
exec gosu appuser "$@"
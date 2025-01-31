#!/bin/bash
set -e

# Création des dossiers nécessaires
mkdir -p /hls /cache /content

# Configuration des permissions
chown -R 1000:1000 /hls /cache /content
chmod -R 755 /hls /cache /content

echo "Initialisation terminée"
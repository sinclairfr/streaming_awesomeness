#!/bin/sh

# Source the .env file to get HLS_DIR
if [ -f /app/.env ]; then
    export $(grep -v '^#' /app/.env | xargs)
fi

# Default HLS_DIR if not set
HLS_DIR=${HLS_DIR:-/mnt/iptv}

# Substitute environment variables in the template
envsubst '${HLS_DIR}' < /etc/nginx/templates/nginx.conf.template > /etc/nginx/nginx.conf

# Start nginx
nginx -g 'daemon off;'
# Utiliser l'image NVIDIA CUDA pour l'accélération matérielle
#FROM nvidia/cuda:11.8.0-base-ubuntu22.04
FROM ubuntu:22.04

# Passer en root pour l'installation des paquets
USER root

# Installation des dépendances système
RUN apt-get update && apt-get install -y \
    python3 python3-pip software-properties-common tree nano sudo cron logrotate wget xz-utils strace \
    && rm -rf /var/lib/apt/lists/*

# Download and install latest FFmpeg static build (currently FFmpeg 7)
RUN wget https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz -O /tmp/ffmpeg.tar.xz && \
    tar -xf /tmp/ffmpeg.tar.xz -C /tmp --strip-components=1 && \
    mv /tmp/ffmpeg /usr/local/bin/ && \
    mv /tmp/ffprobe /usr/local/bin/ && \
    rm -rf /tmp/ffmpeg.tar.xz /tmp/readme.txt /tmp/GPLv3.txt /tmp/LGPLv3.txt /tmp/qt-faststart

# Installation des pilotes VA-API pour l'accélération matérielle
RUN apt-get update && apt-get install -y \
    i965-va-driver \
    vainfo \
    libva-dev \
    && rm -rf /var/lib/apt/lists/*

RUN apt-get install -y procps

RUN apt-get update && apt-get install -y \
    intel-gpu-tools \
    vainfo \
    i965-va-driver \
    && rm -rf /var/lib/apt/lists/*

# Time
# Prévenir les prompts interactifs pour tzdata
ENV DEBIAN_FRONTEND=noninteractive

# Installer tzdata et définir le fuseau horaire
RUN apt-get update && apt-get install -y tzdata
ENV TZ=Europe/Paris

# Permettre à l'utilisateur streamer d'utiliser sudo pour kill
RUN echo "streamer ALL=(ALL) NOPASSWD: /usr/bin/kill" >> /etc/sudoers
# Après la création de l'utilisateur streamer
RUN echo "streamer ALL=(ALL) NOPASSWD: /bin/kill, /usr/bin/kill, /bin/pkill" >> /etc/sudoers.d/streamer

# Création d'un utilisateur non-root pour éviter les problèmes de permissions
RUN useradd -m -s /bin/bash streamer

# Après la création de l'utilisateur streamer
RUN usermod -aG sudo streamer && \
    echo "streamer ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers.d/streamer

# Donne TOUS les droits sur /app
RUN mkdir -p /app/hls /app/logs/ffmpeg /app/restart_requests /app/stats && \
    chown -R streamer:streamer /app && \
    chmod -R 777 /app

# Configuration de logrotate pour nginx
RUN echo '/var/log/nginx/*.log {\n\
    daily\n\
    missingok\n\
    rotate 7\n\
    compress\n\
    delaycompress\n\
    notifempty\n\
    create 0640 www-data adm\n\
    sharedscripts\n\
    postrotate\n\
        [ -f /var/run/nginx.pid ] && kill -USR1 `cat /var/run/nginx.pid`\n\
    endscript\n\
}' > /etc/logrotate.d/nginx && \
    chmod 644 /etc/logrotate.d/nginx

# Configuration du cron pour l'utilisateur root (pour accéder aux logs nginx)
RUN echo "*/5 * * * * /usr/bin/python3 /app/stuck_stream_monitor.py >> /var/log/cron.log 2>&1" > /etc/cron.d/stuck_stream_monitor && \
    chmod 0644 /etc/cron.d/stuck_stream_monitor && \
    crontab /etc/cron.d/stuck_stream_monitor

# Créer les fichiers de log nécessaires
RUN touch /var/log/cron.log && \
    chmod 666 /var/log/cron.log

# Créer les fichiers stats initiaux
RUN touch /app/stats/user_stats_v2.json /app/stats/channel_stats.json /app/stats/channels_status.json /app/stats/channel_stats_bytes.json /app/stats/user_stats_bytes.json && \
    chmod 666 /app/stats/*.json && \
    echo "{}" > /app/stats/user_stats_v2.json && \
    echo "{}" > /app/stats/channel_stats.json && \
    echo "{}" > /app/stats/channels_status.json && \
    echo '{"global":{"total_watch_time":0,"total_bytes_transferred":0,"unique_viewers":[],"last_update":0}}' > /app/stats/channel_stats_bytes.json && \
    echo '{"users":{},"last_updated":0}' > /app/stats/user_stats_bytes.json && \
    chown -R streamer:streamer /app/stats

# Passer à l'utilisateur non-root
USER streamer

# Définir le répertoire de travail
WORKDIR /app

# Copier les fichiers nécessaires à l'application
COPY --chown=streamer:streamer requirements.txt /app/
COPY --chown=streamer:streamer setup_cron.sh /app/
COPY --chown=streamer:streamer app/fix_permissions.sh /app/
COPY --chown=streamer:streamer app/fix_log_permissions.sh /app/
COPY --chown=streamer:streamer app/fix_stats_permissions.sh /app/
RUN chmod +x /app/setup_cron.sh
RUN chmod +x /app/fix_permissions.sh
RUN chmod +x /app/fix_log_permissions.sh
RUN chmod +x /app/fix_stats_permissions.sh

# Installation des dépendances Python
RUN pip3 install --no-cache-dir -r /app/requirements.txt

# Création du script de démarrage
RUN echo '#!/bin/bash\n\
chown -R streamer:streamer /app\n\
chmod -R 777 /app\n\
mkdir -p /app/stats\n\
chown -R streamer:streamer /app/stats\n\
chmod 777 /app/stats\n\
touch /app/stats/user_stats_v2.json /app/stats/channel_stats.json /app/stats/channels_status.json /app/stats/channel_stats_bytes.json /app/stats/user_stats_bytes.json\n\
chmod 666 /app/stats/*.json\n\
chown streamer:streamer /app/stats/*.json\n\
sudo service cron start\n\
echo "Cron service started"\n\
/app/fix_log_permissions.sh\n\
/app/fix_stats_permissions.sh\n\
echo "Log and stats permissions fixed"\n\
exec python3 main.py' > /app/start.sh && \
chmod +x /app/start.sh

# Point d'entrée : lancement du service avec correction des permissions au runtime
ENTRYPOINT ["/app/start.sh"]
# Utiliser l'image NVIDIA CUDA pour l'accélération matérielle
#FROM nvidia/cuda:11.8.0-base-ubuntu22.04
FROM ubuntu:22.04

# Passer en root pour l'installation des paquets
USER root

# Installation des dépendances système
RUN apt-get update && apt-get install -y \
    python3 python3-pip software-properties-common tree nano sudo \
    && add-apt-repository -y ppa:ubuntuhandbook1/ffmpeg6 \
    && apt-get update \
    && apt-get install -y ffmpeg strace \
    && rm -rf /var/lib/apt/lists/*
    
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
    
# Permettre à l'utilisateur streamer d'utiliser sudo pour kill
RUN echo "streamer ALL=(ALL) NOPASSWD: /usr/bin/kill" >> /etc/sudoers

# Création d'un utilisateur non-root pour éviter les problèmes de permissions
RUN useradd -m -s /bin/bash streamer

# Après la création de l'utilisateur streamer
RUN usermod -aG sudo streamer && \
    echo "streamer ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers.d/streamer

# Donne TOUS les droits sur /app
RUN mkdir -p /app/hls /app/content /app/logs/ffmpeg && \
    chown -R streamer:streamer /app && \
    chmod -R 777 /app

# Installation des dépendances Python
RUN pip3 install --no-cache-dir --upgrade psutil watchdog tqdm python-dotenv

# Passer à l'utilisateur non-root
USER streamer

# Définir le répertoire de travail
WORKDIR /app

# Copier les fichiers nécessaires à l'application
COPY --chown=streamer:streamer requirements.txt /app/
#COPY --chown=streamer:streamer app/*.py /app/

# Installation des dépendances Python
RUN pip3 install --no-cache-dir -r /app/requirements.txt

# Point d'entrée : lancement du service avec correction des permissions au runtime
ENTRYPOINT ["/bin/bash", "-c", "chown -R streamer:streamer /app && chmod -R 777 /app && exec python3 main.py"]

# Utiliser l'image NVIDIA CUDA pour l'accélération matérielle
FROM nvidia/cuda:11.8.0-base-ubuntu22.04

# Passer en root pour l'installation des paquets
USER root

# Installation des dépendances système
RUN apt-get update && apt-get install -y \
    python3 python3-pip software-properties-common tree nano \
    && add-apt-repository -y ppa:ubuntuhandbook1/ffmpeg6 \
    && apt-get update \
    && apt-get install -y ffmpeg strace \
    && rm -rf /var/lib/apt/lists/*

# Création d'un utilisateur non-root pour éviter les problèmes de permissions
RUN useradd -m -s /bin/bash streamer

# Définir les répertoires nécessaires et fixer les permissions
RUN mkdir -p /app/hls /app/content /app/logs/ffmpeg && \
    chown -R streamer:streamer /app && \
    chmod -R 777 /app/hls /app/content /app/logs

# Passer à l'utilisateur non-root
USER streamer

# Définir le répertoire de travail
WORKDIR /app

# Copier les fichiers nécessaires à l'application
COPY --chown=streamer:streamer requirements.txt /app/
COPY --chown=streamer:streamer app/*.py /app/

# Installation des dépendances Python
RUN pip3 install --no-cache-dir -r /app/requirements.txt

# Point d'entrée : lancement du service avec correction des permissions au runtime
ENTRYPOINT ["/bin/bash", "-c", "chown -R streamer:streamer /app && chmod -R 777 /app && exec python3 main.py"]

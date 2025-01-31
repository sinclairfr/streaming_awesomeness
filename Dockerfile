FROM nvidia/cuda:11.8.0-base-ubuntu22.04

# Installation des dépendances
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Installation des dépendances Python
COPY requirements.txt /app/
RUN pip3 install -r /app/requirements.txt

# Copie du code
COPY iptv_manager.py /app/

# Création des dossiers nécessaires
RUN mkdir -p /hls /cache /content && \
    chmod 777 /hls /cache /content

WORKDIR /app
CMD ["python3", "iptv_manager.py"]
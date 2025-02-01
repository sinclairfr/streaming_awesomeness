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

WORKDIR /app

# Création des dossiers nécessaires
RUN mkdir -p ./hls ./cache ./content && \
    chmod 777 ./hls ./cache ./content
    
# Utilise python avec -u pour avoir les logs en temps réel
CMD ["python3", "-u", "iptv_manager.py"]
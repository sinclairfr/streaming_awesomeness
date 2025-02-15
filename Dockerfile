FROM nvidia/cuda:11.8.0-base-ubuntu22.04

USER root

# Installation des dépendances
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    software-properties-common \
    && add-apt-repository ppa:ubuntuhandbook1/ffmpeg6 \
    && apt-get update \
    && apt-get install -y ffmpeg \
    && rm -rf /var/lib/apt/lists/*

RUN add-apt-repository universe -y
RUN apt install -y tree nano

# Configuration de l'application
WORKDIR /app

# Copie des fichiers
COPY requirements.txt /app/
COPY app/*.py /app/

# Installation des dépendances Python
RUN pip3 install -r /app/requirements.txt

# Création de la structure des dossiers avec les bonnes permissions
RUN mkdir -p /app/hls /app/content /app/logs/ffmpeg && \
    # On donne les droits à tous les utilisateurs
    chmod -R 777 /app && \
    # On s'assure que les dossiers sont accessibles en écriture
    chmod 777 /app/hls && \
    chmod 777 /app/content && \
    chmod 777 /app/logs

# Point d'entrée
CMD ["python3", "-u", "main.py"]
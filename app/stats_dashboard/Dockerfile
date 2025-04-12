FROM python:3.9-slim

# Ajouter réglage du fuseau horaire
ENV TZ=Europe/Paris
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY stats_dashboard.py ./

# Créer le dossier stats
RUN mkdir -p /app/stats

COPY entrypoint.py ./

# Rendre le script exécutable
RUN chmod +x entrypoint.py

# Utiliser le script d'entrée
CMD ["python", "entrypoint.py"]
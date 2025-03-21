# Dashboard de Statistiques IPTV

Ce dashboard permet de visualiser les statistiques d'utilisation du service IPTV. Il affiche des informations sur les utilisateurs, les chaînes et les temps de visionnage.

## Fonctionnalités

- Vue d'ensemble des statistiques globales
- Répartition du temps de visionnage par chaîne
- Graphique du temps passé par chaîne
- Activité par utilisateur
- Détails par utilisateur (temps total, chaîne préférée, etc.)
- Mise à jour automatique toutes les 10 secondes

## Installation

### Prérequis

- Docker et Docker Compose
- Les fichiers de statistiques dans le dossier `app/stats`

### Installation avec Docker

1. Placez les fichiers dans un répertoire:
   - `stats_dashboard.py`
   - `generate_mock_data.py` (optionnel, pour générer des données de test)
   - `Dockerfile`
   - `docker-compose.yml`
   - `requirements.txt`

2. Construisez et démarrez le container:

```sh
docker-compose up -d
```

3. Accédez au dashboard via votre navigateur:

```
http://localhost:8050
```

### Installation manuelle

1. Installez les dépendances:

```sh
pip install -r requirements.txt
```

2. Générez des données de test (optionnel):

```sh
python generate_mock_data.py
```

3. Démarrez le dashboard:

```sh
python stats_dashboard.py
```

## Structure des fichiers de statistiques

Le dashboard s'attend à trouver les fichiers JSON suivants:

- `/app/stats/channel_stats.json`: Statistiques des chaînes
- `/app/stats/user_stats.json`: Statistiques des utilisateurs

## Intégration avec le service IPTV existant

Pour intégrer ce dashboard avec votre service IPTV existant:

1. Assurez-vous que le dossier de statistiques est monté dans les deux services
2. Vérifiez que les structures de données correspondent à ce que le dashboard attend

## Personnalisation

- Modifiez l'intervalle de rafraîchissement dans `app.layout` (`dcc.Interval`)
- Personnalisez les couleurs et la mise en page dans la section CSS
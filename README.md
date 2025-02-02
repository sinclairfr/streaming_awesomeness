# IPTV Streaming Service

## Description
Ce projet permet de créer un service de diffusion IPTV en continu à partir de fichiers vidéo locaux. Il scanne un dossier contenant des vidéos, normalise les fichiers si nécessaire, puis génère un flux HLS compatible avec la plupart des lecteurs IPTV.

## Fonctionnalités
- Détection automatique des fichiers vidéo et des mises à jour
- Normalisation des fichiers vidéo (H.264, AAC, YUV420p, 25 FPS)
- Génération d'un flux HLS en continu avec segments dynamiques
- Surveillance en temps réel des nouveaux fichiers ajoutés
- Création automatique de playlists M3U pour les lecteurs IPTV
- Gestion multi-chaînes avec Watchdog pour la détection des changements

## Installation
### Prérequis
- **Python 3.8+**
- **FFmpeg** installé sur le système
- **pip** pour gérer les dépendances

### Dépendances
Installez les dépendances nécessaires avec :
```sh
pip install -r requirements.txt
```

### Configuration
- Définir la variable d'environnement `SERVER_URL` avec l'IP du serveur IPTV.
- Modifier le chemin du dossier contenant les vidéos (`./content` par défaut).

## Utilisation
### Démarrer le serveur IPTV
Lancer le script principal pour scanner et diffuser les vidéos :
```sh
python main.py
```

Le service surveillera le dossier `./content` et mettra à jour automatiquement les flux.

### Ajouter des vidéos
Placez simplement vos fichiers vidéo dans `./content/{nom_chaine}/`. Les formats supportés sont : `mp4`, `avi`, `mkv`, `mov`. Les fichiers sont automatiquement convertis si nécessaire.

### Accéder aux flux IPTV
Les flux sont disponibles sous forme de fichiers M3U8 accessibles à :
```
http://{SERVER_URL}/hls/{nom_chaine}/playlist.m3u8
```

Un fichier maître `playlist.m3u` est généré dans `./hls/playlist.m3u` pour regrouper toutes les chaînes.

## Logs et Debug
Les logs sont configurés pour afficher les événements en cours. Vous pouvez ajuster le niveau avec la variable d'environnement `LOG_LEVEL` :
```sh
export LOG_LEVEL=DEBUG
```

## Arrêter le service IPTV
Interrompez le script avec `CTRL + C`. Tous les processus FFmpeg sont arrêtés proprement.

## Contribuer
Les contributions sont les bienvenues ! Forkez ce repo et proposez vos améliorations via une Pull Request.

## Licence
MIT License.


# Fonctionnalité de Relance Manuelle des Chaînes

## Vue d'ensemble

Cette fonctionnalité permet de relancer manuellement une chaîne IPTV depuis le dashboard sans avoir à redémarrer tout le système. La relance s'effectue via une interface web intuitive avec des boutons dédiés pour chaque chaîne.

## Architecture

### 1. API Flask (`channel_api.py`)

Une nouvelle API REST Flask a été créée pour gérer les actions sur les chaînes :

**Endpoints disponibles :**

- `GET /api/channels` - Liste toutes les chaînes avec leur statut
- `POST /api/channels/<channel_name>/restart` - Relance une chaîne spécifique
- `GET /api/channels/<channel_name>/status` - Obtient le statut détaillé d'une chaîne
- `GET /api/health` - Vérification de santé de l'API

**Port :** 5000 (configuré dans `main.py`)

### 2. Interface Dashboard (modifications dans `stats_dashboard.py`)

**Modifications apportées :**

1. **Bouton de relance** : Chaque carte de chaîne affiche maintenant un bouton "🔄 Relancer"
2. **Zone de notification** : Une zone fixe en haut de la page affiche les résultats des actions
3. **Callback interactif** : Gestion des clics sur les boutons avec appels API asynchrones

### 3. Intégration dans l'application principale (`main.py`)

L'API Flask est démarrée automatiquement au lancement de l'application dans un thread séparé, permettant au système de fonctionner normalement sans bloquer.

## Comment utiliser

### Depuis le Dashboard Web

1. Ouvrez le dashboard à l'adresse : `http://<votre-serveur>:8050`
2. Localisez la section "Statut des Chaînes en Direct"
3. Identifiez la chaîne que vous souhaitez relancer
4. Cliquez sur le bouton **🔄 Relancer** de la chaîne concernée
5. Une notification s'affiche confirmant le début de la relance
6. Le statut de la chaîne se met à jour automatiquement

### Via l'API directement

Vous pouvez également utiliser l'API directement avec curl ou tout autre client HTTP :

```bash
# Relancer une chaîne
curl -X POST http://localhost:5000/api/channels/<nom_chaine>/restart

# Obtenir le statut d'une chaîne
curl http://localhost:5000/api/channels/<nom_chaine>/status

# Lister toutes les chaînes
curl http://localhost:5000/api/channels
```

## Éléments rafraîchis lors de la relance

Lorsqu'une chaîne est relancée, les éléments suivants sont automatiquement rafraîchis :

### 1. **Processus FFmpeg**
- L'ancien processus FFmpeg est arrêté proprement
- Un nouveau processus FFmpeg est démarré avec une nouvelle vidéo

### 2. **Segments HLS**
- Le répertoire HLS de la chaîne (`/app/hls/<channel_name>/`) est nettoyé
- De nouveaux segments HLS sont générés par FFmpeg

### 3. **Sélection de vidéo**
- **Mode série** (si `series.txt` existe) : Passe à la vidéo suivante dans l'ordre alphabétique
- **Mode aléatoire** (sans `series.txt`) : Sélectionne une nouvelle vidéo aléatoire différente
- L'index de vidéo courante (`current_video_index`) est mis à jour

### 4. **Statut de la chaîne**
- Le fichier `/app/stats/channels_status.json` est mis à jour
- Les champs `is_live`, `viewers`, `watchers`, `last_updated` sont rafraîchis

### 5. **Playlist maître**
- La playlist maître (`/app/hls/playlist.m3u`) est régénérée
- La chaîne relancée est incluse dans la liste des chaînes actives

### 6. **Dashboard**
- Le dashboard se rafraîchit automatiquement toutes les 2 secondes
- Les nouvelles informations de statut sont affichées

## Détails techniques

### Flux de relance

1. **Clic sur le bouton** dans le dashboard
2. **Callback Dash** intercepte l'événement
3. **Requête HTTP POST** vers l'API Flask (`/api/channels/<nom>/restart`)
   - URL de l'API : configurée via `CHANNEL_API_URL` (variable d'environnement)
   - Par défaut : `http://iptv-manager:5000` dans Docker
4. **L'API** déclenche un thread séparé pour la relance
5. **Thread de relance** exécute `channel._restart_stream()` :
   - Arrêt du processus FFmpeg
   - Nettoyage du répertoire HLS
   - Pause de 1.5-3.0 secondes
   - Vérification de l'existence de `series.txt`
   - **Si mode série** : Passe à la vidéo suivante (index + 1) % nombre_videos
   - **Si mode aléatoire** : Sélection aléatoire d'une nouvelle vidéo
   - Démarrage du nouveau stream
6. **Mise à jour du statut** via `ChannelStatusManager`
7. **Mise à jour de la playlist** via `_update_master_playlist()`
8. **Notification** affichée dans le dashboard

### Mode Série vs Mode Aléatoire

La relance respecte automatiquement le mode de lecture de la chaîne :

- **Mode Série** : Si le fichier `series.txt` existe dans le dossier de la chaîne
  - La relance passe à la **vidéo suivante** dans l'ordre alphabétique
  - Retour au début après la dernière vidéo (lecture en boucle)
  - Logs : `➡️ Passage à la vidéo suivante (mode série): Index X`

- **Mode Aléatoire** : Si `series.txt` n'existe pas
  - La relance sélectionne une **nouvelle vidéo aléatoire** différente de l'actuelle
  - Logs : `🔀 Sélection d'un nouveau fichier aléatoire: Index X`

### Gestion des erreurs

- **Chaîne inexistante** : Retourne une erreur 404
- **Échec de relance** : Logged mais ne bloque pas le système
- **Timeout API** : 5 secondes de timeout pour les requêtes HTTP
- **Thread séparé** : La relance s'exécute de manière asynchrone pour ne pas bloquer l'API

### Sécurité et concurrence

- **Thread-safe** : Utilisation de locks (`self.lock`) pour les modifications
- **Relance non-bloquante** : Exécutée dans un thread daemon séparé
- **API isolée** : L'API Flask tourne dans son propre thread
- **CORS activé** : Permet les requêtes depuis le dashboard Dash

## Fichiers modifiés/créés

### Nouveaux fichiers
- `app/channel_api.py` - API Flask pour la gestion des chaînes
- `RELANCE_MANUELLE.md` - Documentation de la fonctionnalité

### Fichiers modifiés
- `app/main.py` - Intégration du démarrage de l'API
- `app/iptv_channel.py` - Modification de `_restart_stream()` pour respecter le mode série
- `app/stats_dashboard/stats_dashboard.py` - Ajout des boutons et callbacks
- `docker-compose.yml` - Exposition du port 5000 et ajout de `CHANNEL_API_URL`
- `requirements.txt` - Ajout de Flask et Flask-CORS

## Dépendances ajoutées

```
flask>=2.3.0
flask-cors>=4.0.0
requests>=2.31.0
```

## Installation

### Étape 1 : Installer les dépendances

```bash
pip install -r requirements.txt
```

### Étape 2 : Reconstruire les conteneurs Docker

```bash
docker-compose down
docker-compose build
docker-compose up -d
```

### Étape 3 : Vérifier que l'API est accessible

```bash
# Depuis l'hôte
curl http://localhost:5000/api/health

# Depuis le conteneur dashboard
docker exec iptv-stats-dashboard curl http://iptv-manager:5000/api/health
```

Si tout fonctionne, vous devriez obtenir :
```json
{
  "status": "ok",
  "manager_initialized": true,
  "timestamp": 1234567890.123
}
```

## Tests

Pour tester la fonctionnalité :

1. Vérifiez que l'API est accessible :
   ```bash
   curl http://localhost:5000/api/health
   ```

2. Listez les chaînes disponibles :
   ```bash
   curl http://localhost:5000/api/channels
   ```

3. Relancez une chaîne spécifique :
   ```bash
   curl -X POST http://localhost:5000/api/channels/ma_chaine/restart
   ```

4. Vérifiez les logs pour confirmer la relance :
   ```bash
   docker-compose logs -f app
   ```

## Troubleshooting

### Le bouton ne répond pas
- Vérifiez que l'API Flask est bien démarrée sur le port 5000
- Consultez les logs du conteneur : `docker-compose logs app`
- Vérifiez la console du navigateur pour les erreurs JavaScript

### Erreur de connexion à l'API
- Assurez-vous que les conteneurs peuvent communiquer
- Vérifiez que le port 5000 est bien ouvert
- Le dashboard et l'API doivent être accessibles depuis le même réseau

### La chaîne ne redémarre pas
- Consultez les logs FFmpeg : `/app/logs/ffmpeg/`
- Vérifiez que des vidéos sont disponibles pour la chaîne
- Consultez le statut de la chaîne via l'API : `GET /api/channels/<nom>/status`

## Limitations connues

- La relance prend quelques secondes (1.5-3.0s de pause + temps de démarrage FFmpeg)
- Les viewers actuels peuvent subir une interruption temporaire du flux
- Si aucune vidéo n'est disponible, la relance échouera

## Améliorations futures possibles

1. **Confirmation de relance** : Ajouter une boîte de dialogue de confirmation
2. **Historique des relances** : Tracer l'historique des relances manuelles
3. **Relance de toutes les chaînes** : Bouton pour relancer toutes les chaînes en une fois
4. **Sélection de vidéo** : Permettre de choisir quelle vidéo lancer
5. **Notification persistante** : Garder l'historique des notifications
6. **WebSocket** : Utiliser WebSocket pour des mises à jour en temps réel

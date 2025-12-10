# Guide de Démarrage Rapide - Relance Manuelle

## 🚀 Pour activer la fonctionnalité

```bash
# 1. Arrêter les conteneurs
docker-compose down

# 2. Reconstruire les images
docker-compose build

# 3. Redémarrer
docker-compose up -d

# 4. Vérifier que l'API fonctionne
curl http://localhost:5000/api/health
```

## ✅ Utilisation

### Depuis le Dashboard Web

1. Ouvrez `http://<votre-serveur>:8050`
2. Scrollez jusqu'à "Statut des Chaînes en Direct"
3. Cliquez sur **🔄 Relancer** pour n'importe quelle chaîne
4. Une notification verte confirme la relance

### Via la ligne de commande

```bash
# Relancer une chaîne spécifique
curl -X POST http://localhost:5000/api/channels/ma_chaine/restart

# Voir toutes les chaînes disponibles
curl http://localhost:5000/api/channels
```

## 📝 Comportement

### Chaînes normales (sans series.txt)
- ✅ Sélectionne une **nouvelle vidéo aléatoire**
- ✅ Ne rejoue jamais la même vidéo immédiatement

### Chaînes en mode série (avec series.txt)
- ✅ Passe à la **vidéo suivante** dans l'ordre
- ✅ Retour au début après la dernière vidéo

## 🔍 Vérification

```bash
# Voir les logs de l'application
docker-compose logs -f iptv-manager | grep "Relance"

# Voir les logs du dashboard
docker-compose logs -f stats-dashboard
```

## ❌ Dépannage

### Le bouton ne répond pas
```bash
# Vérifier que l'API est accessible depuis le dashboard
docker exec iptv-stats-dashboard curl http://iptv-manager:5000/api/health
```

### L'API ne démarre pas
```bash
# Vérifier les logs
docker-compose logs iptv-manager | grep "API"

# Doit afficher: ✅ API des chaînes démarrée sur le port 5000
```

### Variables d'environnement manquantes
```bash
# Vérifier la configuration
docker-compose config | grep CHANNEL_API_URL

# Doit afficher: CHANNEL_API_URL=http://iptv-manager:5000
```

## 📊 Ports utilisés

- **5000** : API Flask (chaînes)
- **8050** : Dashboard Dash (stats)
- **80** : Nginx (streaming HLS)

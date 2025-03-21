#!/bin/bash

# Couleurs pour les messages
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}Démarrage des tests dans Docker...${NC}"

# Vérifier si le conteneur de test existe déjà
if [ "$(docker ps -aq -f name=streaming_tests)" ]; then
    echo "Suppression de l'ancien conteneur de test..."
    docker rm -f streaming_tests
fi

# Construire l'image de test si elle n'existe pas
if [ "$(docker images -q streaming_awesomeness_test)" ]; then
    echo "Image de test déjà existante"
else
    echo "Construction de l'image de test..."
    docker build -t streaming_awesomeness_test -f Dockerfile.test .
fi

# Exécuter les tests
echo "Exécution des tests..."
docker run --name streaming_tests \
    -v $(pwd):/app \
    streaming_awesomeness_test \
    pytest tests/ -v --cov=app --cov-report=term-missing

# Récupérer le code de retour
TEST_RESULT=$?

# Nettoyer le conteneur
echo "Nettoyage..."
docker rm -f streaming_tests

# Afficher le résultat
if [ $TEST_RESULT -eq 0 ]; then
    echo -e "${GREEN}Tests réussis !${NC}"
else
    echo -e "${RED}Tests échoués !${NC}"
    exit 1
fi 
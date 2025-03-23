#!/usr/bin/env python3
"""
Script pour nettoyer les logs avec des noms trop longs
À exécuter au démarrage du conteneur pour éviter les problèmes de rotation
"""

import os
import time
import logging
from pathlib import Path

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("cleanup_logs")

def cleanup_long_log_files(log_dir="/app/logs/ffmpeg", max_length=100):
    """Nettoie les fichiers de log avec des noms trop longs"""
    logger.info(f"Nettoyage des logs trop longs dans {log_dir}")
    log_path = Path(log_dir)
    
    if not log_path.exists():
        logger.info(f"Dossier {log_dir} n'existe pas, création")
        log_path.mkdir(parents=True, exist_ok=True)
        return
    
    # Obtenir la liste des fichiers log
    log_files = list(log_path.glob("*.log"))
    logger.info(f"Trouvé {len(log_files)} fichiers de log")
    
    renamed_count = 0
    deleted_count = 0
    
    for log_file in log_files:
        file_name = log_file.name
        
        # Si le nom est trop long
        if len(file_name) > max_length:
            logger.info(f"Nom trop long ({len(file_name)} chars): {file_name}")
            
            # Analyser le nom pour extraire les parties importantes
            parts = file_name.split("_")
            
            # Si on peut extraire le nom de la chaîne
            if len(parts) >= 2:
                # Extraire le nom de la chaîne et le type de log
                channel_name = parts[0]
                log_type = parts[1]
                
                # Obtenir un timestamp actuel
                timestamp = time.strftime("%Y%m%d_%H%M%S")
                
                # Nouveau nom simplifié
                new_name = f"{channel_name}_{log_type}_{timestamp}.log"
                new_path = log_file.parent / new_name
                
                try:
                    # Si le fichier est trop vieux ou inactif, on le supprime
                    file_age = time.time() - log_file.stat().st_mtime
                    if file_age > 7 * 24 * 60 * 60:  # Plus de 7 jours
                        logger.info(f"Suppression du vieux log: {file_name}")
                        log_file.unlink()
                        deleted_count += 1
                    else:
                        # Sinon, on le renomme
                        logger.info(f"Renommage: {file_name} → {new_name}")
                        log_file.rename(new_path)
                        renamed_count += 1
                except Exception as e:
                    logger.error(f"Erreur traitement {file_name}: {e}")
            else:
                # Si on ne peut pas analyser le nom, on supprime le fichier
                try:
                    logger.info(f"Suppression du log non parsable: {file_name}")
                    log_file.unlink()
                    deleted_count += 1
                except Exception as e:
                    logger.error(f"Erreur suppression {file_name}: {e}")
    
    logger.info(f"Nettoyage terminé: {renamed_count} renommés, {deleted_count} supprimés")

if __name__ == "__main__":
    cleanup_long_log_files() 
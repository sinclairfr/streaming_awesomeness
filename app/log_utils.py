"""
Module d'utilitaires pour le traitement des logs
"""
import re
from config import logger

def parse_access_log(line):
    """
    Parse une ligne de log nginx pour extraire les informations pertinentes
    
    Args:
        line (str): Ligne de log au format nginx
    
    Returns:
        tuple: (ip, channel, request_type, is_valid, path_or_user_agent)
            - ip: L'adresse IP du client
            - channel: Le nom de la chaîne
            - request_type: Le type de requête (playlist, segment, unknown)
            - is_valid: Booléen indiquant si la requête est valide
            - path_or_user_agent: Le chemin ou l'user agent selon le contexte d'appel
    """
    # Log détaillé pour déboguer le traitement des lignes
    logger.debug(f"PARSE_LOG: {line[:100]}...")
    
    # Si pas de /hls/ dans la ligne, on ignore direct
    if "/hls/" not in line:
        return None, None, None, False, None

    # On récupère l'utilisateur agent si possible
    user_agent = None
    user_agent_match = re.search(r'"([^"]*)"$', line)
    if user_agent_match:
        user_agent = user_agent_match.group(1)

    # Si pas un GET ou un HEAD, on ignore
    if not ("GET /hls/" in line or "HEAD /hls/" in line):
        return None, None, None, False, None

    # Extraction IP (en début de ligne)
    parts = line.split(" ")
    if len(parts) < 1:
        logger.warning("⚠️ Ligne de log invalide - pas assez de parties")
        return None, None, None, False, None

    ip = parts[0]
    
    # Validation plus stricte de l'IP avec une regex plus robuste
    ip_pattern = r'^(\d{1,3}\.){3}\d{1,3}$'
    if not re.match(ip_pattern, ip):
        logger.warning(f"⚠️ Format IP invalide: {ip}")
        return None, None, None, False, None
        
    # Vérification que chaque partie est un nombre valide
    try:
        ip_parts = ip.split('.')
        if not all(0 <= int(part) <= 255 for part in ip_parts):
            logger.warning(f"⚠️ Valeurs IP hors limites: {ip}")
            return None, None, None, False, None
    except ValueError:
        logger.warning(f"⚠️ IP contient des valeurs non numériques: {ip}")
        return None, None, None, False, None

    # Extraction du code HTTP
    status_code = "???"
    status_match = re.search(r'" (\d{3}) ', line)
    if status_match:
        status_code = status_match.group(1)

    # Extraction du canal spécifique
    channel = None
    request_type = None
    path = None

    # Récupérer le chemin complet
    path_match = re.search(r'"(?:GET|HEAD) ([^"]*) HTTP', line)
    if path_match:
        path = path_match.group(1)

    # Format attendu: /hls/CHANNEL/...
    channel_match = re.search(r'/hls/([^/]+)/', line)
    if channel_match:
        channel = channel_match.group(1)
        # Ne pas valider le nom de la chaîne comme une IP
        if re.match(ip_pattern, channel):
            logger.warning(f"⚠️ Nom de chaîne ressemble à une IP: {channel}")
            return None, None, None, False, None
            
        # Type de requête
        request_type = (
            "playlist"
            if ".m3u8" in line
            else "segment" if ".ts" in line else "unknown"
        )
        logger.debug(f"📋 Détecté accès {request_type} pour {channel} par {ip}")

    # Validité - note que 404 est valide pour le suivi même si le contenu n'existe pas
    is_valid = status_code in [
        "200",
        "206",
        "304",  # Ajouter le code 304 (Not Modified) pour les requêtes de mise en cache
        "404",
    ]
    
    # Log de debug pour les codes 304
    if status_code == "304" and is_valid:
        logger.debug(f"✅ Requête 304 validée: {channel} par {ip} - {request_type}")
    
    # Log détaillé du résultat du parsing
    logger.debug(f"PARSE_RESULT: ip={ip}, channel={channel}, type={request_type}, valid={is_valid}, code={status_code}")

    # Pour IPTVManager, on retourne le chemin, pour ClientMonitor, on retourne l'user agent
    return_value = path if path else user_agent
    
    return ip, channel, request_type, is_valid, return_value 
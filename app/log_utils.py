"""
Module d'utilitaires pour le traitement des logs
"""
import re
from config import logger

# Regexp compilées pour améliorer les performances 
IP_PATTERN = re.compile(r'^(\d{1,3}\.){3}\d{1,3}$')
USER_AGENT_PATTERN = re.compile(r'"([^"]*)"$')
STATUS_CODE_PATTERN = re.compile(r'" (\d{3}) ')
PATH_PATTERN = re.compile(r'"(?:GET|HEAD) ([^"]*) HTTP')
CHANNEL_PATTERN = re.compile(r'/hls/([^/]+)/')

def parse_access_log(line):
    """
    Parse une ligne de log nginx pour extraire les informations pertinentes
    VERSION ULTRA-SIMPLIFIÉE: ne traite QUE les segments réussis (HTTP 200)
    
    Args:
        line (str): Ligne de log au format nginx
    
    Returns:
        tuple: (ip, channel, request_type, is_valid, path, user_agent)
            - ip: L'adresse IP du client
            - channel: Le nom de la chaîne
            - request_type: Le type de requête (toujours "segment" dans cette version)
            - is_valid: Booléen indiquant si la requête est valide
            - path: Le chemin de la requête
            - user_agent: User agent du client
    """
    # On part du principe que seules les requêtes 200+segment arrivent ici
    # car le filtre initial a déjà été appliqué dans _process_line
    
    # Essayer d'extraire l'adresse IP (premier élément de la ligne)
    parts = line.split(" ")
    if len(parts) < 1:
        return None, None, None, False, None, None
    
    ip = parts[0]
    # Vérification rapide de l'IP
    if not IP_PATTERN.match(ip):
        return None, None, None, False, None, None
    
    # Extraction du chemin de la requête
    path_match = PATH_PATTERN.search(line)
    if not path_match:
        return None, None, None, False, None, None
    
    path = path_match.group(1)
    # Vérification que c'est bien un segment .ts
    if "segment" not in path or ".ts" not in path:
        return None, None, None, False, None, None
    
    # Extraction du canal
    channel_match = CHANNEL_PATTERN.search(line)
    if not channel_match:
        return None, None, None, False, None, None
    
    channel = channel_match.group(1)
    # Vérifier que le canal n'est pas une IP (sécurité)
    if IP_PATTERN.match(channel) or channel == "master_playlist":
        return None, None, None, False, None, None
    
    # Extraction du user-agent (optionnel)
    user_agent = None
    user_agent_match = USER_AGENT_PATTERN.search(line)
    if user_agent_match:
        user_agent = user_agent_match.group(1)
    
    # La ligne a passé tous les filtres : c'est un segment valide
    return ip, channel, "segment", True, path, user_agent 
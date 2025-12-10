#!/usr/bin/env python3
"""
API Flask pour gérer les actions sur les chaînes IPTV
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import threading
import time
from config import logger
import json
import os

app = Flask(__name__)
CORS(app)  # Permettre les requêtes CORS depuis le dashboard

# Référence globale vers le manager IPTV (sera définie au démarrage)
iptv_manager = None

def init_api(manager):
    """Initialise l'API avec une référence au manager IPTV"""
    global iptv_manager
    iptv_manager = manager
    logger.info("✅ API des chaînes initialisée")

@app.route('/api/channels', methods=['GET'])
def get_channels():
    """Retourne la liste de toutes les chaînes"""
    if not iptv_manager:
        return jsonify({"error": "Manager non initialisé"}), 503

    try:
        channels_info = {}
        for channel_name, channel in iptv_manager.channels.items():
            channels_info[channel_name] = {
                "name": channel_name,
                "is_ready": channel.is_ready_for_streaming(),
                "is_streaming": channel.process_manager.process is not None,
                "video_count": len(channel.processed_videos) if hasattr(channel, 'processed_videos') else 0
            }
        return jsonify({"channels": channels_info})
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des chaînes: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/channels/<channel_name>/restart', methods=['POST'])
def restart_channel(channel_name):
    """Relance une chaîne spécifique"""
    if not iptv_manager:
        return jsonify({"error": "Manager non initialisé"}), 503

    try:
        logger.info(f"🔄 Demande de relance manuelle pour la chaîne: {channel_name}")

        # Vérifier que la chaîne existe
        if channel_name not in iptv_manager.channels:
            logger.warning(f"⚠️ Chaîne '{channel_name}' non trouvée")
            return jsonify({"error": f"Chaîne '{channel_name}' non trouvée"}), 404

        channel = iptv_manager.channels[channel_name]

        # Exécuter le redémarrage dans un thread séparé pour ne pas bloquer l'API
        def restart_thread():
            try:
                logger.info(f"[{channel_name}] Début de la relance manuelle...")
                success = channel._restart_stream(diagnostic="Relance manuelle depuis le dashboard", reset_to_first=True)

                if success:
                    logger.info(f"[{channel_name}] ✅ Relance réussie")

                    # Forcer la mise à jour du statut de la chaîne
                    if hasattr(iptv_manager, 'channel_status'):
                        iptv_manager.channel_status.update_channel(
                            channel_name,
                            is_live=True,
                            viewers=0,
                            watchers=[],
                            force_save=True
                        )

                    # Forcer la mise à jour de la playlist maître
                    if hasattr(iptv_manager, '_update_master_playlist'):
                        iptv_manager._update_master_playlist()
                else:
                    logger.error(f"[{channel_name}] ❌ Échec de la relance")

            except Exception as e:
                logger.error(f"[{channel_name}] ❌ Erreur lors de la relance: {e}", exc_info=True)

        # Lancer le thread de redémarrage
        thread = threading.Thread(target=restart_thread, daemon=True)
        thread.start()

        return jsonify({
            "success": True,
            "message": f"Relance de la chaîne '{channel_name}' en cours...",
            "channel": channel_name
        })

    except Exception as e:
        logger.error(f"❌ Erreur lors de la relance de {channel_name}: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/api/channels/<channel_name>/status', methods=['GET'])
def get_channel_status(channel_name):
    """Retourne le statut détaillé d'une chaîne"""
    if not iptv_manager:
        return jsonify({"error": "Manager non initialisé"}), 503

    try:
        if channel_name not in iptv_manager.channels:
            return jsonify({"error": f"Chaîne '{channel_name}' non trouvée"}), 404

        channel = iptv_manager.channels[channel_name]

        status = {
            "name": channel_name,
            "is_ready": channel.is_ready_for_streaming(),
            "is_streaming": channel.process_manager.process is not None,
            "video_count": len(channel.processed_videos) if hasattr(channel, 'processed_videos') else 0,
            "current_video_index": channel.current_video_index if hasattr(channel, 'current_video_index') else None
        }

        return jsonify(status)

    except Exception as e:
        logger.error(f"Erreur lors de la récupération du statut de {channel_name}: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/channels/<channel_name>/next', methods=['POST'])
def next_video(channel_name):
    """Passe à la vidéo suivante d'une chaîne"""
    if not iptv_manager:
        return jsonify({"error": "Manager non initialisé"}), 503

    try:
        logger.info(f"🎬 Demande de passage à la vidéo suivante pour: {channel_name}")

        # Vérifier que la chaîne existe
        if channel_name not in iptv_manager.channels:
            logger.warning(f"⚠️ Chaîne '{channel_name}' non trouvée")
            return jsonify({"error": f"Chaîne '{channel_name}' non trouvée"}), 404

        channel = iptv_manager.channels[channel_name]

        # Exécuter la navigation dans un thread séparé
        def navigate_thread():
            try:
                logger.info(f"[{channel_name}] ➡️ Navigation vers la vidéo suivante...")

                # Calculer l'index suivant (navigation circulaire)
                with channel.lock:
                    if not channel.processed_videos:
                        logger.warning(f"[{channel_name}] ⚠️ Aucune vidéo disponible")
                        return

                    num_videos = len(channel.processed_videos)
                    next_index = (channel.current_video_index + 1) % num_videos
                    channel.current_video_index = next_index
                    logger.info(f"[{channel_name}] 📹 Index mis à jour: {next_index}/{num_videos}")

                    # Activer le flag de navigation manuelle
                    channel.manual_navigation = True

                # Redémarrer avec la nouvelle vidéo
                success = channel._restart_stream(diagnostic="Navigation manuelle vers vidéo suivante")

                if success:
                    logger.info(f"[{channel_name}] ✅ Navigation réussie vers vidéo suivante")
                else:
                    logger.error(f"[{channel_name}] ❌ Échec de la navigation")

            except Exception as e:
                logger.error(f"[{channel_name}] ❌ Erreur lors de la navigation: {e}", exc_info=True)

        # Lancer le thread de navigation
        thread = threading.Thread(target=navigate_thread, daemon=True)
        thread.start()

        return jsonify({
            "success": True,
            "message": f"Navigation vers la vidéo suivante de '{channel_name}' en cours...",
            "channel": channel_name
        })

    except Exception as e:
        logger.error(f"❌ Erreur lors de la navigation de {channel_name}: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/api/channels/<channel_name>/previous', methods=['POST'])
def previous_video(channel_name):
    """Revient à la vidéo précédente d'une chaîne"""
    if not iptv_manager:
        return jsonify({"error": "Manager non initialisé"}), 503

    try:
        logger.info(f"🎬 Demande de retour à la vidéo précédente pour: {channel_name}")

        # Vérifier que la chaîne existe
        if channel_name not in iptv_manager.channels:
            logger.warning(f"⚠️ Chaîne '{channel_name}' non trouvée")
            return jsonify({"error": f"Chaîne '{channel_name}' non trouvée"}), 404

        channel = iptv_manager.channels[channel_name]

        # Exécuter la navigation dans un thread séparé
        def navigate_thread():
            try:
                logger.info(f"[{channel_name}] ⬅️ Navigation vers la vidéo précédente...")

                # Calculer l'index précédent (navigation circulaire)
                with channel.lock:
                    if not channel.processed_videos:
                        logger.warning(f"[{channel_name}] ⚠️ Aucune vidéo disponible")
                        return

                    num_videos = len(channel.processed_videos)
                    prev_index = (channel.current_video_index - 1) % num_videos
                    channel.current_video_index = prev_index
                    logger.info(f"[{channel_name}] 📹 Index mis à jour: {prev_index}/{num_videos}")

                    # Activer le flag de navigation manuelle
                    channel.manual_navigation = True

                # Redémarrer avec la nouvelle vidéo
                success = channel._restart_stream(diagnostic="Navigation manuelle vers vidéo précédente")

                if success:
                    logger.info(f"[{channel_name}] ✅ Navigation réussie vers vidéo précédente")
                else:
                    logger.error(f"[{channel_name}] ❌ Échec de la navigation")

            except Exception as e:
                logger.error(f"[{channel_name}] ❌ Erreur lors de la navigation: {e}", exc_info=True)

        # Lancer le thread de navigation
        thread = threading.Thread(target=navigate_thread, daemon=True)
        thread.start()

        return jsonify({
            "success": True,
            "message": f"Navigation vers la vidéo précédente de '{channel_name}' en cours...",
            "channel": channel_name
        })

    except Exception as e:
        logger.error(f"❌ Erreur lors de la navigation de {channel_name}: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    """Endpoint de vérification de santé de l'API"""
    return jsonify({
        "status": "ok",
        "manager_initialized": iptv_manager is not None,
        "timestamp": time.time()
    })

def run_api(host='0.0.0.0', port=5000):
    """Lance le serveur API Flask"""
    logger.info(f"🚀 Démarrage de l'API des chaînes sur {host}:{port}")
    app.run(host=host, port=port, debug=False, threaded=True)

def start_api_server(manager, host='0.0.0.0', port=5000):
    """Lance l'API dans un thread séparé"""
    init_api(manager)
    api_thread = threading.Thread(
        target=run_api,
        args=(host, port),
        daemon=True
    )
    api_thread.start()
    logger.info(f"✅ Serveur API démarré en arrière-plan sur {host}:{port}")
    return api_thread

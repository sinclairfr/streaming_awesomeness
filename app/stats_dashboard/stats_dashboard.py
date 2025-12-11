import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State, ALL
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import json
import os
from datetime import datetime, timedelta
import numpy as np
import time
import requests

# Chemin vers les fichiers de statistiques
STATS_DIR = "/app/stats"
USER_STATS_FILE = os.path.join(STATS_DIR, "user_stats_bytes.json")
CHANNEL_STATS_FILE = os.path.join(STATS_DIR, "channel_stats_bytes.json")
CHANNELS_STATUS_FILE = os.path.join(STATS_DIR, "channels_status.json")

# URL de l'API des chaînes (depuis variable d'environnement ou localhost par défaut)
CHANNEL_API_URL = os.getenv("CHANNEL_API_URL", "http://localhost:5000")

def load_stats():
    """Charge les données de statistiques depuis les fichiers bytes"""
    try:
        # Load channel stats
        with open(CHANNEL_STATS_FILE, 'r') as f:
            channel_stats = json.load(f)
            
        # Load user stats
        with open(USER_STATS_FILE, 'r') as f:
            user_stats = json.load(f)
            
        print(f"Stats loaded: {len(channel_stats.get('global', {}).get('unique_viewers', []))} users, {len(channel_stats) -1} channels")
        return channel_stats, user_stats
    except Exception as e:
        print(f"Error loading stats: {e}")
        # Return empty stats
        channel_stats = {"global": {"total_watch_time": 0, "total_bytes_transferred": 0, "unique_viewers": [], "last_update": 0}}
        user_stats = {"users": {}, "last_updated": 0}
        return channel_stats, user_stats

def create_user_activity_df(user_stats):
    """Crée un DataFrame des activités utilisateur avec meilleure détection des chaînes favorites"""
    data = []
    
    for ip, user_data in user_stats.get("users", {}).items():
        # Statistiques globales de l'utilisateur
        total_time = user_data.get("total_watch_time", 0)
        
        # Détecter la chaîne favorite (plus complexe pour gérer différentes structures)
        favorite_channel = None
        max_time = 0
        
        # Si channels est disponible, parcourir chaque chaîne
        if "channels" in user_data and isinstance(user_data["channels"], dict):
            # Trouver la chaîne avec le plus de temps de visionnage
            for channel, channel_data in user_data["channels"].items():
                channel_time = channel_data.get("total_watch_time", 0)
                if channel_time > max_time:
                    max_time = channel_time
                    favorite_channel = channel
            
            # Ajout des données par chaîne
            for channel, channel_data in user_data["channels"].items():
                is_favorite = (channel == favorite_channel)
                data.append({
                    "ip": ip,
                    "channel": channel,
                    "watch_time": channel_data.get("total_watch_time", 0),
                    "favorite": is_favorite,
                    "last_seen": datetime.fromtimestamp(channel_data.get("last_seen", 0)),
                })
    
    return pd.DataFrame(data)

def create_channel_stats_df(channel_stats, channel_status_data=None):
    """Crée un DataFrame des statistiques par chaîne, incluant toutes les chaînes configurées"""
    data = []
    processed_channels = set()

    # Parcourir toutes les chaînes sauf "global"
    for channel, stats in channel_stats.items():
        if channel == "global":
            continue

        viewers = len(stats.get("unique_viewers", []))
        watch_time = stats.get("total_watch_time", 0)
        data.append({
            "channel": channel,
            "viewers": viewers,
            "watch_time": watch_time,
            "last_update": datetime.fromtimestamp(stats.get("last_update", 0))
        })
        processed_channels.add(channel)

    # Ajouter les chaînes configurées mais sans viewers
    if channel_status_data:
        for channel_name in channel_status_data.keys():
            if channel_name not in processed_channels:
                data.append({
                    "channel": channel_name,
                    "viewers": 0,
                    "watch_time": 0,
                    "last_update": datetime.fromtimestamp(0)
                })

    return pd.DataFrame(data)

def format_time(seconds):
    """Formate le temps en heures:minutes:secondes"""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = int(seconds % 60)
    return f"{hours:02d}h {minutes:02d}m {seconds:02d}s"

def create_channels_grid(channel_status_data):
    """Creates a grid of channel cards with status indicators and restart buttons"""

    # Process channel data
    current_time = time.time()
    channels_data = []

    # Itérer directement sur les données de statut des chaînes
    for channel, stats in channel_status_data.items():
        is_active = stats.get("is_live", False)
        is_streaming = stats.get("streaming", False)
        viewers = stats.get("viewers", 0)
        current_video = stats.get("current_video", None)

        # Utiliser last_updated qui est maintenant une chaîne ISO 8601
        last_updated_str = stats.get("last_updated")
        if last_updated_str:
            try:
                # Convertir la chaîne ISO 8601 en objet datetime
                last_seen_dt = datetime.fromisoformat(last_updated_str)
                last_seen = last_seen_dt.strftime('%H:%M:%S')
            except (ValueError, TypeError):
                last_seen = "N/A"
        else:
            last_seen = "N/A"

        watchers = stats.get("watchers", [])

        channels_data.append({
            "name": channel,
            "active": is_active, # Simplifié: is_live est la source de vérité
            "streaming": is_streaming,
            "viewers": viewers,
            "last_seen": last_seen,
            "watchers": watchers,
            "current_video": current_video
        })

    # Sort channels: active ones first, then alphabetically
    channels_data.sort(key=lambda x: (not x["active"], x["name"].lower()))

    # Create grid of cards
    cards = []
    for ch in channels_data:
        # Déterminer la couleur du badge en fonction du statut
        if ch["viewers"] > 0:
            status_color = "#2ecc71"  # Vert pour actif avec viewers
            status_text = "ACTIVE"
        elif ch["active"]:
            status_color = "#f1c40f"  # Jaune pour actif sans viewers
            status_text = "NO VIEWERS"
        else:
            status_color = "#e74c3c"  # Rouge pour inactif
            status_text = "INACTIVE"

        # Afficher le titre de la vidéo en cours avec effet de défilement si trop long
        video_title_element = html.Div("En attente...", className="video-title")
        if ch.get("current_video"):
            video_title = ch["current_video"]
            # Si le titre est trop long (plus de 30 caractères), activer le défilement
            if len(video_title) > 30:
                video_title_element = html.Div([
                    html.Div(video_title, className="video-title scrolling-title")
                ], className="video-title-container")
            else:
                video_title_element = html.Div(video_title, className="video-title")

        card = html.Div([
            html.Div([
                html.Div(status_text, className="channel-status-badge",
                         style={"backgroundColor": status_color}),
                html.H3(ch["name"], className="channel-card-title"),
                video_title_element,
                html.Div([
                    html.P(f"{ch['viewers']} {'viewers' if ch['viewers'] != 1 else 'viewer'}",
                           className="channel-card-stat"),
                    html.P(f"Last activity: {ch['last_seen']}",
                           className="channel-card-stat"),
                    html.P(f"Viewers: {', '.join(ch['watchers'])}" if ch['watchers'] else "Viewers: None",
                           className="channel-card-stat watchers-list")
                ], className="channel-card-stats"),
                html.Div([
                    html.Button(
                        "⬅️",
                        id={"type": "prev-btn", "channel": ch["name"]},
                        className="nav-button nav-button-prev",
                        n_clicks=0,
                        title="Vidéo précédente"
                    ),
                    html.Button(
                        "🔄 Relancer",
                        id={"type": "restart-btn", "channel": ch["name"]},
                        className="restart-button",
                        n_clicks=0
                    ),
                    html.Button(
                        "➡️",
                        id={"type": "next-btn", "channel": ch["name"]},
                        className="nav-button nav-button-next",
                        n_clicks=0,
                        title="Vidéo suivante"
                    ),
                ], className="button-group")
            ], className="channel-card-content")
        ], className="channel-card")

        cards.append(card)

    return html.Div(cards, className="channel-grid")

# Initialiser l'application Dash
app = dash.Dash(__name__,
                title="IPTV Stats Dashboard",
                suppress_callback_exceptions=True,
                meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}])

# Layout de l'application
app.layout = html.Div([
    html.H1("IPTV Streaming Service - Dashboard de Statistiques", className="header-title"),

    # Zone de notification
    html.Div(id="notification-container", className="notification-container"),

    # Onglets pour naviguer entre les vues
    dcc.Tabs(id="tabs", value='tab-stats', children=[
        dcc.Tab(label='📊 Statistiques', value='tab-stats', className='custom-tab', selected_className='custom-tab--selected'),
        dcc.Tab(label='📺 Guide EPG', value='tab-epg', className='custom-tab', selected_className='custom-tab--selected'),
    ]),

    html.Div(id='tabs-content'),

    dcc.Interval(
        id='interval-component',
        interval=2*1000,  # 2 secondes en millisecondes
        n_intervals=0
    )
], className="dashboard-container")

# Contenu de l'onglet Statistiques
def render_stats_tab():
    return html.Div([
        html.Div([
            html.Div([
                html.H2("Statistiques Globales"),
                html.Div(id="global-stats", className="stats-container")
            ], className="six columns"),

            html.Div([
                html.H2("Activité par Chaîne"),
                dcc.Graph(id="channel-pie-chart")
            ], className="six columns"),
        ], className="row"),

        html.Div([
            html.Div([
                html.H2("Temps de Visionnage par Chaîne"),
                dcc.Graph(id="channel-bar-chart")
            ], className="six columns"),

            html.Div([
                html.H2("Activité des Utilisateurs"),
                dcc.Graph(id="user-activity-chart")
            ], className="six columns"),
        ], className="row"),

        html.Div([
            html.Div([
                html.H2("Détails par Utilisateur"),
                html.Div(id="user-details", className="stats-container")
            ], className="twelve columns")
        ], className="row"),

        html.Div([
            html.Div([
                html.H2("Statut des Chaînes en Direct"),
                html.Div(id="live-channels-container", className="live-channels-container")
            ], className="twelve columns")
        ], className="row"),
    ])

# Contenu de l'onglet EPG
def render_epg_tab():
    return html.Div([
        html.H2("Guide des Programmes (EPG)", style={"textAlign": "center", "marginBottom": "20px"}),
        html.Div(id="epg-timeline", className="epg-container")
    ])

# Callback pour changer d'onglet
@app.callback(
    Output('tabs-content', 'children'),
    Input('tabs', 'value')
)
def render_content(tab):
    if tab == 'tab-stats':
        return render_stats_tab()
    elif tab == 'tab-epg':
        return render_epg_tab()

# Fonction pour créer la vue EPG en frise temporelle
def create_epg_timeline(channel_status_data):
    """Crée une vue en frise temporelle des chaînes et de leurs programmes"""
    epg_rows = []

    # Trier les chaînes par nom
    sorted_channels = sorted(channel_status_data.items(), key=lambda x: x[0])

    for channel_name, stats in sorted_channels:
        is_active = stats.get("is_live", False)
        current_video = stats.get("current_video", "Aucun programme")
        viewers = stats.get("viewers", 0)

        # Couleur selon le statut
        if viewers > 0:
            row_color = "#d5f4e6"  # Vert clair
            status_icon = "🟢"
        elif is_active:
            row_color = "#fff3cd"  # Jaune clair
            status_icon = "🟡"
        else:
            row_color = "#f8d7da"  # Rouge clair
            status_icon = "🔴"

        # Créer la ligne EPG
        epg_row = html.Div([
            html.Div([
                html.Span(status_icon, style={"marginRight": "8px"}),
                html.Strong(channel_name, style={"fontSize": "16px"}),
                html.Span(f" ({viewers} viewer{'s' if viewers != 1 else ''})",
                         style={"fontSize": "12px", "color": "#7f8c8d", "marginLeft": "10px"})
            ], className="epg-channel-name"),
            html.Div([
                html.Div([
                    html.Span("▶", style={"marginRight": "8px", "color": "#e74c3c"}),
                    html.Span(current_video, style={"fontSize": "14px", "fontWeight": "500"})
                ], className="epg-program-info")
            ], className="epg-program-bar")
        ], className="epg-row", style={"backgroundColor": row_color})

        epg_rows.append(epg_row)

    return html.Div(epg_rows, className="epg-timeline-container")

# Callback pour mettre à jour l'EPG
@app.callback(
    Output("epg-timeline", "children"),
    Input("interval-component", "n_intervals")
)
def update_epg(n):
    try:
        with open(CHANNELS_STATUS_FILE, 'r') as f:
            live_status = json.load(f)
        live_channels_data = live_status.get('channels', {})
        return create_epg_timeline(live_channels_data)
    except Exception as e:
        return html.Div(f"Erreur lors du chargement de l'EPG: {str(e)}",
                       style={"textAlign": "center", "color": "#e74c3c", "padding": "20px"})

# Callback pour mettre à jour tous les graphiques
@app.callback(
    [Output("global-stats", "children"),
     Output("channel-pie-chart", "figure"),
     Output("channel-bar-chart", "figure"),
     Output("user-activity-chart", "figure"),
     Output("user-details", "children"),
     Output("live-channels-container", "children")],
    [Input("interval-component", "n_intervals")]
)
def update_dashboard(n):
    # Charger les données de statistiques et de statut en une seule fois
    channel_stats, user_stats = load_stats()
    try:
        with open(CHANNELS_STATUS_FILE, 'r') as f:
            live_status = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        live_status = {"channels": {}}

    live_channels_data = live_status.get('channels', {})
    channel_df = create_channel_stats_df(channel_stats, live_channels_data)
    user_df = create_user_activity_df(user_stats)
    
    # 1. Statistiques globales (utilisant live_status)
    total_watch_time = channel_stats.get("global", {}).get("total_watch_time", 0)
    unique_users = len(channel_stats.get("global", {}).get("unique_viewers", []))
    total_channels = sum(1 for ch_data in live_channels_data.values() if ch_data.get('is_live'))

    global_stats = html.Div([
        html.Div([
            html.H3(format_time(total_watch_time)),
            html.P("Temps Total de Visionnage")
        ], className="stat-box"),
        html.Div([
            html.H3(str(unique_users)),
            html.P("Utilisateurs Uniques")
        ], className="stat-box"),
        html.Div([
            html.H3(str(total_channels)),
            html.P("Chaînes Actives")
        ], className="stat-box"),
    ])
    
    # 2. Graphique en secteurs par chaîne
    if not channel_df.empty:
        pie_fig = px.pie(
            channel_df,
            values="watch_time",
            names="channel",
            title="Répartition du Temps de Visionnage",
            color_discrete_sequence=px.colors.qualitative.Plotly
        )
        pie_fig.update_traces(textposition='inside', textinfo='percent+label')
    else:
        pie_fig = go.Figure().add_annotation(text="Aucune donnée disponible", showarrow=False)
    
    # 3. Graphique à barres du temps par chaîne
    if not channel_df.empty:
        channel_df["formatted_time"] = channel_df["watch_time"].apply(format_time)
        bar_fig = px.bar(
            channel_df,
            x="channel",
            y="watch_time",
            text="formatted_time",
            labels={"watch_time": "Temps de Visionnage (s)", "channel": "Chaîne"},
            color="channel",
            color_discrete_sequence=px.colors.qualitative.Plotly
        )
        bar_fig.update_traces(textposition='outside')
    else:
        bar_fig = go.Figure().add_annotation(text="Aucune donnée disponible", showarrow=False)
    
    # 4. Graphique d'activité utilisateur
    if not user_df.empty:
        user_df["formatted_time"] = user_df["watch_time"].apply(format_time)
        user_fig = px.bar(
            user_df,
            x="ip",
            y="watch_time",
            color="channel",
            barmode="stack",
            labels={"watch_time": "Temps de Visionnage (s)", "ip": "Adresse IP"},
            hover_data=["formatted_time", "favorite"]
        )
    else:
        user_fig = go.Figure().add_annotation(text="Aucune donnée disponible", showarrow=False)
    
    # 5. Détails par utilisateur
    user_details_list = []
    for ip, user_data in user_stats.get("users", {}).items():
        total_time = user_data.get("total_watch_time", 0)
        last_seen_ts = user_data.get("last_seen", 0)
        last_seen_dt = datetime.fromtimestamp(last_seen_ts) if last_seen_ts else "N/A"
        
        favorite_channel = "Inconnue"
        max_time = 0
        if "channels" in user_data:
            for channel, data in user_data["channels"].items():
                channel_time = data.get("total_watch_time", 0)
                if channel_time > max_time:
                    max_time = channel_time
                    favorite_channel = channel
        
        user_details_list.append(html.Div([
            html.H3(f"Utilisateur: {ip}"),
            html.P(f"Temps total: {format_time(total_time)}"),
            html.P(f"Dernière activité: {last_seen_dt.strftime('%Y-%m-%d %H:%M:%S') if last_seen_dt != 'N/A' else 'N/A'}"),
            html.P(f"Chaîne favorite: {favorite_channel}")
        ], className="user-detail-box"))
    
    user_details_container = user_details_list if user_details_list else [html.P("Aucune donnée utilisateur disponible")]

    # 6. Grille des chaînes en direct (logique de l'ancien callback)
    live_channels_grid = create_channels_grid(live_channels_data)

    return global_stats, pie_fig, bar_fig, user_fig, user_details_container, live_channels_grid

# Callback pour gérer les clics sur les boutons (relance, précédent, suivant)
@app.callback(
    Output("notification-container", "children"),
    [Input({"type": "restart-btn", "channel": ALL}, "n_clicks"),
     Input({"type": "prev-btn", "channel": ALL}, "n_clicks"),
     Input({"type": "next-btn", "channel": ALL}, "n_clicks")],
    [State({"type": "restart-btn", "channel": ALL}, "id"),
     State({"type": "prev-btn", "channel": ALL}, "id"),
     State({"type": "next-btn", "channel": ALL}, "id")],
    prevent_initial_call=True
)
def handle_channel_buttons(restart_clicks, prev_clicks, next_clicks, restart_ids, prev_ids, next_ids):
    """Gère les clics sur les boutons de relance et de navigation des chaînes"""
    # Identifier quel bouton a été cliqué
    ctx = dash.callback_context

    if not ctx.triggered:
        return html.Div()

    # Récupérer l'ID du bouton qui a déclenché le callback
    triggered_prop = ctx.triggered[0]["prop_id"]

    # Déterminer le type de bouton et le nom de la chaîne
    channel_name = None
    action = None
    api_endpoint = None
    action_message = None

    # Parser l'ID pour extraire le type et la chaîne
    if "restart-btn" in triggered_prop:
        for idx, n_clicks in enumerate(restart_clicks):
            if n_clicks and n_clicks > 0:
                channel_name = restart_ids[idx]["channel"]
                action = "restart"
                api_endpoint = f"{CHANNEL_API_URL}/api/channels/{channel_name}/restart"
                action_message = "Relance"
                break
    elif "prev-btn" in triggered_prop:
        for idx, n_clicks in enumerate(prev_clicks):
            if n_clicks and n_clicks > 0:
                channel_name = prev_ids[idx]["channel"]
                action = "previous"
                api_endpoint = f"{CHANNEL_API_URL}/api/channels/{channel_name}/previous"
                action_message = "Navigation vers vidéo précédente"
                break
    elif "next-btn" in triggered_prop:
        for idx, n_clicks in enumerate(next_clicks):
            if n_clicks and n_clicks > 0:
                channel_name = next_ids[idx]["channel"]
                action = "next"
                api_endpoint = f"{CHANNEL_API_URL}/api/channels/{channel_name}/next"
                action_message = "Navigation vers vidéo suivante"
                break

    if channel_name and api_endpoint:
        # Appeler l'API
        try:
            response = requests.post(api_endpoint, timeout=5)

            if response.status_code == 200:
                message = f"✅ {action_message} de '{channel_name}' en cours..."
                color = "#2ecc71"
            else:
                message = f"❌ Erreur lors de {action_message.lower()} de '{channel_name}'"
                color = "#e74c3c"

        except Exception as e:
            message = f"❌ Erreur de connexion à l'API: {str(e)}"
            color = "#e74c3c"

        # Retourner une notification
        return html.Div(
            message,
            className="notification",
            style={
                "backgroundColor": color,
                "color": "white",
                "padding": "15px",
                "borderRadius": "5px",
                "marginBottom": "20px",
                "textAlign": "center",
                "fontWeight": "bold"
            }
        )

    return html.Div()

# CSS pour le dashboard
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        {%css%}
        <style>
            body {
                font-family: 'Arial', sans-serif;
                margin: 0;
                padding: 0;
                background-color: #f5f5f5;
            }
            .dashboard-container {
                max-width: 1200px;
                margin: 0 auto;
                padding: 20px;
            }
            .header-title {
                text-align: center;
                color: #2c3e50;
                margin-bottom: 30px;
                border-bottom: 2px solid #3498db;
                padding-bottom: 10px;
            }
            .row {
                display: flex;
                flex-wrap: wrap;
                margin-bottom: 20px;
            }
            .six.columns {
                flex: 0 0 50%;
                max-width: 50%;
                padding: 10px;
                box-sizing: border-box;
            }
            .twelve.columns {
                flex: 0 0 100%;
                max-width: 100%;
                padding: 10px;
                box-sizing: border-box;
            }
            .stats-container {
                display: flex;
                justify-content: space-around;
                flex-wrap: wrap;
            }
            .stat-box {
                background-color: #fff;
                border-radius: 5px;
                box-shadow: 0 2px 5px rgba(0,0,0,0.1);
                padding: 15px;
                margin: 10px;
                min-width: 150px;
                text-align: center;
            }
            .stat-box h3 {
                margin: 0;
                color: #3498db;
                font-size: 24px;
            }
            .stat-box p {
                margin: 5px 0 0;
                color: #7f8c8d;
            }
            .user-detail-box {
                background-color: #fff;
                border-radius: 5px;
                box-shadow: 0 2px 5px rgba(0,0,0,0.1);
                padding: 15px;
                margin: 10px;
                width: calc(33.33% - 20px);
            }
            .user-detail-box h3 {
                margin: 0 0 10px;
                color: #2c3e50;
                font-size: 18px;
                border-bottom: 1px solid #eee;
                padding-bottom: 5px;
            }
            .user-detail-box p {
                margin: 5px 0;
                color: #7f8c8d;
            }
            .live-channels-container {
                background-color: #fff;
                border-radius: 5px;
                box-shadow: 0 2px 5px rgba(0,0,0,0.1);
                padding: 15px;
                overflow-x: auto;
            }
            .channel-grid {
                display: grid;
                grid-template-columns: repeat(auto-fill, minmax(250px, 1fr));
                gap: 15px;
            }
            .channel-card {
                background-color: #fff;
                border-radius: 5px;
                box-shadow: 0 2px 5px rgba(0,0,0,0.1);
                overflow: hidden;
                transition: transform 0.2s, box-shadow 0.2s;
            }
            .channel-card:hover {
                transform: translateY(-5px);
                box-shadow: 0 5px 15px rgba(0,0,0,0.1);
            }
            .channel-card-content {
                padding: 15px;
                position: relative;
            }
            .channel-status-badge {
                position: absolute;
                top: 10px;
                right: 10px;
                padding: 5px 10px;
                border-radius: 3px;
                font-size: 12px;
                font-weight: bold;
                color: white;
            }
            .channel-card-title {
                margin-top: 5px;
                margin-bottom: 15px;
                color: #2c3e50;
                font-size: 18px;
                font-weight: bold;
            }
            .channel-card-stats {
                margin-top: 10px;
            }
            .channel-card-stat {
                margin: 5px 0;
                color: #7f8c8d;
                font-size: 14px;
            }
            .watchers-list {
                font-size: 12px;
                color: #3498db;
                word-break: break-all;
            }
            .video-title {
                margin: 10px 0;
                font-size: 13px;
                color: #2c3e50;
                font-weight: 500;
                padding: 8px;
                background-color: #ecf0f1;
                border-radius: 4px;
                text-align: center;
            }
            .video-title-container {
                margin: 10px 0;
                overflow: hidden;
                position: relative;
                background-color: #ecf0f1;
                border-radius: 4px;
                padding: 8px 0;
                height: 30px;
            }
            .scrolling-title {
                display: inline-block;
                white-space: nowrap;
                animation: scroll-left 15s linear infinite;
                font-size: 13px;
                color: #2c3e50;
                font-weight: 500;
                padding-left: 100%;
            }
            @keyframes scroll-left {
                0% {
                    transform: translateX(0);
                }
                100% {
                    transform: translateX(-100%);
                }
            }
            .scrolling-title:hover {
                animation-play-state: paused;
            }
            .custom-tab {
                padding: 12px 24px !important;
                border: none !important;
                background-color: #ecf0f1 !important;
                color: #7f8c8d !important;
                font-weight: 500 !important;
                transition: all 0.3s ease !important;
            }
            .custom-tab--selected {
                background-color: #3498db !important;
                color: white !important;
                border-bottom: 3px solid #2980b9 !important;
            }
            .custom-tab:hover {
                background-color: #d5dbdb !important;
            }
            .epg-container {
                background-color: #fff;
                border-radius: 8px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.1);
                padding: 20px;
                margin-top: 20px;
            }
            .epg-timeline-container {
                display: flex;
                flex-direction: column;
                gap: 10px;
            }
            .epg-row {
                display: flex;
                align-items: center;
                padding: 15px;
                border-radius: 6px;
                border-left: 4px solid #3498db;
                transition: transform 0.2s, box-shadow 0.2s;
                min-height: 60px;
            }
            .epg-row:hover {
                transform: translateX(5px);
                box-shadow: 0 4px 12px rgba(0,0,0,0.15);
            }
            .epg-channel-name {
                min-width: 250px;
                flex-shrink: 0;
                padding-right: 20px;
                border-right: 2px solid #ddd;
            }
            .epg-program-bar {
                flex: 1;
                padding-left: 20px;
                display: flex;
                align-items: center;
            }
            .epg-program-info {
                display: flex;
                align-items: center;
                padding: 8px 16px;
                background-color: rgba(255,255,255,0.7);
                border-radius: 4px;
                border: 1px solid #ddd;
            }
            .button-group {
                display: flex;
                gap: 8px;
                margin-top: 10px;
                align-items: stretch;
            }
            .restart-button {
                background-color: #3498db;
                color: white;
                border: none;
                border-radius: 5px;
                padding: 10px 15px;
                cursor: pointer;
                font-size: 14px;
                font-weight: bold;
                flex: 1;
                transition: background-color 0.3s, transform 0.2s;
            }
            .restart-button:hover {
                background-color: #2980b9;
                transform: scale(1.05);
            }
            .restart-button:active {
                transform: scale(0.95);
            }
            .nav-button {
                background-color: #95a5a6;
                color: white;
                border: none;
                border-radius: 5px;
                padding: 10px;
                cursor: pointer;
                font-size: 16px;
                font-weight: bold;
                min-width: 45px;
                transition: background-color 0.3s, transform 0.2s;
            }
            .nav-button:hover {
                background-color: #7f8c8d;
                transform: scale(1.1);
            }
            .nav-button:active {
                transform: scale(0.9);
            }
            .nav-button-prev {
                background-color: #e67e22;
            }
            .nav-button-prev:hover {
                background-color: #d35400;
            }
            .nav-button-next {
                background-color: #27ae60;
            }
            .nav-button-next:hover {
                background-color: #229954;
            }
            .notification-container {
                position: fixed;
                top: 80px;
                left: 50%;
                transform: translateX(-50%);
                z-index: 1000;
                min-width: 300px;
                max-width: 600px;
            }
            .notification {
                animation: slideDown 0.3s ease-out;
            }
            @keyframes slideDown {
                from {
                    opacity: 0;
                    transform: translateY(-20px);
                }
                to {
                    opacity: 1;
                    transform: translateY(0);
                }
            }
            @media (max-width: 768px) {
                .six.columns {
                    flex: 0 0 100%;
                    max-width: 100%;
                }
                .user-detail-box {
                    width: calc(50% - 20px);
                }
                .notification-container {
                    top: 60px;
                    min-width: 90%;
                }
                .epg-row {
                    flex-direction: column;
                    align-items: flex-start;
                }
                .epg-channel-name {
                    min-width: 100%;
                    border-right: none;
                    border-bottom: 2px solid #ddd;
                    padding-bottom: 10px;
                    margin-bottom: 10px;
                }
                .epg-program-bar {
                    padding-left: 0;
                    width: 100%;
                }
            }
            @media (max-width: 480px) {
                .user-detail-box {
                    width: calc(100% - 20px);
                }
                .custom-tab {
                    padding: 8px 16px !important;
                    font-size: 14px !important;
                }
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

if __name__ == "__main__":
    app.run_server(debug=True, host="0.0.0.0", port=8050)
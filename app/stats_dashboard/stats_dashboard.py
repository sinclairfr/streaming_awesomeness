import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import json
import os
from datetime import datetime, timedelta
import numpy as np
import time

# Chemin vers les fichiers de statistiques
STATS_DIR = "/app/stats"
USER_STATS_FILE = os.path.join(STATS_DIR, "user_stats.json")
CHANNEL_STATS_FILE = os.path.join(STATS_DIR, "channel_stats.json")
CHANNELS_STATUS_FILE = os.path.join(STATS_DIR, "channels_status.json")

def load_stats():
    """Charge et nettoie les données de statistiques"""
    # Charger les statistiques des chaînes
    with open(CHANNEL_STATS_FILE, 'r') as f:
        channel_stats = json.load(f)
    
    # Charger les statistiques utilisateurs
    with open(USER_STATS_FILE, 'r') as f:
        user_stats = json.load(f)
    
    # Fonction récursive pour extraire tous les utilisateurs
    def extract_users(data, result=None):
        if result is None:
            result = {}
        
        if not isinstance(data, dict):
            return result
            
        for key, value in data.items():
            if key != "last_updated" and isinstance(value, dict):
                if "total_watch_time" in value:
                    # Ce semble être une entrée utilisateur
                    result[key] = value
                else:
                    # Récursion sur la structure imbriquée
                    extract_users(value, result)
        
        return result
    
    # Extraire tous les utilisateurs réels quelle que soit la profondeur d'imbrication
    all_users = {}
    
    if "users" in user_stats:
        all_users = extract_users(user_stats["users"], {})
    
    # Créer une structure correcte
    cleaned_user_stats = {
        "users": all_users,
        "last_updated": user_stats.get("last_updated", 0)
    }
    
    return channel_stats, cleaned_user_stats
def create_user_activity_df(user_stats):
    """Crée un DataFrame des activités utilisateur avec meilleure détection des chaînes favorites"""
    data = []
    
    for ip, user_data in user_stats.get("users", {}).items():
        # Statistiques globales de l'utilisateur
        total_time = user_data.get("total_watch_time", 0)
        
        # Détecter la chaîne favorite (plus complexe pour gérer différentes structures)
        favorite_channel = None
        
        # Si channels est disponible, parcourir chaque chaîne
        if "channels" in user_data and isinstance(user_data["channels"], dict):
            # Structure normale - chercher le marqueur "favorite"
            for channel, channel_data in user_data["channels"].items():
                if channel_data.get("favorite", False):
                    favorite_channel = channel
                    break
                
            # Si aucun marqueur trouvé, utiliser celle avec le plus de temps
            if not favorite_channel:
                max_time = 0
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
        else:
            # Utiliser channels_watched si disponible
            channels = []
            if "channels_watched" in user_data:
                if isinstance(user_data["channels_watched"], list):
                    channels = user_data["channels_watched"]
                elif isinstance(user_data["channels_watched"], set):
                    channels = list(user_data["channels_watched"])
            
            # Si au moins une chaîne, la première est considérée comme favorite (approximation)
            if channels:
                favorite_channel = channels[0]
                time_per_channel = total_time / len(channels)
                
                for i, channel in enumerate(channels):
                    data.append({
                        "ip": ip,
                        "channel": channel,
                        "watch_time": time_per_channel,
                        "favorite": (i == 0),  # Première chaîne = favorite
                        "last_seen": datetime.fromtimestamp(user_data.get("last_seen", 0)),
                    })
    
    return pd.DataFrame(data)

def create_channel_stats_df(channel_stats):
    """Crée un DataFrame des statistiques par chaîne"""
    data = []
    
    for channel, stats in channel_stats.get("channels", {}).items():
        viewers = len(stats.get("unique_viewers", []))
        watch_time = stats.get("total_watch_time", 0)
        data.append({
            "channel": channel,
            "viewers": viewers,
            "watch_time": watch_time,
            "last_update": datetime.fromtimestamp(stats.get("last_update", 0))
        })
    
    return pd.DataFrame(data)

def format_time(seconds):
    """Formate le temps en heures:minutes:secondes"""
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = int(seconds % 60)
    return f"{hours:02d}h {minutes:02d}m {seconds:02d}s"

def create_channels_grid(channel_stats):
    """Creates a grid of channel cards with status indicators"""
    
    # Process channel data
    current_time = time.time()
    channels_data = []
    
    for channel, stats in channel_stats.get("channels", {}).items():
        is_active = stats.get("active", False)
        is_streaming = stats.get("streaming", False)
        viewers = stats.get("viewers", 0)
        last_seen = datetime.fromtimestamp(stats.get("last_update", current_time)).strftime('%H:%M:%S')
        
        channels_data.append({
            "name": channel,
            "active": is_active and is_streaming,  # Une chaîne est active si elle est active ET en streaming
            "streaming": is_streaming,
            "viewers": viewers,
            "last_seen": last_seen
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
        
        card = html.Div([
            html.Div([
                html.Div(status_text, className="channel-status-badge", 
                         style={"backgroundColor": status_color}),
                html.H3(ch["name"], className="channel-card-title"),
                html.Div([
                    html.P(f"{ch['viewers']} {'viewers' if ch['viewers'] != 1 else 'viewer'}", 
                           className="channel-card-stat"),
                    html.P(f"Last activity: {ch['last_seen']}", 
                           className="channel-card-stat")
                ], className="channel-card-stats")
            ], className="channel-card-content")
        ], className="channel-card")
        
        cards.append(card)
    
    return html.Div(cards, className="channel-grid")

# Initialiser l'application Dash
app = dash.Dash(__name__, 
                title="IPTV Stats Dashboard",
                meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}])

# Layout de l'application
app.layout = html.Div([
    html.H1("IPTV Streaming Service - Dashboard de Statistiques", className="header-title"),
    
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
    
    dcc.Interval(
        id='interval-component',
        interval=2*1000,  # 2 secondes en millisecondes
        n_intervals=0
    )
], className="dashboard-container")

# Callback pour mettre à jour tous les graphiques
@app.callback(
    [Output("global-stats", "children"),
     Output("channel-pie-chart", "figure"),
     Output("channel-bar-chart", "figure"),
     Output("user-activity-chart", "figure"),
     Output("user-details", "children")],
    [Input("interval-component", "n_intervals")]
)
def update_dashboard(n):
    # Charger les données
    channel_stats, user_stats = load_stats()
    channel_df = create_channel_stats_df(channel_stats)
    user_df = create_user_activity_df(user_stats)
    
    # 1. Statistiques globales
    total_watch_time = channel_stats.get("global", {}).get("total_watch_time", 0)
    unique_viewers = len(channel_stats.get("global", {}).get("unique_viewers", []))
    total_channels = len(channel_stats.get("channels", {}))
    
    global_stats = html.Div([
        html.Div([
            html.H3(format_time(total_watch_time)),
            html.P("Temps Total de Visionnage")
        ], className="stat-box"),
        html.Div([
            html.H3(str(unique_viewers)),
            html.P("Spectateurs Uniques")
        ], className="stat-box"),
        html.Div([
            html.H3(str(total_channels)),
            html.P("Chaînes Actives")
        ], className="stat-box"),
    ])
    
    # 2. Graphique en secteurs par chaîne
    if len(channel_df) > 0:
        pie_fig = px.pie(
            channel_df, 
            values="watch_time", 
            names="channel",
            title="Répartition du Temps de Visionnage",
            color_discrete_sequence=px.colors.qualitative.Plotly
        )
        pie_fig.update_traces(textposition='inside', textinfo='percent+label')
    else:
        pie_fig = go.Figure()
        pie_fig.add_annotation(text="Aucune donnée disponible", showarrow=False)
    
    # 3. Graphique à barres du temps par chaîne
    if len(channel_df) > 0:
        channel_df["formatted_time"] = channel_df["watch_time"].apply(lambda x: format_time(x))
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
        bar_fig = go.Figure()
        bar_fig.add_annotation(text="Aucune donnée disponible", showarrow=False)
    
    # 4. Graphique d'activité utilisateur
    if len(user_df) > 0:
        user_df["formatted_time"] = user_df["watch_time"].apply(lambda x: format_time(x))
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
        user_fig = go.Figure()
        user_fig.add_annotation(text="Aucune donnée disponible", showarrow=False)
    
    # 5. Détails par utilisateur
    user_details = []
    for ip, user_data in user_stats.get("users", {}).items():
        total_time = user_data.get("total_watch_time", 0)
        last_seen = datetime.fromtimestamp(user_data.get("last_seen", 0))
        
        # Trouver la chaîne favorite
        favorite_channel = "Inconnue"
        if "channels" in user_data:
            for channel, data in user_data["channels"].items():
                if data.get("favorite", False):
                    favorite_channel = channel
                    break
        
        user_details.append(html.Div([
            html.H3(f"Utilisateur: {ip}"),
            html.P(f"Temps total: {format_time(total_time)}"),
            html.P(f"Dernière activité: {last_seen.strftime('%Y-%m-%d %H:%M:%S')}"),
            html.P(f"Chaîne favorite: {favorite_channel}")
        ], className="user-detail-box"))
    
    if not user_details:
        user_details = [html.Div(html.P("Aucune donnée utilisateur disponible"))]
    
    return global_stats, pie_fig, bar_fig, user_fig, user_details

# Callback pour mettre à jour le statut des chaînes en direct
@app.callback(
    Output("live-channels-container", "children"),
    [Input("interval-component", "n_intervals")]
)
def update_live_channels(n):
    # Charger les données de statut en direct
    try:
        with open(CHANNELS_STATUS_FILE, 'r') as f:
            channel_status = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        channel_status = {"channels": {}}
    
    # Créer la grille de chaînes
    grid = create_channels_grid(channel_status)
    
    return grid

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
            @media (max-width: 768px) {
                .six.columns {
                    flex: 0 0 100%;
                    max-width: 100%;
                }
                .user-detail-box {
                    width: calc(50% - 20px);
                }
            }
            @media (max-width: 480px) {
                .user-detail-box {
                    width: calc(100% - 20px);
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
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

# Chemin vers les fichiers de statistiques
STATS_DIR = "/app/stats"
USER_STATS_FILE = os.path.join(STATS_DIR, "user_stats.json")
CHANNEL_STATS_FILE = os.path.join(STATS_DIR, "channel_stats.json")

def load_stats():
    """Charge et nettoie les données de statistiques"""
    # Charger les statistiques des chaînes
    with open(CHANNEL_STATS_FILE, 'r') as f:
        channel_stats = json.load(f)
    
    # Charger les statistiques utilisateurs
    with open(USER_STATS_FILE, 'r') as f:
        user_stats = json.load(f)
    
    # Nettoyer et normaliser les données utilisateurs (structure imbriquée)
    cleaned_users = {}
    
    # Parcourir tous les niveaux possibles pour trouver les utilisateurs
    if "users" in user_stats:
        if isinstance(user_stats["users"], dict):
            for k, v in user_stats["users"].items():
                if k == "users" and isinstance(v, dict):
                    # Niveau supplémentaire trouvé
                    for sub_k, sub_v in v.items():
                        if isinstance(sub_v, dict) and "total_watch_time" in sub_v:
                            cleaned_users[sub_k] = sub_v
                elif isinstance(v, dict) and "total_watch_time" in v:
                    # Entrée utilisateur standard
                    cleaned_users[k] = v
    
    return channel_stats, {"users": cleaned_users}

def create_user_activity_df(user_stats):
    """Crée un DataFrame des activités utilisateur"""
    data = []
    
    for ip, user_data in user_stats.get("users", {}).items():
        # Statistiques globales de l'utilisateur
        total_time = user_data.get("total_watch_time", 0)
        
        # Si channels est disponible, parcourir chaque chaîne
        if "channels" in user_data and isinstance(user_data["channels"], dict):
            for channel, channel_data in user_data["channels"].items():
                data.append({
                    "ip": ip,
                    "channel": channel,
                    "watch_time": channel_data.get("total_watch_time", 0),
                    "favorite": channel_data.get("favorite", False),
                    "last_seen": datetime.fromtimestamp(channel_data.get("last_seen", 0)),
                })
        else:
            # Si pas de détail par chaîne, on utilise channels_watched
            channels = user_data.get("channels_watched", [])
            if isinstance(channels, list) and len(channels) > 0:
                # Distribuer le temps de visionnage entre les chaînes (approximation)
                time_per_channel = total_time / len(channels)
                for channel in channels:
                    data.append({
                        "ip": ip,
                        "channel": channel,
                        "watch_time": time_per_channel,
                        "favorite": False,
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
    
    dcc.Interval(
        id='interval-component',
        interval=10*1000,  # 10 secondes en millisecondes
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
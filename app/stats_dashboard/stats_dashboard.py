# Add to stats_dashboard.py

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
CHANNEL_STATUS_FILE = os.path.join(STATS_DIR, "channels_status.json")  # NEW: Real-time status file

def load_status():
    """Load the real-time channel status data"""
    try:
        with open(CHANNEL_STATUS_FILE, 'r') as f:
            status_data = json.load(f)
        return status_data
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error loading channel status: {e}")
        return {"channels": {}, "last_updated": 0, "active_viewers": 0}

# Update the callback for real-time channel status
@app.callback(
    Output("live-channels-container", "children"),
    [Input("interval-component", "n_intervals")]
)
def update_live_channels(n):
    # Load real-time status data
    status_data = load_status()
    
    # Create channel cards
    cards = []
    
    # Process channels
    channels_data = []
    for channel, stats in status_data.get("channels", {}).items():
        is_active = stats.get("active", False)
        is_streaming = stats.get("streaming", False)
        viewers = stats.get("viewers", 0)
        peak_viewers = stats.get("peak_viewers", 0)
        last_update = stats.get("last_update", 0)
        
        # Current state calculation
        if is_streaming and viewers > 0:
            state = "active"  # Active with viewers
        elif is_streaming:
            state = "streaming"  # Streaming but no viewers
        elif is_active:
            state = "ready"  # Ready but not streaming
        else:
            state = "inactive"  # Not ready
            
        channels_data.append({
            "name": channel,
            "state": state,
            "viewers": viewers,
            "peak_viewers": peak_viewers,
            "last_update": last_update
        })
    
    # Sort channels by state and then by name
    state_order = {"active": 0, "streaming": 1, "ready": 2, "inactive": 3}
    channels_data.sort(key=lambda x: (state_order[x["state"]], x["name"].lower()))
    
    # Create grid of cards
    for ch in channels_data:
        # Status color map
        status_colors = {
            "active": "#27ae60",      # Green for active with viewers
            "streaming": "#2980b9",   # Blue for streaming without viewers
            "ready": "#f39c12",       # Orange for ready but not streaming
            "inactive": "#95a5a6"     # Gray for inactive
        }
        
        # Status text map
        status_text = {
            "active": "ACTIVE",
            "streaming": "STREAMING",
            "ready": "READY",
            "inactive": "INACTIVE"
        }
        
        status_color = status_colors.get(ch["state"], "#95a5a6")
        status_text_value = status_text.get(ch["state"], "UNKNOWN")
        
        # Format last update time
        last_seen = datetime.fromtimestamp(ch["last_update"]).strftime('%H:%M:%S')
        
        card = html.Div([
            html.Div([
                html.Div(status_text_value, className="channel-status-badge", 
                         style={"backgroundColor": status_color}),
                html.H3(ch["name"], className="channel-card-title"),
                html.Div([
                    html.P(f"{ch['viewers']} {'viewers' if ch['viewers'] != 1 else 'viewer'}", 
                           className="channel-card-stat"),
                    html.P(f"Peak: {ch['peak_viewers']} viewers", 
                           className="channel-card-stat"),
                    html.P(f"Last activity: {last_seen}", 
                           className="channel-card-stat")
                ], className="channel-card-stats")
            ], className="channel-card-content")
        ], className="channel-card")
        
        cards.append(card)
    
    # If no channels found
    if not cards:
        cards = [html.Div(html.P("No channel data available"), className="no-channels-message")]
    
    # Add summary at top
    summary = html.Div([
        html.H3(f"Total Active Viewers: {status_data.get('active_viewers', 0)}"),
        html.P(f"Last Updated: {datetime.fromtimestamp(status_data.get('last_updated', 0)).strftime('%Y-%m-%d %H:%M:%S')}")
    ], className="channels-summary")
    
    return html.Div([summary, html.Div(cards, className="channel-grid")])

# Add to the CSS for better styling of the new status badges
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
            /* Existing CSS... */
            
            /* New CSS for channel status */
            .channels-summary {
                background-color: #fff;
                border-radius: 5px;
                box-shadow: 0 2px 5px rgba(0,0,0,0.1);
                padding: 15px;
                margin-bottom: 20px;
                text-align: center;
            }
            .channels-summary h3 {
                margin: 0 0 10px;
                color: #2c3e50;
            }
            .channels-summary p {
                margin: 0;
                color: #7f8c8d;
            }
            .no-channels-message {
                background-color: #fff;
                border-radius: 5px;
                box-shadow: 0 2px 5px rgba(0,0,0,0.1);
                padding: 20px;
                text-align: center;
                color: #7f8c8d;
            }
            
            /* Animations for status updates */
            @keyframes pulse {
                0% { opacity: 1; }
                50% { opacity: 0.7; }
                100% { opacity: 1; }
            }
            .channel-card {
                transition: all 0.3s ease;
            }
            .channel-status-badge {
                animation: pulse 2s infinite;
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
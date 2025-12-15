# app.py (Final Error-Free Version)

import streamlit as st
import pandas as pd
import time
import json
import base64
import threading
from kafka import KafkaConsumer
from py2neo import Graph, ClientError

# --- CONFIGURATION FROM YAML ---
import sys
import os
# app.py root mein hai, isliye sirf current dir append karein
sys.path.append(os.path.dirname(os.path.abspath(__file__))) 

try:
    from utils import CONFIG 
except ImportError:
    st.error("Error: utils.py not found. Please ensure it is in the root directory.")
    st.stop()

if not CONFIG:
    st.error("FATAL: Configuration failed to load from utils.py.")
    st.stop()

# Extract settings from the loaded CONFIG dictionary
KAFKA_SERVER = "localhost:9092"
GRAPH_DATA_TOPIC = CONFIG["KAFKA_TOPICS"]["GRAPH_DATA"]
VIDEO_OUTPUT_TOPIC = CONFIG["KAFKA_TOPICS"]["VIDEO_STREAM_PROCESSED"]
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "password"

# --- NEO4J CONNECTION ---
try:
    graph = Graph(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    st.sidebar.markdown('<div style="color: green; font-weight: bold;">✅ Neo4j Database Connected</div>', unsafe_allow_html=True)
except ClientError as e:
    st.sidebar.markdown('<div style="color: red; font-weight: bold;">❌ Neo4j Connection Error</div>', unsafe_allow_html=True)
    st.sidebar.code(f"Error: {e}")
    graph = None
    
# --- KAFKA CONSUMER SETUP FOR VIDEO STREAM ---
try:
    video_consumer = KafkaConsumer(
        VIDEO_OUTPUT_TOPIC,
        bootstrap_servers=[KAFKA_SERVER],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    st.sidebar.markdown('<div style="color: green; font-weight: bold;">✅ Video Stream Connected</div>', unsafe_allow_html=True)
except Exception as e:
    st.sidebar.markdown('<div style="color: red; font-weight: bold;">❌ Kafka Video Stream Error</div>', unsafe_allow_html=True)
    st.sidebar.code(f"Error: {e}")
    video_consumer = None


# --- STREAMLIT UI SETUP ---

st.set_page_config(layout="wide")
st.title("🚦 UrbanGraph: Real-time Traffic Intelligence")

# --- SIDEBAR & CONFIGURATION ---
st.sidebar.header("Configuration")
selected_view = st.sidebar.selectbox(
    "Select Data View:",
    ("Traffic Flow", "Recent Alerts")
)
refresh_rate = st.sidebar.slider("Data Refresh Rate (s)", 1, 10, 1)

# --- LIVE VIDEO DISPLAY SECTION ---

st.header("🎥 Real-time Processed Video Feed")
video_placeholder = st.empty() # Placeholder for the video frame

def read_video_stream():
    """Reads processed frames from Kafka and updates the Streamlit image placeholder."""
    if video_consumer:
        for message in video_consumer:
            try:
                # Kafka se data lo
                data = message.value
                frame_id = data.get('frame_id', 0)
                
                # Base64 string se image decode karo
                jpg_as_text = data['frame']
                image_bytes = base64.b64decode(jpg_as_text)
                
                # Frame ko Streamlit par display karo
                video_placeholder.image(
                    image_bytes, 
                    caption=f"Frame ID: {frame_id} (Processed by AI)", 
                    width='stretch' 
                )
            except Exception as e:
                pass 

# Start video consumer thread
if 'video_thread' not in st.session_state and video_consumer:
    video_thread = threading.Thread(target=read_video_stream, daemon=True)
    video_thread.start()
    st.session_state.video_thread = video_thread

# --- NEO4J DATA QUERY FUNCTIONS ---

def get_traffic_flow(limit=10):
    """Fetches AHEAD_OF relationships for traffic flow analysis."""
    query = f"""
    MATCH (a:Track)-[r:AHEAD_OF]->(b:Track)
    RETURN 
        a.track_id AS Ahead_ID, 
        b.track_id AS Behind_ID, 
        a.speed AS Ahead_Speed, 
        b.speed AS Behind_Speed,
        r.proximity_score AS Proximity
    ORDER BY r.timestamp DESC
    LIMIT {limit}
    """
    return graph.run(query).to_data_frame()

def get_recent_alerts(limit=10):
    """Fetches HAZARD_TO relationships for critical alerts."""
    query = f"""
    MATCH (a:Track)-[r:HAZARD_TO]->(b:Track)
    RETURN 
        a.track_id AS Hazard_Source_ID,
        b.track_id AS Target_ID, 
        r.reason AS Reason, 
        r.proximity_score AS Proximity_Score,
        r.timestamp AS Time
    ORDER BY r.timestamp DESC
    LIMIT {limit}
    """
    return graph.run(query).to_data_frame()


# --- REAL-TIME DATA DISPLAY ---

st.header(f"📊 {selected_view} Dashboard")

# Create a placeholder for the data table
data_placeholder = st.empty()

# Main loop to refresh data
while True:
    if graph:
        try:
            if selected_view == "Traffic Flow":
                df = get_traffic_flow(limit=50)
                # No 'caption' argument here (FIX)
                data_placeholder.dataframe(
                    df.style.format(precision=4), 
                    width='stretch'
                )
            else: # Recent Alerts
                df = get_recent_alerts(limit=50)
                # No 'caption' argument here (FIX)
                data_placeholder.dataframe(
                    df.style.apply(
                        lambda x: ['background-color: #400' if x.name == 'Reason' else '' for i in x], axis=1
                    ).format(precision=4),
                    width='stretch'
                )
                
        except Exception as e:
            # Check if the error is the expected Neo4j data error
            if "Neo4j" in str(e) or "py2neo" in str(e):
                 data_placeholder.error(f"Error querying Neo4j: {e}")
                 st.code("Ensure the Graph Builder is running and sending data.")
            else:
                 # Other unexpected errors
                 data_placeholder.error(f"An unexpected Python error occurred: {e}")
                 st.code(f"Please check terminal output for detailed traceback.")
    else:
        data_placeholder.info("Waiting for Neo4j connection...")
        
    time.sleep(refresh_rate)
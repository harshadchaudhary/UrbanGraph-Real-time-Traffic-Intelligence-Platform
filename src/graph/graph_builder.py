# src/graph/graph_builder.py

import json
import time
from py2neo import Graph, ClientError
from kafka import KafkaConsumer
from py2neo.matching import NodeMatcher


import sys
import os
# Hum do levels upar UrbanGraph root folder ko add kar rahe hain.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
# --- CONFIGURATION (Simple Import due to PYTHONPATH) ---
try:
    from utils import CONFIG 
except ImportError as e:
    print(f"Error importing utils.py: {e} ")
    exit()

if not CONFIG:
    print("FATAL ERROR: Configuration failed to load from config.yaml.")
    exit()

# --- CONFIGURATION CONSTANTS ---
KAFKA_SERVER = "localhost:9092" 
GRAPH_DATA_TOPIC = CONFIG["KAFKA_TOPICS"]["GRAPH_DATA"]

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j" 
NEO4J_PASS = "password" 

PROXIMITY_THRESHOLD = CONFIG["LOGIC_CONFIG"]["PROXIMITY_THRESHOLD_METERS"] 
VELOCITY_THRESHOLD = CONFIG["LOGIC_CONFIG"]["VELOCITY_THRESHOLD_KMH"]       

# --- NEO4J CONNECTION ---
try:
    graph = Graph(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))
    print("INFO: Neo4j Graph Database successfully connected.")
except ClientError as e:
    print(f"ERROR: Neo4j connection failed: {e}")
    exit()

# --- KAFKA CONSUMER SETUP ---
try:
    consumer = KafkaConsumer(
        GRAPH_DATA_TOPIC,
        bootstrap_servers=[KAFKA_SERVER],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    print(f"INFO: Kafka Consumer connected. Topic: {GRAPH_DATA_TOPIC}")
except Exception as e:
    print(f"ERROR: Kafka Consumer connection failed: {e}")
    exit()

# --- HELPER FUNCTIONS ---

def meters_to_pixels(meters, scale_factor=10):
    return meters * scale_factor

def kmh_to_pix_per_sec(kmh, scale_factor=10):
    return kmh * scale_factor

def process_track_data(frame_data):
    """Creates/updates nodes and relationships in Neo4j based on frame data."""
    
    tracks = frame_data.get('tracks', [])
    frame_id = frame_data.get('frame_id', 0)
    timestamp = time.time()
    
    # Note: Using graph.run() directly instead of starting a manual tx for simplicity
    
    current_tracks = {} 
    
    for data in tracks:
        track_id = data['track_id']
        speed = data['speed']
        
        # 1. Create/Update Track Node
        query = f"""
        MERGE (t:Track {{track_id: {track_id}}})
        SET 
            t.x = {data['x']}, 
            t.y = {data['y']},
            t.speed = {speed},
            t.class_id = {data['class_id']},
            t.last_seen = {timestamp}
        RETURN t
        """
        result = graph.run(query).data()
        if result:
            current_tracks[track_id] = result[0]['t']
    
    # 2. Analyze Relationships
    
    sorted_tracks = sorted(current_tracks.items(), key=lambda item: item[1]['y'])
    
    for i in range(len(sorted_tracks)):
        track_id_a, node_a = sorted_tracks[i]
        
        if i + 1 < len(sorted_tracks):
            track_id_b, node_b = sorted_tracks[i+1] # Track B is behind Track A
            
            # Distance Calculation
            dist_x = node_a['x'] - node_b['x']
            dist_y = node_a['y'] - node_b['y']
            proximity_score = (dist_x**2 + dist_y**2)**0.5
            
            # --- RELATIONSHIP LOGIC ---

            # A. AHEAD_OF Relationship: Delete old and create new
            graph.run(f"MATCH (a:Track {{track_id: {track_id_a}}})-[r:AHEAD_OF]->(b:Track {{track_id: {track_id_b}}}) DELETE r")

            query_ahead = f"""
            MATCH (a:Track {{track_id: {track_id_a}}}), (b:Track {{track_id: {track_id_b}}})
            CREATE (a)-[:AHEAD_OF {{
                proximity_score: {round(proximity_score, 2)},
                ahead_speed: {node_a['speed']},
                behind_speed: {node_b['speed']},
                timestamp: {timestamp}
            }}]->(b)
            """
            graph.run(query_ahead)
            
            # B. HAZARD_TO Relationship (Conditional Check)

            proximity_pixel_threshold = meters_to_pixels(PROXIMITY_THRESHOLD)
            speed_diff_threshold = kmh_to_pix_per_sec(VELOCITY_THRESHOLD)

            if (proximity_score < proximity_pixel_threshold and 
                node_b['speed'] > node_a['speed'] and 
                (node_b['speed'] - node_a['speed']) > speed_diff_threshold):
                
                reason = "Close Proximity and High Closing Speed"
                
                # Create HAZARD_TO relationship
                query_hazard = f"""
                MATCH (a:Track {{track_id: {track_id_b}}}), (b:Track {{track_id: {track_id_a}}})
                CREATE (a)-[:HAZARD_TO {{
                    reason: '{reason}',
                    proximity_score: {round(proximity_score, 2)},
                    timestamp: {timestamp}
                }}]->(b)
                """
                graph.run(query_hazard)
    
    return len(tracks)

# --- MAIN EXECUTION LOOP ---

def consume_and_build_graph():
    print("--- UrbanGraph Graph Builder Starting ---")
    
    for message in consumer:
        frame_data = message.value
        tracks_processed = process_track_data(frame_data)
        
        print(f"INFO: Frame {frame_data['frame_id']} processed. Nodes/Relationships updated for {tracks_processed} tracks.")

if __name__ == "__main__":
    consume_and_build_graph()
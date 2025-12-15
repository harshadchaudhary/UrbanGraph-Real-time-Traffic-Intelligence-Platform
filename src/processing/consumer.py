# src/processing/consumer.py

import cv2
import json
import base64
from kafka import KafkaConsumer, KafkaProducer
from ultralytics import YOLO
from deep_sort_realtime.deepsort_tracker import DeepSort
import numpy as np
import time

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
# --- CONFIGURATION (Simple Import due to PYTHONPATH) ---
try:
    from utils import CONFIG 
except ImportError as e:
    print(f"Error importing utils.py: {e}")
    exit()

if not CONFIG:
    print("FATAL ERROR: Configuration failed to load from config.yaml.")
    exit()
    
# Extract settings from the loaded CONFIG dictionary
KAFKA_SERVER = "localhost:9092"
VIDEO_TOPIC = CONFIG["KAFKA_TOPICS"]["VIDEO_STREAM"]
OUTPUT_TOPIC = CONFIG["KAFKA_TOPICS"]["GRAPH_DATA"]
VIDEO_OUTPUT_TOPIC = CONFIG["KAFKA_TOPICS"]["VIDEO_STREAM_PROCESSED"]
MODEL_PATH = CONFIG["MODEL_CONFIG"]["MODEL_NAME"]
TARGET_CLASSES = CONFIG["MODEL_CONFIG"]["CLASSES_TO_DETECT"]
CONF_THRESHOLD = CONFIG["MODEL_CONFIG"]["CONFIDENCE_THRESHOLD"]

# --- INITIALIZATION ---

# AI Models Initialization
print(f"INFO: YOLOv8 model '{MODEL_PATH}' loaded successfully on device: cpu")
model = YOLO(MODEL_PATH)
tracker = DeepSort(
    max_iou_distance=0.7, 
    max_age=30,
    n_init=3,
    max_cosine_distance=0.2, 
    nn_budget=None
)
# ... (Kafka Consumer/Producers initialization code wahi rahega) ...

# Kafka Consumer
try:
    consumer = KafkaConsumer(
        VIDEO_TOPIC,
        bootstrap_servers=[KAFKA_SERVER],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    print(f"INFO: Kafka Consumer successfully connected to {VIDEO_TOPIC}.")
except Exception as e:
    print(f"ERROR: Kafka Consumer connection failed: {e}")
    exit()

# Kafka Data Producer (for Graph Builder)
try:
    DATA_PRODUCER = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(0, 10, 1)
    )
    print(f"INFO: Kafka Result Producer connected. Topic: {OUTPUT_TOPIC}")
except Exception as e:
    print(f"ERROR: Kafka Result Producer connection failed: {e}")
    exit()
    
# Kafka Video Producer (for Streamlit Display)
try:
    VIDEO_PRODUCER = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        api_version=(0, 10, 1)
    )
    print(f"INFO: Kafka Video Producer connected. Topic: {VIDEO_OUTPUT_TOPIC}")
except Exception as e:
    print(f"ERROR: Kafka Video Producer connection failed: {e}")
    exit()

# --- MAIN EXECUTION LOOP ---

def consume_frames():
    print("--- UrbanGraph AI Processing Starting ---")
    
    last_positions = {}
    
    # --- HELPER FUNCTIONS (LOCAL SCOPE) ---

    def calculate_speed(track_id, current_position, frame_time_diff):
        """Simple speed calculation (pixels per second)"""
        nonlocal last_positions 
        if track_id in last_positions:
            last_x, last_y = last_positions[track_id]
            curr_x, curr_y = current_position
            
            distance = np.sqrt((curr_x - last_x)**2 + (curr_y - last_y)**2)
            
            if frame_time_diff > 0:
                speed = distance / frame_time_diff
            else:
                speed = 0
            
            last_positions[track_id] = current_position
            return round(speed, 4)
        else:
            last_positions[track_id] = current_position
            return 0.0

    # --- MAIN PROCESSING LOGIC (LOCAL SCOPE) ---

    def process_detections(frame, frame_id, current_frame_start_time):
        """Runs AI models (YOLO and DeepSORT) and sends data to Kafka"""
        
        # YOLO Detection
        results = model.track(
            frame, 
            persist=True, 
            classes=TARGET_CLASSES, 
            verbose=False,
            conf=CONF_THRESHOLD
        )
        
        # Process YOLO results for DeepSORT
        detections = []
        if results and results[0].boxes and results[0].boxes.data.shape[0] > 0:
            data = results[0].boxes.data.cpu().numpy()
            
            for *box, conf, cls in data:
                x1, y1, x2, y2 = map(int, box)
                w = x2 - x1
                h = y2 - y1
                detections.append(([x1, y1, w, h], conf, int(cls)))

        # DeepSORT Update (Returns list of Track objects)
        tracks = tracker.update_tracks(detections, frame=frame)
        
        current_tracks_data = []
        
        for track in tracks:
            if not track.is_confirmed():
                continue
                
            track_id = track.track_id
            ltrb = track.to_ltrb()
            x1, y1, x2, y2 = map(int, ltrb)
            
            center_x = (x1 + x2) // 2
            center_y = (y1 + y2) // 2
            current_position = (center_x, center_y)
            
            # Speed Calculation
            frame_time_diff = time.time() - current_frame_start_time 
            speed = calculate_speed(track_id, current_position, frame_time_diff) 

            # Draw bounding box and ID on frame
            cv2.rectangle(frame, (x1, y1), (x2, y2), (0, 255, 0), 2)
            cv2.putText(frame, str(track_id), (x1, y1 - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.9, (0, 255, 0), 2)
            
            # Prepare data for Graph Builder
            track_data = {
                "track_id": track_id,
                "class_id": track.get_det_class(),
                "x": center_x,
                "y": center_y,
                "speed": speed,
                "frame_id": frame_id,
                "timestamp": time.time()
            }
            current_tracks_data.append(track_data)
            
        # Send AI data to Graph Builder
        if current_tracks_data:
            DATA_PRODUCER.send(
                OUTPUT_TOPIC,
                value={"frame_id": frame_id, "tracks": current_tracks_data}
            )
        
        # SEND PROCESSED FRAME TO STREAMLIT
        ret, buffer = cv2.imencode('.jpg', frame)
        if ret:
            jpg_as_text = base64.b64encode(buffer).decode('utf-8')
            
            VIDEO_PRODUCER.send(
                VIDEO_OUTPUT_TOPIC,
                value=json.dumps({'frame': jpg_as_text, 'frame_id': frame_id}).encode('utf-8')
            )
        
        return len(current_tracks_data), frame
    # -----------------------------------------------

    for message in consumer:
        frame_id = message.value['frame_id']
        frame_bytes = base64.b64decode(message.value['frame'])
        
        np_arr = np.frombuffer(frame_bytes, np.uint8)
        frame = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)

        if frame is None:
            print(f"ERROR: Could not decode frame {frame_id}")
            continue
        
        current_frame_start_time = time.time() 
        tracks_count, processed_frame = process_detections(frame, frame_id, current_frame_start_time)
        
        end_time = time.time()
        fps = 1 / (end_time - current_frame_start_time) 
        
        print(f"INFO: Processed Frame {frame_id}. Tracks: {tracks_count}. FPS: {fps:.2f}")

if __name__ == "__main__":
    consume_frames()
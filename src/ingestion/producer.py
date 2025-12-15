# src/ingestion/producer.py

import cv2
import json
import base64
import time
from kafka import KafkaProducer

import sys
import os
# Hum do levels upar UrbanGraph root folder ko add kar rahe hain.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
# --- CONFIGURATION (Simple Import due to PYTHONPATH) ---
try:
    from utils import CONFIG 
except ImportError as e:
    print("FATAL ERROR: Could not import utils.py. Ensure PYTHONPATH is set to the UrbanGraph root.")
    exit()

if not CONFIG:
    print("FATAL ERROR: Configuration failed to load from config.yaml.")
    exit()
    
# Extract settings from the loaded CONFIG dictionary
KAFKA_SERVER = "localhost:9092"
VIDEO_TOPIC = CONFIG["KAFKA_TOPICS"]["VIDEO_STREAM"]
SOURCE_TYPE = CONFIG["VIDEO_CONFIG"]["SOURCE_TYPE"]
VIDEO_PATH = CONFIG["VIDEO_CONFIG"]["VIDEO_FILE_PATH"]

# --- INITIALIZATION ---

# Kafka Producer
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        api_version=(0, 10, 1)
    )
    print(f"INFO: Kafka Producer successfully connected. (Using raw bytes)")
except Exception as e:
    print(f"ERROR: Kafka Producer connection failed: {e}")
    exit()

# --- MAIN LOGIC ---

def stream_video_frames():
    """Opens video source (webcam or file) and streams frames to Kafka."""
    
    if SOURCE_TYPE == "FILE":
        print(f"INFO: Source: FILE, Path: {VIDEO_PATH}")
        cap = cv2.VideoCapture(VIDEO_PATH)
    else:
        # LIVE CAMERA ACTIVATION
        print(f"INFO: Source: LIVE CAMERA (Index 0)")
        cap = cv2.VideoCapture(0) # 0 is typically the default webcam
        
        if not cap.isOpened():
            print("ERROR: Cannot open live webcam (Index 0). Please check camera connection.")
            return

    # Frame rate calculation
    fps = cap.get(cv2.CAP_PROP_FPS)
    if fps == 0:
        fps = 30.0
    
    time_per_frame = 1.0 / fps
    print(f"INFO: Streaming video at {fps:.2f} FPS. Time per frame: {time_per_frame:.4f}s")
    
    print("--- UrbanGraph Kafka Producer Starting ---")
    
    frame_count = 0
    start_time = time.time()

    while cap.isOpened():
        ret, frame = cap.read()
        
        if not ret:
            break
            
        frame_count += 1
        
        # Encode frame to JPEG
        ret, buffer = cv2.imencode('.jpg', frame)
        
        if ret:
            jpg_as_text = base64.b64encode(buffer).decode('utf-8')
            
            try:
                # Send the base64 string
                producer.send(
                    VIDEO_TOPIC,
                    value=json.dumps({'frame': jpg_as_text, 'frame_id': frame_count}).encode('utf-8'),
                    key=str(frame_count).encode('utf-8')
                )
            except Exception as e:
                print(f"ERROR: Failed to send frame {frame_count} to Kafka: {e}")

        # Timing control
        time_to_wait = time_per_frame - (time.time() - start_time)
        if time_to_wait > 0:
            time.sleep(time_to_wait)
            
        start_time = time.time()
        

    # Cleanup
    cap.release()
    producer.flush(timeout=10)
    print("\nINFO: Finished video streaming and flushing producer.")

if __name__ == "__main__":
    stream_video_frames()
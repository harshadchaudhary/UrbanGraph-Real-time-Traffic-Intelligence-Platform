# src/processing/detector.py

from ultralytics import YOLO
import torch
import yaml
import numpy as np
from typing import List, Dict, Any

# Config load karna
with open('config/config.yaml', 'r') as f:
    CONFIG = yaml.safe_load(f)

# Model aur Device (CPU/GPU) setup karna
MODEL_NAME = CONFIG['MODEL_CONFIG']['MODEL_NAME']
# Agar GPU (CUDA) available hai, toh 'cuda' use karenge, warna 'cpu'
DEVICE = 'cuda' if torch.cuda.is_available() else 'cpu'

# Model load karna
try:
    # Model ko load karte waqt usse device batana
    YOLO_MODEL = YOLO(MODEL_NAME).to(DEVICE)
    print(f"INFO: YOLOv8 model '{MODEL_NAME}' loaded successfully on device: {DEVICE}")
except Exception as e:
    print(f"FATAL ERROR: Failed to load YOLO model: {e}")
    exit(1)


def detect_objects(frame: np.ndarray) -> List[Dict[str, Any]]:
    """
    Frame par YOLO detection run karna.
    
    Returns:
        detections (list): [{"box": [x1, y1, x2, y2], "conf": 0.9, "cls": 2}, ...]
    """
    
    # Detection parameters
    CONF_THRESHOLD = CONFIG['MODEL_CONFIG']['CONFIDENCE_THRESHOLD']
    # Sirf defined classes ko hi detect karna
    CLASSES = CONFIG['MODEL_CONFIG']['CLASSES_TO_DETECT'] 

    # YOLO inference run karna
    results = YOLO_MODEL.predict(
        source=frame,
        conf=CONF_THRESHOLD,
        classes=CLASSES,
        verbose=False # Console output kam rakhne ke liye
    )
    
    processed_detections = []
    
    # Results ko DeepSORT compatible format mein process karna
    if results and results[0].boxes is not None:
        boxes = results[0].boxes.xyxy.cpu().numpy().astype(int)
        confidences = results[0].boxes.conf.cpu().numpy()
        classes = results[0].boxes.cls.cpu().numpy().astype(int)
        
        for box, conf, cls_id in zip(boxes, confidences, classes):
            processed_detections.append({
                "box": box.tolist(),
                "conf": float(conf),
                "cls": int(cls_id)
            })
            
    return processed_detections

# Abhi hum tracking logic ko skip karke seedhe consumer ko update karenge.
# Tracking ko hum agle step mein add karenge jab yeh detection test ho jayega.
# src/processing/tracker.py

from deep_sort_realtime.deepsort_tracker import DeepSort
import numpy as np
from typing import List, Dict, Any

# DeepSORT Tracker ka object banana
# Hum yahan default parameters use kar rahe hain
DS_TRACKER = DeepSort(
    max_iou_distance=0.7, 
    max_age=30, 
    n_init=3   
)

def format_detections_for_deepsort(detections: List[Dict[str, Any]]) -> List[List[Any]]:
    """
    YOLO output ko DeepSORT ke format mein badalta hai:
    [[x1, y1, x2, y2, confidence, class_id], ...]
    """
    deepsort_format = []
    for det in detections:
        x1, y1, x2, y2 = det['box']
        conf = det['conf']
        cls = det['cls']
        
        # DeepSORT requires (bbox, confidence, class)
        deepsort_format.append([
            [x1, y1, x2, y2],  # BBox (list format)
            conf,              # Confidence
            cls                # Class ID (DeepSORT ko iski zaroorat hai)
        ])
    return deepsort_format


# Dhyan do: Yeh function ab 'frame' argument leta hai.
def update_tracks(detections: List[Dict[str, Any]], frame: np.ndarray) -> List[Dict[str, Any]]:
    """
    Detections par DeepSORT tracking run karta hai.
    """
    
    # 1. Detections ko DeepSORT format mein convert karna
    deepsort_input = format_detections_for_deepsort(detections)
    
    # 2. DeepSORT tracker update karna
    # Yahan hum frame pass kar rahe hain jisse DeepSORT khud embeddings nikal sake.
    tracks = DS_TRACKER.update_tracks(
        raw_detections=deepsort_input, 
        frame=frame 
    )
    
    # 3. Output ko final format mein badalna
    final_tracks = []
    
    for track in tracks:
        # Sirf confirmed tracks ko hi lena
        if not track.is_confirmed():
            continue
            
        track_id = track.track_id
        # Bbox ko integer format mein lena
        x1, y1, x2, y2 = [int(i) for i in track.to_ltrb()]
        class_id = track.get_det_class()
        
        final_tracks.append({
            "track_id": track_id,
            "box": [x1, y1, x2, y2],
            "cls": class_id,
            "speed": 0.0, 
            "centroid": [int((x1+x2)/2), int((y1+y2)/2)] 
        })
        
    return final_tracks
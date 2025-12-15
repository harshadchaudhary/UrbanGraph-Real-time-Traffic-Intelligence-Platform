# UrbanGraph/utils.py

import yaml
import os

# Configuration file ka path
CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "config", 
    "config.yaml"
)

def load_config():
    """Loads configuration from the YAML file."""
    try:
        with open(CONFIG_PATH, 'r') as f:
            config = yaml.safe_load(f)
        return config
    except FileNotFoundError:
        print(f"Error: Configuration file not found at {CONFIG_PATH}")
        return None
    except yaml.YAMLError as exc:
        print(f"Error parsing YAML file: {exc}")
        return None

# Global config object, jise dusri files import karengi
CONFIG = load_config()

if CONFIG is None:
    # Agar config load nahi hua to exit kar do
    exit()
import yaml
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
config_path = os.path.join(BASE_DIR, "config", "config.yaml")

def load_config(path=config_path):
    with open(path, "r") as file:
        return yaml.safe_load(file)

    


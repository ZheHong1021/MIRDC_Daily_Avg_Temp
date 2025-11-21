import json

def load_stations(filename="data/stations.json"):
    """
    Load station data from a JSON file.
    """
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            stations = json.load(f)
        return stations
    except FileNotFoundError:
        print(f"Error: {filename} not found")
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON from {filename}: {e}")
        return None
    except Exception as e:
        print(f"Error loading stations: {e}")
        return None
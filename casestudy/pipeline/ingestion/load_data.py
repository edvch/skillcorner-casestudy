import json
import pandas as pd
from io import StringIO

def load_matches_infos(match_id):
    with open(f'skillcorner/data/raw/{match_id}/matches_infos.json', 'r', encoding='utf-8') as f:
        match_info = json.load(f)
    return pd.json_normalize(match_info)

def load_matches_dynamic_events(match_id):
    with open(f'skillcorner/data/raw/{match_id}/dynamic_events.csv', 'r', encoding='utf-8') as f:
        dynamic_events = f.read()
    return pd.read_csv(StringIO(dynamic_events))

def load_physical(match_id):
    with open(f'skillcorner/data/raw/{match_id}/physical.json', 'r', encoding='utf-8') as f:
        physical_data = json.load(f)
    return pd.json_normalize(physical_data)
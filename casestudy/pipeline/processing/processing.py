from casestudy.pipeline.ingestion.load_data import load_matches_infos, load_matches_dynamic_events, load_physical
import pandas as pd

def process_matches_infos(match_id):
    match_info = load_matches_infos(match_id)
    match_info = match_info.rename(columns = lambda x: x.replace('.', '_').lower())
    match_info['date_time'] = pd.to_datetime(match_info['date_time'])
    return match_info

def process_matches_dynamic_events(match_id):
    dynamic_events_df = load_matches_dynamic_events(match_id)
    dynamic_events_df = dynamic_events_df.rename(columns = lambda x: x.replace('.', '_').lower())
    return dynamic_events_df
    
def process_physical(match_id):
    physical_data_df = load_physical(match_id)
    physical_data_df = physical_data_df.rename(columns = lambda x: x.replace('.', '_').lower())
    return physical_data_df
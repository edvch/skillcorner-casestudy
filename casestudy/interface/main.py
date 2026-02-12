from casestudy.pipeline.ingestion.fetch_data import get_matches_list, get_matches_infos, get_matches_dynamic_events, get_physical, get_event_off_ball_runs, get_event_passing_options, get_event_on_ball_engagements, get_event_phases_of_play, get_event_player_possessions, get_event_tracking
from casestudy.pipeline.processing.processing import process_matches_infos, process_matches_dynamic_events, process_physical
import json
import io
import os

def get_all_data_locally(last_updated=None):
    """
    GET ALL DATA FROM SKILLCORNER API AND SAVE IT IN THE RAW FOLDER LOCALLY
    """
    get_matches_list(updated_time=last_updated)
    with open('casestudy/data/raw/matches_list.json', 'r', encoding='utf-8') as f:
        matches_list = json.load(f)
    for match in matches_list:
        match_id = match['id']
        os.mkdir(f'casestudy/data/raw/{match_id}')
        get_matches_infos(match_id)
        get_matches_dynamic_events(match_id)
        get_physical(match_id)
        get_event_player_possessions(match_id)
        get_event_phases_of_play(match_id)
        get_event_on_ball_engagements(match_id)
        get_event_passing_options(match_id)
        get_event_off_ball_runs(match_id)
        get_event_tracking(match_id)

        
def process_all_data():
    """
    PROCESS ALL DATA AND SAVE IT IN THE PROCESSED FOLDER LOCALLY
    """
    with open('casestudy/data/raw/matches_list.json', 'r', encoding='utf-8') as f:
        matches_list = json.load(f)
    for match in matches_list:
        match_id = match['id']
        process_matches_infos(match_id).to_parquet(f'casestudy/data/processed/{match_id}/matches_infos.parquet', index=False)
        process_matches_dynamic_events(match_id).to_parquet(f'casestudy/data/processed/{match_id}/dynamic_events.parquet', index=False)
        process_physical(match_id).to_parquet(f'casestudy/data/processed/{match_id}/physical.parquet', index=False)

def aggregate_all_data(): 
    """ AGGREGATE ALL DATA AND SAVE IT IN THE ANALYTICS FOLDER LOCALLY """
    with open('casestudy/data/raw/matches_list.json', 'r', encoding='utf-8') as f:
        matches_list = json.load(f)
    for match in matches_list:
        match_id = match['id']
        custom_aggregates = process_matches_infos(match_id)
        custom_aggregates.to_parquet(f'casestudy/data/analytics/aggregated_data/{match_id}.parquet', index=False)

if __name__ == '__main__':
    last_updated = None
    get_all_data_locally(last_updated)
    process_all_data()
    #aggregate_all_data()
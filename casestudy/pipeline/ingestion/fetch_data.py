import requests
import json
import pandas as pd

def get_matches_list(updated_time=None):
    if updated_time == None:
        url = 'https://skillcorner.com/api/matches/?competition_edition=870&user=true&token=3e43d8743069d3c9d589'
    else:
        url = f'https://skillcorner.com/api/matches/?competition_edition=870&user=true&updated_since={updated_time}&token=3e43d8743069d3c9d589'
    response = requests.get(url)
    matches_list = response.json()['results']
    with open('casestudy/data/raw/matches_list.json', 'w', encoding='utf-8') as f:
        json.dump(matches_list, f, ensure_ascii=False, indent=4)

def get_matches_infos(match_id):
    url = f'https://skillcorner.com/api/match/{match_id}/?token=3e43d8743069d3c9d589'
    response = requests.get(url)
    match_info = response.json()
    with open(f'casestudy/data/raw/{match_id}/matches_infos.json', 'w', encoding='utf-8') as f:
        json.dump(match_info, f, ensure_ascii=False, indent=4)
        
def get_matches_dynamic_events(match_id, file_format='csv', de_check='true'):
    url = f'https://skillcorner.com/api/match/{match_id}/dynamic_events/?file_format={file_format}&ignore_dynamic_events_check={de_check}&token=3e43d8743069d3c9d589'
    response = requests.get(url)
    with open(f'casestudy/data/raw/{match_id}/dynamic_events.{file_format}', 'w', encoding='utf-8') as f:
        f.write(response.text)
        
def get_physical(match_id):
    url = f'https://skillcorner.com/api/physical/?match={match_id}&token=3e43d8743069d3c9d589'
    response = requests.get(url)
    physical_data = response.json()['results']
    with open(f'casestudy/data/raw/{match_id}/physical.json', 'w', encoding='utf-8') as f:
        json.dump(physical_data, f, ensure_ascii=False, indent=4)

def get_event_off_ball_runs(match_id, file_format='csv'):
    url = f'https://skillcorner.com/api/match/{match_id}/dynamic_events/off_ball_runs/?file_format={file_format}&token=3e43d8743069d3c9d589'
    response = requests.get(url)
    with open(f'casestudy/data/raw/{match_id}/off_ball_runs.{file_format}', 'w', encoding='utf-8') as f:
        f.write(response.text)

def get_event_passing_options(match_id, file_format='csv'):
    url = f'https://skillcorner.com/api/match/{match_id}/dynamic_events/passing_options/?file_format={file_format}&token=3e43d8743069d3c9d589'
    response = requests.get(url)
    with open(f'casestudy/data/raw/{match_id}/passing_options.{file_format}', 'w', encoding='utf-8') as f:
        f.write(response.text)

def get_event_on_ball_engagements(match_id, file_format='csv'):
    url = f'https://skillcorner.com/api/match/{match_id}/dynamic_events/on_ball_engagements/?file_format={file_format}&token=3e43d8743069d3c9d589'
    response = requests.get(url)
    with open(f'casestudy/data/raw/{match_id}/on_ball_engagements.{file_format}', 'w', encoding='utf-8') as f:
        f.write(response.text)

def get_event_phases_of_play(match_id, file_format='csv'):
    url = f'https://skillcorner.com/api/match/{match_id}/dynamic_events/phases_of_play/?file_format={file_format}&token=3e43d8743069d3c9d589'
    response = requests.get(url)
    with open(f'casestudy/data/raw/{match_id}/phases_of_play.{file_format}', 'w', encoding='utf-8') as f:
        f.write(response.text)

def get_event_player_possessions(match_id, file_format='csv'):
    url = f'https://skillcorner.com/api/match/{match_id}/dynamic_events/player_possessions/?file_format={file_format}&token=3e43d8743069d3c9d589'
    response = requests.get(url)
    with open(f'casestudy/data/raw/{match_id}/player_possessions.{file_format}', 'w', encoding='utf-8') as f:
        f.write(response.text)
        
def get_event_tracking(match_id, file_format='jsonl'):
    url = f'https://skillcorner.com/api/match/{match_id}/tracking/?file_format={file_format}&data_version=3&token=3e43d8743069d3c9d589'
    response = requests.get(url)
    print(response.status_code)
    with open(f'casestudy/data/raw/{match_id}/tracking.{file_format}', 'w', encoding='utf-8') as f:
        f.write(response.text)
        
def get_match_events_data(match_id):
    event_df = pd.read_csv(f"casestudy/data/raw/{match_id}/dynamic_events.csv").drop(columns=['index'])
    offbr = pd.read_csv(f"casestudy/data/raw/{match_id}/off_ball_runs.csv").drop(columns=['index'])
    #onbe = pd.read_csv(f"casestudy/data/raw/events/on_ball_engagements/{match_id}.csv").drop(columns=['index'])
    pposs = pd.read_csv(f"casestudy/data/raw/{match_id}/player_possessions.csv").drop(columns=['index'])
    pop = pd.read_csv(f"casestudy/data/raw/{match_id}/phases_of_play.csv").drop(columns=['index'])
    po = pd.read_csv(f"casestudy/data/raw/{match_id}/passing_options.csv").drop(columns=['index'])
    
    events_df = event_df.merge(offbr, on=['match_id'], how="inner", suffixes=("", "_offbr")).reset_index(drop=True)
    events_df = event_df.merge(pposs, on=['match_id'], how="inner", suffixes=("", "_pposs")).reset_index(drop=True)
    events_df = event_df.merge(pop, on=['match_id'], how="inner", suffixes=("", "_pop")).reset_index(drop=True)
    events_df = event_df.merge(po, on=['match_id'], how="inner", suffixes=("", "_po")).reset_index(drop=True)

    return events_df
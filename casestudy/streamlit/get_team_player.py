import pandas as pd
import json

def load_teams():
    teams = []
    with open('casestudy/data/raw/matches_list.json', 'r', encoding='utf-8') as f:
        matches_list = json.load(f)
    for match in matches_list:
        teams.append(match['home_team']['short_name'])
    teams_list = pd.DataFrame(teams, columns=['team_name'])
    teams_list = teams_list.drop_duplicates().sort_values(by='team_name').reset_index(drop=True)
    return teams_list

def load_players():
    players = []
    with open('casestudy/data/raw/matches_list.json', 'r', encoding='utf-8') as f:
        matches_list = json.load(f)
    for match in matches_list:
        with open(f'casestudy/data/raw/{match["id"]}/matches_infos.json', 'r', encoding='utf-8') as f: 
            match_infos = json.load(f)
        for player in match_infos['players']:
            players.append(player['short_name'])
    players_list = pd.DataFrame(players, columns=['player_name'])
    players_list = players_list.drop_duplicates().sort_values(by='player_name').reset_index(drop=True)
    return players_list

def load_aggregated_data(match_id): 
    return pd.read_parquet(f"casestudy/data/analytics/{match_id}/aggregated_data.parquet")
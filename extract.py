import http.client
import os
from dotenv import load_dotenv  # pip install python-dotenv
import json
import pandas as pd  # pip install pandas
import math


def extract_teams() -> pd.DataFrame:
    """
    Extracts team data via the API, filtering for NBA franchise teams.

    :return: A Pandas DataFrame containing NBA team information with the columns: name, nickname, code, city, logo
    """
    # Request team data from the API
    connection.request("GET", "/teams?league=standard", headers=headers)
    response = connection.getresponse()
    data = response.read()

    # Parse JSON response and filter for NBA franchises
    teams = []
    json_data = json.loads(data)
    for team in json_data['response']:
        if team.get('nbaFranchise') is True and team.get('allStar') is False:
            team['conference'] = team['leagues']['standard'].get('conference')
            team['division'] = team['leagues']['standard'].get('division')
            teams.append(team)

    # Create DataFrame containing the selected columns
    team_columns = ['name', 'nickname', 'code', 'city', 'conference', 'division']
    data_frame = pd.DataFrame(teams)[team_columns]
    print(data_frame.to_string())
    return data_frame


def extract_players() -> pd.DataFrame:
    """
    Extracts player data via the API.

    :return: A Pandas DataFrame containing NBA player information with the columns:
                id, firstname, lastname, team, position, college, birthdate, rookie_year, height_feet,
                height_inches, weight_pounds, jersey_number
    """

    # Request team data from the API - First API call
    connection.request("GET", "/players?team=1&season=2021", headers=headers)
    response = connection.getresponse()
    data = response.read()
    json_data = json.loads(data)
    player_indexes = {}  #

    # Parse JSON response for player info
    players = []
    for i, player in enumerate(json_data['response']):
        player_indexes[player.get('id')] = i  # Used when adding the player's team in the second API call
        player['college'] = player.get('college')
        player['birthdate'] = player['birth'].get('date')
        player['rookie_year'] = "None" if player['nba'].get('start') == 0 else player['nba'].get('start')
        player['height_feet'] = "None" if player['height'].get('feets') in (None, "None") else float(player['height'].get('feets'))
        player['height_inches'] = "None" if player['height'].get('inches') in (None, "None") else float(player['height'].get('inches'))
        player['weight_pounds'] = "None" if player['weight'].get('pounds') in (None, "None") else float(player['weight'].get('pounds'))
        player['jersey_number'] = player.get('leagues', {}).get('standard', {}).get('jersey', "None")
        player['position'] = player.get('leagues', {}).get('standard', {}).get('pos', "None")
        player['team_id'] = "None"  # Team is updated in the second API call
        players.append(player)

    # Second API call - Retrieve team info for each player
    connection.request("GET", "/players/statistics?team=1&season=2021", headers=headers)
    response = connection.getresponse()
    data = response.read()
    json_data = json.loads(data)
    for player_stat in json_data['response']:
        player_id = player_stat['player'].get('id')

        if player_id in player_indexes:
            index = player_indexes[player_id]
            players[index]['team_id'] = player_stat.get('team', {}).get('id', "None")

    # Create DataFrame containing the selected columns
    player_columns = ['id', 'firstname', 'lastname', 'team_id', 'position', 'college', 'birthdate', 'rookie_year', 'height_feet', 'height_inches', 'weight_pounds', 'jersey_number']
    player_data_frame = pd.DataFrame(players)[player_columns]
    print(player_data_frame.to_string())
    return player_data_frame


def extract_games() -> pd.DataFrame:
    """
    Extracts game data in specified season via the API, filtering for NBA franchise teams.

    :return: A Pandas DataFrame containing NBA game information within a season with the columns: id, season, date, duration, arena_name,
                arena_location, home_team, home_team_id, visitor_team, visitor_team_id, winning_team, overtime, home_quarter_points,
                home_points, visitor_quarter_points, visitor_points, times_tied, lead_changes
    """

    # Request game data for seasons 2021-2023 from the API
    games = []
    seasons = ['2021', '2022', '2023']
    for season in seasons:
        endpoint = '/games?league=standard&season=' + season
        connection.request("GET", endpoint, headers=headers)
        response = connection.getresponse()
        data = response.read()

        # Parse JSON response for game data
        json_data = json.loads(data)
        for game in json_data['response']:
            game['id'] = game.get('id')
            game['season'] = game.get('season')
            game['duration'] = game['date'].get('duration') if game['date'].get('duration') is not None else pd.NA
            game['date'] = game['date'].get('start')[5:10] + "-" + game['date'].get('start')[0:4]
            game['arena_name'] = pd.NA if game['arena'].get('name') is None else game['arena'].get('name')
            game['arena_location'] = pd.NA if game['arena'].get('city') is None \
                else pd.NA if game['arena'].get('state') is None \
                else game['arena'].get('city') + ", " + game['arena'].get('state')
            game['home_team_id'] = game['teams']['home'].get('id')
            game['home_team'] = game['teams']['home'].get('name')
            game['visitor_team_id'] = game['teams']['visitors'].get('id')
            game['visitor_team'] = game['teams']['visitors'].get('name')
            game['winning_team'] = pd.NA if game['scores']['home'].get('points') is None \
                else game['home_team'] if game['scores']['home'].get('points') > game['scores']['visitors'].get('points') \
                else game['visitor_team']
            game['overtime'] = 'Yes' if game['periods'].get('current') > 4 else 'No'
            game['home_quarter_points'] = ""
            for quarter in game['scores']['home']['linescore']:
                game['home_quarter_points'] += quarter + ", "
            game['home_quarter_points'] = game['home_quarter_points'][:-2]
            game['home_points'] = game['scores']['home']['points'] if game['scores']['home']['points'] is not None else pd.NA
            game['visitor_quarter_points'] = ""
            for quarter in game['scores']['visitors']['linescore']:
                game['visitor_quarter_points'] += quarter + ", "
            game['visitor_quarter_points'] = game['visitor_quarter_points'][:-2]
            game['visitor_points'] = game['scores']['visitors']['points'] if game['scores']['visitors']['points'] is not None else pd.NA
            game['times_tied'] = game.get('timesTied') if game.get('timesTied') is not None else pd.NA
            game['lead_changes'] = game.get('leadChanges') if game.get('leadChanges') is not None else pd.NA
            games.append(game)

    # Create DataFrame containing the selected columns
    game_columns = ['id', 'season', 'date', 'duration', 'arena_name', 'arena_location', 'home_team_id', 'home_team', 'visitor_team_id',
                    'visitor_team', 'winning_team', 'overtime', 'home_quarter_points', 'home_points', 'visitor_quarter_points', 'visitor_points',
                    'times_tied', 'lead_changes']
    game_data_frame = pd.DataFrame(games)[game_columns]

    # Drop postponed / canceled / unfulfilled playoff games
    game_data_frame.dropna(subset=['home_points', 'visitor_points'], inplace=True)
    game_data_frame.reset_index(drop=True, inplace=True)
    print(game_data_frame.to_string())
    return game_data_frame

# Retrieve API and Database credentials from local .env file
load_dotenv()
API_URL = os.getenv("API_URL")
API_KEY = os.getenv("API_KEY")
# DB_HOST = os.getenv("DB_HOST")
# DB_USER = os.getenv("DB_USER")
# DB_PW = os.getenv("DB_PASS")
# DB_NAME = os.getenv("DB_NAME")

# Verify API and DB credentials are present within the .env file
if not API_URL:
    raise ValueError("API_URL not found in .env file. Please check your configuration.")

if not API_KEY:
    raise ValueError("API_KEY not found in .env file. Please check your configuration.")

# Database credentials commented out until created
# if not DB_HOST:
    # raise ValueError("DB_HOST not found in .env file. Please check your configuration.")

# if not DB_USER:
    # raise ValueError("DB_USER not found in .env file. Please check your configuration.")

# if not DB_PW:
    # raise ValueError("DB_PW not found in .env file. Please check your configuration.")

# if not DB_NAME:
    # raise ValueError("DB_NAME not found in .env file. Please check your configuration.")

connection = http.client.HTTPSConnection(API_URL)

headers = {
    'x-rapidapi-host': API_URL,
    'x-rapidapi-key': API_KEY,
    }

# teams_df = extract_teams()
# players_df = extract_players()
games_df = extract_games()

connection.close()
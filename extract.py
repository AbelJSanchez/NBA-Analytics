import http.client
import os
from dotenv import load_dotenv  # pip install python-dotenv
import json
import pandas as pd  # pip install pandas

pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', None)  # Use maximum width
pd.set_option('display.max_rows', None)  # Use maximum width

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
            team['conference'] = team.get('leagues', {}).get('standard', {}).get('conference')
            team['division'] = team.get('leagues', {}).get('standard', {}).get('division')
            teams.append(team)

    # Create DataFrame containing the selected columns
    team_columns = ['id', 'name', 'nickname', 'code', 'city', 'conference', 'division']
    team_data_frame = pd.DataFrame(teams)[team_columns]
    print(team_data_frame)
    return team_data_frame


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
    print(player_data_frame)
    return player_data_frame


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

teams_df = extract_teams()
players_df = extract_players()

connection.close()

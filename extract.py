import http.client
import os
from dotenv import load_dotenv  # pip install python-dotenv
import json
import pandas as pd  # pip install pandas

pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', None)  # Use maximum width of terminal
pd.set_option('display.max_rows', 250)  # Show up to 250 rows

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
            team['team_id'] = team.get('id')
            team['conference'] = team.get('leagues', {}).get('standard', {}).get('conference')
            team['division'] = team.get('leagues', {}).get('standard', {}).get('division')
            teams.append(team)

    # Create DataFrame containing the selected columns
    team_columns = ['team_id', 'name', 'nickname', 'code', 'city', 'conference', 'division']
    team_data_frame = pd.DataFrame(teams)[team_columns]
    return team_data_frame


def extract_players() -> pd.DataFrame:
    """
    Extracts player data via the API.

    :return: A Pandas DataFrame containing NBA player information with the columns:
                id, firstname, lastname, position, college, birthdate, rookie_year, height_feet,
                height_inches, weight_pounds, jersey_number
    """
    team_ids = teams_df['team_id'].tolist()
    seasons = [2021, 2022, 2023]
    players = []
    seen_players = set()

    # API only allows to query one season and one team at a time
    for season in seasons:
        for team_id in team_ids:
            # Request player data from the API
            connection.request("GET", f"/players?team={team_id}&season={season}", headers=headers)
            response = connection.getresponse()
            data = response.read()
            json_data = json.loads(data)

            # Parse JSON response for player info
            for player in json_data['response']:
                # Check to see if we've already added that player
                if player.get('id') in seen_players:
                    continue

                # Add player to set and player dictionary
                seen_players.add(player.get('id'))
                player['player_id'] = player.get('id')
                player['birthdate'] = player['birth'].get('date')
                player['rookie_year'] = "None" if player['nba'].get('start') == 0 else player['nba'].get('start')
                player['height_feet'] = "None" if player['height'].get('feets') in (None, "None") else float(player['height'].get('feets'))
                player['height_inches'] = "None" if player['height'].get('inches') in (None, "None") else float(player['height'].get('inches'))
                player['weight_pounds'] = "None" if player['weight'].get('pounds') in (None, "None") else float(player['weight'].get('pounds'))
                player['jersey_number'] = player.get('leagues', {}).get('standard', {}).get('jersey', "None")
                player['position'] = player.get('leagues', {}).get('standard', {}).get('pos', "None")
                players.append(player)

    # Create DataFrame containing the selected columns
    player_columns = ['player_id', 'firstname', 'lastname', 'position', 'college', 'birthdate', 'rookie_year', 'height_feet', 'height_inches', 'weight_pounds', 'jersey_number']
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

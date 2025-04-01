import http.client
import os
from dotenv import load_dotenv  # pip install python-dotenv
import json
import pandas as pd  # pip install pandas
import time
from sqlalchemy import create_engine  # pip install sqlalchemy
# import mysql.connector  # pip install mysql-connector-python

API_CALLS = 0  # Global variable that tracks the current number of API calls
MAX_CALLS = 10  # The maximum number of API calls per minute

pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', None)  # Use maximum width of terminal
pd.set_option('display.max_rows', None)  # Show all rows


def extract_teams() -> pd.DataFrame:
    """
    Extracts team data via the API, filtering for NBA franchise teams.

    :return: A Pandas DataFrame containing NBA team information with the columns: name, nickname, code, city, logo
    """
    # Request team data from the API
    
    global API_CALLS
    api_connection.request("GET", "/teams?league=standard", headers=headers)
    API_CALLS += 1
    response = api_connection.getresponse()
    data = response.read()
    # print(data)  # Uncomment to debug
    json_data = json.loads(data)
    teams = []

    # Parse response and filter for NBA franchises
    for team in json_data['response']:
        if team.get('nbaFranchise') is True and team.get('allStar') is False:
            team['team_id'] = team.get('id')
            team['mascot'] = team.get('nickname')
            team['abv'] = team.get('code')
            team['conference'] = team.get('leagues', {}).get('standard', {}).get('conference')
            team['division'] = team.get('leagues', {}).get('standard', {}).get('division')
            teams.append(team)

    # Create DataFrame containing the selected columns
    team_columns = ['team_id', 'name', 'mascot', 'abv', 'city', 'conference', 'division']
    team_data_frame = pd.DataFrame(teams)[team_columns]
    # print(team_data_frame)  # Uncomment to debug
    return team_data_frame


def extract_players() -> pd.DataFrame:
    """
    Extracts player data via the API.

    :return: A Pandas DataFrame containing NBA player information with the columns:
                id, firstname, lastname, position, college, birthdate, rookie_year, height_feet,
                height_inches, weight_pounds, jersey_number
    """
    global API_CALLS, MAX_CALLS
    team_ids = teams_df['team_id'].tolist()
    seasons = [2021, 2022, 2023]
    players = []
    seen_players = set()

    # API only allows us to query one season and one team at a time
    for season in seasons:
        for team_id in team_ids:
            # API only allows 10 calls per minute
            if API_CALLS == MAX_CALLS:
                time.sleep(60)
                API_CALLS = 0

            # Request player data from the API
            api_connection.request("GET", f'/players?season={season}&team={team_id}', headers=headers)
            API_CALLS += 1
            response = api_connection.getresponse()
            data = response.read()
            # print(data)  # Uncomment to debug
            json_data = json.loads(data)

            # Parse response for player info
            for player in json_data['response']:
                # Check to see if we've already added that player
                if player.get('id') in seen_players:
                    continue

                # Add player to set and player dictionary
                seen_players.add(player.get('id'))
                player['player_id'] = player.get('id')
                player['school'] = pd.NA if player.get('college') is None else player.get('college')
                player['birthdate'] = pd.NA if player['birth'].get('date') is None else player['birth'].get('date')
                player['rookie_year'] = pd.NA if player['nba'].get('start') == 0 else player['nba'].get('start')
                player['years_pro'] = pd.NA if player['nba'].get('pro') in [0, None] else player['nba'].get('pro')
                player['height'] = pd.NA if player['height'].get('feets') is None else (int(player['height'].get('feets')) * 12) + int(player['height'].get('inches'))
                player['weight'] = pd.NA if player['weight'].get('pounds') is None else int(player['weight'].get('pounds'))
                player['jersey_number'] = pd.NA if player.get('leagues', {}).get('standard', {}).get('jersey') is None else player.get('leagues', {}).get('standard', {}).get('jersey')
                players.append(player)

    # Create DataFrame containing the selected columns
    player_columns = ['player_id', 'firstname', 'lastname', 'school', 'birthdate', 'rookie_year', 'years_pro', 'height', 'weight', 'jersey_number']
    player_data_frame = pd.DataFrame(players)[player_columns]
    # print(player_data_frame)  # Uncomment to debug
    return player_data_frame


def extract_player_stats() -> pd.DataFrame:
    """
    Extracts player stat data via the API.

    :return: A Pandas DataFrame containing NBA player statistics
    """
    global API_CALLS, MAX_CALLS
    player_stats = []
    seasons = ['2021', '2022', '2023']
    team_ids = teams_df['team_id'].tolist()

    # API only allows us to query one season and one team at a time
    for season in seasons:
        for team_id in team_ids:
            # API only allows 10 calls per minute
            if API_CALLS == MAX_CALLS:
                time.sleep(60)
                API_CALLS = 0

            # Request player stats from the API
            api_connection.request("GET", f"/players/statistics?team={team_id}&season={season}", headers=headers)
            API_CALLS += 1
            response = api_connection.getresponse()
            data = response.read()
            json_data = json.loads(data)
            # print(data)  # Uncomment to debug

            # Parse response for player stats per game
            for player_stat in json_data['response']:
                # if the player did not play in the game
                if player_stat.get('min') == '--':
                    continue
                player_stat['player_id'] = player_stat.get('player', {}).get('id')
                player_stat['game_id'] = player_stat.get('game', {}).get('id')
                player_stat['team_id'] = player_stat.get('team', {}).get('id')
                player_stat['season'] = season
                player_stat['position'] = player_stat.get('pos')
                player_stat['minutes_played'] = int(player_stat.get('min'))
                player_stat['fgp'] = float(player_stat.get('fgp'))
                player_stat['ftp'] = float(player_stat.get('ftp'))
                player_stat['tpp'] = float(player_stat.get('tpp'))
                player_stat['position'] = pd.NA if player_stat.get('pos') is None else player_stat.get('pos')
                player_stat['off_reb'] = player_stat.get('offReb')
                player_stat['def_reb'] = player_stat.get('defReb')
                player_stat['tot_reb'] = player_stat.get('totReb')
                player_stat['p_fouls'] = player_stat.get('pFouls')
                player_stat['plus_minus'] = 0 if player_stat.get('plusMinus') == '--' else int(player_stat.get('plusMinus'))
                player_stats.append(player_stat)

    player_stat_columns = ['player_id', 'game_id', 'team_id', 'season', 'points', 'position', 'minutes_played', 'fgm',
                           'fga', 'fgp', 'ftm', 'fta', 'ftp', 'tpm', 'tpa', 'tpp', 'off_reb', 'def_reb', 'tot_reb', 'assists',
                           'p_fouls', 'steals', 'turnovers', 'blocks', 'plus_minus']
    player_stat_data_frame = pd.DataFrame(player_stats)[player_stat_columns]
    # print(player_stat_data_frame)  # Uncomment to debug
    return player_stat_data_frame
  

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
        api_connection.request("GET", endpoint, headers=headers)
        response = api_connection.getresponse()
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
    # print(game_data_frame.to_string())  # Uncomment to debug
    return game_data_frame


# Retrieve API and Database credentials from local .env file
load_dotenv()
API_URL = os.getenv("API_URL")
API_KEY = os.getenv("API_KEY")
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PW = os.getenv("DB_PW")
DB_NAME = os.getenv("DB_NAME")

# Verify API and DB credentials are present within the .env file
if not API_URL:
    raise ValueError("API_URL not found in .env file. Please check your configuration.")

if not API_KEY:
    raise ValueError("API_KEY not found in .env file. Please check your configuration.")

if not DB_HOST:
    raise ValueError("DB_HOST not found in .env file. Please check your configuration.")

if not DB_USER:
    raise ValueError("DB_USER not found in .env file. Please check your configuration.")

if not DB_PW:
    raise ValueError("DB_PW not found in .env file. Please check your configuration.")

if not DB_NAME:
    raise ValueError("DB_NAME not found in .env file. Please check your configuration.")

# API connection and data extraction (EXTRACT/TRANSFORM)
api_connection = http.client.HTTPSConnection(API_URL)

headers = {
    'x-rapidapi-host': API_URL,
    'x-rapidapi-key': API_KEY,
    }

teams_df = extract_teams()
games_df = extract_games()
players_df = extract_players()
player_stats_df = extract_player_stats()

api_connection.close()

host = DB_HOST
user = DB_USER
passwd = DB_PW
database = DB_NAME

# Database connection and data insertion (LOAD)
connection_string = f'mysql+mysqlconnector://{user}:{passwd}@{host}/{database}'
engine = create_engine(connection_string)

teams_df.to_sql('teams', con=engine, if_exists='append', index=False)
games_df.to_sql('games', con=engine, if_exists='append', index=False)
players_df.to_sql('players', con=engine, if_exists='append', index=False)
player_stats_df.to_sql('playerstats', con=engine, if_exists='append', index=False)

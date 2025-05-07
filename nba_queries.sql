CREATE DATABASE NBA_STATS;

DROP TABLE IF EXISTS teams;
DROP TABLE IF EXISTS playerStats;
DROP TABLE IF EXISTS games;
DROP TABLE IF EXISTS players;

CREATE TABLE teams (
    team_id INT NOT NULL,
    name VARCHAR(50) NOT NULL,
    mascot VARCHAR(30) NOT NULL,
    abv CHAR(3) NOT NULL,
    city VARCHAR(30) NOT NULL,
    conference VARCHAR(30) NOT NULL,
    division VARCHAR(30) NOT NULL,
    PRIMARY KEY (team_id)
);

CREATE TABLE games (
    game_id INT PRIMARY KEY NOT NULL,
    season INT NOT NULL,
    date DATE NOT NULL,
    duration CHAR(4),
    arena_name VARCHAR(30),
    arena_location VARCHAR(20),
    home_team_id INT NOT NULL,
    home_team VARCHAR(30) NOT NULL,
    visitor_team_id INT NOT NULL,
    visitor_team VARCHAR(30) NOT NULL,
    winning_team VARCHAR(30) NOT NULL,
    overtime VARCHAR(3) NOT NULL,
    home_quarter_points VARCHAR(30) NOT NULL,
    home_points INT NOT NULL,
    visitor_quarter_points VARCHAR(30) NOT NULL,
    visitor_points INT NOT NULL,
    times_tied INT,
    lead_changes INT,
    CONSTRAINT FOREIGN KEY (home_team_id) REFERENCES teams(team_id),
    CONSTRAINT FOREIGN KEY (visitor_team_id) REFERENCES teams(team_id)
);

CREATE TABLE players (
    player_id INT NOT NULL,
    firstname VARCHAR(30),
    lastname VARCHAR(30),
    school VARCHAR(30),
    birthdate DATE,
    rookie_year INT,
    years_pro INT,
    height INT,
    weights INT,
    jersey_number INT,
    PRIMARY KEY (player_id)
);

CREATE TABLE playerStats (
    player_stat_id INT NOT NULL AUTO_INCREMENT,
    player_id INT NOT NULL,
    game_id INT NOT NULL,
    team_id INT NOT NULL,
    season INT NOT NULL,
    points INT,
    position varchar(2),
    minutes_played INT,
    fgm INT,
    fga INT,
    fgp FLOAT,
    ftm INT,
    fta INT,
    ftp FLOAT,
    tpm INT,
    tpa INT,
    tpp FLOAT,
    off_reb INT,
    def_reb INT,
    tot_reb INT,
    assists INT,
    p_fouls INT,
    steals INT,
    turnovers INT,
    blocks INT,
    plus_minus INT,
    PRIMARY KEY (player_stat_id),
    CONSTRAINT FOREIGN KEY (team_id) REFERENCES teams(team_id),
    CONSTRAINT FOREIGN KEY (player_id) REFERENCES players(player_id),
    CONSTRAINT FOREIGN KEY (game_id) REFERENCES games(game_id)
);
#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Insert Schedule / Results data into Neon Database


import pandas as pd
from euroleague_api.game_stats import GameStats
import psycopg2
from psycopg2.extras import execute_values
import os

def create_team_records_dataset(df: pd.DataFrame, competition_type: str) -> pd.DataFrame:
    all_team_records = []
    all_teams = set(df['local.club.name'].unique()).union(df['road.club.name'].unique())

    if competition_type == 'E':
        phase_order = {'RS': 0, 'PI': 1, 'PO': 2, 'FF': 3}
    elif competition_type == 'U':
        phase_order = {'RS': 0, '8F': 1, '4F': 2}
    else:
        raise ValueError("Invalid competition_type. Must be 'E' for Euroleague or 'U' for Eurocup.")

    for team in all_teams:
        team_games = df[(df['local.club.name'] == team) | (df['road.club.name'] == team)].copy()

        team_games['PhaseOrder'] = team_games['Phase'].map(phase_order)
        team_games = team_games.sort_values(['Season', 'PhaseOrder', 'Round', 'localDate'])

        for season, season_games in team_games.groupby('Season'):
            wins = 0
            losses = 0
            current_phase_group = None

            for idx, game in season_games.iterrows():
                if game['Phase'] == 'RS':
                    phase_group = 'RS'
                elif competition_type == 'E' and game['Phase'] in ['PI', 'PO', 'FF']:
                    phase_group = 'POSTSEASON'
                elif competition_type == 'U' and game['Phase'] in ['8F', '4F']:
                    phase_group = 'POSTSEASON'
                else:
                    phase_group = game['Phase']

                if current_phase_group != phase_group:
                    current_phase_group = phase_group
                    wins = 0
                    losses = 0

                if game['local.club.name'] == team:
                    location = 'Home'
                    team_score = game['local.score']
                    opponent_score = game['road.score']
                    opponent = game['road.club.name']
                    team_code = game['local.club.code']
                    team_image = game['local.club.images.crest']
                    opponent_code = game['road.club.code']
                    opponent_image = game['road.club.images.crest']
                else:
                    location = 'Away'
                    team_score = game['road.score']
                    opponent_score = game['local.score']
                    opponent = game['local.club.name']
                    team_code = game['road.club.code']
                    team_image = game['road.club.images.crest']
                    opponent_code = game['local.club.code']
                    opponent_image = game['local.club.images.crest']

                if team_score > opponent_score:
                    result = 'Win'
                    wins += 1
                elif team_score < opponent_score:
                    result = 'Loss'
                    losses += 1
                else:
                    result = 'Draw'

                record = f"{wins}-{losses}"

                all_team_records.append({
                    'Team': team,
                    'TeamCode': team_code,
                    'TeamImage': team_image,
                    'Date': game['localDate'],
                    'Opponent': opponent,
                    'OpponentCode': opponent_code,
                    'OpponentImage': opponent_image,
                    'Round': game['Round'],
                    'Result': result,
                    'Location': location,
                    'Record': record,
                    'Team_Score': team_score,
                    'Opponent_Score': opponent_score,
                    'Gamecode': game['Gamecode'],
                    'Season': game['Season'],
                    'Phase': game['Phase'],
                    'PhaseGroup': phase_group
                })

    team_records_df = pd.DataFrame(all_team_records)
    team_records_df = team_records_df.sort_values(['Team', 'Season', 'PhaseGroup', 'Round', 'Date'])

    return team_records_df


def insert_schedule_results_to_db(team_records_df: pd.DataFrame, table_name: str):
    conn_str = os.getenv("DATABASE_URL")
    if not conn_str:
        raise ValueError("DATABASE_URL environment variable not set.")

    conn = psycopg2.connect(conn_str)
    cursor = conn.cursor()

    try:
        cursor.execute(f"""
        DROP TABLE IF EXISTS {table_name};
        CREATE TABLE {table_name} (
            id SERIAL PRIMARY KEY,
            team TEXT,
            teamcode TEXT,
            teamlogo TEXT,
            game_date TEXT,
            opponent TEXT,
            opponentcode TEXT,
            opponentlogo TEXT,
            round INTEGER,
            result TEXT,
            location TEXT,
            record TEXT,
            team_score INTEGER,
            opponent_score INTEGER,
            gamecode TEXT,
            season INTEGER,
            phase TEXT,
            UNIQUE(team, gamecode, season)
        );
        """)
        conn.commit()

        def safe_int(val):
            if pd.isna(val):
                return None
            try:
                return int(val)
            except (ValueError, TypeError):
                return None

        data_tuples = []
        for _, row in team_records_df.iterrows():
            data_tuples.append((
                row["Team"],
                row["TeamCode"],
                row["TeamImage"],
                row["Date"],
                row["Opponent"],
                row["OpponentCode"],
                row["OpponentImage"],
                safe_int(row["Round"]),
                row["Result"],
                row["Location"],
                row["Record"],
                safe_int(row["Team_Score"]),
                safe_int(row["Opponent_Score"]),
                row["Gamecode"],
                safe_int(row["Season"]),
                row["Phase"]
            ))

        insert_query = f"""
        INSERT INTO {table_name} (
            team, teamcode, teamlogo, game_date, opponent, opponentcode, 
            opponentlogo, round, result, location, record, team_score, 
            opponent_score, gamecode, season, phase
        ) VALUES %s
        ON CONFLICT (team, gamecode, season) DO UPDATE SET
            teamcode = EXCLUDED.teamcode,
            teamlogo = EXCLUDED.teamlogo,
            game_date = EXCLUDED.game_date,
            opponent = EXCLUDED.opponent,
            opponentcode = EXCLUDED.opponentcode,
            opponentlogo = EXCLUDED.opponentlogo,
            round = EXCLUDED.round,
            result = EXCLUDED.result,
            location = EXCLUDED.location,
            record = EXCLUDED.record,
            team_score = EXCLUDED.team_score,
            opponent_score = EXCLUDED.opponent_score,
            phase = EXCLUDED.phase;
        """

        execute_values(cursor, insert_query, data_tuples)
        conn.commit()

    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

def update_euro_leagues_schedule_results(competition_type: str):
    if competition_type == 'E':
        table_name = 'schedule_results_euroleague'
    elif competition_type == 'U':
        table_name = 'schedule_results_eurocup'
    else:
        raise ValueError("Invalid competition_type. Must be 'E' for Euroleague or 'U' for Eurocup.")

    gs = GameStats(competition_type)
    gamestats = gs.get_game_reports_range_seasons(2017, 2024)

    team_records_df = create_team_records_dataset(gamestats, competition_type)
    insert_schedule_results_to_db(team_records_df, table_name)

update_euro_leagues_schedule_results('E')

update_euro_leagues_schedule_results('U')


# In[ ]:





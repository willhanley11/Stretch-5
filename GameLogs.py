#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Insert Gamelogs into Neon Database 

import pandas as pd
from euroleague_api.boxscore_data import BoxScoreData
import psycopg2
from psycopg2.extras import execute_values
import os

def calculate_game_sequence(df: pd.DataFrame) -> pd.DataFrame:
    player_mask = ~df['Player_ID'].isin(['Team', 'Total'])
    df.loc[player_mask, 'GameSequence'] = df[player_mask].groupby('Player_ID').cumcount() + 1
    df.loc[~player_mask, 'GameSequence'] = None
    return df

def insert_game_logs_to_db(game_logs_df: pd.DataFrame, table_name: str):
    conn_str = os.getenv("DATABASE_URL")
    if not conn_str:
        raise ValueError("DATABASE_URL environment variable not set.")

    conn = psycopg2.connect(conn_str)
    cursor = conn.cursor()

    try:
        game_logs_df = game_logs_df.copy()
        game_logs_df['row_number'] = game_logs_df.groupby(['Player_ID', 'Gamecode', 'Season', 'Team']).cumcount() + 1

        cursor.execute(f"""
        DROP TABLE IF EXISTS {table_name};
        CREATE TABLE {table_name} (
            id SERIAL PRIMARY KEY,
            season INTEGER,
            phase TEXT,
            round INTEGER,
            gamecode TEXT,
            home INTEGER,
            player_id TEXT,
            is_starter REAL,
            is_playing REAL,
            team TEXT,
            dorsal INTEGER,
            player TEXT,
            minutes TEXT,
            points INTEGER,
            field_goals_made_2 INTEGER,
            field_goals_attempted_2 INTEGER,
            field_goals_made_3 INTEGER,
            field_goals_attempted_3 INTEGER,
            free_throws_made INTEGER,
            free_throws_attempted INTEGER,
            offensive_rebounds INTEGER,
            defensive_rebounds INTEGER,
            total_rebounds INTEGER,
            assistances INTEGER,
            steals INTEGER,
            turnovers INTEGER,
            blocks_favour INTEGER,
            blocks_against INTEGER,
            fouls_commited INTEGER,
            fouls_received INTEGER,
            valuation INTEGER,
            plusminus REAL,
            game_sequence INTEGER,
            season_round TEXT,
            row_type TEXT DEFAULT 'player',
            row_number INTEGER DEFAULT 1,
            UNIQUE(player_id, gamecode, season, team, row_number)
        );
        """)
        conn.commit()

        def safe_int(val):
            if pd.isna(val) or val == 'DNP' or val == 'None':
                return None
            try:
                return int(float(val))
            except (ValueError, TypeError):
                return None

        def safe_float(val):
            if pd.isna(val) or val == 'None':
                return None
            try:
                return float(val)
            except (ValueError, TypeError):
                return None

        def safe_str(val):
            if pd.isna(val) or val == 'None':
                return None
            return str(val)

        data_tuples = []
        for _, row in game_logs_df.iterrows():
            try:
                if row["Player_ID"] == 'Team':
                    row_type = 'team'
                elif row["Player_ID"] == 'Total':
                    row_type = 'total'
                else:
                    row_type = 'player'

                data_tuples.append((
                    safe_int(row["Season"]),
                    safe_str(row["Phase"]),
                    safe_int(row["Round"]),
                    safe_str(row["Gamecode"]),
                    safe_int(row["Home"]),
                    safe_str(row["Player_ID"]),
                    safe_float(row["IsStarter"]),
                    safe_float(row["IsPlaying"]),
                    safe_str(row["Team"]),
                    safe_int(row["Dorsal"]),
                    safe_str(row["Player"]),
                    safe_str(row["Minutes"]),
                    safe_int(row["Points"]),
                    safe_int(row["FieldGoalsMade2"]),
                    safe_int(row["FieldGoalsAttempted2"]),
                    safe_int(row["FieldGoalsMade3"]),
                    safe_int(row["FieldGoalsAttempted3"]),
                    safe_int(row["FreeThrowsMade"]),
                    safe_int(row["FreeThrowsAttempted"]),
                    safe_int(row["OffensiveRebounds"]),
                    safe_int(row["DefensiveRebounds"]),
                    safe_int(row["TotalRebounds"]),
                    safe_int(row["Assistances"]),
                    safe_int(row["Steals"]),
                    safe_int(row["Turnovers"]),
                    safe_int(row["BlocksFavour"]),
                    safe_int(row["BlocksAgainst"]),
                    safe_int(row["FoulsCommited"]),
                    safe_int(row["FoulsReceived"]),
                    safe_int(row["Valuation"]),
                    safe_float(row["Plusminus"]),
                    safe_int(row["GameSequence"]),
                    safe_str(row["SeasonRound"]),
                    row_type,
                    safe_int(row["row_number"])
                ))
            except Exception as e:
                pass

        insert_query = f"""
        INSERT INTO {table_name} (
            season, phase, round, gamecode, home, player_id, is_starter, is_playing,
            team, dorsal, player, minutes, points, field_goals_made_2, field_goals_attempted_2,
            field_goals_made_3, field_goals_attempted_3, free_throws_made, free_throws_attempted,
            offensive_rebounds, defensive_rebounds, total_rebounds, assistances, steals,
            turnovers, blocks_favour, blocks_against, fouls_commited, fouls_received,
            valuation, plusminus, game_sequence, season_round, row_type, row_number
        ) VALUES %s
        ON CONFLICT (player_id, gamecode, season, team, row_number) DO UPDATE SET
            phase = EXCLUDED.phase,
            round = EXCLUDED.round,
            home = EXCLUDED.home,
            is_starter = EXCLUDED.is_starter,
            is_playing = EXCLUDED.is_playing,
            dorsal = EXCLUDED.dorsal,
            player = EXCLUDED.player,
            minutes = EXCLUDED.minutes,
            points = EXCLUDED.points,
            field_goals_made_2 = EXCLUDED.field_goals_made_2,
            field_goals_attempted_2 = EXCLUDED.field_goals_attempted_2,
            field_goals_made_3 = EXCLUDED.field_goals_made_3,
            field_goals_attempted_3 = EXCLUDED.field_goals_attempted_3,
            free_throws_made = EXCLUDED.free_throws_made,
            free_throws_attempted = EXCLUDED.free_throws_attempted,
            offensive_rebounds = EXCLUDED.offensive_rebounds,
            defensive_rebounds = EXCLUDED.defensive_rebounds,
            total_rebounds = EXCLUDED.total_rebounds,
            assistances = EXCLUDED.assistances,
            steals = EXCLUDED.steals,
            turnovers = EXCLUDED.turnovers,
            blocks_favour = EXCLUDED.blocks_favour,
            blocks_against = EXCLUDED.blocks_against,
            fouls_commited = EXCLUDED.fouls_commited,
            fouls_received = EXCLUDED.fouls_received,
            valuation = EXCLUDED.valuation,
            plusminus = EXCLUDED.plusminus,
            game_sequence = EXCLUDED.game_sequence,
            season_round = EXCLUDED.season_round,
            row_type = EXCLUDED.row_type;
        """

        execute_values(cursor, insert_query, data_tuples)
        conn.commit()

    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

def update_euro_leagues_game_logs(competition_type: str):
    if competition_type == 'E':
        table_name = 'game_logs_euroleague'
    elif competition_type == 'U':
        table_name = 'game_logs_eurocup'
    else:
        raise ValueError("Invalid competition_type. Must be 'E' for Euroleague or 'U' for Eurocup.")

    boxdata = BoxScoreData(competition=competition_type)
    boxscore_data = boxdata.get_player_boxscore_stats_multiple_seasons(2016, 2024)

    game_logs = boxscore_data.sort_values(['Player', 'Season', 'Round'], ascending=[True, False, False])
    game_logs = calculate_game_sequence(game_logs)
    game_logs['SeasonRound'] = game_logs['Season'].astype(str) + '-' + game_logs['Round'].astype(str)

    insert_game_logs_to_db(game_logs, table_name)

# Update Euroleague game logs
update_euro_leagues_game_logs('E')

# Update Eurocup game logs
update_euro_leagues_game_logs('U')


# In[ ]:





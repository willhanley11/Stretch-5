#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# schedule_results_euroleague 

import pandas as pd
import numpy as np
from euroleague_api.game_stats import GameStats
gs = GameStats('E')
gamestats = gs.get_game_reports_range_seasons(2017,2024)


def create_team_records_dataset(df):
    """
    Create a dataset where each row represents a team's game with their cumulative record
    after that game, including team code and opponent image. Record resets at the start of each season
    and when transitioning from RS to PI/PO/FF. PI, PO, and FF share the same record group.
    """
    import pandas as pd

    all_team_records = []
    all_teams = set(df['local.club.name'].unique()).union(df['road.club.name'].unique())

    # Define phase order for sorting
    phase_order = {'RS': 0, 'PI': 1, 'PO': 2, 'FF': 3}

    for team in all_teams:
        team_games = df[(df['local.club.name'] == team) | (df['road.club.name'] == team)].copy()

        # Sort with custom phase order
        team_games['PhaseOrder'] = team_games['Phase'].map(phase_order)
        team_games = team_games.sort_values(['Season', 'PhaseOrder', 'Round', 'localDate'])

        for season, season_games in team_games.groupby('Season'):
            wins = 0
            losses = 0
            current_phase_group = None

            for idx, game in season_games.iterrows():
                # Define phase groups: RS is separate, PI+PO+FF are together
                if game['Phase'] == 'RS':
                    phase_group = 'RS'
                elif game['Phase'] in ['PI', 'PO', 'FF']:
                    phase_group = 'POSTSEASON'
                else:
                    phase_group = game['Phase']  # Any unexpected phase

                # Reset record when entering a new phase group
                if current_phase_group != phase_group:
                    current_phase_group = phase_group
                    wins = 0
                    losses = 0

                # Determine game context (home/away)
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

                # Determine result
                if team_score > opponent_score:
                    result = 'Win'
                    wins += 1
                elif team_score < opponent_score:
                    result = 'Loss'
                    losses += 1
                else:
                    result = 'Draw'  # Rare, optional to include

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




import psycopg2
from psycopg2.extras import execute_values
import pandas as pd

def insert_schedule_results_to_db(team_records_df):
    # Connect to the database
    conn_str = "postgresql://euroleague_owner:npg_6WgqinJyK5lz@ep-late-sun-a54sr5mz-pooler.us-east-2.aws.neon.tech/euroleague?sslmode=require"
    conn = psycopg2.connect(conn_str)
    cursor = conn.cursor()

    try:
        # 1. Print local DataFrame info
        print(f"Local DataFrame has {len(team_records_df)} rows")

        # 2. Check for duplicates
        duplicates = team_records_df.groupby(['Team', 'Gamecode', 'Season']).size()
        duplicates = duplicates[duplicates > 1]
        if len(duplicates) > 0:
            print(f"Warning: Found {len(duplicates)} duplicate team-gamecode-season combinations")
            print("First few duplicates:")
            print(duplicates.head())

        # 3. Create the schedule_results table
        cursor.execute("""
        DROP TABLE IF EXISTS schedule_results_euroleague;
        CREATE TABLE schedule_results_euroleague (
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

        # 4. Check current row count
        cursor.execute("SELECT COUNT(*) FROM schedule_results_euroleague;")
        before_count = cursor.fetchone()[0]
        print(f"Rows in database before insert: {before_count}")

        # 5. Define helper to clean values
        def safe_int(val):
            if pd.isna(val):
                return None
            try:
                return int(val)
            except (ValueError, TypeError):
                return None

        # 6. Build the data tuples from the DataFrame
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

        print(f"Prepared {len(data_tuples)} tuples for insertion")

        # 7. Define the INSERT statement - let's use UPDATE for conflicts
        insert_query = """
        INSERT INTO schedule_results_euroleague (
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

        # 8. Bulk insert data
        execute_values(cursor, insert_query, data_tuples)
        rows_affected = cursor.rowcount
        conn.commit()

        print(f"Insert operation affected {rows_affected} rows")

        # 9. Check final row count
        cursor.execute("SELECT COUNT(*) FROM schedule_results_euroleague;")
        after_count = cursor.fetchone()[0]
        print(f"Rows in database after insert: {after_count}")

        # 10. Verify data integrity
        cursor.execute("SELECT COUNT(*) FROM (SELECT DISTINCT team, gamecode, season FROM schedule_results_euroleague) AS unique_combos;")
        unique_combinations = cursor.fetchone()[0]
        print(f"Unique team-gamecode-season combinations: {unique_combinations}")

        print("Schedule results data inserted successfully into Neon database!")

    except Exception as e:
        print(f"Error during database operation: {e}")
        conn.rollback()
        raise
    finally:
        # Close connections
        cursor.close()
        conn.close()

team_records_df = create_team_records_dataset(gamestats)
insert_schedule_results_to_db(team_records_df)



# In[ ]:


# schedule_results_eurocup 

import pandas as pd
import numpy as np
from euroleague_api.game_stats import GameStats
gs = GameStats('U')
gamestats = gs.get_game_reports_range_seasons(2017,2024)


def create_team_records_dataset(df):
    """
    Create a dataset where each row represents a team's game with their cumulative record
    after that game, including team code and opponent image. Record resets at the start of each season
    and when transitioning from RS to PI/PO/FF. PI, PO, and FF share the same record group.
    """
    import pandas as pd

    all_team_records = []
    all_teams = set(df['local.club.name'].unique()).union(df['road.club.name'].unique())

    # Define phase order for sorting
    phase_order = {'RS': 0, '8F': 1, '4F': 2}

    for team in all_teams:
        team_games = df[(df['local.club.name'] == team) | (df['road.club.name'] == team)].copy()

        # Sort with custom phase order
        team_games['PhaseOrder'] = team_games['Phase'].map(phase_order)
        team_games = team_games.sort_values(['Season', 'PhaseOrder', 'Round', 'localDate'])

        for season, season_games in team_games.groupby('Season'):
            wins = 0
            losses = 0
            current_phase_group = None

            for idx, game in season_games.iterrows():
                # Define phase groups: RS is separate, PI+PO+FF are together
                if game['Phase'] == 'RS':
                    phase_group = 'RS'
                elif game['Phase'] in ['8F', '4F']:
                    phase_group = 'POSTSEASON'
                else:
                    phase_group = game['Phase']  # Any unexpected phase

                # Reset record when entering a new phase group
                if current_phase_group != phase_group:
                    current_phase_group = phase_group
                    wins = 0
                    losses = 0

                # Determine game context (home/away)
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

                # Determine result
                if team_score > opponent_score:
                    result = 'Win'
                    wins += 1
                elif team_score < opponent_score:
                    result = 'Loss'
                    losses += 1
                else:
                    result = 'Draw'  # Rare, optional to include

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




import psycopg2
from psycopg2.extras import execute_values
import pandas as pd

def insert_schedule_results_to_db(team_records_df):
    # Connect to the database
    conn_str = "postgresql://euroleague_owner:npg_6WgqinJyK5lz@ep-late-sun-a54sr5mz-pooler.us-east-2.aws.neon.tech/euroleague?sslmode=require"
    conn = psycopg2.connect(conn_str)
    cursor = conn.cursor()

    try:
        # 1. Print local DataFrame info
        print(f"Local DataFrame has {len(team_records_df)} rows")

        # 2. Check for duplicates
        duplicates = team_records_df.groupby(['Team', 'Gamecode', 'Season']).size()
        duplicates = duplicates[duplicates > 1]
        if len(duplicates) > 0:
            print(f"Warning: Found {len(duplicates)} duplicate team-gamecode-season combinations")
            print("First few duplicates:")
            print(duplicates.head())

        # 3. Create the schedule_results table
        cursor.execute("""
        DROP TABLE IF EXISTS schedule_results_eurocup;
        CREATE TABLE schedule_results_eurocup (
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

        # 4. Check current row count
        cursor.execute("SELECT COUNT(*) FROM schedule_results_eurocup;")
        before_count = cursor.fetchone()[0]
        print(f"Rows in database before insert: {before_count}")

        # 5. Define helper to clean values
        def safe_int(val):
            if pd.isna(val):
                return None
            try:
                return int(val)
            except (ValueError, TypeError):
                return None

        # 6. Build the data tuples from the DataFrame
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

        print(f"Prepared {len(data_tuples)} tuples for insertion")

        # 7. Define the INSERT statement - let's use UPDATE for conflicts
        insert_query = """
        INSERT INTO schedule_results_eurocup (
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

        # 8. Bulk insert data
        execute_values(cursor, insert_query, data_tuples)
        rows_affected = cursor.rowcount
        conn.commit()

        print(f"Insert operation affected {rows_affected} rows")

        # 9. Check final row count
        cursor.execute("SELECT COUNT(*) FROM schedule_results_eurocup;")
        after_count = cursor.fetchone()[0]
        print(f"Rows in database after insert: {after_count}")

        # 10. Verify data integrity
        cursor.execute("SELECT COUNT(*) FROM (SELECT DISTINCT team, gamecode, season FROM schedule_results_eurocup) AS unique_combos;")
        unique_combinations = cursor.fetchone()[0]
        print(f"Unique team-gamecode-season combinations: {unique_combinations}")

        print("Schedule results data inserted successfully into Neon database!")

    except Exception as e:
        print(f"Error during database operation: {e}")
        conn.rollback()
        raise
    finally:
        # Close connections
        cursor.close()
        conn.close()

team_records_df = create_team_records_dataset(gamestats)
insert_schedule_results_to_db(team_records_df)



# In[ ]:


# game_logs_eurocup

import psycopg2
from psycopg2.extras import execute_values
import pandas as pd



from euroleague_api.boxscore_data import BoxScoreData
boxdata = BoxScoreData(competition='U')
boxscore_data = boxdata.get_player_boxscore_stats_multiple_seasons(2016, 2024)

# Remove the filter to include Team and Total rows
game_logs = boxscore_data.sort_values(['Player', 'Season', 'Round'], ascending=[True, False, False])

# Handle GameSequence differently for Team and Total rows
def calculate_game_sequence(df):
    # For regular players, calculate sequence as before
    player_mask = ~df['Player_ID'].isin(['Team', 'Total'])
    df.loc[player_mask, 'GameSequence'] = df[player_mask].groupby('Player_ID').cumcount() + 1

    # For Team and Total rows, set GameSequence to None or 0
    df.loc[~player_mask, 'GameSequence'] = None

    return df

game_logs = calculate_game_sequence(game_logs)

# Create a season-round identifier for easier reference
game_logs['SeasonRound'] = game_logs['Season'].astype(str) + '-' + game_logs['Round'].astype(str)

def insert_euroleague_game_logs_to_db(game_logs_df):
    # Connect to the database
    conn_str = "postgresql://euroleague_owner:npg_6WgqinJyK5lz@ep-late-sun-a54sr5mz-pooler.us-east-2.aws.neon.tech/euroleague?sslmode=require"
    conn = psycopg2.connect(conn_str)
    cursor = conn.cursor()

    try:
        # 1. Print local DataFrame info
        print(f"Local DataFrame has {len(game_logs_df)} rows")

        # Check distribution of Player_ID types
        team_count = (game_logs_df['Player_ID'] == 'Team').sum()
        total_count = (game_logs_df['Player_ID'] == 'Total').sum()
        regular_count = len(game_logs_df) - team_count - total_count

        print(f"Regular players: {regular_count}, Team rows: {team_count}, Total rows: {total_count}")

        # 2. Check for duplicates and create a unique identifier
        # Add a row number to handle multiple Team/Total entries per game
        game_logs_df = game_logs_df.copy()
        game_logs_df['row_number'] = game_logs_df.groupby(['Player_ID', 'Gamecode', 'Season', 'Team']).cumcount() + 1

        duplicates = game_logs_df.groupby(['Player_ID', 'Gamecode', 'Season', 'Team', 'row_number']).size()
        duplicates = duplicates[duplicates > 1]
        if len(duplicates) > 0:
            print(f"Warning: Found {len(duplicates)} duplicate combinations after adding row_number")

        # 3. Create the euroleague_game_logs table with a simpler approach
        cursor.execute("""
        DROP TABLE IF EXISTS game_logs_eurocup;
        CREATE TABLE game_logs_eurocup (
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
            -- Simple unique constraint that handles all cases
            UNIQUE(player_id, gamecode, season, team, row_number)
        );
        """)
        conn.commit()

        # 4. Check current row count
        cursor.execute("SELECT COUNT(*) FROM game_logs_eurocup;")
        before_count = cursor.fetchone()[0]
        print(f"Rows in database before insert: {before_count}")

        # 5. Define helper functions with better handling for Team/Total rows
        def safe_int(val):
            if pd.isna(val) or val == 'DNP' or val == 'None':
                return None
            try:
                return int(float(val))  # Convert to float first to handle string numbers
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

        # 6. Build the data tuples from the DataFrame with better error handling
        data_tuples = []
        for idx, row in game_logs_df.iterrows():
            try:
                # Determine row type
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
                print(f"Error processing row {idx}: {e}")
                print(f"Row data: {row.to_dict()}")
                continue

        print(f"Prepared {len(data_tuples)} tuples for insertion")

        # 7. Define the INSERT statement with proper conflict resolution
        insert_query = """
        INSERT INTO game_logs_eurocup (
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

        # 8. Bulk insert data
        execute_values(cursor, insert_query, data_tuples)
        rows_affected = cursor.rowcount
        conn.commit()

        print(f"Insert operation affected {rows_affected} rows")

        # 9. Check final row count
        cursor.execute("SELECT COUNT(*) FROM game_logs_eurocup;")
        after_count = cursor.fetchone()[0]
        print(f"Rows in database after insert: {after_count}")

        # 10. Verify data integrity by row type
        cursor.execute("SELECT row_type, COUNT(*) FROM game_logs_eurocup GROUP BY row_type;")
        row_type_counts = cursor.fetchall()
        print("Row counts by type:")
        for row_type, count in row_type_counts:
            print(f"  {row_type}: {count}")

        # 11. Show some sample data including Team and Total rows
        cursor.execute("""
            SELECT player_id, player, team, season, round, points, total_rebounds, assistances, row_type, row_number
            FROM game_logs_eurocup
            WHERE row_type IN ('team', 'total')
            ORDER BY season DESC, round DESC, team, row_type
            LIMIT 10;
        """)
        sample_team_total = cursor.fetchall()
        print("\nSample Team/Total data:")
        for row in sample_team_total:
            print(f"ID: {row[0]}, Player: {row[1]}, Team: {row[2]}, Season: {row[3]}, Round: {row[4]}, Points: {row[5]}, Rebounds: {row[6]}, Assists: {row[7]}, Type: {row[8]}, Row#: {row[9]}")

        cursor.execute("""
            SELECT player_id, player, team, season, round, points, total_rebounds, assistances, row_type 
            FROM game_logs_eurocup
            WHERE row_type = 'player'
            ORDER BY season DESC, round DESC 
            LIMIT 3;
        """)
        sample_players = cursor.fetchall()
        print("\nSample Player data:")
        for row in sample_players:
            print(f"ID: {row[0]}, Player: {row[1]}, Team: {row[2]}, Season: {row[3]}, Round: {row[4]}, Points: {row[5]}, Rebounds: {row[6]}, Assists: {row[7]}, Type: {row[8]}")

        print("\nEuroleague game logs data (including Team and Total rows) inserted successfully!")

    except Exception as e:
        print(f"Error during database operation: {e}")
        conn.rollback()
        raise
    finally:
        # Close connections
        cursor.close()
        conn.close()

# Call the function with your game_logs DataFrame
insert_euroleague_game_logs_to_db(game_logs)


# In[ ]:


# game_logs_euroleague

import psycopg2
from psycopg2.extras import execute_values
import pandas as pd



from euroleague_api.boxscore_data import BoxScoreData
boxdata = BoxScoreData(competition='E')
boxscore_data = boxdata.get_player_boxscore_stats_multiple_seasons(2016, 2024)

# Remove the filter to include Team and Total rows
game_logs = boxscore_data.sort_values(['Player', 'Season', 'Round'], ascending=[True, False, False])

# Handle GameSequence differently for Team and Total rows
def calculate_game_sequence(df):
    # For regular players, calculate sequence as before
    player_mask = ~df['Player_ID'].isin(['Team', 'Total'])
    df.loc[player_mask, 'GameSequence'] = df[player_mask].groupby('Player_ID').cumcount() + 1

    # For Team and Total rows, set GameSequence to None or 0
    df.loc[~player_mask, 'GameSequence'] = None

    return df

game_logs = calculate_game_sequence(game_logs)

# Create a season-round identifier for easier reference
game_logs['SeasonRound'] = game_logs['Season'].astype(str) + '-' + game_logs['Round'].astype(str)

def insert_euroleague_game_logs_to_db(game_logs_df):
    # Connect to the database
    conn_str = "postgresql://euroleague_owner:npg_6WgqinJyK5lz@ep-late-sun-a54sr5mz-pooler.us-east-2.aws.neon.tech/euroleague?sslmode=require"
    conn = psycopg2.connect(conn_str)
    cursor = conn.cursor()

    try:
        # 1. Print local DataFrame info
        print(f"Local DataFrame has {len(game_logs_df)} rows")

        # Check distribution of Player_ID types
        team_count = (game_logs_df['Player_ID'] == 'Team').sum()
        total_count = (game_logs_df['Player_ID'] == 'Total').sum()
        regular_count = len(game_logs_df) - team_count - total_count

        print(f"Regular players: {regular_count}, Team rows: {team_count}, Total rows: {total_count}")

        # 2. Check for duplicates and create a unique identifier
        # Add a row number to handle multiple Team/Total entries per game
        game_logs_df = game_logs_df.copy()
        game_logs_df['row_number'] = game_logs_df.groupby(['Player_ID', 'Gamecode', 'Season', 'Team']).cumcount() + 1

        duplicates = game_logs_df.groupby(['Player_ID', 'Gamecode', 'Season', 'Team', 'row_number']).size()
        duplicates = duplicates[duplicates > 1]
        if len(duplicates) > 0:
            print(f"Warning: Found {len(duplicates)} duplicate combinations after adding row_number")

        # 3. Create the euroleague_game_logs table with a simpler approach
        cursor.execute("""
        DROP TABLE IF EXISTS game_logs_euroleague;
        CREATE TABLE game_logs_euroleague (
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
            -- Simple unique constraint that handles all cases
            UNIQUE(player_id, gamecode, season, team, row_number)
        );
        """)
        conn.commit()

        # 4. Check current row count
        cursor.execute("SELECT COUNT(*) FROM game_logs_euroleague;")
        before_count = cursor.fetchone()[0]
        print(f"Rows in database before insert: {before_count}")

        # 5. Define helper functions with better handling for Team/Total rows
        def safe_int(val):
            if pd.isna(val) or val == 'DNP' or val == 'None':
                return None
            try:
                return int(float(val))  # Convert to float first to handle string numbers
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

        # 6. Build the data tuples from the DataFrame with better error handling
        data_tuples = []
        for idx, row in game_logs_df.iterrows():
            try:
                # Determine row type
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
                print(f"Error processing row {idx}: {e}")
                print(f"Row data: {row.to_dict()}")
                continue

        print(f"Prepared {len(data_tuples)} tuples for insertion")

        # 7. Define the INSERT statement with proper conflict resolution
        insert_query = """
        INSERT INTO game_logs_euroleague (
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

        # 8. Bulk insert data
        execute_values(cursor, insert_query, data_tuples)
        rows_affected = cursor.rowcount
        conn.commit()

        print(f"Insert operation affected {rows_affected} rows")

        # 9. Check final row count
        cursor.execute("SELECT COUNT(*) FROM game_logs_euroleague;")
        after_count = cursor.fetchone()[0]
        print(f"Rows in database after insert: {after_count}")

        # 10. Verify data integrity by row type
        cursor.execute("SELECT row_type, COUNT(*) FROM game_logs_euroleague GROUP BY row_type;")
        row_type_counts = cursor.fetchall()
        print("Row counts by type:")
        for row_type, count in row_type_counts:
            print(f"  {row_type}: {count}")

        # 11. Show some sample data including Team and Total rows
        cursor.execute("""
            SELECT player_id, player, team, season, round, points, total_rebounds, assistances, row_type, row_number
            FROM game_logs_euroleague
            WHERE row_type IN ('team', 'total')
            ORDER BY season DESC, round DESC, team, row_type
            LIMIT 10;
        """)
        sample_team_total = cursor.fetchall()
        print("\nSample Team/Total data:")
        for row in sample_team_total:
            print(f"ID: {row[0]}, Player: {row[1]}, Team: {row[2]}, Season: {row[3]}, Round: {row[4]}, Points: {row[5]}, Rebounds: {row[6]}, Assists: {row[7]}, Type: {row[8]}, Row#: {row[9]}")

        cursor.execute("""
            SELECT player_id, player, team, season, round, points, total_rebounds, assistances, row_type 
            FROM game_logs_euroleague
            WHERE row_type = 'player'
            ORDER BY season DESC, round DESC 
            LIMIT 3;
        """)
        sample_players = cursor.fetchall()
        print("\nSample Player data:")
        for row in sample_players:
            print(f"ID: {row[0]}, Player: {row[1]}, Team: {row[2]}, Season: {row[3]}, Round: {row[4]}, Points: {row[5]}, Rebounds: {row[6]}, Assists: {row[7]}, Type: {row[8]}")

        print("\nEuroleague game logs data (including Team and Total rows) inserted successfully!")

    except Exception as e:
        print(f"Error during database operation: {e}")
        conn.rollback()
        raise
    finally:
        # Close connections
        cursor.close()
        conn.close()

# Call the function with your game_logs DataFrame
insert_euroleague_game_logs_to_db(game_logs)


# In[ ]:


# player_stats_eurocup and then will become player_stats_with_logo_eurocup

import pandas as pd
import time
import psycopg2

from euroleague_api.player_stats import PlayerStats

def create_combined_player_stats(player_stats_instance) -> pd.DataFrame:
    """
    Get specific columns from each endpoint and merge with suffixes
    """

    seasons = list(range(2017, 2025))
    endpoints = ['traditional', 'misc', 'scoring', 'advanced']
    phases = ['RS', '8F', '4F']  # Keep all phases separate

    # Define the columns we want from each endpoint
    endpoint_columns = {
        'traditional': ['gamesPlayed', 'gamesStarted', 'minutesPlayed', 'pointsScored', 
                       'twoPointersMade', 'twoPointersAttempted', 'twoPointersPercentage', 
                       'threePointersMade', 'threePointersAttempted', 'threePointersPercentage', 
                       'freeThrowsMade', 'freeThrowsAttempted', 'freeThrowsPercentage',
                       'offensiveRebounds', 'defensiveRebounds', 'totalRebounds', 'assists',
                       'steals', 'turnovers', 'blocks', 'blocksAgainst', 'foulsCommited', 
                       'foulsDrawn', 'pir'],
        'misc': ['doubleDoubles', 'tripleDoubles'],
        'scoring': ['twoPointRate', 'threePointerRate', 'pointsFromTwoPointersPercentage', 
                   'pointsFromThreePointersPercentage', 'pointsFromFreeThrowsPercentage_scoring'],
        'advanced': ['effectiveFieldGoalPercentage', 'offensiveReboundsPercentage', 
                    'defensiveReboundsPercentage', 'reboundsPercentage', 'assistsToTurnoversRatio',
                    'freeThrowsRate']
    }

    # Join keys that should not get suffixes
    join_keys = ['player.code', 'player.name', 'player.age', 'player.imageUrl', 
                 'player.team.code', 'player.team.name']

    all_data = []

    for season in seasons:
        for phase in phases:

            # Get data for all endpoints
            pergame_dfs = []

            for endpoint in endpoints:
                try:
                    # Get PerGame data
                    pergame_data = player_stats_instance.get_player_stats_single_season(
                        endpoint=endpoint,
                        season=season,
                        statistic_mode='PerGame',
                        phase_type_code=phase
                    )
                    if pergame_data is not None and not pergame_data.empty:
                        # Select only the columns we want
                        cols_to_select = join_keys + [col for col in endpoint_columns[endpoint] if col in pergame_data.columns]
                        selected_data = pergame_data[cols_to_select].copy()

                        pergame_dfs.append(selected_data)

                    time.sleep(0.1)

                except Exception as e:
                    print(f"Error with {endpoint}, {season}, {phase}: {e}")
                    continue

            # Merge all data for this season/phase
            if pergame_dfs:
                combined = None

                if pergame_dfs:
                    # Start with the first dataframe
                    combined = pergame_dfs[0].copy()

                    # Merge all the rest on join keys
                    for df in pergame_dfs[1:]:
                        combined = combined.merge(df, on=join_keys, how='outer')

                    # Add metadata - Keep phases separate
                    combined['Season'] = season
                    combined['Phase'] = phase  # Keep FF and PO separate

                    all_data.append(combined)

    # Combine all seasons/phases
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)

        # Check for duplicates before returning
        duplicate_check = final_df.groupby(['player.code', 'Season', 'Phase', 'player.team.code']).size()
        duplicates = duplicate_check[duplicate_check > 1]

        if len(duplicates) > 0:
            print(f"Warning: Found {len(duplicates)} duplicate combinations in the final dataset.")
            print("Removing duplicates (keeping last occurrence)...")
            final_df = final_df.drop_duplicates(
                subset=['player.code', 'Season', 'Phase', 'player.team.code'], 
                keep='last'
            ).reset_index(drop=True)

        return final_df

    return pd.DataFrame()

def insert_player_stats_to_db(player_stats_df):
    """
    Insert player stats into the database with proper handling of phases
    """
    # Connect to the database
    conn_str = "postgresql://euroleague_owner:npg_6WgqinJyK5lz@ep-late-sun-a54sr5mz-pooler.us-east-2.aws.neon.tech/euroleague?sslmode=require"
    conn = psycopg2.connect(conn_str)
    cursor = conn.cursor()

    try:
        print(f"Original DataFrame has {len(player_stats_df)} rows")

        # Create the euroleague_player_stats table
        cursor.execute("""
        DROP TABLE IF EXISTS player_stats_eurocup;
        CREATE TABLE player_stats_eurocup (
            id SERIAL PRIMARY KEY,
            player_code TEXT,
            player_name TEXT,
            player_age INTEGER,
            player_imageurl TEXT,
            player_team_code TEXT,
            player_team_name TEXT,
            games_played DECIMAL,
            games_started DECIMAL,
            minutes_played DECIMAL,
            points_scored DECIMAL,
            two_pointers_made DECIMAL,
            two_pointers_attempted DECIMAL,
            two_pointers_percentage DECIMAL,
            three_pointers_made DECIMAL,
            three_pointers_attempted DECIMAL,
            three_pointers_percentage DECIMAL,
            free_throws_made DECIMAL,
            free_throws_attempted DECIMAL,
            free_throws_percentage DECIMAL,
            offensive_rebounds DECIMAL,
            defensive_rebounds DECIMAL,
            total_rebounds DECIMAL,
            assists DECIMAL,
            steals DECIMAL,
            turnovers DECIMAL,
            blocks DECIMAL,
            blocks_against DECIMAL,
            fouls_commited DECIMAL,
            fouls_drawn DECIMAL,
            pir DECIMAL,
            double_doubles DECIMAL,
            triple_doubles DECIMAL,
            two_point_rate DECIMAL,
            points_from_two_pointers_percentage DECIMAL,
            points_from_three_pointers_percentage DECIMAL,
            effective_field_goal_percentage DECIMAL,
            offensive_rebounds_percentage DECIMAL,
            defensive_rebounds_percentage DECIMAL,
            rebounds_percentage DECIMAL,
            assists_to_turnovers_ratio DECIMAL,
            free_throws_rate DECIMAL,
            season INTEGER,
            phase TEXT,
            UNIQUE(player_code, season, phase, player_team_code)
        );
        """)
        conn.commit()

        def safe_value(val):
            if pd.isna(val):
                return None

            # Handle percentage strings (remove % and convert to decimal)
            if isinstance(val, str) and val.endswith('%'):
                try:
                    return float(val.replace('%', '')) / 100
                except ValueError:
                    return None

            # Handle other string numbers
            if isinstance(val, str):
                try:
                    return float(val)
                except ValueError:
                    return None

            return val

        # Build data tuples
        data_tuples = []
        for _, row in player_stats_df.iterrows():
            data_tuples.append((
                row.get("player.code"),
                row.get("player.name"),
                safe_value(row.get("player.age")),
                row.get("player.imageUrl"),
                row.get("player.team.code"),
                row.get("player.team.name"),
                safe_value(row.get("gamesPlayed")),
                safe_value(row.get("gamesStarted")),
                safe_value(row.get("minutesPlayed")),
                safe_value(row.get("pointsScored")),
                safe_value(row.get("twoPointersMade")),
                safe_value(row.get("twoPointersAttempted")),
                safe_value(row.get("twoPointersPercentage")),
                safe_value(row.get("threePointersMade")),
                safe_value(row.get("threePointersAttempted")),
                safe_value(row.get("threePointersPercentage")),
                safe_value(row.get("freeThrowsMade")),
                safe_value(row.get("freeThrowsAttempted")),
                safe_value(row.get("freeThrowsPercentage")),
                safe_value(row.get("offensiveRebounds")),
                safe_value(row.get("defensiveRebounds")),
                safe_value(row.get("totalRebounds")),
                safe_value(row.get("assists")),
                safe_value(row.get("steals")),
                safe_value(row.get("turnovers")),
                safe_value(row.get("blocks")),
                safe_value(row.get("blocksAgainst")),
                safe_value(row.get("foulsCommited")),
                safe_value(row.get("foulsDrawn")),
                safe_value(row.get("pir")),
                safe_value(row.get("doubleDoubles")),
                safe_value(row.get("tripleDoubles")),
                safe_value(row.get("twoPointRate")),
                safe_value(row.get("pointsFromTwoPointersPercentage")),
                safe_value(row.get("pointsFromThreePointersPercentage")),
                safe_value(row.get("effectiveFieldGoalPercentage")),
                safe_value(row.get("offensiveReboundsPercentage")),
                safe_value(row.get("defensiveReboundsPercentage")),
                safe_value(row.get("reboundsPercentage")),
                safe_value(row.get("assistsToTurnoversRatio")),
                safe_value(row.get("freeThrowsRate")),
                safe_value(row.get("Season")),
                row.get("Phase")
            ))

        insert_query = """
        INSERT INTO player_stats_eurocup (
            player_code, player_name, player_age, player_imageurl, player_team_code, player_team_name,
            games_played, games_started, minutes_played, points_scored,
            two_pointers_made, two_pointers_attempted, two_pointers_percentage,
            three_pointers_made, three_pointers_attempted, three_pointers_percentage,
            free_throws_made, free_throws_attempted, free_throws_percentage,
            offensive_rebounds, defensive_rebounds, total_rebounds, assists,
            steals, turnovers, blocks, blocks_against, fouls_commited,
            fouls_drawn, pir, double_doubles, triple_doubles,
            two_point_rate, points_from_two_pointers_percentage, points_from_three_pointers_percentage,
            effective_field_goal_percentage, offensive_rebounds_percentage, defensive_rebounds_percentage,
            rebounds_percentage, assists_to_turnovers_ratio, free_throws_rate,
            season, phase
        ) VALUES %s
        ON CONFLICT (player_code, season, phase, player_team_code) DO UPDATE SET
            player_name = EXCLUDED.player_name,
            player_age = EXCLUDED.player_age,
            player_imageurl = EXCLUDED.player_imageurl,
            player_team_name = EXCLUDED.player_team_name,
            games_played = EXCLUDED.games_played,
            games_started = EXCLUDED.games_started,
            minutes_played = EXCLUDED.minutes_played,
            points_scored = EXCLUDED.points_scored,
            two_pointers_made = EXCLUDED.two_pointers_made,
            two_pointers_attempted = EXCLUDED.two_pointers_attempted,
            two_pointers_percentage = EXCLUDED.two_pointers_percentage,
            three_pointers_made = EXCLUDED.three_pointers_made,
            three_pointers_attempted = EXCLUDED.three_pointers_attempted,
            three_pointers_percentage = EXCLUDED.three_pointers_percentage,
            free_throws_made = EXCLUDED.free_throws_made,
            free_throws_attempted = EXCLUDED.free_throws_attempted,
            free_throws_percentage = EXCLUDED.free_throws_percentage,
            offensive_rebounds = EXCLUDED.offensive_rebounds,
            defensive_rebounds = EXCLUDED.defensive_rebounds,
            total_rebounds = EXCLUDED.total_rebounds,
            assists = EXCLUDED.assists,
            steals = EXCLUDED.steals,
            turnovers = EXCLUDED.turnovers,
            blocks = EXCLUDED.blocks,
            blocks_against = EXCLUDED.blocks_against,
            fouls_commited = EXCLUDED.fouls_commited,
            fouls_drawn = EXCLUDED.fouls_drawn,
            pir = EXCLUDED.pir,
            double_doubles = EXCLUDED.double_doubles,
            triple_doubles = EXCLUDED.triple_doubles,
            two_point_rate = EXCLUDED.two_point_rate,
            points_from_two_pointers_percentage = EXCLUDED.points_from_two_pointers_percentage,
            points_from_three_pointers_percentage = EXCLUDED.points_from_three_pointers_percentage,
            effective_field_goal_percentage = EXCLUDED.effective_field_goal_percentage,
            offensive_rebounds_percentage = EXCLUDED.offensive_rebounds_percentage,
            defensive_rebounds_percentage = EXCLUDED.defensive_rebounds_percentage,
            rebounds_percentage = EXCLUDED.rebounds_percentage,
            assists_to_turnovers_ratio = EXCLUDED.assists_to_turnovers_ratio,
            free_throws_rate = EXCLUDED.free_throws_rate;
        """

        from psycopg2.extras import execute_values
        execute_values(cursor, insert_query, data_tuples)
        conn.commit()

        cursor.execute("SELECT COUNT(*) FROM player_stats_eurocup;")
        final_count = cursor.fetchone()[0]
        print(f"Successfully inserted data. Total rows in database: {final_count}")

        # Show phase distribution
        cursor.execute("""
        SELECT phase, COUNT(*) 
        FROM player_stats_eurocup
        GROUP BY phase 
        ORDER BY COUNT(*) DESC;
        """)
        phase_counts = cursor.fetchall()
        print("\nPhase distribution in database:")
        for phase, count in phase_counts:
            print(f"  {phase}: {count} records")

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# Example usage:
# 1. Initialize your PlayerStats instance
tsTest = PlayerStats("U")
# 
# 2. Get the dataset with separate phases
df = create_combined_player_stats(tsTest)
# 
# 3. Insert into database
insert_player_stats_to_db(df)


# In[ ]:


# player_stats_euroleague and then will become player_stats_with_logo_euroleague

import pandas as pd
import time
import psycopg2

from euroleague_api.player_stats import PlayerStats

def create_combined_player_stats(player_stats_instance) -> pd.DataFrame:
    """
    Get specific columns from each endpoint and merge with suffixes
    """

    seasons = list(range(2017, 2025))
    endpoints = ['traditional', 'misc', 'scoring', 'advanced']
    phases = ['RS', 'PO', 'FF']  # Keep all phases separate

    # Define the columns we want from each endpoint
    endpoint_columns = {
        'traditional': ['gamesPlayed', 'gamesStarted', 'minutesPlayed', 'pointsScored', 
                       'twoPointersMade', 'twoPointersAttempted', 'twoPointersPercentage', 
                       'threePointersMade', 'threePointersAttempted', 'threePointersPercentage', 
                       'freeThrowsMade', 'freeThrowsAttempted', 'freeThrowsPercentage',
                       'offensiveRebounds', 'defensiveRebounds', 'totalRebounds', 'assists',
                       'steals', 'turnovers', 'blocks', 'blocksAgainst', 'foulsCommited', 
                       'foulsDrawn', 'pir'],
        'misc': ['doubleDoubles', 'tripleDoubles'],
        'scoring': ['twoPointRate', 'threePointerRate', 'pointsFromTwoPointersPercentage', 
                   'pointsFromThreePointersPercentage', 'pointsFromFreeThrowsPercentage_scoring'],
        'advanced': ['effectiveFieldGoalPercentage', 'offensiveReboundsPercentage', 
                    'defensiveReboundsPercentage', 'reboundsPercentage', 'assistsToTurnoversRatio',
                    'freeThrowsRate']
    }

    # Join keys that should not get suffixes
    join_keys = ['player.code', 'player.name', 'player.age', 'player.imageUrl', 
                 'player.team.code', 'player.team.name']

    all_data = []

    for season in seasons:
        for phase in phases:

            # Get data for all endpoints
            pergame_dfs = []

            for endpoint in endpoints:
                try:
                    # Get PerGame data
                    pergame_data = player_stats_instance.get_player_stats_single_season(
                        endpoint=endpoint,
                        season=season,
                        statistic_mode='PerGame',
                        phase_type_code=phase
                    )
                    if pergame_data is not None and not pergame_data.empty:
                        # Select only the columns we want
                        cols_to_select = join_keys + [col for col in endpoint_columns[endpoint] if col in pergame_data.columns]
                        selected_data = pergame_data[cols_to_select].copy()

                        pergame_dfs.append(selected_data)

                    time.sleep(0.1)

                except Exception as e:
                    print(f"Error with {endpoint}, {season}, {phase}: {e}")
                    continue

            # Merge all data for this season/phase
            if pergame_dfs:
                combined = None

                if pergame_dfs:
                    # Start with the first dataframe
                    combined = pergame_dfs[0].copy()

                    # Merge all the rest on join keys
                    for df in pergame_dfs[1:]:
                        combined = combined.merge(df, on=join_keys, how='outer')

                    # Add metadata - Keep phases separate
                    combined['Season'] = season
                    combined['Phase'] = phase  # Keep FF and PO separate

                    all_data.append(combined)

    # Combine all seasons/phases
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)

        # Check for duplicates before returning
        duplicate_check = final_df.groupby(['player.code', 'Season', 'Phase', 'player.team.code']).size()
        duplicates = duplicate_check[duplicate_check > 1]

        if len(duplicates) > 0:
            print(f"Warning: Found {len(duplicates)} duplicate combinations in the final dataset.")
            print("Removing duplicates (keeping last occurrence)...")
            final_df = final_df.drop_duplicates(
                subset=['player.code', 'Season', 'Phase', 'player.team.code'], 
                keep='last'
            ).reset_index(drop=True)

        return final_df

    return pd.DataFrame()

def insert_player_stats_to_db(player_stats_df):
    """
    Insert player stats into the database with proper handling of phases
    """
    # Connect to the database
    conn_str = "postgresql://euroleague_owner:npg_6WgqinJyK5lz@ep-late-sun-a54sr5mz-pooler.us-east-2.aws.neon.tech/euroleague?sslmode=require"
    conn = psycopg2.connect(conn_str)
    cursor = conn.cursor()

    try:
        print(f"Original DataFrame has {len(player_stats_df)} rows")

        # Create the euroleague_player_stats table
        cursor.execute("""
        DROP TABLE IF EXISTS player_stats_euroleague;
        CREATE TABLE player_stats_euroleague (
            id SERIAL PRIMARY KEY,
            player_code TEXT,
            player_name TEXT,
            player_age INTEGER,
            player_imageurl TEXT,
            player_team_code TEXT,
            player_team_name TEXT,
            games_played DECIMAL,
            games_started DECIMAL,
            minutes_played DECIMAL,
            points_scored DECIMAL,
            two_pointers_made DECIMAL,
            two_pointers_attempted DECIMAL,
            two_pointers_percentage DECIMAL,
            three_pointers_made DECIMAL,
            three_pointers_attempted DECIMAL,
            three_pointers_percentage DECIMAL,
            free_throws_made DECIMAL,
            free_throws_attempted DECIMAL,
            free_throws_percentage DECIMAL,
            offensive_rebounds DECIMAL,
            defensive_rebounds DECIMAL,
            total_rebounds DECIMAL,
            assists DECIMAL,
            steals DECIMAL,
            turnovers DECIMAL,
            blocks DECIMAL,
            blocks_against DECIMAL,
            fouls_commited DECIMAL,
            fouls_drawn DECIMAL,
            pir DECIMAL,
            double_doubles DECIMAL,
            triple_doubles DECIMAL,
            two_point_rate DECIMAL,
            points_from_two_pointers_percentage DECIMAL,
            points_from_three_pointers_percentage DECIMAL,
            effective_field_goal_percentage DECIMAL,
            offensive_rebounds_percentage DECIMAL,
            defensive_rebounds_percentage DECIMAL,
            rebounds_percentage DECIMAL,
            assists_to_turnovers_ratio DECIMAL,
            free_throws_rate DECIMAL,
            season INTEGER,
            phase TEXT,
            UNIQUE(player_code, season, phase, player_team_code)
        );
        """)
        conn.commit()

        def safe_value(val):
            if pd.isna(val):
                return None

            # Handle percentage strings (remove % and convert to decimal)
            if isinstance(val, str) and val.endswith('%'):
                try:
                    return float(val.replace('%', '')) / 100
                except ValueError:
                    return None

            # Handle other string numbers
            if isinstance(val, str):
                try:
                    return float(val)
                except ValueError:
                    return None

            return val

        # Build data tuples
        data_tuples = []
        for _, row in player_stats_df.iterrows():
            data_tuples.append((
                row.get("player.code"),
                row.get("player.name"),
                safe_value(row.get("player.age")),
                row.get("player.imageUrl"),
                row.get("player.team.code"),
                row.get("player.team.name"),
                safe_value(row.get("gamesPlayed")),
                safe_value(row.get("gamesStarted")),
                safe_value(row.get("minutesPlayed")),
                safe_value(row.get("pointsScored")),
                safe_value(row.get("twoPointersMade")),
                safe_value(row.get("twoPointersAttempted")),
                safe_value(row.get("twoPointersPercentage")),
                safe_value(row.get("threePointersMade")),
                safe_value(row.get("threePointersAttempted")),
                safe_value(row.get("threePointersPercentage")),
                safe_value(row.get("freeThrowsMade")),
                safe_value(row.get("freeThrowsAttempted")),
                safe_value(row.get("freeThrowsPercentage")),
                safe_value(row.get("offensiveRebounds")),
                safe_value(row.get("defensiveRebounds")),
                safe_value(row.get("totalRebounds")),
                safe_value(row.get("assists")),
                safe_value(row.get("steals")),
                safe_value(row.get("turnovers")),
                safe_value(row.get("blocks")),
                safe_value(row.get("blocksAgainst")),
                safe_value(row.get("foulsCommited")),
                safe_value(row.get("foulsDrawn")),
                safe_value(row.get("pir")),
                safe_value(row.get("doubleDoubles")),
                safe_value(row.get("tripleDoubles")),
                safe_value(row.get("twoPointRate")),
                safe_value(row.get("pointsFromTwoPointersPercentage")),
                safe_value(row.get("pointsFromThreePointersPercentage")),
                safe_value(row.get("effectiveFieldGoalPercentage")),
                safe_value(row.get("offensiveReboundsPercentage")),
                safe_value(row.get("defensiveReboundsPercentage")),
                safe_value(row.get("reboundsPercentage")),
                safe_value(row.get("assistsToTurnoversRatio")),
                safe_value(row.get("freeThrowsRate")),
                safe_value(row.get("Season")),
                row.get("Phase")
            ))

        insert_query = """
        INSERT INTO player_stats_euroleague (
            player_code, player_name, player_age, player_imageurl, player_team_code, player_team_name,
            games_played, games_started, minutes_played, points_scored,
            two_pointers_made, two_pointers_attempted, two_pointers_percentage,
            three_pointers_made, three_pointers_attempted, three_pointers_percentage,
            free_throws_made, free_throws_attempted, free_throws_percentage,
            offensive_rebounds, defensive_rebounds, total_rebounds, assists,
            steals, turnovers, blocks, blocks_against, fouls_commited,
            fouls_drawn, pir, double_doubles, triple_doubles,
            two_point_rate, points_from_two_pointers_percentage, points_from_three_pointers_percentage,
            effective_field_goal_percentage, offensive_rebounds_percentage, defensive_rebounds_percentage,
            rebounds_percentage, assists_to_turnovers_ratio, free_throws_rate,
            season, phase
        ) VALUES %s
        ON CONFLICT (player_code, season, phase, player_team_code) DO UPDATE SET
            player_name = EXCLUDED.player_name,
            player_age = EXCLUDED.player_age,
            player_imageurl = EXCLUDED.player_imageurl,
            player_team_name = EXCLUDED.player_team_name,
            games_played = EXCLUDED.games_played,
            games_started = EXCLUDED.games_started,
            minutes_played = EXCLUDED.minutes_played,
            points_scored = EXCLUDED.points_scored,
            two_pointers_made = EXCLUDED.two_pointers_made,
            two_pointers_attempted = EXCLUDED.two_pointers_attempted,
            two_pointers_percentage = EXCLUDED.two_pointers_percentage,
            three_pointers_made = EXCLUDED.three_pointers_made,
            three_pointers_attempted = EXCLUDED.three_pointers_attempted,
            three_pointers_percentage = EXCLUDED.three_pointers_percentage,
            free_throws_made = EXCLUDED.free_throws_made,
            free_throws_attempted = EXCLUDED.free_throws_attempted,
            free_throws_percentage = EXCLUDED.free_throws_percentage,
            offensive_rebounds = EXCLUDED.offensive_rebounds,
            defensive_rebounds = EXCLUDED.defensive_rebounds,
            total_rebounds = EXCLUDED.total_rebounds,
            assists = EXCLUDED.assists,
            steals = EXCLUDED.steals,
            turnovers = EXCLUDED.turnovers,
            blocks = EXCLUDED.blocks,
            blocks_against = EXCLUDED.blocks_against,
            fouls_commited = EXCLUDED.fouls_commited,
            fouls_drawn = EXCLUDED.fouls_drawn,
            pir = EXCLUDED.pir,
            double_doubles = EXCLUDED.double_doubles,
            triple_doubles = EXCLUDED.triple_doubles,
            two_point_rate = EXCLUDED.two_point_rate,
            points_from_two_pointers_percentage = EXCLUDED.points_from_two_pointers_percentage,
            points_from_three_pointers_percentage = EXCLUDED.points_from_three_pointers_percentage,
            effective_field_goal_percentage = EXCLUDED.effective_field_goal_percentage,
            offensive_rebounds_percentage = EXCLUDED.offensive_rebounds_percentage,
            defensive_rebounds_percentage = EXCLUDED.defensive_rebounds_percentage,
            rebounds_percentage = EXCLUDED.rebounds_percentage,
            assists_to_turnovers_ratio = EXCLUDED.assists_to_turnovers_ratio,
            free_throws_rate = EXCLUDED.free_throws_rate;
        """

        from psycopg2.extras import execute_values
        execute_values(cursor, insert_query, data_tuples)
        conn.commit()

        cursor.execute("SELECT COUNT(*) FROM player_stats_euroleague;")
        final_count = cursor.fetchone()[0]
        print(f"Successfully inserted data. Total rows in database: {final_count}")

        # Show phase distribution
        cursor.execute("""
        SELECT phase, COUNT(*) 
        FROM player_stats_euroleague
        GROUP BY phase 
        ORDER BY COUNT(*) DESC;
        """)
        phase_counts = cursor.fetchall()
        print("\nPhase distribution in database:")
        for phase, count in phase_counts:
            print(f"  {phase}: {count} records")

    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# Example usage:
# 1. Initialize your PlayerStats instance
tsTest = PlayerStats("U")
# 
# 2. Get the dataset with separate phases
df = create_combined_player_stats(tsTest)
# 
# 3. Insert into database
insert_player_stats_to_db(df)


# In[7]:


# shot_data_euroleague

import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import math
from euroleague_api.shot_data import ShotData

# --- 1. Define Court Parameters (from JavaScript's findCourtParameters) ---
# These values are based on standard FIBA court dimensions, converted and scaled for the chart.
COURT_PARAMS = {
    'basket_x': 0,
    'basket_y': 0, # Basket at origin of court coordinate system
    'three_point_radius': 675, # 6.75m in coordinate units (e.g., centimeters)
    'corner_line_x': 660, # Distance from center to corner line (6.60m)
    'corner_intersection_y': 157.5, # Y-coordinate where 3pt arc meets straight line (1.575m from basket on the Y-axis)
    'restricted_area_radius': 125, # 1.25m
    'baseline_y': -100, # Baseline position relative to basket_y (e.g., -1.0m behind basket)
    'paint_width': 490, # 4.9m
    'paint_height': 580, # 5.8m (distance from baseline to free throw line)
    'free_throw_distance': 580, # 5.8m from baseline to free throw line (same as paint height)
    'free_throw_circle_radius': 180, # 1.8m radius for free throw circle
    'court_min_x': -750, # Minimum X-coordinate of the visible court area
    'court_max_x': 750, # Maximum X-coordinate of the visible court area
    'court_min_y': -100, # Minimum Y-coordinate (baseline)
    'court_max_y': 850, # Maximum Y-coordinate (extended for top 3-point area, ~8.5m from baseline)
}

# --- 2. Shot Classification Function (adapted from JavaScript's classifyShots) ---
def classify_shots_py(data_df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters out free throws and adds 'made' status to the DataFrame.
    """
    # Filter out free throws based on 'ID_ACTION' or 'ACTION'
    filtered_df = data_df[
        ~(
            data_df['ID_ACTION'].str.lower().str.contains("ft", na=False) |
            data_df['ID_ACTION'].str.lower().str.contains("free", na=False) |
            data_df['ACTION'].str.lower().str.contains("free throw", na=False) |
            data_df['ACTION'].str.lower().str.contains("ft", na=False)
        )
    ].copy() # Use .copy() to avoid SettingWithCopyWarning

    # Add 'made' column: 1 if points > 0, 0 otherwise
    filtered_df['made'] = filtered_df['POINTS'].apply(lambda p: 1 if p > 0 else 0)
    return filtered_df

# --- 3. Zone Classification Function (adapted from JavaScript's classifyZones) ---
def classify_zones_py(shot_data_row, court_params):
    """
    Classifies a single shot into one of the 11 specified zones.
    """
    x = shot_data_row['COORD_X']
    y = shot_data_row['COORD_Y']

    # Handle potential NaN values for coordinates
    if pd.isna(x) or pd.isna(y):
        return "Unknown"

    basket_x = court_params['basket_x']
    basket_y = court_params['basket_y']
    three_point_radius = court_params['three_point_radius']
    corner_line_x = court_params['corner_line_x']
    corner_intersection_y = court_params['corner_intersection_y']
    restricted_area_radius = court_params['restricted_area_radius']

    distance = math.sqrt((x - basket_x)**2 + (y - basket_y)**2)
    # Angle in degrees, atan2(x, y) gives angle from +Y axis (basket_y is 0)
    # This matches the JS atan2(shot.coord_x - basket_x, shot.coord_y - basket_y)
    angle = math.degrees(math.atan2(x - basket_x, y - basket_y))

    bin_zone = "Other" # Default value

    # Determine if it's a 3-point attempt based on location
    is_in_corner_3_zone = abs(x) >= corner_line_x and y <= corner_intersection_y
    is_in_arc_3_zone = distance >= three_point_radius and y > corner_intersection_y
    is_actually_3pt_location = is_in_corner_3_zone or is_in_arc_3_zone

    if is_actually_3pt_location:
        if is_in_corner_3_zone:
            bin_zone = "corner 3 left" if x < 0 else "right corner 3"
        else: # It's in the arc 3 zone
            if angle < -30: # Right side of the court from basket perspective
                bin_zone = "right side 3"
            elif angle > 30: # Left side of the court from basket perspective
                bin_zone = "left side 3"
            else:
                bin_zone = "top 3"
    else: # It's a 2-point shot based on location
        if distance <= restricted_area_radius: # At Rim (distance <= 125)
            bin_zone = "at the rim"
        elif distance <= 300: # Short 2pt (125 < distance <= 300)
            if x < -50: # Arbitrary threshold for left/right/center for short-range
                bin_zone = "short 2pt left"
            elif x > 50:
                bin_zone = "short 2pt right"
            else: # -50 <= x <= 50
                bin_zone = "short 2pt center" # Added for symmetry, though not explicitly in JS
        else: # Mid 2pt (300 < distance < three_point_radius (675))
            if x < -50: # Arbitrary threshold for left/right/center for mid-range
                bin_zone = "mid 2pt left"
            elif x > 50:
                bin_zone = "mid 2pt right"
            else: # -50 <= x <= 50
                bin_zone = "mid 2pt center"

    return bin_zone

# --- Data Fetching ---
shotdata_api = ShotData(competition='E')
shot_data_df = shotdata_api.get_game_shot_data_multiple_seasons(2017, 2024)

# --- Apply Classification to DataFrame ---
if not shot_data_df.empty:
    # Filter out free throws and add 'made' column
    shot_data_df = classify_shots_py(shot_data_df)
    # Apply the new zone classification
    shot_data_df['Bin'] = shot_data_df.apply(lambda row: classify_zones_py(row, COURT_PARAMS), axis=1)
    print("Shot data processed with 'Bin' column.")
    print(shot_data_df[['ID_PLAYER', 'Gamecode', 'COORD_X', 'COORD_Y', 'Bin', 'made']].head())
else:
    print("No shot data retrieved from API.")

def insert_shot_data_to_db(shot_data_df):
    # Connect to the database
    conn_str = "postgresql://euroleague_owner:npg_6WgqinJyK5lz@ep-late-sun-a54sr5mz-pooler.us-east-2.aws.neon.tech/euroleague?sslmode=require"
    conn = psycopg2.connect(conn_str)
    cursor = conn.cursor()

    try:
        # 1. Print local DataFrame info
        print(f"Local DataFrame has {len(shot_data_df)} rows")

        # 2. Check for duplicates
        duplicates = shot_data_df.groupby(['ID_PLAYER', 'Gamecode', 'Season', 'NUM_ANOT']).size()
        duplicates = duplicates[duplicates > 1]
        if len(duplicates) > 0:
            print(f"Warning: Found {len(duplicates)} duplicate player-gamecode-season-annotation combinations")
            print("First few duplicates:")
            print(duplicates.head())

        # 3. Create the shot_data table (with new 'Bin' column)
        cursor.execute("""
        DROP TABLE IF EXISTS shot_data_euroleague;
        CREATE TABLE shot_data_euroleague (
            id SERIAL PRIMARY KEY,
            season INTEGER,
            phase TEXT,
            round INTEGER,
            gamecode TEXT,
            num_anot INTEGER,
            team TEXT,
            id_player TEXT,
            player TEXT,
            id_action TEXT,
            action TEXT,
            points INTEGER,
            coord_x INTEGER,
            coord_y INTEGER,
            zone TEXT, -- Keeping original zone column if it exists in data, otherwise it will be null
            bin TEXT, -- New column for detailed shot zones
            fastbreak INTEGER,
            second_chance INTEGER,
            points_off_turnover INTEGER,
            minute INTEGER,
            console TEXT,
            points_a INTEGER,
            points_b INTEGER,
            utc TEXT,
            UNIQUE(id_player, gamecode, season, num_anot)
        );
        """)
        conn.commit()

        # 4. Check current row count
        cursor.execute("SELECT COUNT(*) FROM shot_data_euroleague;")
        before_count = cursor.fetchone()[0]
        print(f"Rows in database before insert: {before_count}")

        # 5. Define helper to clean values
        def safe_int(val):
            if pd.isna(val):
                return None
            try:
                return int(val)
            except (ValueError, TypeError):
                return None

        def safe_str(val):
            if pd.isna(val):
                return None
            return str(val)

        # 6. Build the data tuples from the DataFrame
        data_tuples = []
        for _, row in shot_data_df.iterrows():
            data_tuples.append((
                safe_int(row["Season"]),
                safe_str(row["Phase"]),
                safe_int(row["Round"]),
                safe_str(row["Gamecode"]),
                safe_int(row["NUM_ANOT"]),
                safe_str(row["TEAM"]),
                safe_str(row["ID_PLAYER"]),
                safe_str(row["PLAYER"]),
                safe_str(row["ID_ACTION"]),
                safe_str(row["ACTION"]),
                safe_int(row["POINTS"]),
                safe_int(row["COORD_X"]),
                safe_int(row["COORD_Y"]),
                safe_str(row["ZONE"]) if "ZONE" in row else None, # Keep original ZONE if present
                safe_str(row["Bin"]), # New Bin column
                safe_int(row["FASTBREAK"]),
                safe_int(row["SECOND_CHANCE"]),
                safe_int(row["POINTS_OFF_TURNOVER"]),
                safe_int(row["MINUTE"]),
                safe_str(row["CONSOLE"]),
                safe_int(row["POINTS_A"]),
                safe_int(row["POINTS_B"]),
                safe_str(row["UTC"])
            ))

        print(f"Prepared {len(data_tuples)} tuples for insertion")

        # 7. Define the INSERT statement with conflict resolution (with new 'Bin' column)
        insert_query = """
        INSERT INTO shot_data_euroleague (
            season, phase, round, gamecode, num_anot, team, id_player, player,
            id_action, action, points, coord_x, coord_y, zone, bin, fastbreak,
            second_chance, points_off_turnover, minute, console, points_a,
            points_b, utc
        ) VALUES %s
        ON CONFLICT (id_player, gamecode, season, num_anot) DO UPDATE SET
            phase = EXCLUDED.phase,
            round = EXCLUDED.round,
            team = EXCLUDED.team,
            player = EXCLUDED.player,
            id_action = EXCLUDED.id_action,
            action = EXCLUDED.action,
            points = EXCLUDED.points,
            coord_x = EXCLUDED.coord_x,
            coord_y = EXCLUDED.coord_y,
            zone = EXCLUDED.zone,
            bin = EXCLUDED.bin, -- Update the new Bin column
            fastbreak = EXCLUDED.fastbreak,
            second_chance = EXCLUDED.second_chance,
            points_off_turnover = EXCLUDED.points_off_turnover,
            minute = EXCLUDED.minute,
            console = EXCLUDED.console,
            points_a = EXCLUDED.points_a,
            points_b = EXCLUDED.points_b,
            utc = EXCLUDED.utc;
        """

        # 8. Bulk insert data
        execute_values(cursor, insert_query, data_tuples)
        rows_affected = cursor.rowcount
        conn.commit()

        print(f"Insert operation affected {rows_affected} rows")

        # 9. Check final row count
        cursor.execute("SELECT COUNT(*) FROM shot_data_euroleague;")
        after_count = cursor.fetchone()[0]
        print(f"Rows in database after insert: {after_count}")

        # 10. Verify data integrity
        cursor.execute("SELECT COUNT(*) FROM (SELECT DISTINCT id_player, gamecode, season, num_anot FROM shot_data_euroleague) AS unique_combos;")
        unique_combinations = cursor.fetchone()[0]
        print(f"Unique player-gamecode-season-annotation combinations: {unique_combinations}")

        # 11. Show some sample data (including 'Bin' column)
        cursor.execute("""
            SELECT player, team, action, points, coord_x, coord_y, zone, bin
            FROM shot_data_euroleague
            ORDER BY season DESC, round DESC, gamecode DESC
            LIMIT 5;
        """)
        sample_data = cursor.fetchall()
        print("\nSample data from shot_data table:")
        for row in sample_data:
            print(f"Player: {row[0]}, Team: {row[1]}, Action: {row[2]}, Points: {row[3]}, Coords: ({row[4]}, {row[5]}), Zone (Original): {row[6]}, Bin (New): {row[7]}")

        # 12. Show action type distribution
        cursor.execute("""
            SELECT action, COUNT(*) as count
            FROM shot_data_euroleague
            GROUP BY action
            ORDER BY count DESC;
        """)
        action_stats = cursor.fetchall()
        print("\nAction type distribution:")
        for action, count in action_stats:
            print(f"{action}: {count}")

        # 13. Show Bin distribution
        cursor.execute("""
            SELECT bin, COUNT(*) as count
            FROM shot_data_euroleague
            GROUP BY bin
            ORDER BY count DESC;
        """)
        bin_stats = cursor.fetchall()
        print("\nShot Bin distribution:")
        for bin_name, count in bin_stats:
            print(f"{bin_name}: {count}")

        print("\nShot data inserted successfully into Neon database!")

    except Exception as e:
        print(f"Error during database operation: {e}")
        conn.rollback()
        raise
    finally:
        # Close connections
        cursor.close()
        conn.close()

# Call the function with your shot_data DataFrame
# Make sure your DataFrame is named 'shot_data_df'
insert_shot_data_to_db(shot_data_df)


# In[8]:


# Shooting League Averages Euroleague


import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import math
from euroleague_api.shot_data import ShotData

# --- 1. Define Court Parameters (from JavaScript's findCourtParameters) ---
# These values are based on standard FIBA court dimensions, converted and scaled for the chart.
COURT_PARAMS = {
    'basket_x': 0,
    'basket_y': 0, # Basket at origin of court coordinate system
    'three_point_radius': 675, # 6.75m in coordinate units (e.g., centimeters)
    'corner_line_x': 660, # Distance from center to corner line (6.60m)
    'corner_intersection_y': 157.5, # Y-coordinate where 3pt arc meets straight line (1.575m from basket on the Y-axis)
    'restricted_area_radius': 125, # 1.25m
    'baseline_y': -100, # Baseline position relative to basket_y (e.g., -1.0m behind basket)
    'paint_width': 490, # 4.9m
    'paint_height': 580, # 5.8m (distance from baseline to free throw line)
    'free_throw_distance': 580, # 5.8m from baseline to free throw line (same as paint height)
    'free_throw_circle_radius': 180, # 1.8m radius for free throw circle
    'court_min_x': -750, # Minimum X-coordinate of the visible court area
    'court_max_x': 750, # Maximum X-coordinate of the visible court area
    'court_min_y': -100, # Minimum Y-coordinate (baseline)
    'court_max_y': 850, # Maximum Y-coordinate (extended for top 3-point area, ~8.5m from baseline)
}

# --- 2. Shot Classification Function (adapted from JavaScript's classifyShots) ---
def classify_shots_py(data_df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters out free throws and adds 'made' status to the DataFrame.
    """
    # Filter out free throws based on 'ID_ACTION' or 'ACTION'
    filtered_df = data_df[
        ~(
            data_df['ID_ACTION'].str.lower().str.contains("ft", na=False) |
            data_df['ID_ACTION'].str.lower().str.contains("free", na=False) |
            data_df['ACTION'].str.lower().str.contains("free throw", na=False) |
            data_df['ACTION'].str.lower().str.contains("ft", na=False)
        )
    ].copy() # Use .copy() to avoid SettingWithCopyWarning

    # Add 'made' column: 1 if points > 0, 0 otherwise
    filtered_df['made'] = filtered_df['POINTS'].apply(lambda p: 1 if p > 0 else 0)
    return filtered_df

# --- 3. Zone Classification Function (adapted from JavaScript's classifyZones) ---
def classify_zones_py(shot_data_row, court_params):
    """
    Classifies a single shot into one of the 11 specified zones.
    """
    x = shot_data_row['COORD_X']
    y = shot_data_row['COORD_Y']

    # Handle potential NaN values for coordinates
    if pd.isna(x) or pd.isna(y):
        return "Unknown"

    basket_x = court_params['basket_x']
    basket_y = court_params['basket_y']
    three_point_radius = court_params['three_point_radius']
    corner_line_x = court_params['corner_line_x']
    corner_intersection_y = court_params['corner_intersection_y']
    restricted_area_radius = court_params['restricted_area_radius']

    distance = math.sqrt((x - basket_x)**2 + (y - basket_y)**2)
    # Angle in degrees, atan2(x, y) gives angle from +Y axis (basket_y is 0)
    # This matches the JS atan2(shot.coord_x - basket_x, shot.coord_y - basket_y)
    angle = math.degrees(math.atan2(x - basket_x, y - basket_y))

    bin_zone = "Other" # Default value

    # Determine if it's a 3-point attempt based on location
    is_in_corner_3_zone = abs(x) >= corner_line_x and y <= corner_intersection_y
    is_in_arc_3_zone = distance >= three_point_radius and y > corner_intersection_y
    is_actually_3pt_location = is_in_corner_3_zone or is_in_arc_3_zone

    if is_actually_3pt_location:
        if is_in_corner_3_zone:
            bin_zone = "corner 3 left" if x < 0 else "right corner 3"
        else: # It's in the arc 3 zone
            if angle < -30: # Right side of the court from basket perspective
                bin_zone = "right side 3"
            elif angle > 30: # Left side of the court from basket perspective
                bin_zone = "left side 3"
            else:
                bin_zone = "top 3"
    else: # It's a 2-point shot based on location
        if distance <= restricted_area_radius: # At Rim (distance <= 125)
            bin_zone = "at the rim"
        elif distance <= 300: # Short 2pt (125 < distance <= 300)
            if x < -50: # Arbitrary threshold for left/right/center for short-range
                bin_zone = "short 2pt left"
            elif x > 50:
                bin_zone = "short 2pt right"
            else: # -50 <= x <= 50
                bin_zone = "short 2pt center" # Added for symmetry, though not explicitly in JS
        else: # Mid 2pt (300 < distance < three_point_radius (675))
            if x < -50: # Arbitrary threshold for left/right/center for mid-range
                bin_zone = "mid 2pt left"
            elif x > 50:
                bin_zone = "mid 2pt right"
            else: # -50 <= x <= 50
                bin_zone = "mid 2pt center"

    return bin_zone

# --- Data Fetching ---
# Fetching data for multiple seasons if needed, for averages per season
start_season = 2017  # Example: Start from 2020 season
end_season = 2024    # Example: End at 2024 season
shotdata_api = ShotData(competition='E')
all_shot_data_df = pd.DataFrame()

for season in range(start_season, end_season + 1):
    print(f"Fetching shot data for season {season}")
    season_data = shotdata_api.get_game_shot_data_multiple_seasons(season, season)
    if not season_data.empty:
        season_data['Season'] = season # Add season column for grouping later
        all_shot_data_df = pd.concat([all_shot_data_df, season_data], ignore_index=True)
    else:
        print(f"No data for season {season}")

# --- Apply Classification to DataFrame ---
if not all_shot_data_df.empty:
    # Filter out free throws and add 'made' column
    all_shot_data_df = classify_shots_py(all_shot_data_df)
    # Apply the new zone classification
    all_shot_data_df['Bin'] = all_shot_data_df.apply(lambda row: classify_zones_py(row, COURT_PARAMS), axis=1)
    print("All shot data processed with 'Bin' column.")
    print(all_shot_data_df[['Season', 'ID_PLAYER', 'Gamecode', 'COORD_X', 'COORD_Y', 'Bin', 'made']].head())
else:
    print("No shot data retrieved from API across specified seasons.")

def insert_league_averages_to_db(shot_data_df: pd.DataFrame):
    """
    Calculates league averages for shot zones per season and inserts them into
    the 'shot_data_euroleague_averages' table.
    """
    if shot_data_df.empty:
        print("No shot data to process for league averages.")
        return

    # Calculate league averages per season per bin
    # We need total shots and made shots for each Bin and Season
    league_averages = shot_data_df.groupby(['Season', 'Bin']).agg(
        total_shots=('made', 'size'),
        made_shots=('made', 'sum')
    ).reset_index()

    # Calculate Shot Percentage
    league_averages['shot_percentage'] = (league_averages['made_shots'] / league_averages['total_shots']).fillna(0)

    # Calculate Effective Field Goal Percentage (eFG%)
    # eFG% = (FGM + 0.5 * 3PM) / FGA
    # To calculate eFG%, we need to know which 'Bin' corresponds to 3-pointers.
    # From your classify_zones_py: "corner 3 left", "right corner 3", "right side 3", "left side 3", "top 3" are 3-pointers.
    # For simplicity, let's assume 'POINTS' column in the original data tells us if it was a 3pt attempt.
    # We'll need to re-aggregate if we want true eFG% per bin.
    # For now, let's just use regular shot percentage. If you need eFG% per bin,
    # we'd need to modify the aggregation to count 3-point attempts and makes within each bin.
    # For this example, we'll just store regular shot percentage.

    # If you later want eFG%, you'd need to adjust the aggregation:
    # shot_data_df['is_three_pointer'] = shot_data_df['POINTS'].apply(lambda p: 1 if p == 3 else 0)
    # league_averages_efg = shot_data_df.groupby(['Season', 'Bin']).agg(
    #     total_shots=('made', 'size'),
    #     made_shots=('made', 'sum'),
    #     made_threes=('is_three_pointer', lambda x: shot_data_df.loc[x.index, 'made'] * x).sum() # Sum of made 3s
    # ).reset_index()
    # league_averages_efg['efg_percentage'] = (league_averages_efg['made_shots'] + 0.5 * league_averages_efg['made_threes']) / league_averages_efg['total_shots']

    print("\nCalculated League Averages per Bin per Season:")
    print(league_averages.head())
    print(f"Total average rows to insert: {len(league_averages)}")

    # Connect to the database
    conn_str = "postgresql://euroleague_owner:npg_6WgqinJyK5lz@ep-late-sun-a54sr5mz-pooler.us-east-2.aws.neon.tech/euroleague?sslmode=require"
    conn = psycopg2.connect(conn_str)
    cursor = conn.cursor()

    try:
        # Create the shot_data_euroleague_averages table
        cursor.execute("""
        DROP TABLE IF EXISTS shot_data_euroleague_averages;
        CREATE TABLE shot_data_euroleague_averages (
            id SERIAL PRIMARY KEY,
            season INTEGER NOT NULL,
            bin TEXT NOT NULL,
            total_shots INTEGER,
            made_shots INTEGER,
            shot_percentage REAL,
            UNIQUE(season, bin)
        );
        """)
        conn.commit()
        print("Table 'shot_data_euroleague_averages' created successfully.")

        # Prepare data for insertion
        data_tuples = []
        for _, row in league_averages.iterrows():
            data_tuples.append((
                row["Season"],
                row["Bin"],
                row["total_shots"],
                row["made_shots"],
                row["shot_percentage"]
            ))

        print(f"Prepared {len(data_tuples)} tuples for insertion into averages table")

        # Define the INSERT statement
        insert_query = """
        INSERT INTO shot_data_euroleague_averages (
            season, bin, total_shots, made_shots, shot_percentage
        ) VALUES %s
        ON CONFLICT (season, bin) DO UPDATE SET
            total_shots = EXCLUDED.total_shots,
            made_shots = EXCLUDED.made_shots,
            shot_percentage = EXCLUDED.shot_percentage;
        """

        # Bulk insert data
        execute_values(cursor, insert_query, data_tuples)
        rows_affected = cursor.rowcount
        conn.commit()

        print(f"Insert operation affected {rows_affected} rows in averages table")

        # Verify data integrity
        cursor.execute("SELECT COUNT(*) FROM shot_data_euroleague_averages;")
        after_count = cursor.fetchone()[0]
        print(f"Rows in averages table after insert: {after_count}")

        # Show some sample data from the new table
        cursor.execute("""
            SELECT season, bin, total_shots, made_shots, shot_percentage
            FROM shot_data_euroleague_averages
            ORDER BY season DESC, bin
            LIMIT 10;
        """)
        sample_data = cursor.fetchall()
        print("\nSample data from shot_data_euroleague_averages table:")
        for row in sample_data:
            print(f"Season: {row[0]}, Bin: {row[1]}, Total Shots: {row[2]}, Made Shots: {row[3]}, Shot %: {row[4]:.4f}")

        print("\nLeague average shot data inserted successfully into Neon database!")

    except Exception as e:
        print(f"Error during database operation for averages: {e}")
        conn.rollback()
        raise
    finally:
        # Close connections
        cursor.close()
        conn.close()

# Call the function to insert league averages
insert_league_averages_to_db(all_shot_data_df)


# In[13]:


# shot_data_eurocup

import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import math
from euroleague_api.shot_data import ShotData

# --- 1. Define Court Parameters (from JavaScript's findCourtParameters) ---
# These values are based on standard FIBA court dimensions, converted and scaled for the chart.
COURT_PARAMS = {
    'basket_x': 0,
    'basket_y': 0, # Basket at origin of court coordinate system
    'three_point_radius': 675, # 6.75m in coordinate units (e.g., centimeters)
    'corner_line_x': 660, # Distance from center to corner line (6.60m)
    'corner_intersection_y': 157.5, # Y-coordinate where 3pt arc meets straight line (1.575m from basket on the Y-axis)
    'restricted_area_radius': 125, # 1.25m
    'baseline_y': -100, # Baseline position relative to basket_y (e.g., -1.0m behind basket)
    'paint_width': 490, # 4.9m
    'paint_height': 580, # 5.8m (distance from baseline to free throw line)
    'free_throw_distance': 580, # 5.8m from baseline to free throw line (same as paint height)
    'free_throw_circle_radius': 180, # 1.8m radius for free throw circle
    'court_min_x': -750, # Minimum X-coordinate of the visible court area
    'court_max_x': 750, # Maximum X-coordinate of the visible court area
    'court_min_y': -100, # Minimum Y-coordinate (baseline)
    'court_max_y': 850, # Maximum Y-coordinate (extended for top 3-point area, ~8.5m from baseline)
}

# --- 2. Shot Classification Function (adapted from JavaScript's classifyShots) ---
def classify_shots_py(data_df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters out free throws and adds 'made' status to the DataFrame.
    """
    # Filter out free throws based on 'ID_ACTION' or 'ACTION'
    filtered_df = data_df[
        ~(
            data_df['ID_ACTION'].str.lower().str.contains("ft", na=False) |
            data_df['ID_ACTION'].str.lower().str.contains("free", na=False) |
            data_df['ACTION'].str.lower().str.contains("free throw", na=False) |
            data_df['ACTION'].str.lower().str.contains("ft", na=False)
        )
    ].copy() # Use .copy() to avoid SettingWithCopyWarning

    # Add 'made' column: 1 if points > 0, 0 otherwise
    filtered_df['made'] = filtered_df['POINTS'].apply(lambda p: 1 if p > 0 else 0)
    return filtered_df

# --- 3. Zone Classification Function (adapted from JavaScript's classifyZones) ---
def classify_zones_py(shot_data_row, court_params):
    """
    Classifies a single shot into one of the 11 specified zones.
    """
    x = shot_data_row['COORD_X']
    y = shot_data_row['COORD_Y']

    # Handle potential NaN values for coordinates
    if pd.isna(x) or pd.isna(y):
        return "Unknown"

    basket_x = court_params['basket_x']
    basket_y = court_params['basket_y']
    three_point_radius = court_params['three_point_radius']
    corner_line_x = court_params['corner_line_x']
    corner_intersection_y = court_params['corner_intersection_y']
    restricted_area_radius = court_params['restricted_area_radius']

    distance = math.sqrt((x - basket_x)**2 + (y - basket_y)**2)
    # Angle in degrees, atan2(x, y) gives angle from +Y axis (basket_y is 0)
    # This matches the JS atan2(shot.coord_x - basket_x, shot.coord_y - basket_y)
    angle = math.degrees(math.atan2(x - basket_x, y - basket_y))

    bin_zone = "Other" # Default value

    # Determine if it's a 3-point attempt based on location
    is_in_corner_3_zone = abs(x) >= corner_line_x and y <= corner_intersection_y
    is_in_arc_3_zone = distance >= three_point_radius and y > corner_intersection_y
    is_actually_3pt_location = is_in_corner_3_zone or is_in_arc_3_zone

    if is_actually_3pt_location:
        if is_in_corner_3_zone:
            bin_zone = "corner 3 left" if x < 0 else "right corner 3"
        else: # It's in the arc 3 zone
            if angle < -30: # Right side of the court from basket perspective
                bin_zone = "right side 3"
            elif angle > 30: # Left side of the court from basket perspective
                bin_zone = "left side 3"
            else:
                bin_zone = "top 3"
    else: # It's a 2-point shot based on location
        if distance <= restricted_area_radius: # At Rim (distance <= 125)
            bin_zone = "at the rim"
        elif distance <= 300: # Short 2pt (125 < distance <= 300)
            if x < -50: # Arbitrary threshold for left/right/center for short-range
                bin_zone = "short 2pt left"
            elif x > 50:
                bin_zone = "short 2pt right"
            else: # -50 <= x <= 50
                bin_zone = "short 2pt center" # Added for symmetry, though not explicitly in JS
        else: # Mid 2pt (300 < distance < three_point_radius (675))
            if x < -50: # Arbitrary threshold for left/right/center for mid-range
                bin_zone = "mid 2pt left"
            elif x > 50:
                bin_zone = "mid 2pt right"
            else: # -50 <= x <= 50
                bin_zone = "mid 2pt center"

    return bin_zone

# --- Data Fetching ---
shotdata_api = ShotData(competition='U')
shot_data_df = shotdata_api.get_game_shot_data_multiple_seasons(2017, 2024)

# --- Apply Classification to DataFrame ---
if not shot_data_df.empty:
    # Filter out free throws and add 'made' column
    shot_data_df = classify_shots_py(shot_data_df)
    # Apply the new zone classification
    shot_data_df['Bin'] = shot_data_df.apply(lambda row: classify_zones_py(row, COURT_PARAMS), axis=1)
    print("Shot data processed with 'Bin' column.")
    print(shot_data_df[['ID_PLAYER', 'Gamecode', 'COORD_X', 'COORD_Y', 'Bin', 'made']].head())
else:
    print("No shot data retrieved from API.")

def insert_shot_data_to_db(shot_data_df):
    # Connect to the database
    conn_str = "postgresql://euroleague_owner:npg_6WgqinJyK5lz@ep-late-sun-a54sr5mz-pooler.us-east-2.aws.neon.tech/euroleague?sslmode=require"
    conn = psycopg2.connect(conn_str)
    cursor = conn.cursor()

    try:
        # 1. Print local DataFrame info
        print(f"Local DataFrame has {len(shot_data_df)} rows")

        # 2. Check for duplicates
        duplicates = shot_data_df.groupby(['ID_PLAYER', 'Gamecode', 'Season', 'NUM_ANOT']).size()
        duplicates = duplicates[duplicates > 1]
        if len(duplicates) > 0:
            print(f"Warning: Found {len(duplicates)} duplicate player-gamecode-season-annotation combinations")
            print("First few duplicates:")
            print(duplicates.head())

        # 3. Create the shot_data table (with new 'Bin' column)
        cursor.execute("""
        DROP TABLE IF EXISTS shot_data_eurocup;
        CREATE TABLE shot_data_eurocup (
            id SERIAL PRIMARY KEY,
            season INTEGER,
            phase TEXT,
            round INTEGER,
            gamecode TEXT,
            num_anot INTEGER,
            team TEXT,
            id_player TEXT,
            player TEXT,
            id_action TEXT,
            action TEXT,
            points INTEGER,
            coord_x INTEGER,
            coord_y INTEGER,
            zone TEXT, -- Keeping original zone column if it exists in data, otherwise it will be null
            bin TEXT, -- New column for detailed shot zones
            fastbreak INTEGER,
            second_chance INTEGER,
            points_off_turnover INTEGER,
            minute INTEGER,
            console TEXT,
            points_a INTEGER,
            points_b INTEGER,
            utc TEXT,
            UNIQUE(id_player, gamecode, season, num_anot)
        );
        """)
        conn.commit()

        # 4. Check current row count
        cursor.execute("SELECT COUNT(*) FROM shot_data_eurocup;")
        before_count = cursor.fetchone()[0]
        print(f"Rows in database before insert: {before_count}")

        # 5. Define helper to clean values
        def safe_int(val):
            if pd.isna(val):
                return None
            try:
                return int(val)
            except (ValueError, TypeError):
                return None

        def safe_str(val):
            if pd.isna(val):
                return None
            return str(val)

        # 6. Build the data tuples from the DataFrame
        data_tuples = []
        for _, row in shot_data_df.iterrows():
            data_tuples.append((
                safe_int(row["Season"]),
                safe_str(row["Phase"]),
                safe_int(row["Round"]),
                safe_str(row["Gamecode"]),
                safe_int(row["NUM_ANOT"]),
                safe_str(row["TEAM"]),
                safe_str(row["ID_PLAYER"]),
                safe_str(row["PLAYER"]),
                safe_str(row["ID_ACTION"]),
                safe_str(row["ACTION"]),
                safe_int(row["POINTS"]),
                safe_int(row["COORD_X"]),
                safe_int(row["COORD_Y"]),
                safe_str(row["ZONE"]) if "ZONE" in row else None, # Keep original ZONE if present
                safe_str(row["Bin"]), # New Bin column
                safe_int(row["FASTBREAK"]),
                safe_int(row["SECOND_CHANCE"]),
                safe_int(row["POINTS_OFF_TURNOVER"]),
                safe_int(row["MINUTE"]),
                safe_str(row["CONSOLE"]),
                safe_int(row["POINTS_A"]),
                safe_int(row["POINTS_B"]),
                safe_str(row["UTC"])
            ))

        print(f"Prepared {len(data_tuples)} tuples for insertion")

        # 7. Define the INSERT statement with conflict resolution (with new 'Bin' column)
        insert_query = """
        INSERT INTO shot_data_eurocup (
            season, phase, round, gamecode, num_anot, team, id_player, player,
            id_action, action, points, coord_x, coord_y, zone, bin, fastbreak,
            second_chance, points_off_turnover, minute, console, points_a,
            points_b, utc
        ) VALUES %s
        ON CONFLICT (id_player, gamecode, season, num_anot) DO UPDATE SET
            phase = EXCLUDED.phase,
            round = EXCLUDED.round,
            team = EXCLUDED.team,
            player = EXCLUDED.player,
            id_action = EXCLUDED.id_action,
            action = EXCLUDED.action,
            points = EXCLUDED.points,
            coord_x = EXCLUDED.coord_x,
            coord_y = EXCLUDED.coord_y,
            zone = EXCLUDED.zone,
            bin = EXCLUDED.bin, -- Update the new Bin column
            fastbreak = EXCLUDED.fastbreak,
            second_chance = EXCLUDED.second_chance,
            points_off_turnover = EXCLUDED.points_off_turnover,
            minute = EXCLUDED.minute,
            console = EXCLUDED.console,
            points_a = EXCLUDED.points_a,
            points_b = EXCLUDED.points_b,
            utc = EXCLUDED.utc;
        """

        # 8. Bulk insert data
        execute_values(cursor, insert_query, data_tuples)
        rows_affected = cursor.rowcount
        conn.commit()

        print(f"Insert operation affected {rows_affected} rows")

        # 9. Check final row count
        cursor.execute("SELECT COUNT(*) FROM shot_data_eurocup;")
        after_count = cursor.fetchone()[0]
        print(f"Rows in database after insert: {after_count}")

        # 10. Verify data integrity
        cursor.execute("SELECT COUNT(*) FROM (SELECT DISTINCT id_player, gamecode, season, num_anot FROM shot_data_eurocup) AS unique_combos;")
        unique_combinations = cursor.fetchone()[0]
        print(f"Unique player-gamecode-season-annotation combinations: {unique_combinations}")

        # 11. Show some sample data (including 'Bin' column)
        cursor.execute("""
            SELECT player, team, action, points, coord_x, coord_y, zone, bin
            FROM shot_data_eurocup
            ORDER BY season DESC, round DESC, gamecode DESC
            LIMIT 5;
        """)
        sample_data = cursor.fetchall()
        print("\nSample data from shot_data table:")
        for row in sample_data:
            print(f"Player: {row[0]}, Team: {row[1]}, Action: {row[2]}, Points: {row[3]}, Coords: ({row[4]}, {row[5]}), Zone (Original): {row[6]}, Bin (New): {row[7]}")

        # 12. Show action type distribution
        cursor.execute("""
            SELECT action, COUNT(*) as count
            FROM shot_data_eurocup
            GROUP BY action
            ORDER BY count DESC;
        """)
        action_stats = cursor.fetchall()
        print("\nAction type distribution:")
        for action, count in action_stats:
            print(f"{action}: {count}")

        # 13. Show Bin distribution
        cursor.execute("""
            SELECT bin, COUNT(*) as count
            FROM shot_data_eurocup
            GROUP BY bin
            ORDER BY count DESC;
        """)
        bin_stats = cursor.fetchall()
        print("\nShot Bin distribution:")
        for bin_name, count in bin_stats:
            print(f"{bin_name}: {count}")

        print("\nShot data inserted successfully into Neon database!")

    except Exception as e:
        print(f"Error during database operation: {e}")
        conn.rollback()
        raise
    finally:
        # Close connections
        cursor.close()
        conn.close()

# Call the function with your shot_data DataFrame
# Make sure your DataFrame is named 'shot_data_df'
insert_shot_data_to_db(shot_data_df)


# In[12]:


# Shooting League Averages Eurocup
insert_shot_data_to_db(shot_data_df)

import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import math
from euroleague_api.shot_data import ShotData

# --- 1. Define Court Parameters (from JavaScript's findCourtParameters) ---
# These values are based on standard FIBA court dimensions, converted and scaled for the chart.
COURT_PARAMS = {
    'basket_x': 0,
    'basket_y': 0, # Basket at origin of court coordinate system
    'three_point_radius': 675, # 6.75m in coordinate units (e.g., centimeters)
    'corner_line_x': 660, # Distance from center to corner line (6.60m)
    'corner_intersection_y': 157.5, # Y-coordinate where 3pt arc meets straight line (1.575m from basket on the Y-axis)
    'restricted_area_radius': 125, # 1.25m
    'baseline_y': -100, # Baseline position relative to basket_y (e.g., -1.0m behind basket)
    'paint_width': 490, # 4.9m
    'paint_height': 580, # 5.8m (distance from baseline to free throw line)
    'free_throw_distance': 580, # 5.8m from baseline to free throw line (same as paint height)
    'free_throw_circle_radius': 180, # 1.8m radius for free throw circle
    'court_min_x': -750, # Minimum X-coordinate of the visible court area
    'court_max_x': 750, # Maximum X-coordinate of the visible court area
    'court_min_y': -100, # Minimum Y-coordinate (baseline)
    'court_max_y': 850, # Maximum Y-coordinate (extended for top 3-point area, ~8.5m from baseline)
}

# --- 2. Shot Classification Function (adapted from JavaScript's classifyShots) ---
def classify_shots_py(data_df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters out free throws and adds 'made' status to the DataFrame.
    """
    # Filter out free throws based on 'ID_ACTION' or 'ACTION'
    filtered_df = data_df[
        ~(
            data_df['ID_ACTION'].str.lower().str.contains("ft", na=False) |
            data_df['ID_ACTION'].str.lower().str.contains("free", na=False) |
            data_df['ACTION'].str.lower().str.contains("free throw", na=False) |
            data_df['ACTION'].str.lower().str.contains("ft", na=False)
        )
    ].copy() # Use .copy() to avoid SettingWithCopyWarning

    # Add 'made' column: 1 if points > 0, 0 otherwise
    filtered_df['made'] = filtered_df['POINTS'].apply(lambda p: 1 if p > 0 else 0)
    return filtered_df

# --- 3. Zone Classification Function (adapted from JavaScript's classifyZones) ---
def classify_zones_py(shot_data_row, court_params):
    """
    Classifies a single shot into one of the 11 specified zones.
    """
    x = shot_data_row['COORD_X']
    y = shot_data_row['COORD_Y']

    # Handle potential NaN values for coordinates
    if pd.isna(x) or pd.isna(y):
        return "Unknown"

    basket_x = court_params['basket_x']
    basket_y = court_params['basket_y']
    three_point_radius = court_params['three_point_radius']
    corner_line_x = court_params['corner_line_x']
    corner_intersection_y = court_params['corner_intersection_y']
    restricted_area_radius = court_params['restricted_area_radius']

    distance = math.sqrt((x - basket_x)**2 + (y - basket_y)**2)
    # Angle in degrees, atan2(x, y) gives angle from +Y axis (basket_y is 0)
    # This matches the JS atan2(shot.coord_x - basket_x, shot.coord_y - basket_y)
    angle = math.degrees(math.atan2(x - basket_x, y - basket_y))

    bin_zone = "Other" # Default value

    # Determine if it's a 3-point attempt based on location
    is_in_corner_3_zone = abs(x) >= corner_line_x and y <= corner_intersection_y
    is_in_arc_3_zone = distance >= three_point_radius and y > corner_intersection_y
    is_actually_3pt_location = is_in_corner_3_zone or is_in_arc_3_zone

    if is_actually_3pt_location:
        if is_in_corner_3_zone:
            bin_zone = "corner 3 left" if x < 0 else "right corner 3"
        else: # It's in the arc 3 zone
            if angle < -30: # Right side of the court from basket perspective
                bin_zone = "right side 3"
            elif angle > 30: # Left side of the court from basket perspective
                bin_zone = "left side 3"
            else:
                bin_zone = "top 3"
    else: # It's a 2-point shot based on location
        if distance <= restricted_area_radius: # At Rim (distance <= 125)
            bin_zone = "at the rim"
        elif distance <= 300: # Short 2pt (125 < distance <= 300)
            if x < -50: # Arbitrary threshold for left/right/center for short-range
                bin_zone = "short 2pt left"
            elif x > 50:
                bin_zone = "short 2pt right"
            else: # -50 <= x <= 50
                bin_zone = "short 2pt center" # Added for symmetry, though not explicitly in JS
        else: # Mid 2pt (300 < distance < three_point_radius (675))
            if x < -50: # Arbitrary threshold for left/right/center for mid-range
                bin_zone = "mid 2pt left"
            elif x > 50:
                bin_zone = "mid 2pt right"
            else: # -50 <= x <= 50
                bin_zone = "mid 2pt center"

    return bin_zone

# --- Data Fetching ---
# Fetching data for multiple seasons if needed, for averages per season
start_season = 2017  # Example: Start from 2020 season
end_season = 2024    # Example: End at 2024 season
shotdata_api = ShotData(competition='U')
all_shot_data_df = shot_data_df

# for season in range(start_season, end_season + 1):
#     print(f"Fetching shot data for season {season}")
#     season_data = shotdata_api.get_game_shot_data_multiple_seasons(season, season)
#     if not season_data.empty:
#         season_data['Season'] = season # Add season column for grouping later
#         all_shot_data_df = pd.concat([all_shot_data_df, season_data], ignore_index=True)
#     else:
#         print(f"No data for season {season}")

# # --- Apply Classification to DataFrame ---
# if not all_shot_data_df.empty:
#     # Filter out free throws and add 'made' column
#     all_shot_data_df = classify_shots_py(all_shot_data_df)
#     # Apply the new zone classification
#     all_shot_data_df['Bin'] = all_shot_data_df.apply(lambda row: classify_zones_py(row, COURT_PARAMS), axis=1)
#     print("All shot data processed with 'Bin' column.")
#     print(all_shot_data_df[['Season', 'ID_PLAYER', 'Gamecode', 'COORD_X', 'COORD_Y', 'Bin', 'made']].head())
# else:
#     print("No shot data retrieved from API across specified seasons.")

def insert_league_averages_to_db(shot_data_df: pd.DataFrame):
    """
    Calculates league averages for shot zones per season and inserts them into
    the 'shot_data_eurocup_averages' table.
    """
    if shot_data_df.empty:
        print("No shot data to process for league averages.")
        return

    # Calculate league averages per season per bin
    # We need total shots and made shots for each Bin and Season
    league_averages = shot_data_df.groupby(['Season', 'Bin']).agg(
        total_shots=('made', 'size'),
        made_shots=('made', 'sum')
    ).reset_index()

    # Calculate Shot Percentage
    league_averages['shot_percentage'] = (league_averages['made_shots'] / league_averages['total_shots']).fillna(0)

    # Calculate Effective Field Goal Percentage (eFG%)
    # eFG% = (FGM + 0.5 * 3PM) / FGA
    # To calculate eFG%, we need to know which 'Bin' corresponds to 3-pointers.
    # From your classify_zones_py: "corner 3 left", "right corner 3", "right side 3", "left side 3", "top 3" are 3-pointers.
    # For simplicity, let's assume 'POINTS' column in the original data tells us if it was a 3pt attempt.
    # We'll need to re-aggregate if we want true eFG% per bin.
    # For now, let's just use regular shot percentage. If you need eFG% per bin,
    # we'd need to modify the aggregation to count 3-point attempts and makes within each bin.
    # For this example, we'll just store regular shot percentage.

    # If you later want eFG%, you'd need to adjust the aggregation:
    # shot_data_df['is_three_pointer'] = shot_data_df['POINTS'].apply(lambda p: 1 if p == 3 else 0)
    # league_averages_efg = shot_data_df.groupby(['Season', 'Bin']).agg(
    #     total_shots=('made', 'size'),
    #     made_shots=('made', 'sum'),
    #     made_threes=('is_three_pointer', lambda x: shot_data_df.loc[x.index, 'made'] * x).sum() # Sum of made 3s
    # ).reset_index()
    # league_averages_efg['efg_percentage'] = (league_averages_efg['made_shots'] + 0.5 * league_averages_efg['made_threes']) / league_averages_efg['total_shots']

    print("\nCalculated League Averages per Bin per Season:")
    print(league_averages.head())
    print(f"Total average rows to insert: {len(league_averages)}")

    # Connect to the database
    conn_str = "postgresql://euroleague_owner:npg_6WgqinJyK5lz@ep-late-sun-a54sr5mz-pooler.us-east-2.aws.neon.tech/euroleague?sslmode=require"
    conn = psycopg2.connect(conn_str)
    cursor = conn.cursor()

    try:
        # Create the shot_data_euroleague_averages table
        cursor.execute("""
        DROP TABLE IF EXISTS shot_data_eurocup_averages;
        CREATE TABLE shot_data_eurocup_averages (
            id SERIAL PRIMARY KEY,
            season INTEGER NOT NULL,
            bin TEXT NOT NULL,
            total_shots INTEGER,
            made_shots INTEGER,
            shot_percentage REAL,
            UNIQUE(season, bin)
        );
        """)
        conn.commit()
        print("Table 'shot_data_eurocup_averages' created successfully.")

        # Prepare data for insertion
        data_tuples = []
        for _, row in league_averages.iterrows():
            data_tuples.append((
                row["Season"],
                row["Bin"],
                row["total_shots"],
                row["made_shots"],
                row["shot_percentage"]
            ))

        print(f"Prepared {len(data_tuples)} tuples for insertion into averages table")

        # Define the INSERT statement
        insert_query = """
        INSERT INTO shot_data_eurocup_averages (
            season, bin, total_shots, made_shots, shot_percentage
        ) VALUES %s
        ON CONFLICT (season, bin) DO UPDATE SET
            total_shots = EXCLUDED.total_shots,
            made_shots = EXCLUDED.made_shots,
            shot_percentage = EXCLUDED.shot_percentage;
        """

        # Bulk insert data
        execute_values(cursor, insert_query, data_tuples)
        rows_affected = cursor.rowcount
        conn.commit()

        print(f"Insert operation affected {rows_affected} rows in averages table")

        # Verify data integrity
        cursor.execute("SELECT COUNT(*) FROM shot_data_eurocup_averages;")
        after_count = cursor.fetchone()[0]
        print(f"Rows in averages table after insert: {after_count}")

        # Show some sample data from the new table
        cursor.execute("""
            SELECT season, bin, total_shots, made_shots, shot_percentage
            FROM shot_data_eurocup_averages
            ORDER BY season DESC, bin
            LIMIT 10;
        """)
        sample_data = cursor.fetchall()
        print("\nSample data from shot_data_eurocup_averages table:")
        for row in sample_data:
            print(f"Season: {row[0]}, Bin: {row[1]}, Total Shots: {row[2]}, Made Shots: {row[3]}, Shot %: {row[4]:.4f}")

        print("\nLeague average shot data inserted successfully into Neon database!")

    except Exception as e:
        print(f"Error during database operation for averages: {e}")
        conn.rollback()
        raise
    finally:
        # Close connections
        cursor.close()
        conn.close()

# Call the function to insert league averages
insert_league_averages_to_db(all_shot_data_df)


# In[ ]:





# In[ ]:





#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Insert shot data into Neon Database

import pandas as pd
import time
import psycopg2
from psycopg2.extras import execute_values
import os
import math

from euroleague_api.shot_data import ShotData

COURT_PARAMS = {
    'basket_x': 0,
    'basket_y': 0,
    'three_point_radius': 675,
    'corner_line_x': 660,
    'corner_intersection_y': 157.5,
    'restricted_area_radius': 125,
}

def classify_shots(data_df: pd.DataFrame) -> pd.DataFrame:
    filtered_df = data_df[
        ~(
            data_df['ID_ACTION'].str.lower().str.contains("ft", na=False) |
            data_df['ID_ACTION'].str.lower().str.contains("free", na=False) |
            data_df['ACTION'].str.lower().str.contains("free throw", na=False) |
            data_df['ACTION'].str.lower().str.contains("ft", na=False)
        )
    ].copy()
    filtered_df['made'] = filtered_df['POINTS'].apply(lambda p: 1 if p > 0 else 0)
    return filtered_df

def classify_zones(shot_data_row, court_params):
    x = shot_data_row['COORD_X']
    y = shot_data_row['COORD_Y']

    if pd.isna(x) or pd.isna(y):
        return "Unknown"

    basket_x = court_params['basket_x']
    basket_y = court_params['basket_y']
    three_point_radius = court_params['three_point_radius']
    corner_line_x = court_params['corner_line_x']
    corner_intersection_y = court_params['corner_intersection_y']
    restricted_area_radius = court_params['restricted_area_radius']

    distance = math.sqrt((x - basket_x)**2 + (y - basket_y)**2)
    angle = math.degrees(math.atan2(x - basket_x, y - basket_y))

    bin_zone = "Other"

    is_in_corner_3_zone = abs(x) >= corner_line_x and y <= corner_intersection_y
    is_in_arc_3_zone = distance >= three_point_radius and y > corner_intersection_y
    is_actually_3pt_location = is_in_corner_3_zone or is_in_arc_3_zone

    if is_actually_3pt_location:
        if is_in_corner_3_zone:
            bin_zone = "corner 3 left" if x < 0 else "right corner 3"
        else:
            if angle < -30:
                bin_zone = "right side 3"
            elif angle > 30:
                bin_zone = "left side 3"
            else:
                bin_zone = "top 3"
    else:
        if distance <= restricted_area_radius:
            bin_zone = "at the rim"
        elif distance <= 300:
            if x < -50:
                bin_zone = "short 2pt left"
            elif x > 50:
                bin_zone = "short 2pt right"
            else:
                bin_zone = "short 2pt center"
        else:
            if x < -50:
                bin_zone = "mid 2pt left"
            elif x > 50:
                bin_zone = "mid 2pt right"
            else:
                bin_zone = "mid 2pt center"

    return bin_zone

def insert_shot_data_to_db(shot_data_df: pd.DataFrame, table_name: str):
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
            zone TEXT,
            bin TEXT,
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
                safe_str(row["ZONE"]) if "ZONE" in row else None,
                safe_str(row["Bin"]),
                safe_int(row["FASTBREAK"]),
                safe_int(row["SECOND_CHANCE"]),
                safe_int(row["POINTS_OFF_TURNOVER"]),
                safe_int(row["MINUTE"]),
                safe_str(row["CONSOLE"]),
                safe_int(row["POINTS_A"]),
                safe_int(row["POINTS_B"]),
                safe_str(row["UTC"])
            ))

        insert_query = f"""
        INSERT INTO {table_name} (
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
            bin = EXCLUDED.bin,
            fastbreak = EXCLUDED.fastbreak,
            second_chance = EXCLUDED.second_chance,
            points_off_turnover = EXCLUDED.points_off_turnover,
            minute = EXCLUDED.minute,
            console = EXCLUDED.console,
            points_a = EXCLUDED.points_a,
            points_b = EXCLUDED.points_b,
            utc = EXCLUDED.utc;
        """

        execute_values(cursor, insert_query, data_tuples)
        conn.commit()

    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()

def update_euro_leagues_shot_data(competition_type: str):
    if competition_type == 'E':
        table_name = 'shot_data_euroleague'
    elif competition_type == 'U':
        table_name = 'shot_data_eurocup'
    else:
        raise ValueError("Invalid competition_type. Must be 'E' for Euroleague or 'U' for Eurocup.")

    shotdata_api = ShotData(competition=competition_type)
    shot_data_df = shotdata_api.get_game_shot_data_multiple_seasons(2017, 2024)

    if not shot_data_df.empty:
        shot_data_df = classify_shots(shot_data_df)
        shot_data_df['Bin'] = shot_data_df.apply(lambda row: classify_zones(row, COURT_PARAMS), axis=1)
        insert_shot_data_to_db(shot_data_df, table_name)

update_euro_leagues_shot_data('E')
update_euro_leagues_shot_data('U')


# In[ ]:





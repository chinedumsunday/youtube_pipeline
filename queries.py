import sqlite3
import pandas as pd 
import numpy as np
import isodate
import os

os.mkdirs('query_logs', exist_ok=True)
import logging
logging.basicConfig(level=logging.INFO,filename='./query_logs/queries.log',
                    format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_top_videos_by_views(limit=10):
    try:
        conn = sqlite3.connect('youtube_data.db')
        query = f"""SELECT video_id, title, view_count 
                    FROM youtube_data 
                    ORDER BY view_count DESC 
                    LIMIT {limit};"""
        df = pd.read_sql_query(query, conn)
        logging.info(f"Fetched top {limit} videos by views successfully ✅")
        return df
    except Exception as e:
        logging.error(f"Error fetching top videos by views: {e}")
        raise
    finally:
        conn.close()
        logging.info("SQLite database connection closed ✅")


def daily_growth_in_views():
    try:
        conn = sqlite3.connect('youtube_data.db')
        query = """SELECT video_id, title, fetch_date,view_count,
                   LAG(view_count) OVER (PARTITION BY video_id ORDER BY fetch_date) AS previous_view_count,
                   (view_count - LAG(view_count) OVER (PARTITION BY video_id ORDER BY fetch_date)) AS daily_view_growth
                   FROM youtube_data;"""
        df = pd.read_sql_query(query, conn)
        logging.info("Fetched daily growth in views successfully ✅")   
    except Exception as e:
        logging.error(f"Error fetching daily growth in views: {e}")
        raise

    finally:
        conn.close()
        logging.info("SQLite database connection closed ✅")
    return df

def daily_rank_movers():
    try:
        conn = sqlite3.connect('youtube_data_db')
        query = """SELECT video_id, title """
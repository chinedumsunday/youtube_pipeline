import sqlite3
import pandas as pd 
import numpy as np
import isodate
import os
import logging

os.makedirs('results', exist_ok=True)


def fetch_top_videos_by_views(limit=10):
    try:
        conn = sqlite3.connect('youtube_data.db')
        query = f"""SELECT video_id, title, view_count 
                    FROM youtube_data 
                    ORDER BY view_count DESC 
                    LIMIT {limit};"""
        df = pd.read_sql_query(query, conn)
        df.to_csv('./results/top_videos_by_views.csv', index=False)
        logging.info(f"Fetched top {limit} videos by views successfully ✅")
        return df
    except Exception as e:
        logging.error(f"Error fetching top videos by views: {e}")
        raise
    finally:
        conn.close()
        logging.info("SQLite database connection closed ✅")


def daily_growth_in_views():
    """
    Docstring for daily_growth_in_views, this function gets the daily growth in views for each video. by taking the difference between the current day's view count and the previous day's view count.
    """
    try:
        conn = sqlite3.connect('youtube_data.db')
        query = """SELECT video_id, title, fetched_date,view_count,
                   LAG(view_count) OVER (PARTITION BY video_id ORDER BY fetched_date) AS previous_view_count,
                   (view_count - LAG(view_count) OVER (PARTITION BY video_id ORDER BY fetched_date)) AS daily_view_growth
                   FROM youtube_data;"""
        df = pd.read_sql_query(query, conn)
        df.to_csv('./results/daily_growth.csv', index=False)
        logging.info("Fetched daily growth in views successfully ✅")   
    except Exception as e:
        logging.error(f"Error fetching daily growth in views: {e}")
        raise

    finally:
        conn.close()
        logging.info("SQLite database connection closed ✅")
    return df

def daily_rank_movers():
    """
    Docstring for daily_rank_movers, this function gets the daily rank change for each video by comparing the current day's rank with the previous day's rank.  
    """
    try:
        conn = sqlite3.connect('youtube_data.db')
        query = """SELECT video_id, title fetched_date, rank, lag(rank) over (partition by video_id order by fetched_date) as previous_rank,
                   (lag(rank) over (partition by video_id order by fetched_date) - rank) as daily_rank_change
                   FROM youtube_data;"""
        df =pd.read_sql_query(query, conn)
        df.to_csv('./results/daily_rank_movement.csv', index=False)
        logging.info("Fetched daily rank difference ✅")
    except Exception as e:
        logging.error(f"Error fetching daily Rank difference due to error {e}")
        raise
    finally:
        conn.close()
        logging.info("Sqlite database connection closed ✅")
    return df

def new_entries():
    """docstring for new_entries, this function fetches the new video entries added to the database today that were not present yesterday."""
    try: 
        conn = sqlite3.connect('youtube_data.db')
        query = """With previous_day AS (SELECT video_id, title, fetched_date FROM youtube_data WHERE fetched_date = DATE('now','-1 day'))
                    SELECT video_id, title, fetched_date FROM youtube_data WHERE fetched_date = DATE('now') AND video_id not in (select video_id from previous_day);"""
        df = pd.read_sql_query(query, conn)
        df.to_csv('./results/new_entries.csv', index=False)
        logging.info("Fetched new entries successfully ✅")
    except Exception as e:
        logging.error(f"Error fetching new entries due to error {e}")
        raise  
    finally:   
        conn.close()
        logging.info("Sqlite database connection closed ✅")

def channel_insights():
    try:
        conn = sqlite3.connect('youtube_data.db')
        query = """SELECT channel_id,channel_title, sum(view_count) as view_count FROM youtube_data GROUP BY channel_id;"""
        df = pd.read_sql_query(query, conn)
        df.to_csv('./results/channel_insights.csv', index=False)
        logging.info("Fetched CHANNEL AND TOTAL VIEWS ✅")
    except Exception as e:
        logging.error("Error Fetching channel total views with error {e}")
        raise
    finally:
        conn.close()
        logging.info("Sqlite database connection closed ✅")
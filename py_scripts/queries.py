import pandas as pd 
import numpy as np
import os
import logging
# try importing the standard library, fallback to experimental if needed
try:
    import libsql
except ImportError:
    import libsql_experimental as libsql

os.makedirs('results', exist_ok=True)

# LOAD ENV VARS
TURSO_DB_URL = os.getenv("TURSO_DB_URL")
TURSO_AUTH_TOKEN = os.getenv("TURSO_AUTH_TOKEN")

def get_connection():
    """Helper function to get DB connection"""
    try:
        # We use keyword arguments to avoid the 'timeout' TypeError
        conn = libsql.connect(database=TURSO_DB_URL, auth_token=TURSO_AUTH_TOKEN)
        return conn
    except Exception as e:
        logging.error(f"Error connecting to Turso: {e}")
        raise

def fetch_top_videos_by_views(limit=10):
    conn = None
    try:
        conn = get_connection()
        # FIX: Added WHERE clause to only get the LATEST date
        query = f"""
            SELECT video_id, title, view_count 
            FROM youtube_data 
            WHERE fetched_date = (SELECT MAX(fetched_date) FROM youtube_data)
            ORDER BY view_count DESC 
            LIMIT {limit};
        """
        df = pd.read_sql_query(query, conn)
        df.to_csv('./results/top_videos_by_views.csv', index=False)
        logging.info(f"Fetched top {limit} videos by views successfully ✅")
        return df
    except Exception as e:
        logging.error(f"Error fetching top videos by views: {e}")
        raise
    finally:
        if conn: conn.close()

def daily_growth_in_views():
    """
    Calculates growth, but filters output to ONLY show the latest day's changes.
    """
    conn = None
    try:
        conn = get_connection()
        # FIX: Wrapped in CTE to calculate history, but only SELECT the latest day
        query = """
        WITH GrowthCalc AS (
            SELECT 
                video_id, 
                title, 
                fetched_date,
                view_count,
                LAG(view_count) OVER (PARTITION BY video_id ORDER BY fetched_date) AS previous_view_count,
                (view_count - LAG(view_count) OVER (PARTITION BY video_id ORDER BY fetched_date)) AS daily_view_growth
            FROM youtube_data
        )
        SELECT * FROM GrowthCalc
        WHERE fetched_date = (SELECT MAX(fetched_date) FROM youtube_data)
        AND daily_view_growth IS NOT NULL
        ORDER BY daily_view_growth DESC;
        """
        df = pd.read_sql_query(query, conn)
        df.to_csv('./results/daily_growth.csv', index=False)
        logging.info("Fetched daily growth in views successfully ✅")   
        return df
    except Exception as e:
        logging.error(f"Error fetching daily growth in views: {e}")
        raise
    finally:
        if conn: conn.close()

def daily_rank_movers():
    """
    Calculates rank changes, filtering to return only the latest day's movement.
    """
    conn = None
    try:
        conn = get_connection()
        # FIX: Wrapped in CTE to filter for MAX(fetched_date)
        query = """
        WITH RankChanges AS (
            SELECT 
                video_id, 
                title, 
                fetched_date, 
                rank, 
                LAG(rank) OVER (PARTITION BY video_id ORDER BY fetched_date) as previous_rank,
                (LAG(rank) OVER (PARTITION BY video_id ORDER BY fetched_date) - rank) as daily_rank_change
            FROM youtube_data
        )
        SELECT * FROM RankChanges
        WHERE fetched_date = (SELECT MAX(fetched_date) FROM youtube_data)
          AND daily_rank_change IS NOT NULL
          AND daily_rank_change != 0
        ORDER BY ABS(daily_rank_change) DESC;
        """
        df = pd.read_sql_query(query, conn)
        df.to_csv('./results/daily_rank_movers.csv', index=False)
        logging.info("Fetched daily rank difference ✅")
        return df
    except Exception as e:
        logging.error(f"Error fetching daily Rank difference: {e}")
        raise
    finally:
        if conn: conn.close()

def new_entries():
    """
    Fetches videos that exist in the latest snapshot but NOT in the previous one.
    """
    conn = None
    try: 
        conn = get_connection()
        # FIX: Dynamic dates instead of hardcoded 'now'
        query = """
        WITH Dates AS (
            SELECT DISTINCT fetched_date FROM youtube_data ORDER BY fetched_date DESC LIMIT 2
        ),
        LatestDate AS (SELECT fetched_date FROM Dates LIMIT 1),
        PreviousDate AS (SELECT fetched_date FROM Dates LIMIT 1 OFFSET 1)

        SELECT video_id, title, fetched_date 
        FROM youtube_data 
        WHERE fetched_date = (SELECT fetched_date FROM LatestDate)
        AND video_id NOT IN (
            SELECT video_id FROM youtube_data 
            WHERE fetched_date = (SELECT fetched_date FROM PreviousDate)
        );
        """
        df = pd.read_sql_query(query, conn)
        df.to_csv('./results/new_entries.csv', index=False)
        logging.info("Fetched new entries successfully ✅")
        return df
    except Exception as e:
        logging.error(f"Error fetching new entries: {e}")
        # We don't raise here because sometimes (day 1) there are no new entries, which is fine.
        return pd.DataFrame() 
    finally:   
        if conn: conn.close()

def channel_insights():
    """
    Sums views per channel, but ONLY for the latest snapshot to avoid double counting.
    """
    conn = None
    try:
        conn = get_connection()
        # FIX: Filter for latest date BEFORE summing
        query = """
        SELECT 
            channel_id,
            channel_title, 
            SUM(view_count) as view_count 
        FROM youtube_data 
        WHERE fetched_date = (SELECT MAX(fetched_date) FROM youtube_data)
        GROUP BY channel_id
        ORDER BY view_count DESC;
        """
        df = pd.read_sql_query(query, conn)
        df.to_csv('./results/channel_insights.csv', index=False)
        logging.info("Fetched CHANNEL INSIGHTS ✅")
        return df
    except Exception as e:
        logging.error(f"Error Fetching channel total views: {e}")
        raise
    finally:
        if conn: conn.close()
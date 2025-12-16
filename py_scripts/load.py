import pandas as pd 
import numpy as np 
import isodate
import os
import logging
import sqlite3
import libsql

os.makedirs('logs', exist_ok=True)
db_url = os.getenv("DB_URL")
db_auth = os.getenv("db_auth")

def load_youtube_data(df):
    try:
        conn = libsql.connect(database=db_url,auth_token=db_auth)
        logging.info("Connected to SQLite database successfully ✅")
    except Exception as e:  
        logging.error(f"Error connecting to SQLite database: {e}")
        raise   
    try:
        conn = libsql.connect(database=db_url,auth_token=db_auth)
        cursor = conn.cursor()
        cursor.execute("""CREATE TABLE IF NOT EXISTS youtube_data (
                       video_id TEXT,
                       title TEXT,
                       channel_id TEXT,
                       channel_title TEXT,
                       published_at TIMESTAMP,
                       fetched_time TIMESTAMP,
                       view_count INTEGER,
                       like_count INTEGER,
                       comment_count INTEGER,
                       category_id INTEGER,
                       duration INTEGER,
                       description TEXT,
                       tags TEXT,
                       thumbnail_url TEXT,
                       is_live BOOLEAN,
                       rank INTEGER,
                       fetched_date DATE,
                       PRIMARY KEY (video_id, fetched_date))""")
        logging.info("Table created successfully ✅")
    except Exception as e:
        logging.error(f"Error Creating Table in SQLite database: {e}")
        raise
    try:
        cursor = conn.cursor()
        for index, df in df.iterrows():
            cursor.execute("""INSERT OR REPLACE INTO youtube_data (
                        video_id, title, channel_id, channel_title, published_at, fetched_time, view_count, like_count, comment_count, category_id, duration, description, tags, thumbnail_url,is_live,rank,fetched_date) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (df['video_id'], df['title'], df['channel_id'], df['channel_title'], df['published_at'], df['fetched_time'], df['view_count'], df['like_count'], df['comment_count'], df['category_id'], df['duration'], df['description'], df['tags'], df['thumbnail_url'], df['is_live'], df['rank'], df['fetch_date']))
            conn.commit()
        logging.info("Data loaded into SQLite database successfully ✅")
    except Exception as e:
        logging.error(f"Error loading data into SQLite database: {e}")
        raise   
    finally:
        conn.close()
        logging.info("SQLite database connection closed ✅")

    try:
        conn = libsql.connect(database=db_url,auth_token=db_auth)
        df_loaded = pd.read_sql_query("SELECT * FROM youtube_data LIMIT 5", conn)
        logging.info("Data read from SQLite database successfully ✅")
        print(df_loaded.head(5))
    except Exception as e:
        logging.error(f"Error reading data from SQLite database: {e}")
        raise
        
        

    
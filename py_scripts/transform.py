import pandas as pd 
import numpy as np
import isodate
import os
import logging
os.makedirs('logs', exist_ok=True) 



def make_dataframe(json_file):
    try:
        df = pd.read_json(json_file)
        logging.info(f"JSON file {json_file} successfully read into DataFrame")
        print(df.head(5))
        return df
    except Exception as e:
        logging.error(f"Error reading JSON file {json_file} into DataFrame: {e}")
        raise

# def transform_youtube_data(df):
logging.info("Checking and Transforming datatypes")
def transform_youtube_data(df):
    df.published_at = pd.to_datetime(df.published_at, errors='coerce')
    df.published_at = df[['published_at']].astype(str)
    df.video_id = df.video_id.astype(str)
    # df.video_id = df.video_id.drop_duplicates(inplace=True)
    df.title = df.title.astype(str)
    df.channel_id = df.channel_id.astype(str)
    df.channel_title = df.channel_title.astype(str)
    df.description = df.description.astype(str)
    df.view_count = pd.to_numeric(df.view_count, errors='coerce').fillna(0).astype(np.int64)
    df.like_count = pd.to_numeric(df.like_count, errors='coerce').fillna(0).astype(np.int64)
    df.comment_count = pd.to_numeric(df.comment_count, errors='coerce').fillna(0).astype(np.int64)
    df.category_id = pd.to_numeric(df.category_id, errors='coerce').fillna(0).astype(np.int64)
    df.duration = pd.to_numeric(df.duration, errors='coerce').fillna(0).astype(np.int64)
    df.is_live = df.is_live.astype(bool)    
    df['tags'] = df.tags.apply(lambda x: ', '.join(x))
    df['rank'] = pd.to_numeric(df['rank'], errors='coerce')
    df['rank'] = df['rank'].fillna(0)
    df['fetch_date'] = pd.to_datetime(df.fetch_date, errors='coerce').dt.date
    df['fetch_date'] = df[['fetch_date']].astype(str)
    df['fetched_time'] = pd.to_datetime(df.fetched_time, errors='coerce')
    df['fetched_time'] = df[['fetched_time']].astype(str)
    df['region'] = df.get('region', 'Unknown')
    df['thumbnail_url'] = df['thumbnail'].astype(str)
    logging.info("Data transformation completed successfully ✅")
    return df

# df = make_dataframe('data_list_2025-12-08.json')
# print(df.columns)

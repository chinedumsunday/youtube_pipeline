import pandas as pd
import numpy as np 
import os
import logging



def validate_youtube_data(df):
    x = "video_id"
    y = "title"

    if x in df.columns and y in df.columns:
        logging.info("Validation Successful: 'video_id' and 'title' columns are present in the DataFrame ✅")
    else:
        logging.error("Validation Failed: 'video_id' and/or 'title' columns are missing from the DataFrame ❌")
        raise ValueError("Missing required columns in DataFrame")
    
    if df['view_count'].dtype == np.int64 and (df['view_count'] >= 0).all():
        logging.info("Validation Successful: 'view_count' column has correct datatype and non-negative values ✅")
    else:           
        logging.warning("Validation Warning: 'view_count' column contains null values ⚠️")
    
    if df['video_id'].is_unique: 
        logging.info(f"duplicate numbers in video_id column: {df['video_id'].duplicated().sum()}")
        logging.info(f"Printing duplicate video_ids: {df[df['video_id'].duplicated()]['video_id']}")
        logging.info("Validation Successful: 'video_id' column contains unique values ✅")
    else:
        logging.info(f"duplicate numbers in video_id column: {df['video_id'].duplicated().sum()}")
        logging.info(f"Printing duplicate video_ids: {df[df['video_id'].duplicated()]['video_id']}")
        logging.error("Validation Failed: 'video_id' column contains duplicate values ❌")
        raise ValueError("Duplicate values found in 'video_id' column")
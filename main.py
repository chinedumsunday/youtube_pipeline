import logging
import os 
os.makedirs('logs', exist_ok=True)
os.makedirs('data', exist_ok=True)

logging.basicConfig(level=logging.INFO,filename='./logs/main.log',
                    format='%(asctime)s - %(levelname)s - %(message)s')

from dotenv import load_dotenv
load_dotenv()
from py_scripts.extract import extract_youtube_data
from py_scripts.transform import make_dataframe, transform_youtube_data
from py_scripts.validate import validate_youtube_data
from py_scripts.load import load_youtube_data
from py_scripts.queries import fetch_top_videos_by_views, daily_growth_in_views, daily_rank_movers, new_entries, channel_insights
import pandas as pd



YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
region = "NG"
maxResult = 50
url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics,contentDetails&chart=mostPopular&regionCode={region}&maxResults={maxResult}&key={YOUTUBE_API_KEY}"

# Extract data
data = extract_youtube_data(url, region, maxResult)

logging.info("Data Extraction completed successfully ✅")

# Transform data
df = make_dataframe(f"./data/data_list_{pd.Timestamp.now().date()}.json")
df = transform_youtube_data(df)

logging.info("Data Transformation completed successfully ✅")



# Validate data
validate_youtube_data(df)
# df.head(5)
logging.info("Data Validation completed successfully ✅")


# load data
load_youtube_data(df)
logging.info("Data Loading completed successfully ✅")

# Run Queries
top_videos_df = fetch_top_videos_by_views(limit=10)
daily_growth_df = daily_growth_in_views()
daily_rank_movers_df = daily_rank_movers()
new_entries_df = new_entries()
channel_insights_df = channel_insights()
logging.info("ETL Pipeline executed successfully ✅")
from dotenv import load_dotenv
load_dotenv()
import os 
from extract import extract_youtube_data
from transform import make_dataframe, transform_youtube_data
from validate import validate_youtube_data
from load import load_youtube_data
import pandas as pd
import logging
os.makedirs('logs', exist_ok=True)

logging.basicConfig(level=logging.INFO,filename='./logs/main.log',
                    format='%(asctime)s - %(levelname)s - %(message)s')

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
region = "NG"
maxResult = 50
url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics,contentDetails&chart=mostPopular&regionCode={region}&maxResults={maxResult}&key={YOUTUBE_API_KEY}"

# Extract data
data = extract_youtube_data(url, region, maxResult)

# Transform data
df = make_dataframe(f"./data/data_list_{pd.Timestamp.now().date()}.json")
df = transform_youtube_data(df)



# Validate data
validate_youtube_data(df)
# df.head(5)



# load data
load_youtube_data(df)

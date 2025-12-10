import requests 
import json
import pandas 
import os 
import logging
from pprint import pprint
import sys 
import isodate
os.makedirs('logs', exist_ok=True)
os.makedirs('data', exist_ok=True)
logging.basicConfig(level=logging.INFO,filename='./logs/extract.log',
                    format='%(asctime)s - %(levelname)s - %(message)s')


def extract_youtube_data(url,region, maxResult):
    try:
        response = requests.get(url)
        data = response.json()
        logging.info(f"Data fetched sucessfully from YouTube API with response code {response.status_code}")
    except Exception as e:
        logging.error(f"Error fetching data from YouTube API: {e}")
        print("Stopping execution due to error in data extraction.  Please check the logs for more details.")
        sys.exit(1, "Error fetching data from YouTube API")
    try:
        data_list = []
        for i in data['items']:
            {"video_id": i['id'],
            "title": i['snippet']['title'],
            "channel_id": i['snippet']['channelId'],
            "channel_title": i['snippet']['channelTitle'],
            "published_at": i['snippet']['publishedAt'],
            "fetched_time": pandas.Timestamp.now(),
            "view_count": i['statistics']['viewCount'],
            "like_count": i['statistics'].get('likeCount',0),
            "comment_count": i['statistics']['commentCount'],
            "category_id": i['snippet']['categoryId'],
            "duration": isodate.parse_duration(i['contentDetails']['duration']).total_seconds(),
            "description": i['snippet']['localized']['description'],
            "tags": i['snippet'].get('tags', []),
            "thumbnail": i['snippet']['thumbnails']['high']['url'],
            "is_live": i['snippet']['liveBroadcastContent'] != 'none',
            "rank": data['items'].index(i) + 1,
            "fetch_date": pandas.Timestamp.now().date()}
            data_list.append({
                "video_id": i['id'],
                "title": i['snippet']['title'],
                "channel_id": i['snippet']['channelId'],
                "channel_title": i['snippet']['channelTitle'],
                "published_at": i['snippet']['publishedAt'],
                "fetched_time": pandas.Timestamp.now(),
                "view_count": i['statistics']['viewCount'],
                "like_count": i['statistics'].get('likeCount',0),
                "comment_count": i['statistics']['commentCount'],
                "category_id": i['snippet']['categoryId'],
                "duration": isodate.parse_duration(i['contentDetails']['duration']).total_seconds(),
                "description": i['snippet']['localized']['description'],
                "tags": i['snippet'].get('tags', []),
                "thumbnail": i['snippet']['thumbnails']['high']['url'],
                "is_live": i['snippet']['liveBroadcastContent'] != 'none',
                "rank": data['items'].index(i) + 1,
                "fetch_date": pandas.Timestamp.now().date()})
        with open(f'./data/data_list_{pandas.Timestamp.now().date()}.json', 'w') as f:
            json.dump(data_list, f, indent=4, default=str)
        logging.info("Data extraction completed successfully.")
        return data_list
    except Exception as e:
        logging.error(f"Error processing data from YouTube API: {e}")
        print("Stopping execution due to error in data processing.  Please check the logs for more details.")
        sys.exit(1, "Error processing data from YouTube API")

    
# data = requests.get(url).json()
# # with open('data.json', 'w') as f:
# #         json.dump(data, f, indent=4)

# data_list = []
# for i in data['items']:
#     {"video_id": i['id'],
#      "title": i['snippet']['title'],
#      "channel_id": i['snippet']['channelId'],
#      "channel_title": i['snippet']['channelTitle'],
#      "published_at": i['snippet']['publishedAt'],
#      "fetched_time": pandas.Timestamp.now(),
#      "view_count": i['statistics']['viewCount'],
#      "like_count": i['statistics'].get('likeCount',0),
#      "comment_count": i['statistics']['commentCount'],
#      "category_id": i['snippet']['categoryId'],
#      "duration": isodate.parse_duration(i['contentDetails']['duration']).total_seconds(),
#      "description": i['snippet']['localized']['description'],
#      "tags": i['snippet'].get('tags', []),
#      "thumbnail": i['snippet']['thumbnails']['high']['url'],
#      "is_live": i['snippet']['liveBroadcastContent'] != 'none',
#      "rank": data['items'].index(i) + 1,
#      "fetch_date": pandas.Timestamp.now().date()}
#     data_list.append({
#         "video_id": i['id'],
#         "title": i['snippet']['title'],
#         "channel_id": i['snippet']['channelId'],
#         "channel_title": i['snippet']['channelTitle'],
#         "published_at": i['snippet']['publishedAt'],
#         "fetched_time": pandas.Timestamp.now(),
#         "view_count": i['statistics']['viewCount'],
#         "like_count": i['statistics'].get('likeCount',0),
#         "comment_count": i['statistics']['commentCount'],
#         "category_id": i['snippet']['categoryId'],
#         "duration": isodate.parse_duration(i['contentDetails']['duration']).total_seconds(),
#         "description": i['snippet']['localized']['description'],
#         "tags": i['snippet'].get('tags', []),
#         "thumbnail": i['snippet']['thumbnails']['high']['url'],
#         "is_live": i['snippet']['liveBroadcastContent'] != 'none',
#         "rank": data['items'].index(i) + 1,
#         "fetch_date": pandas.Timestamp.now().date()})






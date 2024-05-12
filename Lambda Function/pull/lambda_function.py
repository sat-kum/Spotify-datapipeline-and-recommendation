import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):
    print("Date", datetime.now())
    # Setting enviroment variables
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
    print(client_id)
    
    # Retrieving information fromn the Spotify API
    client_credential_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credential_manager)
    playlist_link = 'https://open.spotify.com/playlist/37i9dQZEVXbMDoHDwVN2tF'
    playlist_id = playlist_link.split('/')[-1]
    data = sp.playlist_tracks(playlist_id)
    
    # Loading raw data to Amazon S3 bucket
    # filename = 'spotify_raw_data' + str(datetime.now()) + '.json'
    # client = boto3.client('s3')
    # client.put_object(
    #     Bucket='spotify-etl-datapipeline',
    #     Key = 'raw_data/to_process/' + filename,
    #     Body = json.dumps(data)
    #     )
        
    bucket_name = "spotify-etl-datapipeline"
    file_name = 'spotify_raw_data' + str(datetime.now()) + '.json'
    s3_path = "raw_data/" + file_name
    
    s3 = boto3.client('s3') 
    s3.put_object(Body=data, Bucket=bucket_name, Key=s3_path) 
    return { 
        'statusCode': 200, 
        'body': 'File uploaded successfully.' 
    }


#     s3 = boto3.resource("s3")
#     s3.Bucket(bucket_name).put_object(Key=s3_path, Body=data)

# def lambda_handler(event, context): 
#   bucket_name = 'spotify-etl-datapipeline'
#   file_name = 'file_name.txt' 
#   file_content = 'This is the content of the file.' 


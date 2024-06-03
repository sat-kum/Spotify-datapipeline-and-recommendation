import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import date, datetime
import requests, base64
import pandas as pd

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
    data = sp.playlist_tracks
    
    
    id_list = ['37i9dQZF1DX76Wlfdnj7AP', '37i9dQZEVXbMDoHDwVN2tF', '3cEYpjA9oz9GiPac4AsH4n']
    
    # Dictionary to store playlist IDs and their corresponding data
    playlist_data = {}
    
    for i in id_list:
        playlist_link = 'https://open.spotify.com/playlist/{}'.format(i)
        playlist_id = playlist_link.split('/')[-1]
        print(playlist_id)
    
        data = sp.playlist_tracks(playlist_id)
        print(data)
        
        # Append data to the dictionary with playlist ID as the key
        playlist_data[playlist_id] = data
    
    print(type(playlist_data))
    
    s3 = boto3.resource('s3')
    obj = s3.Object('spotify-etl-datapipeline', 'raw_data/spotify_raw_data_' + str(date.today()) + '.json')
    obj.put(Body=(bytes(json.dumps(playlist_data).encode('UTF-8'))))
    
    
    client_credentials = f"{client_id}:{client_secret}"
    client_credentials_base64 = base64.b64encode(client_credentials.encode())

    # Request the access token
    token_url = 'https://accounts.spotify.com/api/token'
    headers = {
        'Authorization': f'Basic {client_credentials_base64.decode()}'
    }
    data = {
        'grant_type': 'client_credentials'
    }
    response = requests.post(token_url, data=data, headers=headers)

    if response.status_code == 200:
        access_token = response.json()['access_token']
        print(access_token)
        print("Access token obtained successfully.")
    else:
        print("Error obtaining access token.")

    def get_trending_playlist_data(playlist_id, access_token):
        # Set up Spotipy with the access token
        sp = spotipy.Spotify(auth=access_token)

        # Get the tracks from the playlist
        playlist_tracks = sp.playlist_tracks(playlist_id, fields='items(track(id, name, artists, album(id, name)))')

        # Extract relevant information and store in a list of dictionaries
        music_data = []
        for track_info in playlist_tracks['items']:
            track = track_info['track']
            track_name = track['name']
            artists = ', '.join([artist['name'] for artist in track['artists']])
            album_name = track['album']['name']
            album_id = track['album']['id']
            track_id = track['id']

            # Get audio features for the track
            audio_features = sp.audio_features(track_id)[0] if track_id != 'Not available' else None

            # Get release date of the album
            try:
                album_info = sp.album(album_id) if album_id != 'Not available' else None
                release_date = album_info['release_date'] if album_info else None
            except:
                release_date = None

            # Get popularity of the track
            try:
                track_info = sp.track(track_id) if track_id != 'Not available' else None
                popularity = track_info['popularity'] if track_info else None
            except:
                popularity = None

            # Add additional track information to the track data
            track_data = {
                'Track Name': track_name,
                'Artists': artists,
                'Album Name': album_name,
                'Album ID': album_id,
                'Track ID': track_id,
                'Popularity': popularity,
                'Release Date': release_date,
                'Duration (ms)': audio_features['duration_ms'] if audio_features else None,
                'Explicit': track_info.get('explicit', None),
                'External URLs': track_info.get('external_urls', {}).get('spotify', None),
                'Danceability': audio_features['danceability'] if audio_features else None,
                'Energy': audio_features['energy'] if audio_features else None,
                'Key': audio_features['key'] if audio_features else None,
                'Loudness': audio_features['loudness'] if audio_features else None,
                'Mode': audio_features['mode'] if audio_features else None,
                'Speechiness': audio_features['speechiness'] if audio_features else None,
                'Acousticness': audio_features['acousticness'] if audio_features else None,
                'Instrumentalness': audio_features['instrumentalness'] if audio_features else None,
                'Liveness': audio_features['liveness'] if audio_features else None,
                'Valence': audio_features['valence'] if audio_features else None,
                'Tempo': audio_features['tempo'] if audio_features else None,
                # Add more attributes as needed
            }

            music_data.append(track_data)

        # Create a pandas DataFrame from the list of dictionaries
        df = pd.DataFrame(music_data)

        return df
    
    all_music_dfs = []

    
    for i in id_list:
        # Call the function to get the music data from the playlist and store it in a DataFrame
        music_df = get_trending_playlist_data(i, access_token)
        
        # Add a column to identify the playlist ID
        music_df['playlist_id'] = i
        print(len(music_df))
        
        # Append the DataFrame to the list
        all_music_dfs.append(music_df)

    all_music_df = pd.concat(all_music_dfs, axis=0)
    obj = s3.Object('spotify-etl-datapipeline', 'recommendraw_data/spotify_raw_data_' + str(date.today()) + '.json')
    obj.put(Body=(bytes(json.dumps(playlist_data).encode('UTF-8'))))

    return {
    'statusCode': 200,
    'body': json.dumps('Raw data pushed!!')
    }

#! usr/bin/env python3
# Let's first get the access token.
# Suppose you already created a spotify developer account, and you get started with any application of your choice !
# Then you'll need an access token.
# This token is for future requests.
# Bear in mind that the generated token has an expiry delay of 3600 seconds.

import requests
import datetime
import pandas as pd

# ===================== Generate token step !=========================
# generate your token from here : https://developer.spotify.com/console/get-recently-played/
# ==================== Get Recently Played Tracks step !=============


def extract_from_spotify():
    TOKEN = "Your Token"
    base_url = "https://api.spotify.com/v1/me/player/recently-played"

    headers = {
        "Accept": "application/json",
        "Authorization": "Bearer {}".format(TOKEN),
        "Content-Type": "application/json"
    }

    today = datetime.datetime.now()
    lastWeek = today - datetime.timedelta(days=8)
    lastWeek_unix_milliseconds = int(lastWeek.timestamp())*1000
    limit = 50

    spotify_response = requests.get(
        "https://api.spotify.com/v1/me/player/recently-played?after={}&limit={}".format(lastWeek_unix_milliseconds, limit), headers=headers)

    data = spotify_response.json()

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(
            song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    songs_dict = {
        "song_name": song_names,
        "artist_name": artist_names,
        "played_at": played_at_list,
        "timestamp": timestamps
    }

    df_songs = pd.DataFrame(songs_dict, columns=[
        "song_name", "artist_name", "played_at", "timestamp"])
    return df_songs

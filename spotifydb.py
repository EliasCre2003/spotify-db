import json
from sqlite3 import connect, Connection
from pandas import DataFrame



def process_url(url: str) -> str | None:
    return (
        None if url is None
        else f"https://open.spotify.com/track/{url.split(':')[-1]}"
    )

def add_song_stream(connection: Connection, stream_struct: dict[str, str], songs: set[str] = set()) -> bool:
    url = process_url(stream_struct['spotify_track_uri'])
    if url is None:
        return False
    cursor = connection.cursor()
    if url not in songs:
        data = parse_song(stream_struct)
        cursor.execute(
            """--sql
            INSERT INTO songs ("url", song_name, artist_name, album_name) 
            VALUES (?, ?, ?, ?);
            """, data
        )
        songs.add(url)
    time_stamp = stream_struct['ts'][:-1].replace('T', ' ')
    time_played = stream_struct['ms_played']
    cursor.execute(
        """--sql 
        INSERT INTO song_streams (time_stamp, ms_listened, "url")
        VALUES (?, ?, ?);
        """, (time_stamp, time_played, url)
    )
    return True

def add_episode_stream(connection: Connection, stream_struct: dict[str, str], episodes: set[str] = set()):
    url = process_url(stream_struct['spotify_episode_uri'])
    if url is None:
        return False
    cursor = connection.cursor()
    if url not in episodes:
        data = parse_episode(stream_struct)
        cursor.execute(
            """--sql
            INSERT INTO podcast_episodes ("url", episode, podcast) 
            VALUES (?, ?, ?);
            """, data
        )
        episodes.add(url)
    time_stamp = stream_struct['ts'][:-1].replace('T', ' ')
    time_played = stream_struct['ms_played']
    cursor.execute(
        """--sql 
        INSERT INTO podcast_streams (time_stamp, ms_listened, "url")
        VALUES (?, ?, ?);
        """, (time_stamp, time_played, url)
    )
    return True
    

def parse_song(json_struct: dict[str, str]):
    url = process_url(json_struct['spotify_track_uri'])
    name = json_struct['master_metadata_track_name'].removesuffix(' - Remastered')
    artist = json_struct['master_metadata_album_artist_name']
    album = json_struct['master_metadata_album_album_name'].replace('...', '…')
    return url, name, artist, album

def parse_episode(json_struct: dict[str, str]):
    url = process_url(json_struct['spotify_episode_uri'])
    episode = json_struct['episode_name']
    podcast = json_struct['episode_show_name']
    return url, episode, podcast

def run_single_query(query: str, arguments = ()) -> DataFrame:
    connection = connect("spotify.db")
    cursor = connection.cursor()
    output = (cursor
              .execute(query, arguments)
              .fetchall())
    columns = [description[0] for description in cursor.description]
    data_frame = DataFrame(output, columns=columns)
    connection.commit()
    connection.close()
    return data_frame
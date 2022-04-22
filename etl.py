import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    
    """
    Loads the song dataset into a Pandas DataFrame
    The 'song_id','title','artist_id','year','duration' are extracted and inserted into the songs table 
    The 'artist_id','artist_name','artist_location','artist_latitude','artist_longitude' are extracted and inserted into the artists table 

    """
    
    # open song file
    df = pd.read_json(filepath, typ = 'DataFrame')

    # insert song record
    song_data = list(df[['song_id','title','artist_id','year','duration']].values)
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values)
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    
     """
    Loads the log dataset into a Pandas DataFrame
    The timestamp is transformed and inserted into the time table
    The 'userId', 'firstName', 'lastName', 'gender', 'level' are extracted and inserted into the users table
     Because the log file(s) do not specify an ID for either the song or the artist, got the song ID and artist ID by querying the songs and artists tables to find matches based on song title, artist name, and song duration time.

    The timestamp, user ID, level, song ID, artist ID, session ID, location, and user agent are extracted and inserted into the songplay_data
    """
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']== 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    
    time_data = [t,t.dt.hour,t.dt.day,t.dt.week,t.dt.month,t.dt.year,t.dt.weekday]
    column_labels = ['timestamp', 'hour', 'day', 'week of year', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(dict(zip(column_labels,time_data)))
    
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = pd.DataFrame(df[['userId', 'firstName', 'lastName', 'gender', 'level']].values, columns = ['userId', 'firstName', 'lastName', 'gender', 'level']).drop_duplicates('userId')

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms'),row.userId,row.level,songid,artistid,row.sessionId,row.location,row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    
    """
    Imports all relevant files from the same dir.
    Counts total number of files present and processed in a dir.
    """
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    
    """
    Establishes connection to the database sparkifydb and gets cursor in it.
    Closes the connection when processing is done.
    """
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
import glob
import os

import pandas as pd
import psycopg2

from sql_queries import *


def process_song_file(cur, filepath):
    # open song file
    try:
        df = pd.read_json(filepath, lines=True)
        print(df)
        # insert song record
        song_data = (df.song_id.item(), df.title.item(), df.artist_id.item(), df.year.item(), df.duration.item())
        cur.execute(song_table_insert, song_data)

        # insert artist record
        artist_data = (df.artist_id.item(), df.artist_name.item(), df.artist_location.item(), df.artist_latitude.item(),
                       df.artist_longitude.item())
        cur.execute(artist_table_insert, artist_data)
    except Exception as e:
        print(e)


def process_log_file(cur, filepath):
    # open log file
    """
        This procedure processes a song file whose filepath has been provided as an arugment.
        It extracts the song information in order to store it into the songs table.
        Then it extracts the artist information in order to store it into the artists table.

        INPUTS:
        * cur the cursor variable
        * filepath the file path to the song file
    """
    try:
        df = pd.read_json(filepath, lines=True)
        # filter by NextSong action

        df = df[df['page'] == "NextSong"]

        #     # convert timestamp column to datetime
        t = pd.to_datetime(df.ts, unit='ms')

        #     # insert time data records
        time_data = [t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday]
        time_df = pd.concat(time_data, axis=1)

        for i, row in time_df.iterrows():
            cur.execute(time_table_insert, list(row))

    #     # load user table
        user_df = pd.concat([df.userId, df.firstName, df.lastName, df.gender, df.level], axis=1)

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
            try:
                songplay_data = [row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
                cur.execute(songplay_table_insert, songplay_data)
            except Exception as e:
                print(e)
    except Exception as e:
        print(e)
        pass


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=password")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    cur.execute("select * from songplays WHERE song_id is not null and artist_id is not null")
    results = cur.fetchall()
    print("Result of `select * from songplays WHERE song_id is not null and artist_id is not null`:")
    print(results)
    conn.close()


if __name__ == "__main__":
    main()

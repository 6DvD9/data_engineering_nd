import os
import glob
import psycopg2
import pandas as pd
import sql_queries 


def process_song_file(cur, filepath):
    # open song file
    df_song = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df_song[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0]
    song_data = list(song_data)
    
    cur.execute(sql_queries.song_table_insert, song_data)
    
    # insert artist record
    artist_data = df_song[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0]
    artist_data = list(artist_data)
    
    cur.execute(sql_queries.artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    df_log = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df_log = df_log[df_log.page == 'NextSong'] 

    # convert timestamp column to datetime
    df_log['ts'] = pd.to_datetime(df_log['ts'], unit='ms')
    
    # insert time data records
    time_data = [df_log.ts, df_log.ts.dt.hour, df_log.ts.dt.day, df_log.ts.dt.week, df_log.ts.dt.month, df_log.ts.dt.year, df_log.ts.dt.weekday]
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(sql_queries.time_table_insert, list(row))

    # load user table
    user_df = df_log[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(sql_queries.user_table_insert, row)

    # insert songplay records
    for index, row in df_log.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(sql_queries.song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (index, row['ts'], row['userId'], row['level'], songid, artistid, row['sessionId'], row['location'], row['userAgent'])
        cur.execute(sql_queries.songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
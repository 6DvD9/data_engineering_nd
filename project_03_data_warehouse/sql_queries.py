import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"


# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_events(
        event_id INTEGER IDENTITY(0,1) PRIMARY KEY,
        artist_name VARCHAR,
        auth VARCHAR,
        user_first_name VARCHAR,
        user_gender VARCHAR,
        item_in_session INTEGER,
        user_last_name VARCHAR,
        song_length FLOAT,
        user_level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration VARCHAR,
        session_id INTEGER,
        song_title VARCHAR,
        status INTEGER,
        ts TIMESTAMP,
        user_agent VARCHAR,
        user_id INTEGER  
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs(
        song_id VARCHAR PRIMARY KEY,
        num_songs INTEGER,
        artist_id VARCHAR,
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location VARCHAR,
        artist_name VARCHAR,
        title VARCHAR,
        duration FLOAT,
        year INTEGER
    )
""")

songplay_table_create = ("""
    CREATE TABLE songplays(
        songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP REFERENCES time(start_time),
        user_id INTEGER REFERENCES users(user_id),
        level VARCHAR,
        song_id VARCHAR REFERENCES songs(song_id),
        artist_id VARCHAR REFERENCES artists(artist_id),
        session_id INTEGER,
        location VARCHAR,
        user_agent VARCHAR
    )
""")

user_table_create = ("""
    CREATE TABLE users(
        user_id INTEGER PRIMARY KEY,
        first_name VARCHAR NOT NULL,
        last_name VARCHAR NOT NULL,
        gender VARCHAR NOT NULL,
        level VARCHAR NOT NULL
    )
""")

song_table_create = ("""
    CREATE TABLE songs(
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        year INTEGER NOT NULL,
        duration FLOAT        
    )
""")

artist_table_create = ("""
    CREATE TABLE artists(
        artist_id VARCHAR PRIMARY KEY,
        name VARCHAR NOT NULL,
        location VARCHAR,
        latitude FLOAT,
        longitude FLOAT
    )
""")

time_table_create = ("""
    CREATE TABLE time(
        start_time TIMESTAMP PRIMARY KEY,
        hour INTEGER NOT NULL,
        day INTEGER NOT NULL,
        week INTEGER NOT NULL,
        month INTEGER NOT NULL,
        year INTEGER NOT NULL,
        weekday INTEGER NOT NULL
    )
""")


# STAGING TABLES
# Load from JSON using a JSONPaths file (LOG_JSONPATH)

staging_events_copy = (f"""
    copy staging_events from {config.get('S3','LOG_DATA')}
    credentials 'aws_iam_role={config.get('IAM_ROLE','ARN')}'
    region 'us-west-2' format as JSON {config.get('S3','LOG_JSONPATH')}
    timeformat as 'epochmillisecs';
""")

staging_songs_copy = (f"""
    copy staging_songs from {config.get('S3','SONG_DATA')}
    credentials 'aws_iam_role={config.get('IAM_ROLE','ARN')}'
    region 'us-west-2' format as JSON 'auto';
""")


# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT(staging_events.ts) AS start_time,
        staging_events.user_id,
        staging_events.user_level AS level,
        staging_songs.song_id,
        staging_songs.artist_id,
        staging_events.session_id,
        staging_events.location,
        staging_events.user_agent
    FROM staging_events
    JOIN staging_songs
    ON staging_events.song_title = staging_songs.title
    AND staging_events.artist_name = staging_songs.artist_name
    AND staging_events.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT(user_id) AS user_id,
        user_first_name AS first_name,
        user_last_name AS last_name,
        user_gender AS gender,
        user_level AS level
    FROM staging_events
    WHERE user_id IS NOT NULL
    AND page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT(song_id) AS song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;    
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT(artist_id) AS artist_id,
        artist_name AS name,
        artist_location AS location,
        artist_latitude AS latitude,
        artist_longitude AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT(start_time) AS start_time,
        EXTRACT(hour from start_time) AS hour,
        EXTRACT(day from start_time) AS day,
        EXTRACT(week from start_time) AS week,
        EXTRACT(month from start_time) AS month,
        EXTRACT(year from start_time) AS year,
        EXTRACT(weekday from start_time) AS weekday
    FROM songplays;
""")


# GET ROW COUNT FROM EACH TABLE

get_count_staging_events = """
    SELECT COUNT(*) 
    FROM staging_events;
"""

get_count_staging_songs = """
    SELECT COUNT(*)
    FROM staging_songs;
"""

get_count_songplays = """
    SELECT COUNT(*)
    FROM songplays;
"""

get_count_users = """
    SELECT COUNT(*)
    FROM users;
"""

get_count_songs = """
    SELECT COUNT(*)
    FROM songs;
"""

get_count_artists = """
    SELECT COUNT(*)
    FROM artists;
"""

get_count_time = """
    SELECT COUNT(*)
    FROM time;
"""


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, 
                        song_table_create, artist_table_create, time_table_create, songplay_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, 
                      user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, artist_table_insert, time_table_insert, song_table_insert]

row_count_queries = [get_count_staging_events, get_count_staging_songs, get_count_songplays,
                     get_count_users, get_count_songs, get_count_artists, get_count_time]

Introduction:

Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. 

At the moment they are unable to easily query their data on user activity on the app, and on the metadata on the songs, as they are all held on a directory of JSON logs. 


The goal:

The would like a Postgres database optimized for queries on song play analysis.

The would want the following:

1 - Database schema and ETL pipeline to be created for analysis.
2 - Be able to test the database and ETL pipeline by running queries given, and be able to compare results with their expected results.


Database & ETL pipeline:

Using the song and log datasets, I created a star schema which includes the following:

One fact table containing the following columns:

songplays - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Four dimension tables containg the following columns:

users - user_id, first_name, last_name, gender, level

songs - song_id, title, artist_id, year, duration

artists - artist_id, name, location, latitude, longitude

time - start_time, hour, day, week, month, year, weekday






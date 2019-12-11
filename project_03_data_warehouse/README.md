## Introduction

Sparkify has grown their user base and song database, and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## The goal

They would like an ETL pipline built: 

1. That extracts their data from S3. 
2. Stages the extracted data in Redshift. 
3. Transforms data into a set of dimensional tables.

### Database Schema Design

For staging tables, 2 tables were used for the copy from S3:

1. staging_events
2. staging_songs

The following are the Dimension tables:

1. users(users in the app), with the following columns: user_id, first_name, last_name, gender, level 

2. songs(songs in music database), with the following columns: song_id, title, artist_id, year, duration

3. artists(artists in music database), with the following columns: artist_id, name, location, latitude, longitude

4. time(timestamps of records in songplays broken down into specific units), with the following columns: start_time, hour, day, week, month, year, weekday

Fact table:

1. songplays(records in event data associated with song plays i.e. records with page NextSong), with the following columns: songplay_id, start_time which references start_time of the time table, user_id which references user_id of the users table, level, song_id which references song_id of the songs table, artist_id which references artist_id of the artists table, session_id, location, user_agent


### Running the project

In order to run the project the following to be done in order:

1. Fill in all the required details in the 'dwh.cfg' file.

2. Run the 'create_delete_cluster.py' - Note: There is a toggle switch within the code to switch whether to create or delete the cluster.

3. Run the 'create_tables.py' - This will drop and create all the tables required (all being read from the sql_queries.py).

4. Run the 'etl.py' - This will copy data from S3 onto staging tables, and data inserted into the final tables (all being read from the sql_queries.py).

5. Run the 'analytics.py' - This is to verify that data had been inserted correctly without issues.

6. Run the 'create_delete_cluster.py' to delete the cluster by by toggling switch with the code.

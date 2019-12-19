### Introduction

Sparkify has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.


### The goal

They would like an ETL pipline built:

1) That extracts their data from S3.
2) Processes them using Spark.
3) Loads the data back into S3 as a set of dimensional tables.
4) All data need to be in parquet format.


#### Dimensions and fact table

The following are dimension tables:
1) songs_table
2) artists_table
3) users_table
4) time_table

Whilst the fact table being:
5) songplays_table


#### Running the project

1) Fill in the required details in the 'dl.cfg' file.
2) Run the 'etl.py' file.
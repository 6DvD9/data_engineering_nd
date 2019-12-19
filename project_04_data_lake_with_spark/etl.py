import configparser
from datetime import datetime
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, DateType, TimestampType


config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS', 'AWS_SECRET_ACCESS_KEY')


class MySparkDataLake:
    """Class to create a spark session and perform ETL"""

    def create_spark_session(self):
        """Function to retrieve or create a spark session"""
        
        spark = SparkSession \
            .builder \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
            .getOrCreate()
        
        return spark


    def process_song_data(self, spark, input_data, output_data):
        """
        Function that loads song and artist data (JSON format) from S3, 
        processes them by extracting songs and artist tables, and then loading back into S3
        
        Parameters:
            spark       : Spark Session
            input_data  : Location of song_data in JSON format
            output_data : Location of S3 bucket where tables are to be stored in parquet format
        """
        
        # JSON structure to spark
        song_schema = StructType([
            StructField("artist_id", StringType()),
            StructField("artist_latitude", DoubleType()),
            StructField("artist_location", StringType()),
            StructField("artist_longitude", DoubleType()),
            StructField("artist_name", StringType()),
            StructField("duration", DoubleType()),
            StructField("num_songs", IntegerType()),
            StructField("title", StringType()),
            StructField("year", IntegerType()),
        ])
        
        # Get filepath to song data file
        song_data = input_data + 'song_data/*/*/*/*.json'

        # Read song data file
        df = spark.read.json(song_data, schema=song_schema)

        # Extract columns to create songs table
        songs_table = df.select("title", "artist_id", "year", "duration").dropDuplicates() \
                .withColumn("song_id", monotonically_increasing_id())

        # Write songs table to parquet files partitioned by year and artist
        songs_table.write.partitionBy("year", "artist_id").parquet(output_data + 'songs/')

        # Extract columns to create artists table
        artist_fields = ["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]
        artists_table = df.select(artist_fields).dropDuplicates()

        # Write artists table to parquet files
        artists_table.write.parquet(output_data + 'artists/')
        
        print('songs_table\n', songs_table)
        print('\n')
        print('artists_table\n', artists_table)
        
        
    def process_log_data(self, spark, input_data, output_data):
        """
        Function that loads log data (JSON format) from S3, 
        processes them by extracting songs and artist tables, and then loading back into S3.
        Output from process_song_data function is used as an input for this function.
        
        Parameters:
            spark       : Spark Session
            input_data  : Location of log_data in JSON format
            output_data : Location of S3 bucket where tables are to be stored in parquet format
        
        """
        
        # Get filepath to log data file
        log_data = input_data + 'log-data/*.json'

        # Read log data file
        df = spark.read.json(log_data)

        # Filter by actions for song plays
        df = df.filter(df.page == 'NextSong')

        # Extract columns for users table
        user_fields = ["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"] 
        users_table = df.selectExpr(user_fields).dropDuplicates()

        # Write users table to parquet files
        users_table.write.parquet(output_data + 'users/')

        # Get datetime 
        get_datetime = udf(date_convert, TimestampType())
        df = df.withColumn("start_time", get_datetime('ts'))

        # Extract columns to create time table
        time_table = df.select("start_time").dropDuplicates() \
                .withColumn("hour", hour(col("start_time"))) \
                .withColumn("day", day(col("start_time"))) \
                .withColumn("week", week(col("start_time"))) \
                .withColumn("month", month(col("start_time"))) \
                .withColumn("year", year(col("start_time"))) \
                .withColumn("weekday", date_format(col("start_time"), 'E'))

        # Write time table to parquet files partitioned by year and month
        time_table.write.partitionBy("year", "month").parquet(output_data + 'time/')

        # Read in song data to use for songplays table
        df_songs = spark.read.parquet(output_data + 'songs/*/*/*')

        df_artists = spark.read.parquet(output_data + 'artists/*')

        songs_logs = df.join(songs_df, (df.song == songs_df.title))
                            
        artists_songs_logs = songs_logs.join(df_artists, (songs_logs.artist == df_artists.artist_name))

        songplays = artists_songs_logs.join(
            time_table,
            artists_songs_logs.ts == time_table.start_time, 'left'
        ).drop(artists_songs_logs.year)

        # Extract columns from joined song and log datasets to create songplays table 
        songplays_table = songplays.select(
            col('start_time').alias('start_time'),
            col('userId').alias('user_id'),
            col('level').alias('level'),
            col('song_id').alias('song_id'),
            col('artist_id').alias('artist_id'),
            col('sessionId').alias('session_id'),
            col('location').alias('location'),
            col('userAgent').alias('user_agent'),
            col('year').alias('year'),
            col('month').alias('month'),
        ).repartition("year", "month")
                                                                
        # Write songplays table to parquet files partitioned by year and month
        songplays_table.write.partitionBy("year", "month").parquet(output_data + 'songplays/')
        
        print('users_table\n', users_table)
        print('\n')
        print('time_table\n', time_table)
        print('\n')
        print('songplays_table\n', songplays_table)
        
              
    def main(self):
        """Function that uses all functions and runs as main function"""
        
        spark = self.create_spark_session()
        input_data = "s3a://udacity-dend/"
        output_data = "s3a://udacity-dend/"

        self.process_song_data(spark, input_data, output_data)    
        self.process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    my_spark_data_lake = MySparkDataLake()
    my_spark_data_lake.main()

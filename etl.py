from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
import configparser
import os


config = configparser.ConfigParser()
config.read("dl.cfg")

os.environ["AWS_ACCESS_KEY_ID"] = config["CREDENTIALS"]["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["CREDENTIALS"]["AWS_SECRET_ACCESS_KEY"]


def create_spark_session():

    """ Creating a function to set spark session"""

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):

    """
    Defining function to process songs JSON files located in S3 (input_data).
    Here we are going to create songs and artists tables in parquet format and write them into another S3 bucket (output_data)
    """

    # get filepath to song data file
    global song_data 
    song_data = f"{input_data}/song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(
        col("song_id"), 
        col("title"), 
        col("artist_id"), 
        col("year"), 
        col("duration")
    ).distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table \
        .write \
        .partitionBy("year", "artist_id") \
        .mode("overwrite") \
        .parquet(f"{output_data}/songs/songs.parquet")

    # extract columns to create artists table
    artists_table = df.select(
        col("artist_id"),
        col("artist_name").alias("name"), 
        col("artist_location").alias("location"),
        col("artist_latitude").alias("latitude"),
        col("artist_longitude").alias("longitude")
    ).distinct()
    
    # write artists table to parquet files
    artists_table \
        .write \
        .mode("overwrite") \
        .parquet(f"{output_data}/artists/artists.parquet")


def process_log_data(spark, input_data, output_data):

    """
    Process log JSON files and creating users, time dimensions and songplays tables in S3.
    users and songplays tables are created filtering only 'NextSong' events from sparkify logs.
    """

    # get filepath to log data file
    log_data = f"{input_data}/log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    log_df = log_df.where('page="NextSong"')

    # extract columns for users table    
    users_table = log_df.select(
        col("userId").alias("user_id"), 
        col("firstName").alias("first_name"), 
        col("lastName").alias("last_name"), 
        col("gender"), 
        col("level")
    ).distinct()
    
    # write users table to parquet files
    users_table \
        .write \
        .partitionBy("user_id") \
        .mode("overwrite") \
        .parquet(f"{output_data}/users/users.parquet")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn("timestamp", get_timestamp(col("ts")))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    df = df.withColumn("datetime", get_datetime(col("ts")))
    
    # extract columns to create time table
    time_table = df.select(col("datetime") \
        .withColumn("start_time", df.datetime) \
        .withColumn("hour", hour("datetime")) \
        .withColumn("day", dayofmonth("datetime")) \
        .withColumn("week", weekofyear("datetime")) \
        .withColumn("month", month("datetime")) \
        .withColumn("year", year("datetime")) \
        .withColumn("weekday", dayofweek("datetime"))
    ).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table \
        .write \
        .partitionBy("year", "month") \
        .mode("overwrite") \
        .parquet(f"{output_data}/time/time.parquet")

    # read in song data to use for songplays table
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    song_and_logs = log_df.join(song_df, col("log_df.artist") == col("song_df.artist_name"), "inner")

    songplays = song_and_logs.select(
        col("log_df.datetime").alias("start_time"),
        col("log_df.userId").alias("user_id"),
        col("log_df.level").alias("level"),
        col("song_df.song_id").alias("song_id"),
        col("song_df.artist_id").alias("artist_id"),
        col("log_df.sessionId").alias("session_id"),
        col("log_df.location").alias("location"), 
        col("log_df.userAgent").alias("user_agent"),
        year("log_df.datetime").alias("year"),
        month("log_df.datetime").alias("month")
    ).withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays \
        .write \
        .partitionBy("year", "month") \
        .mode("overwrite") \
        .parquet(f"{output_data}/songplays/songplays.parquet")


def main():
    """"
    Main function to run ETL process
    """
    spark = create_spark_session()

    input_data = "s3a://udacity-dend"
    output_data = "s3a://sparkify"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

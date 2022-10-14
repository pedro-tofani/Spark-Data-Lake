import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get("AWS", "AWS_ACCESS_KEY_ID")
os.environ['AWS_SECRET_ACCESS_KEY']=config.get("AWS", "AWS_SECRET_ACCESS_KEY")


def create_spark_session():
    """
    Function to create or get a spark session. Returns the instance created. 
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Function to process song data. The data is loaded from S3 in JSON files and after some transformations the data is than converted to parquet files.
    Two dimentional tables are created: songs_table and artists_table.
    inputs:
      spark: spark session,
      input_data: s3 path to the data input,
      output_data: s3 path to the data output
    """
    # get filepath to song data file
    song_data = song_data = input_data + 'song_data/A/A/A/*.json'
    
    # Defininig the schema
    songSchema = R([
        Fld("num_songs", Int()),
        Fld("artist_id", Str()),
        Fld("artist_latitude", Dbl()),
        Fld("artist_longitude", Dbl()),
        Fld("artist_location", Str()),
        Fld("artist_name", Str()),
        Fld("song_id", Str()),
        Fld("title", Str()),
        Fld("duration", Dbl()),
        Fld("year", Int()),
    ])

    # read song data file
    df = spark.read.json(song_data, schema=songSchema)

    # extract columns to create songs table
    songs_table = df.select("song_id","title","artist_id","year","duration").drop_duplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + "songs_table/", partitionBy=["year","artist_id"])

    # extract columns to create artists table
    artists_table = df.select("artist_id","artist_name","artist_location","artist_latitude","artist_longitude").drop_duplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists_table/')


def process_log_data(spark, input_data, output_data):
    """
    Function to process log data. The data is loaded from S3 in JSON files and after some transformations the data is than converted to parquet files.
    Two dimentional tables are created: users_table and time_table.
    One fact table is created: songplays_table.
    inputs:
      spark: spark session,
      input_data: s3 path to the data input,
      output_data: s3 path to the data output
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/2018/11/*.json'

    logSchema = R([
      Fld("artist", Str()),
      Fld("auth", Str()),
      Fld("firstName", Str()),
      Fld("gender", Str()),
      Fld("itemInSession", Int()),
      Fld("lastName", Str()),
      Fld("length", Dbl()),
      Fld("level", Str()),
      Fld("location", Str()),
      Fld("method", Str()),
      Fld("page", Str()),
      Fld("registration", Dbl()),
      Fld("sessionId", Int()),
      Fld("song", Str()),
      Fld("status", Int()),
      Fld("ts", Int()),
      Fld("userAgent", Str()),
      Fld("userId", Str()),
    ])

    # read log data file
    df = spark.read.json(log_data, schema=logSchema)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.select("userId", "firstName", "lastName", "gender", "level").drop_duplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users_table/")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda date_in_ms : datetime.utcfromtimestamp(int(date_in_ms)/1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp("ts"))
        
    # extract columns to create time table
    time_table = df\
                    .withColumn("weekday",dayofweek("start_time"))\
                    .withColumn("hour",hour("start_time"))\
                    .withColumn("day",dayofmonth("start_time"))\
                    .withColumn("week",weekofyear("start_time"))\
                    .withColumn("month",month("start_time"))\
                    .withColumn("year",year("start_time"))\
                    .select("start_time","hour", "day", "week", "month", "year", "weekday", "ts").drop_duplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + "time_table/", partitionBy=["year","month"])

    # read in song data to use for songplays table
    df_songs = spark.read.parquet(output_data + 'songs_table')

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(df_songs, df.song == df_songs.title, how='left').select(\
                      monotonically_increasing_id().alias("songplay_id"),
                      col('start_time').alias('st'),"userId","level","song_id","artist_id","sessionId", "location", "userAgent")
    songplays_table = songplays_table.join(time_table, songplays_table.st == time_table.start_time, how='left').select(\
                      "songplay_id",\
                      col('userId').alias('user_id'),\
                      "level",\
                      "song_id",\
                      "artist_id",\
                      col('sessionId').alias('session_id'),\
                      "location",\
                      col('userAgent').alias('user_agent'),\
                      "year",\
                      "start_time",\
                      "month")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + "songplays_table/", partitionBy=["year","month"])


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://myawsbucket-phtf/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

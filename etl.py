import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType
)
import pyspark.sql.functions as F



config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']

# get the schema that we used in etl.ipynb
songs_schema = StructType([
    StructField('artist_id', StringType()),
    StructField('artist_latitude', FloatType()),
    StructField('artist_location', StringType()),
    StructField('artist_longitude', FloatType()),
    StructField('artist_name', StringType()),
    StructField('duration', FloatType()),
    StructField('num_songs', IntegerType()),
    StructField('song_id', StringType()),
    StructField('title', StringType()),
    StructField('year', IntegerType())
])



def create_spark_session() -> SparkSession:
    """Creates or Gets the SparkSession, the entrypoint to our Spark Program

    Returns:
        SparkSession: The SparkSession Object
    """
    spark = (SparkSession
        .Builder()
        .appName("PySpark Data Lake")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    )
    return spark


def process_song_data(spark: SparkSession,
                      input_data: str,
                      output_data: str):
    """Processes the json song data, and saves 2 tables, one for songs, and for artists

    Args:
        spark (SparkSession): The SparkSession Object.
        input_data (str): The path where the parent directory of json data is located.
        output_data (str): The path to save the two tables as parquet files.
    """

    # get filepath to song data file
    song_data = input_data + "song_data/A/*/*/*.json"

    # read song data file
    df_songs = spark.read.json(song_data, schema = songs_schema)

    # extract columns to create songs table
    songs_table = (
        df_songs
        .select('song_id', 'title', 'artist_id', 'year', 'duration')
        .drop_duplicates(["song_id"])
        .filter(df_songs['song_id'].isNotNull())
    )
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(
        path = output_data + 'songs.parquet',
        partitionBy = ['year', 'artist_id'],
        mode = 'overwrite'
    )

    # extract columns to create artists table
    artists_table = (
        df_songs
        .select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")
        .withColumnRenamed("artist_name", "name")
        .drop_duplicates(["artist_id"])
        .filter(df_songs['artist_id'].isNotNull())
    )

    # write artists table to parquet files
    artists_table.write.parquet(
        path = output_data + 'artists.parquet',
        mode = 'overwrite'
    )


def process_log_data(spark: SparkSession,
                     input_data: str,
                     output_data: str):
    """Processes songplay data and saves tables for user data, songplays, and time

    Args:
        spark (SparkSession): the SparkSession Object
        input_data (str): The path where the parent directory of json data is located.
        output_data (str): The path to save the two tables as parquet files.
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file and do some cleaning as in etl.ipynb
    df_logs = spark.read.json(log_data)
    df_logs = (
        df_logs
        # cleaning timestamp columns
        .withColumn('ts',  (df_logs["ts"].cast('float')/1000).cast('timestamp'))
        .withColumn('registration', (df_logs["registration"].cast('float')/1000).cast('timestamp'))
        .withColumn('userId', df_logs["userId"].cast(IntegerType()))
        # renaming camelCase to snake_case
        .withColumnRenamed('firstName', 'first_name')
        .withColumnRenamed('lastName', 'last_name')
        .withColumnRenamed('userId', 'user_id')
        .withColumnRenamed('itemInSession','item_in_session')
        .withColumnRenamed('sessionId','session_id')
        .withColumnRenamed('userAgent', 'user_agent')
    )

    # load the songs data
    song_data = input_data + "song_data/A/*/*/*.json"
    df_songs = spark.read.json(song_data, schema = songs_schema)

    # songplay table. Use the `monotonically_increasing_id` function to assign a unique ID to each row
    songplays_table = (
        df_logs
        .filter(F.lower(df_logs["page"]) == "nextsong")
        .select('ts', 'user_id',  'level', 'song', 'artist', 'session_id', 'location', 'user_agent')
        .join(df_songs, 
              on = [df_logs["song"] == df_songs["title"],
                    df_logs["artist"] == df_songs["artist_name"]],
              how = "inner")
        .withColumnRenamed("ts", "start_time")
        .withColumn('songplay_id', F.monotonically_increasing_id())
    )

    # write the songplayes table to parquet
    songplays_table.write.parquet(
        path = output_data + 'songplays.parquet',
        mode = 'overwrite'
    )

    # extract columns for users table 
    ### We MUST use the user's entry with the latest timestamp
    ### That's why we're grouping by `user_id`
    ### Then, aggregating by maximum (latest) `ts`
    users_events = (
        df_logs
        .filter(df_logs['user_id'].isNotNull())
        .select('user_id', 'first_name', 'last_name', 'gender', 'level', 'ts')
    )

    max_timestamps = (
        users_events
        .groupBy('user_id')
        .agg(F.max('ts').alias('max_ts'))
    )

    users_table = (
        users_events
        .join(max_timestamps, on = "user_id", how = "inner")
        .select('user_id', 'first_name', 'last_name', 'gender', 'level')
    )
    
    # write users table to parquet files
    users_table.write.parquet(
        output_data + 'users.parquet',
        partitionBy = ['gender', 'level'],
        mode = 'overwrite'
    )

    # create time table
    time_table = (
        df_logs
        .select('ts')
        .filter(df_logs['ts'].isNotNull())
        .withColumnRenamed('ts', 'start_time')
        .withColumn('hour', F.hour('start_time'))
        .withColumn('day', F.dayofmonth('start_time'))
        .withColumn('week', F.weekofyear('start_time'))
        .withColumn('month', F.month('start_time'))
        .withColumn('year', F.year('start_time'))
        .withColumn('weekday', F.dayofweek('start_time'))
    )
    
    # write time table to parquet files partitioned by year and month
    # The partitioning isn't specified, but it makes sense to partition by year and month
    time_table.write.parquet(
        output_data + 'time.parquet',
        partitionBy = ['year', 'month'],
        mode = 'overwrite'
    )


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = config["OUTPUT_DATA_S3"]
    
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)

    spark.stop()


if __name__ == "__main__":
    main()

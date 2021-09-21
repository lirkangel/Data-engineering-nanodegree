import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StructType as R, StructField as StrFld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Dat, TimestampType


config = configparser.ConfigParser()
config.read('dwh.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['KEY']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['SECRET']
os.environ['jdk.xml.entityExpansionLimit'] = '0'


def create_spark_session():
    """
        Create or retrieve a Spark Session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", 'com.johnsnowlabs.nlp:spark-nlp_2.11:1.8.2') \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        Description: This function loads song_data from S3 and processes it by extracting the songs and artist tables
        and then again loaded back to S3

        Parameters:
            spark       : Spark Session
            input_data  : location of song_data json files with the songs metadata
            output_data : S3 bucket were dimensional tables in parquet format will be stored
    """

    song_data = os.path.join(input_data + 'song_data/*/*/*/*.json')

    # Load input by schema
    songSchema = R([
        StrFld("artist_id", Str()),
        StrFld("artist_latitude", Dbl()),
        StrFld("artist_location", Str()),
        StrFld("artist_longitude", Dbl()),
        StrFld("artist_name", Str()),
        StrFld("duration", Dbl()),
        StrFld("num_songs", Int()),
        StrFld("title", Str()),
        StrFld("year", Int()),
    ])

    df = spark.read.json(song_data, schema=songSchema)

    # drop duplicate and write file song table parquet by year and artist id
    song_fields = ["title", "artist_id", "year", "duration"]

    songs_table = df.select(song_fields).dropDuplicates().withColumn(
        "song_id", monotonically_increasing_id())

    songs_table.write.mode("overwrite").partitionBy(
        "year", "artist_id").parquet(output_data + 'songs/')

    # drop duplicate and write file artist parquet
    artists_fields = ["artist_id", "artist_name as name", "artist_location",
                      "artist_latitude as latitude", "artist_longitude as longitude"]

    artists_table = df.selectExpr(artists_fields).dropDuplicates()

    artists_table.write.mode("overwrite").parquet(output_data + 'artists/')


def process_log_data(spark, input_data, output_data):
    """
        Description: This function loads log_data from S3 and processes it by extracting the songs and artist tables
        and then again loaded back to S3. Also output from previous function is used in by spark.read.json command

        Parameters:
            spark       : Spark Session
            input_data  : location of log_data json files with the events data
            output_data : S3 bucket were dimensional tables in parquet format will be stored

    """

    log_data = os.path.join(input_data + 'log_data/*.json')

    log_df = spark.read.json(log_data)

    log_df = log_df.filter(log_df.page == 'NextSong')

    # drop duplicates and write file users
    users_fields = ["userId as user_id", "firstName as first_name",
                    "lastName as last_name", "gender", "level"]
    users_table = log_df.selectExpr(users_fields).dropDuplicates()

    users_table.write.mode("overwrite").parquet(output_data + 'users/')

    # correct timestamp by datetime
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    log_df = log_df.withColumn('timestamp', get_timestamp(log_df.ts))

    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    log_df = log_df.withColumn("start_time", get_datetime(log_df.timestamp))

    # split log to column
    log_df = log_df.withColumn("hour", hour("start_time")) \
        .withColumn("day", dayofmonth("start_time")) \
        .withColumn("week", weekofyear("start_time")) \
        .withColumn("month", month("start_time")) \
        .withColumn("year", year("start_time")) \
        .withColumn("weekday", dayofweek("start_time"))

    time_table = log_df.select(
        "ts", "start_time", "hour", "day", "week", "month", "year", "weekday")

    # write files time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy(
        "year", "month").parquet(output_data + "time")

    # load file in song data to use for songplays table
    songs_df = spark.read.parquet(os.path.join(output_data, "songs/*/*/*"))
    songs_logs = log_df.join(songs_df, (log_df.song == songs_df.title))

    # extract columns from joined song and log datasets to create songplays table
    artists_df = spark.read.parquet(os.path.join(output_data, "artists"))
    artists_songs_logs = songs_logs.join(
        artists_df, (songs_logs.artist == artists_df.name))


    # write songplays table to parquet files partitioned by year and month
    songplays_table = artists_songs_logs.select(
        col('start_time'),
        col('userId').alias('user_id'),
        col('level'),
        col('song_id'),
        col('artist_id'),
        col('sessionId').alias('session_id'),
        col('location'),
        col('userAgent').alias('user_agent'),
        col('year'),
        col('month'),
    ).repartition("year", "month")

    songplays_table.write.mode("overwrite").partitionBy(
        "year", "month").parquet(output_data + 'songplays')


def main():
    """
        Extract songs and events data from S3, Transform it into dimensional tables format, and Load it back to S3 in Parquet format
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-dend/"
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

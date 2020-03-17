import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import types as t


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    create_spark_session():
    """Create a Apache Spark session to process the data.
    Keyword arguments:
    * N/A
    Output:
    * spark -- An Apache Spark session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark 


def process_song_data(spark, input_data, output_data):
    """
        Description: This function loads song_data from S3 and processes it by extracting the songs and artist tables
        and then again loaded back to S3
        
          Keyword arguments:
            * spark         -- reference to Spark session.
            * input_data    -- path to input_data to be processed (song_data)
            * output_data   -- path to location to store the output (parquet files)
            
    """
    # get filepath to song data file
    #song_data = input_data + 'song_data/*/*/*/*.json'
    start = datetime.now()
    song_data = input_data + "/song_data/A/B/C/*.json"
    print("Reading song_data files from {}...".format(song_data))
    
    # read song data file
    df = spark.read.json(song_data)
    end = datetime.now()
    total = end - start
    print("...finished reading song_data in {}.".format(total))
    print("Song_data schema:")
    df.printSchema()
    
    start = datetime.now()
    df.createOrReplaceTempView("song_data_table")

    # extract columns to create songs table
    songs_table = spark.sql("""
                            SELECT sdtn.song_id, 
                            sdtn.title,
                            sdtn.artist_id,
                            sdtn.year,
                            sdtn.duration
                            FROM song_data_table sdtn
                            WHERE song_id IS NOT NULL
                        """)
    
    print("Songs_table schema: \n")
    songs_table.printSchema()
    print("Songs_table examples: \n")
    songs_table.show(5, truncate=False)
    
    
    # write songs table to parquet files partitioned by year and artist
    songs_table_path = output_data+'songs_table/'
    print("Writing songs_table parquet files to {}...".format(songs_table_path))
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(songs_table_path)
    end = datetime.now()
    total = end - start
    print("...finished writing songs_table in {}.".format(total))
    
    
    
    # extract columns to create artists table
    start = datetime.now()
    df.createOrReplaceTempView("artist_table")
    artists_table = spark.sql("""
                                SELECT DISTINCT arti.artist_id, 
                                    arti.artist_name,
                                    arti.artist_location,
                                    arti.artist_latitude,
                                    arti.artist_longitude
                                FROM artist_table arti
                                WHERE arti.artist_id IS NOT NULL
                                ORDER BY arti.artist_id
                            """)
    
    # write artists table to parquet files
    print("artists_table schema:")
    artists_table.printSchema()
    
    print("artists_table examples:")
    artists_table.show(5, truncate=False)
    
    artists_table_path = output_data+'artists_table/'
    print("Writing artists_table parquet files to {}...".format(artists_table_path))

    artists_table.write.mode('overwrite').partitionBy('artist_name').parquet(artists_table_path)
    end = datetime.now()
    total = end - start
    print("...finished writing artists_table in {}.".format(total))
    
    return songs_table,artists_table



def process_log_data(spark, input_data, output_data):
    
    # get filepath to log data file
    start = datetime.now()
    print("Start processing log_data JSON files...")
    log_data = input_data + 'log_data/*/*/*.json'
    # read log data file
    df = spark.read.json(log_data)
    end = datetime.now()
    end = end - start 
    print("...finished reading log_data in {}.".format(total))
    
    # filter by actions for song plays
    start = datetime.now()
    df = df.filter(df.page == 'NextSong')
    df.createOrReplaceTempView('log_data_table')

    # extract columns for users table    
    users_table = spark.sql('''SELECT DISTINCT userT.userId as user_id, 
                            userT.firstName as first_name,
                            userT.lastName as last_name,
                            userT.gender as gender,
                            userT.level as level
                            FROM log_data_table userT
                            WHERE userT.userId IS NOT NULL 
    ''')
    
    print("Users_table schema:")
    users_table.printSchema()
    print("Users_table examples:")
    users_table.show(5)
    # write users table to parquet files
    users_table_path = output_data+'users_table/'
    print("Writing users_table parquet files to {}...".format(users_table_path))
    artists_table.write.mode('overite').partitionBy('level').parquet(output_data+'users_table/')
    end = datetime.now()
    total = end - start
    print("...finished writing users_table in {}.".format(total))

    ## time table more code way !
    
#    
#     time_table = spark.sql("""
#                             SELECT 
#                             A.start_time_sub as start_time,
#                             hour(A.start_time_sub) as hour,
#                             dayofmonth(A.start_time_sub) as day,
#                             weekofyear(A.start_time_sub) as week,
#                             month(A.start_time_sub) as month,
#                             year(A.start_time_sub) as year,
#                             dayofweek(A.start_time_sub) as weekday
#                             FROM
#                             (SELECT to_timestamp(timeSt.ts/1000) as start_time_sub
#                             FROM log_data_table timeSt
#                             WHERE timeSt.ts IS NOT NULL
#                             ) A
#                         """)
    
    start = datetime.now()
    print("Creating timestamp column...")
    @udf(t.TimestampType()) # this is a decorator 
    def get_timestamp (ts):
        return datetime.fromtimestamp(ts / 1000.0)
    
    # transform new time features
    df = df.withColumn("timestamp",get_timestamp("ts"))
    print("New time table schema:")
    df.printSchema()
    print('New time table examples')
    df.show(5)
    print("Creating datetime column...")
    @udf(t.StringType())
    def get_datetime(ts):
        return datetime.fromtimestamp(ts / 1000.0)\
                       .strftime('%Y-%m-%d %H:%M:%S')

    df = df.withColumn("datetime", \
                        get_datetime("ts"))
    print("Log_data + timestamp + datetime columns schema:")
    df.printSchema()
    print("Log_data + timestamp + datetime columns examples:")
    df.show(5)
    
    df.createOrReplaceTempView("time_table_DF")
    time_table = spark.sql("""
        SELECT  DISTINCT datetime AS start_time,
                         hour(timestamp) AS hour,
                         day(timestamp)  AS day,
                         weekofyear(timestamp) AS week,
                         month(timestamp) AS month,
                         year(timestamp) AS year,
                         dayofweek(timestamp) AS weekday
        FROM time_table_DF
        ORDER BY start_time
    """)
    print("Time_table schema:")
    time_table.printSchema()
    print("Time_table examples:")
    time_table.show(5)

    time_table_path = output_data+'time_table/'
    print("Writing time_table parquet files to {}...".format(time_table_path))
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'time_table/')
    end = datetime.now()
    total = end - start
    print("...finished writing time_table in {}.".format(total))
    
    

    # read in song data to use for songplays table
    print("Reading song_data files !! ")
    song_df = spark.read.parquet(output_data+'songs_table/')
    print("Joining log_data and song_data DFs...")
    df_joined = df.join(song_df, (df.artist == song_df.artist_name) & (df.song == song_df.title))
    print("...finished joining song_data and log_data DFs.")
    print("Joined song_data + log_data schema:")
    df_joined.printSchema()
    print("Joined song_data + log_data examples:")
    df_joined.show(5)
    
    df_joined = df_joined.withColumn("songplay_id",monotonically_increasing_id())
    df_joined.createOrReplaceTempView("songplays_table_DF")
    #This is the center dataset
    songplays_table = spark.sql("""
        SELECT  songplay_id AS songplay_id,
                timestamp   AS start_time,
                userId      AS user_id,
                level       AS level,
                song_id     AS song_id,
                artist_id   AS artist_id,
                sessionId   AS session_id,
                location    AS location,
                userAgent   AS user_agent
        FROM songplays_table_DF
        ORDER BY (user_id, session_id)
    """)
    print("Songplays_table schema:")
    songplays_table.printSchema()
    print("Songplays_table examples:")
    songplays_table.show(5, truncate=False)

    # write songplays table to parquet files partitioned by year and month
    songplays_table_path = output_data+'songplays_table/'
    print("Writing songplays_table parquet files to {}...".format(songplays_table_path))
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(songplays_table_path)

    end = datetime.now()
    total = end - start
    print("...finished writing songplays_table in {}.".format(total))
    return users_table, time_table, songplays_table


def main():
    start = datetime.now()
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/marcoput/"
    
    songs_table, artists_table = process_song_data(spark, input_data, output_data)    
    users_table, time_table, songplays_table = process_log_data(spark, input_data, output_data)
    print("Finished the ETL pipeline processing.")
    print("ALL DONE !!!")

    end = datetime.now()
    print("FINISHED ETL pipeline (to process song_data and log_data) at {}".format(end))
    print("TIME: {}".format(end-start))


if __name__ == "__main__":
    main()

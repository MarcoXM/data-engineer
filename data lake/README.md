

## Quick start

First, rename dl_template.cfg to dl.cfg and fill in the open fields. Fill in AWS acces key (KEY) and secret (SECRET).

Example data is in data folder. To run the script to use that data, do the wfollowing:

* Create an AWS S3 bucket.
* Edit dl.cfg: add your S3 bucket name.
* Copy **log_data** and **song_data** folders to your own S3 bucket.
* Create **output_data** folder in your S3 bucket.
* NOTE: You can also run script locally and use local input files. Just comment/uncomment rows in `etl.py` main() function.

After installing python3 + Apache Spark (pyspark) libraries and dependencies, run from command line:

* `python3 etl.py` (to process all the input JSON data into Spark parquet files.)

---

## Overview

This Project-4 handles data of a music streaming startup, Sparkify. Data set is a set of files in JSON format stored in AWS S3 buckets and contains two parts:

Project builds an ETL pipeline (Extract, Transform, Load) to Extract data from JSON files stored in AWS S3, process the data with Apache Spark, and write the data back to AWS S3 as Spark parquet files. As technologies, Project-4 uses python, AWS S3 and Apache Spark.

As a local run on laptop, pipeline takes around 1min 40sec to execute with the given test data set.



<b>Project Description</b>

Apply the knowledge of Spark and Data Lakes to build and **ETL pipeline** for a Data Lake hosted on Amazon S3

1. we have to build an ETL Pipeline that extracts their data from S3 and process them using Spark 
2.  load back into S3 in a set of Fact and Dimension Tables. This will allow their analytics team to continue finding insights in what songs their users are listening. Will have to deploy this Spark process on a Cluster using AWS
3. 

<b>Project Datasets</b>

**Song Data Path** --> s3://udacity-dend/song_data 

**Log Data Path** --> s3://udacity-dend/log_data 



<b>Song Dataset</b>

The first dataset is a subset of real data from the Million Song Dataset(https://labrosa.ee.columbia.edu/millionsong/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example:

song_data/A/B/C/TRABCEI128F424C983.json song_data/A/A/B/TRAABJL12903CDCF1A.json

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

<b>Log Dataset</b>

The second dataset consists of log files in JSON format. The log files in the dataset with are partitioned by year and month. For example:

log_data/2018/11/2018-11-12-events.json log_data/2018/11/2018-11-13-events.json

And below is an example of what a single log file, 2018-11-13-events.json, looks like.

{"artist":"Pavement", "auth":"Logged In", "firstName":"Sylvie", "gender", "F", "itemInSession":0, "lastName":"Cruz", "length":99.16036, "level":"free", "location":"Klamath Falls, OR", "method":"PUT", "page":"NextSong", "registration":"1.541078e+12", "sessionId":345, "song":"Mercy:The Laundromat", "status":200, "ts":1541990258796, "userAgent":"Mozilla/5.0(Macintosh; Intel Mac OS X 10_9_4...)", "userId":10}

<b>Schema for Song Play Analysis</b>

A Star Schema would be required for optimized queries on song play queries

<b>Fact Table</b>

<b>songplays</b> - records in event data associated with song plays i.e. records with page NextSong songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

<b>Dimension Tables</b>

<b>users</b> - users in the app user_id, first_name, last_name, gender, level

<b>songs</b> - songs in music database song_id, title, artist_id, year, duration

<b>artists</b> - artists in music database artist_id, name, location, lattitude, longitude

<b>time</b> - timestamps of records in songplays broken down into specific units start_time, hour, day, week, month, year, weekday



## How to use

**Project has one script:**

* **etl.py**: This script uses data in s3:/udacity-dend/song_data and s3:/udacity-dend/log_data, processes it, and inserts the processed data into DB.

### Prerequisites

Python3 is recommended as the environment. The most convenient way to install python is to use Anaconda (https://www.anaconda.com/distribution/) either via GUI or command line.
Also, the following libraries are needed for the python environment to make Jupyter Notebook and Apache Spark to work:

* _pyspark_ (+ dependencies) to enable script to create a SparkSession. (See https://spark.apache.org/docs/latest/api/python/pyspark.sql.html)
* NOTE: in the beginning of the execution, script downloads hadoop-aws package to enable connection to AWS.

### Run etl.py

Type to command line:

`python3 etl.py`

* Script executes Apache Spark SQL commands to read source data (JSON files) from S3 to memory as Spark DataFrames.
* In memory, data is further manipulated to analytics DataFrames.
* Analytics dataFrames are stored back to S4 as Spark parquet files.
* Script writes to console the query it's executing at any given time and if the query was successfully executed.
* Also, script writes to console DataFrame schemas and show a handful of example data.
* In the end, script tells if whole ETL-pipeline was successfully executed.

Output: input JSON data is processed and analysed data is written back to S3 as Spark parquet files.

## Data cleaning process

`etl.py` works the following way to process the data from source files to analytics tables:

* Loading part of the script (COPY from JSON to staging tables) query takes the data as it is.
* When inserting data from staging tables to analytics tables, queries remove any duplicates (INSERT ... SELECT DISTINCT ...).
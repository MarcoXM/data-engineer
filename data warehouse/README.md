## Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

Task is to build an ETL Pipeline that extracts their data from S3, staging it in Redshift and then transforming data into a set of Dimensional and Fact Tables for their Analytics Team to continue finding Insights to what songs their users are listening to.

### Project Description

Application of Data warehouse and AWS to build an ETL Pipeline for a database hosted on Redshift Will need to load data from S3 to staging tables on Redshift and execute SQL Statements that create fact and dimension tables from these staging tables to create analytics

### Project Datasets</b>

Song Data Path     -->     s3://udacity-dend/song_data
Log Data Path      -->     s3://udacity-dend/log_data
Log Data JSON Path -->     s3://udacity-dend/log_json_path.json

## Quick Start

First, rename dwh_template.cfg to dwh.cfg and fill in the open fields. Fill in AWS acces key (KEY) and secret (SECRET).

To access AWS, you need to do in AWS the following:

* create IAM user (e.g. dwhuser)
* create IAM role (e.g. dwhRole) with AmazonS3ReadOnlyAccess access rights
* get ARN
* create and run Redshift cluster (e.g. dwhCluster => HOST)

For creating IAM role, getting ARN and running cluster, you can use `Udacity-DEND-Project-3-AWS-Setup.ipynb`.

Example data is in data folder. To run the script to use that data, do the wfollowing:

* Create an AWS S3 bucket.
* Edit cwh.cfg: add your S3 bucket name in LOG_PATH and SONG_PATH variables.
* Copy log_data and song_data folders to your own S3 bucket.

After installing python3 + AWS SDK (boto3) libraries and dependencies, run from command line:

* `python3 create_tables.py` (to create the DB to AWS Redshift)
* `python3 etl.py` (to process all the input data to the DB)

---


### Schema for Song Play Analysis

A Star Schema would be required for optimized queries on song play queries

<b>Fact Table</b>

<b>songplays</b> - records in event data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

<b>Dimension Tables</b>

<b>users</b> - users in the app
user_id, first_name, last_name, gender, level

<b>songs</b> - songs in music database
song_id, title, artist_id, year, duration

<b>artists</b> - artists in music database
artist_id, name, location, lattitude, longitude

<b>time</b> - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday

## How to use

**Project has two scripts:**

* **create_tables.py**: This script drops existing tables and creates new ones.
* **etl.py**: This script uses data in s3:/udacity-dend/song_data and s3:/udacity-dend/log_data, processes it, and inserts the processed data into DB.

### Prerequisites

Python3 is recommended as the environment. The most convenient way to install python is to use Anaconda (https://www.anaconda.com/distribution/) either via GUI or command line.
Also, the following libraries are needed for the python environment to make Jupyter Notebook and AWS Redshift to work:

* _AWS SDK (boto3)_ (+ dependencies) to enable scripts and Jupyter to connect to AWS Redshift DB. (See https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
* _jupyter_ (+ dependencies) to enable Jupyter Notebook.
* _ipython-sql_ (https://anaconda.org/conda-forge/ipython-sql) to make Jupyter Notebook and SQL queries to AWS Redshift work together. NOTE: you may need to install this library from command line.

### Run create_tables.py

Type to command line:

`python3 create_tables.py`

* All tables are dropped.
* New tables are created: 2x staging tables + 4x dimensional tables + 1x fact table.
* Output: Script writes _"Tables dropped successfully"_ and _"Tables created successfully"_ if all tables were dropped and created without errors.

### Run etl.py

Type to command line:

`python3 etl.py`

* Script executes AWS Redshift COPY commands to insert source data (JSON files) to DB staging tables.
* From staging tables, data is further inserted to analytics tables.
* Script writes to console the query it's executing at any given time and if the query was successfully executed.
* In the end, script tells if whole ETL-pipeline was successfully executed.

Output: raw data is in staging_tables + selected data in analytics tables.

## Data cleaning process

`etl.py`works the following way to process the data from source files to analytics tables:

* Loading part of the script (COPY from JSON to staging tables) query takes the data as it is.
* When inserting data from staging tables to analytics tables, queries remove any duplicates (INSERT ... SELECT DISTINCT ...).

<b>Wrap UP</b>

1. Import all the necessary libraries
2. Write the configuration of AWS Cluster, store the important parameter in some other file
3. Configuration of boto3 which is an AWS SDK for Python
4. Using the bucket, can check whether files log files and song data files are present
5. Create an IAM User Role, Assign appropriate permissions and create the Redshift Cluster
6. Get the Value of Endpoint and Role for put into main configuration file
7. Authorize Security Access Group to Default TCP/IP Address
8. Launch database connectivity configuration
9. Go to Terminal write the command "python create_tables.py" and then "etl.py"
10. Should take around 4-10 minutes in total
11. Then you go back to jupyter notebook to test everything is working fine
12. I counted all the records in my tables
13. Now can delete the cluster, roles and assigned permission
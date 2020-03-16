<b>Project: Data Modeling with Cassandra</b>

<b>Introduction:</b>
    
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. There is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app. My role is to create an Apache Cassandra database which can create queries on song play data to answer the questions.

<b>Project Overview:</b>

In this project, I would be applying Data Modeling with Apache Cassandra and complete an ETL pipeline using Python. I am provided with part of the ETL pipeline that transfers data from a set of CSV files within a directory to create a streamlined CSV file to model and insert data into Apache Cassandra tables.

<b>Datasets:</b>

For this project, you'll be working with one dataset: event_data. The directory of CSV files partitioned by date. Here are examples of filepaths to two files in the dataset:
event_data/2018-11-08-events.csv
event_data/2018-11-09-events.csv

<b>Project Template:</b>

These instructions help you to install and run the project locally. After installing python3 + Apache Cassandra + libraries and dependencies, run from command line:

- `python3 create_tables.py` (to create the DB to Cassandra)

- `python3 etl.py` (to process all the input data to the DB)

  

<b>Table:</b>

- **song_in_session**: songs and artists in a session (session_id, item_in_session, artist, song, length)
- **artist_in_session**: artist, song, and user in a session (user_id, session_id, artist, song, item_in_session, first_name, last_name)
- **user_and_song**: user listening certain song (song, user_id, first_name, last_name)

<b>Build ETL Pipeline:</b>

1.	Implement the logic in section Part I of the notebook template to iterate through each event file in event_data to process and create a new CSV file in Python
2.	Make necessary edits to Part II of the notebook template to include Apache Cassandra CREATE and INSERT three statements to load processed records into relevant tables in your data model
3.	Test by running three SELECT statements after running the queries on your database
4.	Finally, drop the tables and shutdown the cluster

### Run create_tables.py

Type to command line:

```
python3 create_tables.py
```

Output: Script writes *"Tables dropped successfully"* and *"Tables created successfully"* if all tables were dropped and created without errors.

### Run etl.py

Type to command line:

```
python3 etl.py
```

Then you could do thing like SQL
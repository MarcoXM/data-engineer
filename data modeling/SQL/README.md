<b>Introduction</b>

A startup called <b>Sparkify</b> want to analyze the data they have been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to.

The aim is to create a Postgres Database Schema and ETL pipeline to optimize queries for song play analysis.

<b>Project Description </b>

In this project, I have to model data with Postgres and build and ETL pipeline using Python. On the database side, I have to define fact and dimension tables for a Star Schema for a specific focus. On the other hand, ETL pipeline would transfer data from files located in two local directories into these tables in Postgres using Python and SQL

<b>Schema for Song Play Analysis</b>

<b>Fact Table</b>

<b> 	songplays </b> records in log data associated with song plays

<b>Dimension Tables</b>

<b> 	users </b> in the app

<b> 	songs </b> in music database

<b> 	artists </b> in music database

<b> 	time: </b> timestamps of records in songplays broken down into specific units

<b>Project Design</b>

Database Design is very optimized because with a ew number of tables and doing specific join, we can get the most information and do analysis

ETL Design is also simplified have to read json files and parse accordingly to store the tables into specific columns and proper formatting

**How to use**

prerequisites: 

Python3 is recommended as the environment. The most convenient way to install python is to use Anaconda (https://www.anaconda.com/distribution/) either via GUI or command line. Also, the following libraries are needed for the python environment to make Jupyter Notebook and Postgresql to work:

- *postgresql* (+ dependencies) to enable sripts and Jupyter to connect to Postgresql DB.
- *jupyter* (+ dependencies) to enable Jupyter Notebook.
- *ipython-sql* (https://anaconda.org/conda-forge/ipython-sql) to make Jupyter Notebook and SQL queries to Postgresql work together. NOTE: you may need to install this library from command line.

 

**Run the code** (py)

Type to command line:

```
python create_tables.py
```

Output: Script writes *"Tables dropped successfully"* and *"Tables created successfully"* if all tables were dropped and created without errors.



Type to command line:

```
python etl.py
```



More information could check in the ipynb file!
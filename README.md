# data-modelling-with-postgres
Project to help master data modelling with Postgres and building an ETL pipeline with Python

## Project Description

This project entails data modeling with Postgres and building an ETL pipeline using Python. The ETL pipeline transfers data from files in two two datasets, ***song_data and log_data***, into these tables in Postgres using Python and SQL. 


### Song Dataset

The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID.

### Log Dataset

The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate activity logs from a music streaming app based on specified configurations. The log files in the dataset are partitioned by year and month.

### Schema for Song Play Analysis

Using the song and log datasets to create a star schema optimized for queries on song play analysis that includes the following tables:

#### Fact Table

    * songplays - records in log data associated with song plays i.e. records with page ```NextSong```
        * Table columns: songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### Dimension Tables

    * users - users in the app
        * Table columns: user_id, first_name, last_name, gender, level
    * songs - songs in music database
        * Table columns: song_id, title, artist_id, year, duration
    * artists - artists in music database
        * Table columns: artist_id, name, location, latitude, longitude
    * time - timestamps of records in songplays broken down into specific units
        * Table columns: start_time, hour, day, week, month, year, weekday

### Files in the Repository

    * test.ipynb: displays the first few rows of each table to verify the data is inserted into the database. Also has sanity checks for constraints such as primary key, not null and data types of the tables created
    * create_tables.py: drops and creates the schema tables. Run this file to reset the tables before each time the ETL scripts are run.
    * etl.ipynb: reads and processes a single file from song_data and log_data and loads the data into the tables. This notebook contains detailed instructions on the ETL process for each of the tables.
    * etl.py: reads and processes files from song_data and log_data and loads them into the tables.
    * sql_queries.py contains all sql queries for creating the schema, and is imported into the last three files above.

### How to run the Scripts

* To create the sparkifydb and the tables, run the create_tables.py by executing ```python or python3  create_tables.py``` in terminal
* This imports the sql_queries.py script holding all the queries for creating and inserting records into the database. 
* To see how each function works, run the individual cells in the etl.ipynb notebook
* Otherwise run the etl.py script to load all the data into the database by: 
        * First make sure the kernel in etl.ipynb aren't running to end the connection to the database
        * then execute ```python or python3  create_tables.py``` in terminal
        * then execute ```python or python3  etl.py``` in terminal
* To test the data inserted into the database run the individual cells in the etl.ipynb notebook, either after running etl.ipynb or etl.py
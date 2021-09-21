#Introduction

In this project we will build an ETL pipeline that extracts their data from the data lake hosted on S3, processes them using Spark which will be deployed on an EMR cluster using AWS, and load the data back into S3 as a set of dimensional tables in parquet format.

From this tables we will be able to find insights in what songs their users are listening to.

##How to run

To run this project in local mode, create a file dl.cfg in the root of this project with the following data:

[AWS]
KEY=YOUR_AWS_ACCESS_KEY
SECRET=YOUR_AWS_SECRET_KEY

Finally, run the following command:

python3 etl.py

##Project structure

The files found at this project are the following:

dwh.cfg: not uploaded to github - AWS credentials.
etl.py: Script that load songs and log data from S3, transforms it using Spark, loads the dimensional tables created in parquet format back to S3.
README.md: Current file, contains detailed information about the project.

##ETL pipeline

Load credentials

Read data from S3

Song data: s3://udacity-dend/song_data
Log data: s3://udacity-dend/log_data
The script reads song_data and load_data from S3.

Process data using spark

Transforms them to create five different tables listed under Dimension Tables and Fact Table. Duplicates are addressed where appropriate.

Writes them to partitioned parquet files in table directories on S3.

Each of the five tables are written to parquet files in a separate analytics directory on S3. Each table has its own folder within the directory.

Source Data

Song datasets: all json files are nested in subdirectories under s3a://udacity-dend/song_data.

Dimension Tables and Fact Table

songplays - Fact table - records with next songs

Tables:

    Fact:
    songplays - records in log data associated with song plays with page NextSong

    Dimension Tables:
    users - users in the website will store credential
    
    songs - is just songs (database)
    
    artists - is artists in music (database)
    
    time - timestamps of records in songplays broken down into specific units
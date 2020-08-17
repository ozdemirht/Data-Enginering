# Analysis of Song Play Data

Sparkify is collecting user activity data from their new music streaming app into JSON files. 
These log files contain data about songs and how their users consume these content. 

Motivation: Sparkify is interested in analysing these user data to understand their customers' behavior. 
These insights may create new opportunties for Sparkify while catering their customers and gaining new customers. 

There is a need to provide a solution allowing Sparkify to query their data flexibly. 

This proposal applies star schema model to define relational tables, loaded from log data. 
Sparkify can run variety of queries on this relational database to extract customer insights. 

# Design

The proposed design uses [star schema](https://en.wikipedia.org/wiki/Star_schema) to support many different queries. 

The following tables are dimensional tables;
  1. <b>songs</b> keeps track of song data. Each song record contains the title, year, duration, and artist identifier (links to artists table).
  1. <b>artists</b> keeps track of artists data. Each record contains the name and location of the artist. 
  1. <b>users</b> keeps track of users with paid and free subscription levels.  
  1. <b>time</b> keeps track of time of song played events.
  
The following table is a fact table;
  1. <b>songplay</b> keeps track of song play events by users. 

# Explanation of files in the repository

  1. <b>sql_queries.py</b> : create_tables.py and etl.py scripts include. 
  1. <b>create_tables.py</b> : Defines database and tables. One needs to run this script before loading any data.
  1. <b>etl.py</b> : Script extracts, translates, and loads data from json files into tables in PostgreSQL. 
  1. <b>[test.ipynb](./test.ipynb)</b> : Notebook to check the database, tables, and content of each table.
  1. <b>[etl.ipynb](./etl.ipynb)</b> : Notebook to test steps of etl processes for each json file. 
  
# How to run?

  1. Run [create_tables.py](./create_tables.py) from Notebook by using '%run create_tables.py'
  2. Run [etl.py](./etl.py) from Notebook by using '%run etl.py' to load data from JSON files in ./data/song_data and ./data/long_data directories.
  3. Can use [test.ipynb](./test.ipynb) notebook to check the data in the relational database. 
  
# Example queries

  1. Find out for each user, which songs or artists they like the most. [<i>enable promotion of activities around the artist or songs. recommendation engine.</i>]
  1. Which songs/content are popular? By day of week or time of day. [<i> top 10 songs of the week</i>]
  1. For each user, extract a weekly/daily usage profile. [<i>may support predictive content distribution</i>] 



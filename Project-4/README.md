# Project-4: (Data Lake) Sparkify's Song Play Data as Parquet Files in AWS S3 by using Apache Spark

Sparkify is collecting user activity data from their new music streaming app into [JSON](https://en.wikipedia.org/wiki/JSON) files (in [AWS S3](https://aws.amazon.com/s3/) bucket). 
These log and song files contain data about songs and how their users consume these content. 

<b>Motivation</b>: Sparkify is interested in analysing these user data to understand their customers' behavior. These insights may create new opportunties for Sparkify while catering their existing customers (<i>retention</i>) and gaining new customers (<i>acquisition</i>). 

There is a need to provide a solution allowing business departments of Sparkify to query their data flexibly for their business needs. [Schema-on-Read](https://www.blue-granite.com/blog/bid/402596/top-five-differences-between-data-lakes-and-data-warehouses) approach is a viable solution for their analysis using [Apache Spark](https://spark.apache.org/) ([PySpark](https://spark.apache.org/docs/latest/api/python/index.html)) and [AWS S3](https://aws.amazon.com/s3/) (as [Data Lake](https://en.wikipedia.org/wiki/Data_lake)). 

This proposal applies star schema model by 
- extracting [JSON](https://en.wikipedia.org/wiki/JSON) data files on [AWS S3](https://aws.amazon.com/s3/) to [DataFrames](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html) of [Apache Spark](https://spark.apache.org/) ([PySpark](https://spark.apache.org/docs/latest/api/python/index.html)),
- transforming raw data to [star schema](https://en.wikipedia.org/wiki/Star_schema) by using [Apache Spark](https://spark.apache.org/), and
- loading these dataframes of [star schema](https://en.wikipedia.org/wiki/Star_schema) as [Parquet files](https://parquet.apache.org/) (column-oriented storage format) on [AWS S3](https://aws.amazon.com/s3/). 

Sparkify can run variety of queries on this star schema-based data representation to extract customer insights. 

# Design

The raw data is hosted in the following [AWS S3](https://aws.amazon.com/s3/) buckets; 
 1. Song data: [s3a://udacity-dend/song_data](s3a://udacity-dend/song_data)
 1. Log data: [s3a://udacity-dend/log_data](s3a://udacity-dend/log_data)

![System View](/images/Udacity-Nano-DE2.png)
__Figure 1__: Overview (AWS S3 as Data Lake)

[PySpark](https://spark.apache.org/docs/latest/api/python/index.html)
code extracts [JSON](https://en.wikipedia.org/wiki/JSON) files into [DataFrames](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html).

<i>song</i> [JSON](https://en.wikipedia.org/wiki/JSON) files contain the following fields;

| Field            | Data Type | Description |
|------------------|-----------|-------------|
| artist_id        | string    | Id of <b>artist</b> |
| artist_latitude  | double    | Latitude of <b>artist</b> |
| artist_longitude | double    | Longitude of <b>artist</b>  | 
| artist_location  | string    | Location of <b>artist</b> | 
| artist_name      | string    | Name of <b>artist</b>  |
| duration         | double    | Duration of <b>song</b> |
| num_songs        | long      | |
| song_id          | string    | Id of <b>song</b> |
| title            | string    | Title of <b>song</b> |
| year             | long      | Year of <b>song</b> |

<i>Etl.py</i> transforms [song data](s3a://udacity-dend/song_data) to <i>songs</i> and <i>artists</i> dimensions as dataframes, and loads dataframes as [Parquet files](https://parquet.apache.org/) in [AWS S3](https://aws.amazon.com/s3/). 

<i>log</i> [JSON](https://en.wikipedia.org/wiki/JSON) files contain the following fields;

| Field         | Data Type | Description |
|---------------|-----------|-------------|
| artist        | string    | Name of <b>artist</b>|
| auth          | string    | Auhenticated|
| firstName     | string    | First Name of <b>user</b> |
| gender        | string    | Gender of <b>user</b> |
| itemInSession | long      | Play sequence number in a session|
| lastName      | string    | Last Name of <b>user</b> | 
| length        | double    | | 
| level         | string    | Level (free/paid) of <b>user</b>| 
| location      | string    | Location of <b>artist</b>| 
| method        | string    | [HTTP Request](https://tools.ietf.org/html/rfc7231) Method | 
| page          | string    | | 
| registration  | double    | | 
| sessionId     | long      | __Session__ Identifier |
| song          | string    | Title of <b>song</b>|
| status        | string    | [HTTP Response](https://tools.ietf.org/html/rfc7231) Status|
| ts            | bigint    | Timestamp of <b>songplay</b> event|
| userAgent     | string    | Browser |
| userId        | long      | Id of <b>user</b>|


The proposed design uses [star schema](https://en.wikipedia.org/wiki/Star_schema) to support many different queries. 
The following [Parquet files](https://parquet.apache.org/) files are for dimensional tables;
  1. <b>users.parquet</b> keeps track of users with paid and free subscription levels.  
  1. <b>artists.parquet</b> keeps track of artists data, containings the identifier, name and location of the artist. 
  1. <b>time.parquet</b> keeps track of <b>time</b> of song played events.
  1. <b>songs.parquet</b> keeps track of song data, containing the identifier, title, year, duration, and artist identifier (links to <b>artists.parquet</b>).

![Data Flow in ETL](/images/Udacity-Nano-DE-ETL_v2.png)
__Figure 2__: ETL: Input and Output

![Data Model](/images/datamodel3.png)
__Figure 3__: Star Schema Model

<i>users.parquet</i> has the following columns;

| Column     | Data Type | Description  |
|------------|-----------|--------------|
| userId    | long      | Unique Identifier  |
| firstName | string    | First name  | 
| lastName  | string    | Last name   | 
| gender     | string    | Gender      |
| level      | string    | free/paid   |

<i>artists.parquet</i> has the following columns;

| Column    | Data Type |Description  |
|-----------|-----------|-------------|
| artist_id | string    | Unique Identifier |
| artist_name      | string    | Name      | 
| artist_location  | string    | Location  | 
| artist_latitude  | double    | Latitude  | 
| artist_longitude | double    | Longitude |

<i>songs.parquet</i> has the following columns;

| Column    | Data Type | Description  |
|-----------|-----------|--------------|
| song_id   | string    | Unique Identifier |
| title     | string    | Title    | 
| artist_id | string    | Id of <b>artist</b>|
| year      | long      | Year     |
| duration  | double    | Duration |

<i>time.parquet</i> has the following columns;

| Column     | Data Type | Description  |
|------------|-----------|--------------|
| start_time | long      | Timestamp    | 
| hour       | integer   | Hour of timestamp    |
| day        | integer   | Day of timestamp     |
| week       | integer   | Week of timestamp    |
| month      | integer   | Month of timestamp   |
| year       | integer   | Year of timestamp    |
| weekday    | integer   | Weekday of timestamp |

The following file is a fact table;
  1. <b>songplays.parquet</b> keeps track of song play events by users. 

<i>songplays.parquet</i> has the following columns;

| Column      | Data Type | Description  |
|-------------|-----------|--------------|
| songplay_id | long    | Unique identifier of songplayed event, auto increments, __monotonically_increasing_id__() |  
| start_time  | long    | (FK) Identifier of <b>time</b>| 
| user_id     | string      | (FK) Identifier of <b>user</b> | 
| level       | string    | free/paid |
| song_id     | string    | (FK) Identifier of <b>song</b>|
| artist_id   | string    | (FK) Identifier of <b>artist</b> | 
| session_id  | long      | Identifier of <b>session</b>| 
| location    | string    | Location | 
| user_agent  | string    | Browser |
| year        | integer    | Year of **songplay** event | 
| month       | integer    | Month of **songplay** event |

# Explanation of files in the repository

  1. <b>etl.py</b> : Extracts data from log and song [JSON](https://en.wikipedia.org/wiki/JSON) files in [AWS S3](https://aws.amazon.com/s3/) bucket into dataframes of Apache Spark. Then, <b>etl.py</b> transforms dataframes(log and song) to dimension and facts dataframes (star schema). Finaly, <b>etl.py</b> loads dimension and facts dataframes (star schema) to [AWS S3](https://aws.amazon.com/s3/) as [Parquet files](https://parquet.apache.org/).   
  1. <b>dl.cfg</b> : Contains <i>AWS_ACCESS_KEY_ID</i> and <i>AWS_SECRET_ACCESS_KEY</i> to able to access [1,2] to [AWS S3](https://aws.amazon.com/s3/) (for read and write). Please configure before running **etl.py** script. 
  
# How to run?
### etl.py
  - Run <b>etl.py</b> from Notebook by using '%run etl.py' that will
    - extract data from [JSON](https://en.wikipedia.org/wiki/JSON) files in [AWS S3](https://aws.amazon.com/s3/) into dataframes,
    - transform these into dimensions and fact of star schema (as dataframes), and 
    - load these dataframes to [AWS S3](https://aws.amazon.com/s3/) bucket as [Parquet files](https://parquet.apache.org/). 


# References
 1. [AWS Identity and Access Management](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user.html)
 1. [Amazon Simple Storage Service](https://docs.aws.amazon.com/AmazonS3/latest/user-guide/set-bucket-permissions.html)
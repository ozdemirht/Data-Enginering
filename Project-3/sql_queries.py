import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

HOST = config.get("CLUSTER","HOST")
DB_NAME = config.get("CLUSTER","DB_NAME")
DB_USER = config.get("CLUSTER","DB_USER")
DB_PASSWORD= config.get("CLUSTER","DB_PASSWORD")
DB_PORT= config.get("CLUSTER","DB_PORT")

ARN = config.get("IAM_ROLE","ARN")

LOG_DATA = config.get("S3","LOG_DATA")
LOG_JSONPATH = config.get("S3","LOG_JSONPATH")
SONG_DATA = config.get("S3","SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events_table (
    artist        varchar,
    auth          varchar,
    firstName     varchar,
    gender        varchar,
    itemInSession integer,
    lastName      varchar,
    length        numeric,
    level         varchar,
    location      varchar,
    method        varchar,
    page          varchar,
    registration  float8,
    sessionId     integer,
    song          varchar,
    status        varchar,
    ts            bigint,
    userAgent     varchar,
    userId        integer
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs_table (
    num_songs        integer, 
    artist_id        varchar, 
    artist_latitude  float4, 
    artist_longitude float4, 
    artist_location  varchar, 
    artist_name      varchar, 
    song_id          varchar, 
    title            varchar, 
    duration         float8, 
    year             integer
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time bigint PRIMARY KEY, 
    tstamp     timestamp,
    hour       integer, 
    day        integer, 
    week       integer, 
    month      integer, 
    year       integer, 
    weekday    integer
) diststyle all;
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id    integer PRIMARY KEY,
    first_name varchar NOT NULL, 
    last_name  varchar NOT NULL, 
    gender     character, 
    level      varchar
) diststyle all;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar PRIMARY KEY,
    name      varchar NOT NULL, 
    location  varchar, 
    latitude  numeric, 
    longitude numeric
) diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id   varchar PRIMARY KEY,
    title     varchar NOT NULL, 
    artist_id varchar REFERENCES artists(artist_id), 
    year      integer, 
    duration  numeric
) diststyle all;
""")

# Fact table of Star Schema
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id bigint  IDENTITY(0,1) PRIMARY KEY,  
    start_time  bigint  REFERENCES time(start_time), 
    user_id     integer REFERENCES users(user_id), 
    level       varchar, 
    song_id     varchar REFERENCES songs(song_id), 
    artist_id   varchar REFERENCES artists(artist_id), 
    session_id  integer NOT NULL, 
    location    varchar, 
    user_agent  varchar
) diststyle auto;
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events_table 
    from {}
    iam_role {}
    json {};
""").format(LOG_DATA,ARN,LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs_table 
    from {}
    iam_role {}
    json 'auto';
""").format(SONG_DATA,ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO 
  songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)  
  SELECT  ts as start_time, userId as user_id, level,
        song as song_id, artist as artist_id, 
        sessionId as session_id, location, userAgent as user_agent 
  FROM staging_events_table
;
""")

# ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level
user_table_insert = ("""
INSERT INTO users (SELECT userId as user_id, firstName as first_name, lastName as last_name, gender, level FROM staging_events_table WHERE userId is NOT NULL)  
;
""")

# ON CONFLICT (song_id) DO NOTHING
song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) 
 SELECT song_id, title, artist_id, year, duration 
 FROM staging_songs_table
 WHERE song_id is NOT NULL and artist_id is NOT NULL
;
""")

# ON CONFLICT (artist_id) DO NOTHING
artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)  
  SELECT artist_id, 
       artist_name as name, artist_location as location, 
       artist_latitude as latitude, artist_longitude as longitude 
  FROM staging_songs_table
  WHERE artist_id is NOT NULL
;
""")

# ON CONFLICT (start_time) DO NOTHING
time_table_insert = ("""
INSERT INTO time (start_time, tstamp, hour, day, week, month, year, weekday)  
  SELECT ts as start_time, TIMESTAMP 'epoch' + ts * INTERVAL '1 second' as tstamp,
       EXTRACT(hour from tstamp) as hour, EXTRACT(day from tstamp) as day, 
       EXTRACT(week from tstamp) as week, EXTRACT(month from tstamp) as month, 
       EXTRACT(year from tstamp) as year, EXTRACT(weekday from tstamp) as weekday 
  FROM staging_events_table 
;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, 
                        time_table_create, user_table_create, artist_table_create, 
                        song_table_create, songplay_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [user_table_insert, artist_table_insert, song_table_insert, time_table_insert, songplay_table_insert]





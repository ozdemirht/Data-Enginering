# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
songplay_id SERIAL PRIMARY KEY,  
start_time bigint, 
user_id integer, 
level varchar, 
song_id varchar, 
artist_id varchar, 
session_id integer, 
location varchar, 
user_agent varchar
);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
user_id integer PRIMARY KEY,
first_name varchar, 
last_name varchar, 
gender character, 
level varchar
);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
song_id varchar PRIMARY KEY,
title varchar, 
artist_id varchar, 
year integer, 
duration numeric
);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
artist_id varchar PRIMARY KEY,
name varchar, 
location varchar, 
lattitude numeric, 
longitude numeric
);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
start_time bigint, 
hour integer, 
day integer, 
week integer, 
month integer, 
year integer, 
weekday integer
);
""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)  VALUES (%s, %s, %s, %s, %s, %s, %s, %s) 
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)  VALUES (%s, %s, %s, %s, %s)
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s) 
;""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, lattitude, longitude)  VALUES (%s, %s, %s, %s, %s);""")


time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)  VALUES (%s, %s, %s, %s, %s, %s, %s) 
""")

# FIND SONGS

song_select = ("""SELECT song_id, songs.artist_id \
                    FROM songs, artists \
                    WHERE songs.title = TRIM(%s) AND \
                            songs.artist_id = artists.artist_id AND artists.name = TRIM(%s) AND songs.duration = %s;
""")

user_select = ("""SELECT * FROM users WHERE user_id = %s;
""") 

artist_select = ("""SELECT * FROM artists WHERE artist_id = %s;
""") 

song_exist = ("""SELECT * FROM songs WHERE song_id = %s;
""") 

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
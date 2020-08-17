import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

# tim.o.> convert timestamp column to datetime
import datetime

# tim.o.> insert time data records
import numpy as np

def process_song_file(cur,      # type: Database SQL cursor
                      filepath  # type: File Name
                     ):
    """ Reads JSON file, inserts artist and song table """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df.iloc[0][['song_id','title','artist_id','year','duration']].values

    # Check whether the song exists in songs table
    song_q_values = (song_data[0],)
    cur.execute(song_exist,song_q_values) 
    results = cur.fetchone()

    # Insert when the song does not exist in songs table
    if(results == None):
        song_data = list(song_data)
        try:
            cur.execute(song_table_insert, np.asarray(song_data))
        except (Exception, psycopg2.Error) as error:
            if(cur):
                print('[{}] Failed to insert {} into songs table : {}'.format(filepath,song_data,error))
        
    # insert artist record
    artist_data = list( df.iloc[0][['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values) 
    
    # Check whether the artist exists in artists table
    artist_q_values = (artist_data[0],)
    cur.execute(artist_select,artist_q_values) 
    results = cur.fetchone()

    # Insert when the artist does not exist in artists table
    if(results == None):
        try:
            cur.execute(artist_table_insert, artist_data)
        except (Exception, psycopg2.Error) as error:
            if(cur):
                print('[{}] Failed to insert a row into artists table : {}'.format(filepath,error))


def process_log_file(cur,      # type: Database SQL cursor
                     filepath  # type: File Name
                    ):
    """ Reads JSON file, inserts time, users and songplay table """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    # fromtimestamp does not handle millisecond, hence convert to seconds
    # revise microsecond attribute by adding the milliseconds part of the original timestamp
    t = df['ts'].apply(lambda x: datetime.datetime.fromtimestamp(x/1000.0).replace(microsecond=1000*(x%1000)))
    
    # insert time data records
    time_data = np.asarray([t.dt.hour, t.dt.day, 0,t.dt.month, t.dt.year, t.dt.weekday])
    column_labels = ('start_time','hour','day','week','month','year','weekday')

    # tim.o.> get values from dt['ts'] and index from t
    df_tstamp = pd.DataFrame(data=df['ts'].values,index=t.index,columns=['start_time'])

    # tim.o.> use original index from t
    df_hours = pd.DataFrame(data=np.asarray(t.dt.hour),index=t.index,columns=['hour'])
    df_days = pd.DataFrame(data=np.asarray(t.dt.day),index=t.index,columns=['day'])
    df_wofyear = pd.DataFrame(data=np.asarray(t.dt.day/7),index=t.index,columns=['week'])
    df_month = pd.DataFrame(data=np.asarray(t.dt.month),index=t.index,columns=['month'])
    df_year = pd.DataFrame(data=np.asarray(t.dt.year),index=t.index,columns=['year'])
    df_weekday = pd.DataFrame(data=np.asarray(t.dt.weekday),index=t.index,columns=['weekday'])

    dff = pd.concat([df_tstamp,df_hours, df_days,df_wofyear,df_month,df_year,df_weekday], join='outer', axis=1)
    time_df = pd.DataFrame(data=dff,columns=column_labels)
    # tim.o.> integer for week column
    time_df['week'] = time_df['week'].apply(lambda x: int(x))

    for i, row in time_df.iterrows():
        try:
            cur.execute(time_table_insert, list(row))
        except (Exception, psycopg2.Error) as error:
            if(cur):
                print('[{}] Failed to insert a row into time table : {}'.format(filepath,error))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]
    user_df = user_df.drop_duplicates()

    # insert user records
    for i, row in user_df.iterrows():
        # Check whether the user exists in users table
        user_q_values = (row['userId'],)
        cur.execute(user_select,user_q_values) 
        results = cur.fetchone()
        
        # Insert when the user does not exist in users table
        if(results == None):
            #print('Insert user ',row['userId'])            
            try:
                cur.execute(user_table_insert, list(row))
            except (Exception, psycopg2.Error) as error:
                if(cur):
                    print('[{}] Failed to insert {} into users table : {}'.format(filepath,list(row),error))

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent,)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        try:
            conn.commit()
        except (Exception, psycopg2.Error) as error:
            if(conn):
                print('{}/{} Failed to commit() : {}'.format(i, num_files,error))
        #print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    print('ETL is completed!')

    # Close databse connection 
    conn.close()


if __name__ == "__main__":
    main()
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur,   # type: Database cursor
                        conn   # type: Database connection
                       ):
    """ Loads JSON data in S3 to Staging Tables """
    for query in copy_table_queries:
        print("Executing [{}]\n".format(query))
        cur.execute(query)
        conn.commit()


def insert_tables(cur,   # type: Database cursor 
                  conn   # type: Database connection
                 ):
    """ Insert data from staging tables into star schema's fact and dims tables """
    for query in insert_table_queries:
        print("Executing [{}]\n".format(query))
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
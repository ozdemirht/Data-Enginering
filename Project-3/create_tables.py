import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur,   # type: Database cursor 
                conn   # type: Database connection
               ):
    """ Drops tables """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur,   # type: Database cursor 
                  conn   # type: Database connection
                 ):
    """ Creates tables """
    for query in create_table_queries:
        print("Executing [{}]\n".format(query))
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # Drop tables of exist
    drop_tables(cur, conn)
    
    # Create tables
    create_tables(cur, conn)
    
    # Close DB Connection
    conn.close()


if __name__ == "__main__":
    main()

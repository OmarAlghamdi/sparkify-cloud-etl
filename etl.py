import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Extract data from AWS S3 bucket into staging tables

    Copies the data from S3 (stored as json) into staging tables.
     Each key in the json files is represented as column in the staging tables
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Transforms the source data into Star Schema and loads them into dimensional tables
    
    Transformation is carried out in Redshift using SQL queries to select and manpulate data from the staging tables. 
    Then the data is loaded into dimensional tables following Star Schema.
    The tables are: songplays (fact), users, artists, songs, time (dimensions).
    Duplicates are skiped in dimension tables except for users table, level column will be updated.
    """
    for query in insert_table_queries:
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
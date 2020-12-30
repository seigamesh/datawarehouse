import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
     
    """
    Function to extract data from S3 bucket nad copy the contents of data to the stagin tables in the Redshift cluster. 
    This excutes the queries in the variable copy_table_queries in the file sql_queries2.py.
    Parameters:
    - cur: cursor connection object
    - conn: connection object
    """
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    
    """
    Function to transform data from the stagin tables into the fact and dimension tables in the Redshift cluster. 
    This excutes the queries in the variable insert_table_queries in the file sql_queries2.py.
    Parameters:
    - cur: cursor connection object
    - conn: connection object
    """
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    """
    Function to connect database in the Redshift cluster by reading configurations in the file dwh.cfg. 
    Additioanlly, this excutes the functions load_staging_tables(cur, conn) and insert_tables(cur, conn).
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
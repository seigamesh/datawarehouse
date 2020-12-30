import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    
    """
    Function to drop tables which already exist in the Redshift cluster. This excutes the queries in the variable drop_table_queries in the file sql_queries2.py.
    Parameters:
    - cur: cursor connection object
    - conn: connection object
    """
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        
def create_tables(cur, conn):
    
    """
    Function to create tables in the Redshift cluster. This excutes the queries in the variable create_table_queries in the file sql_queries2.py.
    Parameters:
    - cur: cursor connection object
    - conn: connection object
    """
    
    for query in create_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()

def main():
    
    """
    Function to connect database in the Redshift cluster by reading configurations in the file dwh.cfg. 
    Additioanlly, this excutes the functions drop_tables(cur, conn) and create_tables(cur, conn).
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
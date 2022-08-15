
import mysql.connector
from mysql.connector import Error
import pandas as pd
from sql_queries import create_table_queries, drop_table_queries, movies_table_insert

def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            auth_plugin = 'mysql_native_password'
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

db = "movies" 
pw ="yourPassword"
connection = create_server_connection("localhost", "root", pw)


#def create_database(connection):
#    cursor = connection.cursor()
    #try:
    #    cursor.execute(query)
    #    print("Database created successfully")
    #except Error as err:
    #    print(f"Error: '{err}'")
#    return cursor, connection

def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as err:
        print(f"Error: '{err}'")

create_database_query = "CREATE DATABASE movies"
create_database(connection, create_database_query)


def create_db_connection(host_name, user_name, user_password, db_name):
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name,
            auth_plugin = 'mysql_native_password'
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")
        connection = None

    return connection

def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")

def drop_tables(connection):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        execute_query(connection, query)
        #cursor.execute(query)
        #connection.commit()

def create_tables(connection):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        execute_query(connection, query)
         

connection = create_db_connection("localhost", "root", pw, db) # Connect to the Database
drop_tables(connection)

connection = create_db_connection("localhost", "root", pw, db) # Connect to the Database
create_tables(connection)

def insert_movies(connection, data):
    execute_query(connection, movies_table_insert)

def main():
    """
    - Establishes connection database and gets cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    db = "movies" 
    pw ="yourPassword"
    connection = create_server_connection("localhost", "root", pw)

    #cursor, connection = create_database(connection)
    #create_database()
    

    connection.close()


if __name__ == "__main__":
    main()

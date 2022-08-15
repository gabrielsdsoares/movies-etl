import os
import glob
import mysql.connector 
import mysql.connector as db_connect
from mysql.connector import Error
import pandas as pd
from sql_queries import *

def process_data(cursor, connection, filepath):
    """
    This procedure processes the csvs files whose filepath has been provided as an arugment.
    It extracts the data information in order to store it into the tables: movies, ratings, and tags.
    
    INPUTS: 
    * the cursor variable
    * the connection variable
    * filepath the file path to the files
    """    
    # get all files matching extension from directory
    data_files = get_files(filepath)
    links_csv = data_files[0]
    movies_csv = data_files[1]
    ratings_csv = data_files[2]
    tags_csv = data_files[3]

    

    #ETL for ratings table
    df_ratings = pd.read_csv(ratings_csv)
    df_ratings = df_ratings.rename(columns={"userId":"user_id", "movieId":"movie_id"})
    df_ratings = df_ratings.drop("timestamp", axis = 1)

    #ETL for tags table
    df_tags = pd.read_csv(tags_csv)
    df_tags = df_tags.rename(columns={"userId":"user_id", "movieId":"movie_id"})
    df_tags = df_tags.drop("timestamp", axis = 1)

    #Insert data into tables
    process_movie_table(cursor, connection, movies_csv, links_csv)
    process_ratings_table(cursor, connection, df_ratings)
    process_tags_table(cursor, connection, df_tags)


def process_movie_table(cursor, connection, movies_csv, links_csv):
    """
    Perform data reading
    Then perform ETL for insertion into the movie table

    INPUTS: 
    * cursor variable
    * connection variable
    * The movies csv
    * The links csv
    """
    #ETL for movies table
    df_movies = pd.read_csv(movies_csv) 
    df_movies = df_movies.rename(columns={"movieId":"movie_id"})
    df_movies['year'] = df_movies['title'].str[-6:]
    df_movies['year'] = df_movies['year'].str.replace(r"[()]","")
    #df_temp = df_movies.drop_duplicates(subset=['title'])

    #ETL for links table
    df_links = pd.read_csv(links_csv)
    df_links = df_links.rename(columns={"movieId":"movie_id", 'imdbId':'imdb_id','tmdbId':'tmdb_id'})

    #Movies and Links tables merge
    df_movies = pd.merge(df_movies, df_links, on="movie_id")
    df_erro = pd.DataFrame()
    # insert movie
    for i, row in df_movies.iterrows():
        try:
            # Executing the SQL command
            cursor.execute(movies_table_insert, list(row))
            connection.commit()
        except:
            # Rolling back in case of error
            connection.rollback()
            df_erro = df_erro.append(row)
            print("Erro")
    df_erro
    

def process_ratings_table(cursor, connection, df):
    """
    Perform data reading
    Then perform ETL for insertion into the ratings table

    INPUTS: 
    * cursor variable
    * connection variable
    * The ratings csv
    """
    # insert movie
    for i, row in df.iterrows():
        try:
            # Executing the SQL command
            cursor.execute(ratings_table_insert, list(row))
            connection.commit()
        except:
            # Rolling back in case of error
            connection.rollback()
            print("Erro Ratings")


def process_tags_table(cursor, connection, df):
    """
    Perform data reading
    Then perform ETL for insertion into the tags table

    INPUTS: 
    * cursor variable
    * connection variable
    * The tags csv
    """
    # insert movie
    for i, row in df.iterrows():
        try:
            # Executing the SQL command
            cursor.execute(tags_table_insert, list(row))
            connection.commit()
        except:
            # Rolling back in case of error
            connection.rollback()
            print("Erro Tags")
    

def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.csv'))
        for f in files :
            all_files.append(os.path.abspath(f))
    
    return all_files

def main():
    """
    This is the main code.
    It creates the database connection and call the functions that will process each type of files.
    """    
    pw ="yourPassword"
    connection = mysql.connector.connect(
            host="localhost",
            user="root",
            passwd=pw,
            database='movies',
            auth_plugin = 'mysql_native_password'
        )

    cursor = connection.cursor()

    #CLEAN TABLES
    sql = "DELETE FROM movies"
    cursor.execute(sql)
    connection.commit()
    sql = "DELETE FROM ratings"
    cursor.execute(sql)
    connection.commit()
    sql = "DELETE FROM tags"
    cursor.execute(sql)
    connection.commit()

    process_data(cursor, connection, filepath= 'ml-latest-small')

    #ends connection
    connection.close()


if __name__ == "__main__":
    main()
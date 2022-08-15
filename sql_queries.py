
# DROP TABLES

movies_table_drop = "DROP TABLE IF EXISTS movies;"
ratings_table_drop = "DROP TABLE IF EXISTS ratings;"
tags_table_drop = "DROP TABLE IF EXISTS tags;"

# CREATE TABLES

movies_table_create = ("""
    CREATE TABLE IF NOT EXISTS movies (
        movie_id SERIAL PRIMARY KEY,
        title VARCHAR(120),
        genres VARCHAR(100),
        year VARCHAR(100),
        imdb_id INT,
        tmdb_id FLOAT
    );
""")


ratings_table_create = ("""
    CREATE TABLE IF NOT EXISTS ratings (
        rating_id SERIAL PRIMARY KEY,
        user_id INT NOT NULL,
        movie_id INT,
        rating FLOAT
    );
""")

tags_table_create = ("""
    CREATE TABLE IF NOT EXISTS tags (
        tag_id SERIAL PRIMARY KEY,
        user_id INT NOT NULL,
        movie_id INT NOT NULL,
        tag VARCHAR(50)
    );
""")

#user_id	movie_id	tag


# INSERT RECORDS

movies_table_insert = ("""
    INSERT INTO movies (movie_id, title, genres, year, imdb_id, tmdb_id) 
    VALUES (%s, %s, %s, %s, %s, %s)
""")

ratings_table_insert = ("""
    INSERT INTO ratings (rating_id, user_id, movie_id, rating)
    VALUES (DEFAULT, %s, %s, %s)
""")

tags_table_insert = ("""
    INSERT INTO tags (tag_id, user_id, movie_id, tag)
    VALUES (DEFAULT, %s, %s, %s)
""")

temp_table_insert = ("""
    INSERT INTO movie (movie_id)
    VALUES (%s)
""")

# FIND MOVIE

#song_select = ("""
#    SELECT m.movie_id
#      
#""")

# QUERY LISTS

create_table_queries = [movies_table_create, ratings_table_create, tags_table_create]
drop_table_queries = [movies_table_drop, ratings_table_drop, tags_table_drop]
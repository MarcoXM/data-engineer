# DROP TABLES
# The following SQL queries drop all the tables from sparkifydb.

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
# The following SQL queries create tables to sparkifydb.
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay_table (  songplay_id SERIAL PRIMARY KEY NOT NULL, 
                                            start_time TIMESTAMP,
                                            user_id INTEGER,
                                            level VARCHAR(10),
                                            song_id VARCHAR(20),
                                            artist_id VARCHAR(20),
                                            session_id INTEGER,
                                            location VARCHAR(50),
                                            user_agent VARCHAR(150));
""")


user_table_create = ("""
    CREATE TABLE IF NOT EXISTS user_table ( user_id int PRIMARY KEY UNIQUE NOT NULL, 
                                        first_name VARCHAR(50),
                                        last_name VARCHAR(50),
                                        gender CHAR(1),
                                        level VARCHAR(10));
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (  song_id varchar PRIMARY KEY UNIQUE NOT NULL, 
                                       title VARCHAR(100),
                                       artist_id VARCHAR(20) NOT NULL,
                                       year INTEGER,
                                       duration FLOAT(5));
""")


artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (    artist_id varchar PRIMARY KEY UNIQUE NOT NULL,
                                            name VARCHAR(100),
                                            location VARCHAR(100),
                                            lattitude FLOAT(5),
                                            longitude FLOAT(5));
""")


time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (  start_time timestamp NOT NULL, \
                                        hour INTEGER,
                                        day INTEGER,
                                        week INTEGER,
                                        month INTEGER,
                                        year INTEGER,
                                        weekday INTEGER);
""")
# INSERT RECORDS

songplay_table_insert = ("""
    INSERT INTO songplay_table (
                                songplay_id,
                                start_time, 
                                user_id, level, 
                                song_id, artist_id, 
                                session_id, 
                                location, 
                                user_agent)
    VALUES ( %s, %s, %s, %s, %s, %s, %s, %s,%s)
    ON CONFLICT(songplay_id) DO NOTHING;
""") 


user_table_insert = ("""
    INSERT INTO user_table (
                            user_id, 
                            first_name, 
                            last_name, 
                            gender, 
                            level)
    VALUES (%s, %s, %s, %s, %s) 
    ON CONFLICT(user_id) DO UPDATE 
    SET level = EXCLUDED.level;
""")

song_table_insert = ("""
    INSERT INTO song_table (
                            song_id, 
                            title, 
                            artist_id, 
                            year, 
                            duration)
    VALUES (%s, %s, %s, %s, %s) 
    ON CONFLICT(song_id) DO NOTHING;
""")

artist_table_insert = ("""
    INSERT INTO artist_table (
                                artist_id, 
                                name, 
                                location, 
                                lattitude, 
                                longitude)
    VALUES (%s, %s, %s, %s, %s) 
    ON CONFLICT(artist_id) DO NOTHING;
""")


time_table_insert = ("""
    INSERT INTO time_table (
                            start_time, 
                            hour, 
                            day, 
                            week, 
                            month, 
                            year, 
                            weekday)
    VALUES (%s,%s,%s,%s,%s,%s,%s) 
    ON CONFLICT(start_time) DO NOTHING;

""")

# FIND SONGS

song_select = ("""
    SELECT st.song_id, st.artist_id 
    FROM song_table st
    JOIN artist_table at 
        ON st.artist_id = at.artist_id
    WHERE st.title = %s
        AND at.name = %s
        AND st.duration = %s;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
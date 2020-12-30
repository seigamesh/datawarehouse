import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events(
    event_id INT IDENTITY(0,1) NOT NULL,
    artist_name VARCHAR(300),
    auth VARCHAR(50),
    user_first_name VARCHAR(300),
    user_gender  VARCHAR(1),
    item_in_session INTEGER,
    user_last_name VARCHAR(300),
    song_length DOUBLE PRECISION, 
    user_level VARCHAR(50),
    location VARCHAR(300),
    method VARCHAR(30),
    page VARCHAR(50),
    registration VARCHAR(50),
    session_id BIGINT,
    song_title VARCHAR(300),
    status INTEGER, 
    ts VARCHAR(50) NOT NULL,
    user_agent TEXT,
    user_id VARCHAR(100),
    PRIMARY KEY (event_id))
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs(
    song_id VARCHAR(100) NOT NULL,
    num_songs INTEGER,
    artist_id VARCHAR(100) NOT NULL,
    artist_latitude DOUBLE PRECISION,
    artist_longitude DOUBLE PRECISION,
    artist_location VARCHAR(300),
    artist_name VARCHAR(300),
    title VARCHAR(300),
    duration DOUBLE PRECISION,
    year INTEGER,
    PRIMARY KEY (song_id))
""")

songplay_table_create = ("""
CREATE TABLE songplays(
    songplay_id INT IDENTITY(0,1),
    start_time TIMESTAMP NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    level VARCHAR(50),
    song_id VARCHAR(100) NOT NULL,
    artist_id VARCHAR(100) NOT NULL,
    session_id BIGINT NOT NULL,
    location VARCHAR(300),
    user_agent TEXT,
    PRIMARY KEY (songplay_id))
""")

user_table_create = ("""
CREATE TABLE users(
    user_id VARCHAR NOT NULL,
    first_name VARCHAR(300),
    last_name VARCHAR(300),
    gender VARCHAR(1),
    level VARCHAR(50),
    PRIMARY KEY (user_id))
""")

song_table_create = ("""
CREATE TABLE songs(
    song_id VARCHAR(100) NOT NULL,
    title VARCHAR(300),
    artist_id VARCHAR(100) NOT NULL,
    year INTEGER,
    duration DOUBLE PRECISION,
    PRIMARY KEY (song_id))
""")

artist_table_create = ("""
CREATE TABLE artists(
    artist_id VARCHAR(100) NOT NULL,
    name VARCHAR(300),
    location VARCHAR(300),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    PRIMARY KEY (artist_id))
""")

time_table_create = ("""
CREATE TABLE time(
    start_time TIMESTAMP NOT NULL,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER,
    PRIMARY KEY (start_time))
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events 
                          from {}
                          iam_role {}
                          json {};
                       """).format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""copy staging_songs 
                          from {} 
                          iam_role {}
                          json 'auto';
                      """).format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
SELECT  
    DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second' as start_time, 
    se.user_id, 
    se.user_level, 
    ss.song_id,
    ss.artist_id, 
    se.session_id,
    se.location, 
    se.user_agent
FROM staging_events se, staging_songs ss
WHERE se.page = 'NextSong' 
AND se.song_title = ss.title 
AND se.artist_name = ss.artist_name 
AND se.song_length = ss.duration
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT  
    user_id, 
    user_first_name, 
    user_last_name, 
    user_gender, 
    user_level
FROM staging_events
WHERE page = 'NextSong' 
and user_id NOT IN (SELECT DISTINCT user_id FROM users)
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) 
SELECT DISTINCT 
    song_id, 
    title,
    artist_id,
    year,
    duration
FROM staging_songs
WHERE song_id IS NOT NULL
and song_id NOT IN (SELECT DISTINCT song_id FROM songs)
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) 
SELECT DISTINCT 
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL
and artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekDay)
SELECT DISTINCT start_time, 
    extract(hour from start_time),
    extract(day from start_time),
    extract(week from start_time), 
    extract(month from start_time),
    extract(year from start_time), 
    extract(dayofweek from start_time)
FROM songplays
WHERE start_time IS NOT NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
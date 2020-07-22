import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

IAM_ROLE = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')
REGION = config.get('S3', 'REGION')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (
                                artist varchar,
                                auth varchar,
                                firstName varchar,
                                gender char(1),
                                itemInSession int,
                                lastName varchar,
                                length real,
                                level char(4),
                                location varchar,
                                method varchar,
                                page varchar,
                                registration real,
                                sessionId int,
                                song varchar,
                                status int,
                                ts bigint,
                                userAgent varchar,
                                userId int
                                )""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
                                num_songs int,
                                artist_id varchar,
                                artist_latitude real,
                                artist_longitude real,
                                artist_location varchar,
                                artist_name varchar,
                                song_id varchar,
                                title varchar,
                                duration real,
                                year int
                                )""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                            songplay_id bigint IDENTITY(1,1) PRIMARY KEY,
                            start_time bigint REFERENCES time(start_time),
                            user_id int REFERENCES users(user_id),
                            level char(4),
                            song_id varchar REFERENCES songs(song_id),
                            artist_id varchar REFERENCES artists(artist_id),
                            session_id int,
                            location varchar,
                            user_agent varchar,
                            UNIQUE(start_time, user_id, session_id)
                            )""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                        user_id int PRIMARY KEY,
                        first_name varchar,
                        last_name varchar,
                        gender char(1),
                        level char(4)
                        )""")

song_table_create = ("""create table if not exists songs (
                        song_id varchar PRIMARY KEY,
                        title varchar,
                        artist_id varchar,
                        year int,
                        duration real
                        )""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                        artist_id varchar PRIMARY KEY,
                        name varchar,
                        location varchar,
                        latitude real,
                        longitude real
                        )""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                        start_time bigint PRIMARY KEY,
                        hour int, 
                        day int, 
                        week int, 
                        month varchar,
                        year int,
                        weekday varchar
                        )""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events --redshit code
                            from '{}'
                            iam_role '{}'
                            json {}
                            REGION '{}';""").format(LOG_DATA, IAM_ROLE, LOG_JSONPATH, REGION)

staging_songs_copy = ("""copy staging_songs from '{}'
                            iam_role '{}'
                            format as json 'auto'
                            REGION '{}';""").format(SONG_DATA, IAM_ROLE, REGION)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays 
                            (start_time, user_id, level, song_id, artist_id,
                            session_id, location, user_agent)
                            SELECT se.ts, se.userId, se.level, ss.song_id, ss.artist_id,
                            se.sessionId, se.location, se.userAgent
                            FROM staging_events se
                            JOIN staging_songs ss
                            ON se.song = ss.title AND se.artist = ss.artist_name
                            WHERE se.page='NextSong'
                            """)

user_table_insert = ("""CREATE TEMP TABLE temp_users AS SELECT * FROM staging_events;

                        BEGIN TRANSACTION;
                        
                        UPDATE users
                        SET level = tu.level
                        FROM temp_users tu
                        WHERE users.user_id = tu.userId
                        AND tu.page='NextSong'
                        AND users.level != tu.level;

                        DELETE FROM temp_users
                        USING users
                        WHERE users.user_id = temp_users.userId
                        AND temp_users.page='NextSong';

                        INSERT INTO users
                        SELECT DISTINCT tu.userId, tu.firstName, tu.lastName, tu.gender, tu.level
                        FROM temp_users tu
                        WHERE tu.page='NextSong'
                        AND tu.userId NOT IN (SELECT DISTINCT user_id FROM users);

                        END TRANSACTION;

                        DROP TABLE temp_users;
                        """)                        

song_table_insert = ("""INSERT INTO songs (song_id, artist_id, year, duration, title)
                        SELECT DISTINCT ss.song_id, ss.artist_id, ss.year, ss.duration, ss.title
                        FROM staging_songs ss
                        WHERE ss.song_id NOT IN (SELECT DISTINCT song_id FROM songs)
                        """)

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
                        SELECT DISTINCT ss.artist_id, ss.artist_name, ss.artist_location, ss.artist_latitude, ss.artist_longitude
                        FROM staging_songs ss
                        WHERE ss.artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)                     
                        """)
                        
time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                        SELECT DISTINCT se.ts, EXTRACT(hour from timestamp 'epoch' + se.ts * interval '1 second'),
                        EXTRACT(day from timestamp 'epoch' + se.ts / 1000 * interval '1 second'),
                        EXTRACT(week from timestamp 'epoch' + se.ts / 1000 * interval '1 second'), 
                        EXTRACT(month from timestamp 'epoch' + se.ts / 1000 * interval '1 second'),
                        EXTRACT(year from timestamp 'epoch' + se.ts / 1000 * interval '1 second'), 
                        EXTRACT(dow from timestamp 'epoch' + se.ts / 1000 * interval '1 second')
                        FROM staging_events se
                        WHERE se.page='NextSong'
                        AND se.ts NOT IN (SELECT DISTINCT start_time FROM time)
                        """)

# QUERY LISTS

create_table_queries = [time_table_create, user_table_create, artist_table_create, song_table_create, staging_events_table_create, staging_songs_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
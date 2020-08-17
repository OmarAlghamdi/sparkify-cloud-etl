# Sparkify Cloud ETL

This is the third project in Udacity's Data Engineering Nanodegree.

In this project I played the role of the data engineer in Sparkify the music company to extract data from song files as well as from log file by users.
Song files are subset of [Million Song Dataset](http://millionsongdataset.com/) & log files are simulated by [eventsim](https://github.com/Interana/eventsim)

## Source Data.
Source data are JSON files divided into two groups, Song data and Log data. The data is hosted in AWS S3 public bucket. You can access them via `s3://udacity-dend/log_data` and `s3://udacity-dend/song_data`

Below is the is sample of each type of data

```json
{"num_songs": 1, "artist_id": "ARD7TVE1187B99BFB1", "artist_latitude": null, "artist_longitude": null, "artist_location": "California - LA", "artist_name": "Casual", "song_id": "SOMZWCG12A8C13C480", "title": "I Didn't Mean To", "duration": 218.93179, "year": 0}
```

```json
{"artist":"N.E.R.D. FEATURING MALICE","auth":"Logged In","firstName":"Jayden","gender":"M","itemInSession":0,"lastName":"Fox","length":288.9922,"level":"free","location":"New Orleans-Metairie, LA","method":"PUT","page":"NextSong","registration":1541033612796.0,"sessionId":184,"song":"Am I High (Feat. Malice)","status":200,"ts":1541121934796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.3; WOW64) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"101"}
{"artist":null,"auth":"Logged In","firstName":"Stefany","gender":"F","itemInSession":0,"lastName":"White","length":null,"level":"free","location":"Lubbock, TX","method":"GET","page":"Home","registration":1540708070796.0,"sessionId":82,"song":null,"status":200,"ts":1541122176796,"userAgent":"\"Mozilla\/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"83"}
```

## ETL Pipeline
Extraction: Data is copied from S3 into a stang tables in Redshift. The staging tables are `staging_events` and `staging_songs`.

Transformation: Data is transformed in to a [Start Schema](https://en.wikipedia.org/wiki/Star_schema) using SQL queries. The schema consists of one fact table `songplays` and 4 dimension tables `users`, `songs`, `artists` & `time`. 

- A row in the fact table `songplay` is based on user action `NextSong` from the log data.
- A row in `users` table is based on 'userId' from the log data. `userId` is the primary key.
- A row in `time` table is based on timestaps `ts` in the log data. With the timestamp in milliseconds being the primary.
- A row in `songs` table is based on song data. Each song has a unique ID. 
- A row in the `artists` table is based also on the song data. Each artist has a unique ID used as primary key.

## Dependencies
 - Python 3
 - configparser
 - psycopg2

## How To use it

- Create a Redshift cluster.
- Update the configuration under `CLUSTER` in `dwh.cfg`.
- Install the dependencies.
- Run `create_tables.py` to create the staging tables and the Star Schema in you Redshift cluster.
- Run `etl.py` to copy the data from S3 into the staging tables, transform the data and load it into the dimensional tables.


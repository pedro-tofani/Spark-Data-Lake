# Project: Data Lake

## Summary of the project

This project was developed during Udacity's Data Engineer Nanodegree program and aimed building a data lake after extracting and transform data listed from json files.
The raw data is stored inside s3 buckets. The script is charged to load it, process and transform it in dimentional and fact tables in parquet format.
* Input detailed:
  * Song data
  ```json
  {
    "num_songs": 1,
    "artist_id": "ARKULSX1187FB45F84",
    "artist_latitude": 39.49974,
    "artist_longitude": -111.54732,
    "artist_location": "Utah",
    "artist_name": "Trafik",
    "song_id": "SOQVMXR12A81C21483",
    "title": "Salt In NYC",
    "duration": 424.12363,
    "year": 0
  }
  ```
  * Log data
  ```json
  {
    "artist":"Stephen Lynch",
    "auth":"Logged In",
    "firstName":"Jayden",
    "gender":"M",
    "itemInSession":0,
    "lastName":"Bell",
    "length":182.85669,
    "level":"free",
    "location":"Dallas-Fort Worth-Arlington, TX","method":"PUT",
    "page":"NextSong",
    "registration":1540991795796.0,
    "sessionId":829,
    "song":"Jim Henson's Dead",
    "status":200,
    "ts":1543537327796,
    "userAgent":"Mozilla\/5.0 (compatible; MSIE 10.0; Windows NT 6.2; WOW64; Trident\/6.0)",
    "userId":"91"
  }
  ```
* Output detailed:
  * DIM users_table - users in the app => user_id, first_name, last_name, gender, level
  * DIM songs_table - songs in music database => song_id, title, artist_id, year, duration
  * DIM artists_table - artists in music database => artist_id, name, location, lattitude, longitude
  * DIM time_table - timestamps of records in songplays broken down into specific units => start_time, hour, day, week, month, year, weekday
  * FACT songplays - records in log data associated with song plays i.e. records with page NextSong => songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

## How to run the Python scripts

First you will need to fill the dl.cfg file with your credentials information.
```
[AWS]
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
```
After this, acces your cluster via ssh and execute the etl.py script with:
```
/usr/bin/spark-submit --master yarn ./etl.py
```

## Files
### dl.cfg
File that contains personal information about the AWS account.

### etl.py
File that will execute the data extraction, transformation and load. After running this script the tables will be created at S3 storage.
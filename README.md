# Spark and Datalakes

## Objectives
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. In this project, the main objective is building an ETL pipeline that extract the data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. 

## Data
The data used in this project is available in AWS S3. You can access song and logs datasets in the following buckets or reading them locally in ```data/``` directory.

* s3://udacity-dend/song_data
* s3://udacity-dend/log_data

### song_data
JSON files containing songs metadata. Ex.:

```json
{
    "num_songs": 1,
    "artist_id": "ARD7TVE1187B99BFB1",
    "artist_latitude": null,
    "artist_longitude": null,
    "artist_location": "California - LA",
    "artist_name": "Casual",
    "song_id": "SOMZWCG12A8C13C480",
    "title": "I Didn't Mean To",
    "duration": 218.93179,
    "year": 0
}
```

### log_data
JSON files representing users activities/events in the application. Ex.:

```json
{
    "artist": "Des'ree",
    "auth": "Logged In",
    "firstName": "Kaylee",
    "gender": "F",
    "itemInSession": 1,
    "lastName": "Summers",
    "length": 246.30812,
    "level": "free",
    "location": "Phoenix-Mesa-Scottsdale, AZ",
    "method": "PUT",
    "page": "NextSong",
    "registration": 1540344794796,
    "sessionId": 139,
    "song": "You Gotta Be",
    "status": 200,
    "ts": 1541106106796,
    "userAgent": "\"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36\"",
    "userId": "8"
}
```


## Running ETL pipeline
First of all, you must have [Python](https://www.python.org/downloads/). The others Python packages can be installed running the command below:

``` pip install -r requirements.txt ```

The second step is configuring the ```dl.cfg```, setting your own AWS credentials (mandatory to read and write into S3).

Now, you can run the ELT process executing ```etl.py```




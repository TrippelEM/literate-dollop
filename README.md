# TDT4225 Assignment 3 – Movies to MongoDB
## Container setup
### Create the container

```bash
docker pull mongo:latest
docker volume create mongo_data

docker run -d \
  --name mongodb \
  -p 27017:27017 \
  -v mongo_data:/data/db \
  mongo
```

### Create users in the container

```bash
docker exec -it mongodb mongosh
```

Then inside the Mongo shell:

```js
use admin;

db.createUser({
  user: 'driver',
  pwd: 'root',
  roles: ['root']
});

use movie_db;

db.createUser({
  user: 'slaver',
  pwd: 'root',
  roles: ['readWrite']
});
```



## Structure

```
.
├─ DbConnector.py      # Mongo client, bulk insert, index helpers
├─ DbUploader.py       # Wrapper for drop, insert, indexes, stats
├─ eda.ipynb           # EDA, build docs, load to MongoDB
├─ requirements.txt    # pip install -r requirements.txt
└─ movies/             # CSVs: movies_metadata.csv, credits.csv, keywords.csv, links.csv, ratings.csv
```


## Data

Place CSVs in the `movies/` directory:

- `movies_metadata.csv`
- `credits.csv`
- `keywords.csv`
- `links.csv`  (use `links_small.csv` for dev if needed)
- `ratings.csv`  (use `ratings_small.csv` for dev if needed)

`links.csv` maps MovieLens `movieId` to TMDB `tmdbId` and IMDb `imdbId`.  
IMDb IDs are stored as zero-padded strings (e.g., `"0114709"`).



## Schemas

### `movies`

```json
{
  "_id": <tmdb_id:int>,
  "title": "string",
  "original_title": "string",
  "release_date": ISODate | null,
  "runtime": int,
  "budget": int,
  "revenue": int,
  "vote_average": double,
  "vote_count": int,
  "genres": ["string"],
  "production_companies": ["string"],
  "production_countries": ["string"],
  "spoken_languages": ["string"],
  "collection": {"name": "string", "id": int} | null,
  "keywords": ["string"],
  "crew": [{"credit_id":"string","name":"string","job":"string","person_id":int}],
  "cast": [{"credit_id":"string","name":"string","character":"string","person_id":int,"order":int}],
  "movieId": <movielens_id:int|null>,
  "imdbId": "0114709" | null
}
```

### `people`

```json
{
  "_id": <person_id:int>,
  "name": "string",
  "gender": int | null,
  "profile_path": "string",
  "cast": [{"credit_id":"string","character":"string","billing_order":int,"tmdb_id":int}],
  "crew": [{"credit_id":"string","department":"string","job":"string","tmdb_id":int}]
}
```

### `ratings`

```json
{
  "userId": int,
  "movieId": int,
  "rating": double,
  "timestamp": ISODate
}
```

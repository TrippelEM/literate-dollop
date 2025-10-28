import os
from pprint import pprint
from DbConnector import DbConnector
from DbConnector_mini import DbConnectorMini
from pymongo import ASCENDING

class Q:
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
    def close(self):
        self.connection.close_connection()

class QMini:
    def __init__(self):
        self.connection = DbConnectorMini()
        self.client = self.connection.client
        self.db = self.connection.db
    def close(self):
        self.connection.close_connection()

class TaskProgram(Q):
    def task1(self):
        pipeline = [
            {"$unwind": "$crew"},
            {"$match": {"crew.job": "Director", "revenue": {"$type": "number"}}},
            {"$group": {
                "_id": {"pid": "$crew.person_id", "name": "$crew.name"},
                "count": {"$sum": 1},
                "votes": {"$push": "$vote_average"},
                "revenues": {"$push": "$revenue"}
            }},
            {"$addFields": {
                "avg_vote": {"$avg": "$votes"},
                "median_revenue": {
                    "$let": {
                        "vars": {
                            "s": {"$sortArray": {"input": "$revenues", "sortBy": 1}},
                            "n": {"$size": "$revenues"}
                        },
                        "in": {
                            "$cond": [
                                {"$eq": [{"$mod": ["$$n", 2]}, 1]},
                                {"$arrayElemAt": ["$$s", {"$floor": {"$divide": ["$$n", 2]}}]},
                                {"$avg": [
                                    {"$arrayElemAt": ["$$s", {"$subtract": [{"$divide": ["$$n", 2]}, 1]}]},
                                    {"$arrayElemAt": ["$$s", {"$divide": ["$$n", 2]}]}
                                ]}
                            ]
                        }
                    }
                }
            }},
            {"$project": {
                "_id": 0,
                "person_id": "$_id.pid",
                "director": "$_id.name",
                "movie_count": "$count",
                "median_revenue": 1,
                "mean_vote_average": {"$round": ["$avg_vote", 3]}
            }},
            {"$match": {"movie_count": {"$gte": 5}}},
            {"$sort": {"median_revenue": -1}},
            {"$limit": 10}
        ]
        self.print_task(list(self.db["movies"].aggregate(pipeline)),1)

    def task2(self):
        pipeline = [
            {"$match": {
                "cast": {"$exists": True, "$ne": []},
                "vote_average": {"$type": "number"}
            }},
            {"$project": {
                "vote_average": 1,
                "castA": {"$map": {"input": "$cast", "as": "c", "in": {"id": "$$c.person_id", "name": "$$c.name"}}},
                "castB": {"$map": {"input": "$cast", "as": "c", "in": {"id": "$$c.person_id", "name": "$$c.name"}}}
            }},
            {"$unwind": "$castA"},
            {"$unwind": "$castB"},
            {"$match": {"$expr": {"$lt": ["$castA.id", "$castB.id"]}}},
            {"$group": {
                "_id": {"a": "$castA.id", "b": "$castB.id"},
                "actor1": {"$first": "$castA.name"},
                "actor2": {"$first": "$castB.name"},
                "co_appearances": {"$sum": 1},
                "avg_vote_average": {"$avg": "$vote_average"}
            }},
            {"$match": {"co_appearances": {"$gte": 3}}},
            {"$sort": {"co_appearances": -1, "avg_vote_average": -1}},
            {"$project": {
                "_id": 0,
                "actor1": 1,
                "actor2": 1,
                "co_appearances": 1,
                "avg_vote_average": {"$round": ["$avg_vote_average", 2]}
            }}
        ]
        self.print_task(list(self.db.movies.aggregate(pipeline)),2)

    def task3(self):
        pipeline = [
            {"$project": {"cast": 1, "genres": 1}},
            {"$match": {"cast": {"$type": "array"}, "genres": {"$type": "array"}}},
            {"$unwind": "$cast"},
            {"$match": {"cast.person_id": {"$type": "number"}}},
            {"$unwind": "$genres"},
            {
                "$group": {
                    "_id": {"pid": "$cast.person_id", "name": "$cast.name"},
                    "movies": {"$addToSet": "$_id"},
                    "genres": {"$addToSet": "$genres"}
                }
            },
            {"$addFields": {
                "movie_count": {"$size": "$movies"},
                "genre_breadth": {"$size": "$genres"}
            }},
            {"$match": {"movie_count": {"$gte": 10}}},
            {"$project": {
                "_id": 0,
                "actor_id": "$_id.pid",
                "actor": "$_id.name",
                "movie_count": 1,
                "genre_breadth": 1,
                "example_genres": {"$slice": [{"$sortArray": {"input": "$genres", "sortBy": 1}}, 5]}
            }},
            {"$sort": {"genre_breadth": -1, "movie_count": -1, "actor": 1}},
            {"$limit": 10}
        ]
        self.print_task(list(self.db["movies"].aggregate(pipeline)),3)

    def task4(self):
        pipeline = [
            {"$match": {"collection": {"$ne": None}}},
            {"$group": {
                "_id": {"id": "$collection.id", "name": "$collection.name"},
                "movie_count": {"$sum": 1},
                "total_revenue": {"$sum": "$revenue"},
                "median_vote_average": {
                    "$median": {"input": "$vote_average", "method": "approximate"}
                },
                "earliest_release_date": {"$min": "$release_date"},
                "latest_release_date": {"$max": "$release_date"}
            }},
            {"$match": {"movie_count": {"$gte": 3}}},
            {"$project": {
                "_id": 0,
                "collection_id": "$_id.id",
                "collection_name": "$_id.name",
                "movie_count": 1,
                "total_revenue": 1,
                "median_vote_average": {"$round": ["$median_vote_average", 2]},
                "earliest_release_date": 1,
                "latest_release_date": 1
            }},
            {"$sort": {"total_revenue": -1}},
            {"$limit": 10}
        ]
        self.print_task(list(self.db.movies.aggregate(pipeline)),4)

    def task5(self):
        pipeline = [
            {"$match": {
                "genres": {"$exists": True, "$ne": []},
                "release_date": {"$type": "date"}
            }},
            {"$project": {
                "runtime": 1,
                "primary_genre": {"$first": "$genres"},
                "decade": {
                    "$multiply": [
                        {"$floor": {"$divide": [{"$year": "$release_date"}, 10]}},
                        10
                    ]
                }
            }},
            {"$group": {
                "_id": {"decade": "$decade", "primary_genre": "$primary_genre"},
                "movie_count": {"$sum": 1},
                "median_runtime": {"$median": {"input": "$runtime", "method": "approximate"}}
            }},
            {"$project": {
                "_id": 0,
                "decade": "$_id.decade",
                "decade_label": {"$concat": [{"$toString": "$_id.decade"}, "s"]},
                "primary_genre": "$_id.primary_genre",
                "movie_count": 1,
                "median_runtime": {"$round": ["$median_runtime", 1]}
            }},
            # sort by decade asc, then median runtime desc
            {"$sort": {"decade": 1, "median_runtime": -1}}
        ]
        self.print_task(list(self.db.movies.aggregate(pipeline)), 5)
    
    def task6(self):
        pipeline = [
            {"$project": {"cast": 1, "release_date": 1}},
            {"$match": {"release_date": {"$type": "date"}, "cast": {"$type": "array"}}},
            {"$unwind": "$cast"},

            # add gender from people
            {"$lookup": {
                "from": "people",
                "localField": "cast.person_id",
                "foreignField": "_id",
                "as": "p"
            }},
            {"$unwind": "$p"},

            # top-5 billed with known gender (TMDB: 1=female, 2=male)
            {"$match": {"cast.order": {"$lte": 4}, "p.gender": {"$in": [1, 2]}}},

            {"$group": {
                "_id": {
                    "movie": "$_id",
                    "decade": {"$multiply": [{"$floor": {"$divide": [{"$year": "$release_date"}, 10]}}, 10]}
                },
                "total_top5_known_cast": {"$sum": 1},
                "female_top5": {"$sum": {"$cond": [{"$eq": ["$p.gender", 1]}, 1, 0]}}
            }},
            {"$addFields": {
                "female_ratio": {
                    "$cond": [
                        {"$eq": ["$total_top5_known_cast", 0]},
                        0,
                        {"$divide": ["$female_top5", "$total_top5_known_cast"]}
                    ]
                }
            }},
            {"$group": {
                "_id": "$_id.decade",
                "avg_female_ratio": {"$avg": "$female_ratio"},
                "movie_count": {"$sum": 1}
            }},
            {"$project": {
                "_id": 0,
                "decade": {"$concat": [{"$toString": "$_id"}, "s"]},
                "avg_female_ratio": {"$round": ["$avg_female_ratio", 4]},
                "movie_count": 1
            }},
            {"$sort": {"avg_female_ratio": -1, "decade": 1}}
        ]
        self.print_task(list(self.db["movies"].aggregate(pipeline, allowDiskUse=True)),6)

    def task7(self):
        # Make sure the text index exists (idempotent)
        self.db.movies.create_index([("overview", "text"), ("tagline", "text")])

        base_match = {
            "vote_count": {"$gte": 50},
            "vote_average": {"$type": "number"},
            "release_date": {"$type": "date"},
        }
        # we have to do it in two separate queries because
        # text search with exact strings requires all documents to have the exact term meaning
        # that documents that only contain noir won't be included.
        # using regex is not allowed with text search so, I just chose to use 2 text searches
        # instead of using regex.
        # https://www.mongodb.com/docs/manual/reference/operator/query/text/#exact-strings
        #
        # Query 1: exact phrase "neo-noir"
        pipeline_neo = [
            {"$match": {**base_match, "$text": {"$search": "\"neo-noir\""}}},
            {"$project": {
                "_id": 1,
                "title": 1,
                "year": {"$year": "$release_date"},
                "vote_average": 1,
                "vote_count": 1,
            }},
        ]

        # Query 2: plain noir
        pipeline_noir = [
            {"$match": {**base_match, "$text": {"$search": "noir"}}},
            {"$project": {
                "_id": 1,
                "title": 1,
                "year": {"$year": "$release_date"},
                "vote_average": 1,
                "vote_count": 1,
            }},
        ]

        # Execute both
        neo_rows = list(self.db.movies.aggregate(pipeline_neo))
        noir_rows = list(self.db.movies.aggregate(pipeline_noir))

        # Union by _id
        by_id = {}
        for row in neo_rows + noir_rows:
            by_id[row["_id"]] = row
        combined = list(by_id.values())

        # Sort & take top 20
        combined.sort(key=lambda d: (-d["vote_average"], -d["vote_count"], d["title"]))
        combined = combined[:20]

        # Drop _id for printing
        for movie in combined:
            movie.pop("_id", None)

        # Print + save using your print_task
        self.print_task(combined, 7)

    def task8(self):
        pipeline = [
            {"$match": {
                "vote_count": {"$gte": 100},
                "vote_average": {"$type": "number"},
                "cast": {"$type": "array"},
                "crew": {"$type": "array"}
            }},
            {"$project": {
                "vote_average": 1,
                "revenue": 1,
                "cast": {"$ifNull": ["$cast", []]},
                "directors": {
                    "$filter": {
                        "input": {"$ifNull": ["$crew", []]},
                        "as": "c",
                        "cond": {"$eq": ["$$c.job", "Director"]}
                    }
                }
            }},
            {"$unwind": "$directors"},
            {"$unwind": "$cast"},
            {"$match": {
                "directors.person_id": {"$type": "number"},
                "cast.person_id": {"$type": "number"},
                "$expr": {"$ne": ["$directors.person_id", "$cast.person_id"]}
            }},
            {"$group": {
                "_id": {
                    "movie": "$_id",
                    "dir_id": "$directors.person_id",
                    "actor_id": "$cast.person_id",
                    "dir_name": "$directors.name",
                    "actor_name": "$cast.name"
                },
                "vote_avg_movie": {"$first": "$vote_average"},
                "rev_movie": {"$first": "$revenue"}
            }},
            {"$group": {
                "_id": {"dir_id": "$_id.dir_id", "actor_id": "$_id.actor_id"},
                "director": {"$first": "$_id.dir_name"},
                "actor": {"$first": "$_id.actor_name"},
                "films_count": {"$sum": 1},
                "mean_vote_average": {"$avg": "$vote_avg_movie"},
                "mean_revenue": {"$avg": "$rev_movie"}
            }},
            {"$match": {"films_count": {"$gte": 3}}},
            {"$project": {
                "_id": 0,
                "director_id": "$_id.dir_id",
                "director": 1,
                "actor_id": "$_id.actor_id",
                "actor": 1,
                "films_count": 1,
                "mean_vote_average": {"$round": ["$mean_vote_average", 3]},
                "mean_revenue": {"$round": ["$mean_revenue", 0]}
            }},
            {"$sort": {"mean_vote_average": -1, "films_count": -1, "director": 1, "actor": 1}},
            {"$limit": 20}
        ]
        self.print_task(list(self.db["movies"].aggregate(pipeline, allowDiskUse=True)),8)
    
    def task9(self):
        pipeline = [
            {"$match": {
                "original_language": {"$ne": "en"},
                "$or": [
                    {"production_countries": {"$elemMatch": {"$in": ["US", "USA", "United States"]}}},
                    {"production_companies": {"$elemMatch": {"$regex": r"^United States$", "$options": "i"}}}
                ]
            }},
            {"$project": {
                "original_language": 1,
                "title": 1
            }},
            {"$group": {
                "_id": "$original_language",
                "count": {"$sum": 1},
                "example_title": {"$first": "$title"}
            }},
            {"$project": {
                "_id": 0,
                "language": "$_id",
                "count": 1,
                "example_title": 1
            }},
            {"$sort": {"count": -1, "language": 1}},
            {"$limit": 10}
        ]
        self.print_task(list(self.db["movies"].aggregate(pipeline, allowDiskUse=True)),9)

    def task10(self):
        pipeline = [
            # Per-user stats and distinct movie ids
            {"$group": {
                "_id": "$userId",
                "ratings_count": {"$sum": 1},
                "std_pop": {"$stdDevPop": "$rating"},
                "mids": {"$addToSet": "$movieId"}
            }},
            {"$match": {"ratings_count": {"$gte": 20}}},

            # Fetch only those users' movies once, project genres only
            {"$lookup": {
                "from": "movies",
                "let": {"mids": "$mids"},
                "pipeline": [
                    {"$match": {"$expr": {"$in": ["$movieId", "$$mids"]}}},
                    {"$project": {"_id": 0, "genres": 1}}
                ],
                "as": "m"
            }},

            # Union all genres across fetched movies
            {"$set": {
                "genres_all": {
                    "$reduce": {
                        "input": {"$map": {"input": "$m", "as": "mm", "in": "$$mm.genres"}},
                        "initialValue": [],
                        "in": {"$setUnion": ["$$value", "$$this"]}
                    }
                }
            }},

            # Final metrics
            {"$set": {
                "variance_pop": {"$pow": ["$std_pop", 2]},
                "genre_count": {"$size": "$genres_all"}
            }},

            # Final shape + two leaderboards
            {"$project": {
                "_id": 0,
                "userId": "$_id",
                "ratings_count": 1,
                "genre_count": 1,
                "variance_pop": 1,
            }},
            {"$facet": {
                "most_genre_diverse": [
                    {"$sort": {"genre_count": -1, "ratings_count": -1, "userId": 1}},
                    {"$limit": 10}
                ],
                "highest_variance": [
                    {"$sort": {"variance_pop": -1, "ratings_count": -1, "userId": 1}},
                    {"$limit": 10}
                ]
            }}
        ]
        self.print_task10(list(self.db["ratings"].aggregate(pipeline, allowDiskUse=True))[0])

    def ensure_indexes(self):
        self.db.ratings.create_index([("userId", ASCENDING), ("movieId", ASCENDING)])
        self.db.movies.create_index([("movieId", ASCENDING)])

    def print_task(self, rows, n):
        os.makedirs("log",exist_ok=True)
        output_file = os.path.join("log",f"task{n}_results.txt")
        with open(output_file, "w", encoding="utf-8") as f:
            if not rows:
                print("No results.")
                f.write("No results.\n")
                return

            for doc in rows:
                pprint(doc, sort_dicts=False, width=100)
                pprint(doc, sort_dicts=False, width=100, stream=f)

        print(f"\nResults saved to {output_file}")

    def print_task10(self, result, filepath="log/task10_results.txt"):
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        most = result.get("most_genre_diverse", [])
        var  = result.get("highest_variance", [])
        with open(filepath, "w", encoding="utf-8") as f:
            print("\nMost genre diverse (top 10):")
            f.write("Most genre diverse (top 10):\n")
            if not most:
                print("No results.")
                f.write("No results.\n")
            for doc in most:
                pprint(doc, sort_dicts=False, width=100)
                pprint(doc, sort_dicts=False, width=100, stream=f)

            print("\nHighest variance (top 10):")
            f.write("\nHighest variance (top 10):\n")
            if not var:
                print("No results.")
                f.write("No results.\n")
            for doc in var:
                pprint(doc, sort_dicts=False, width=100)
                pprint(doc, sort_dicts=False, width=100, stream=f)

        print(f"\nResults saved to {filepath}")
        
if __name__ == "__main__":
    task = TaskProgram()
    try:
        task.task1()
        task.task2()
        task.task3()
        task.task4()
        task.task5()
        task.task6()
        task.task7()
        task.task8()
        task.task9()
        #Takes a long time, uncomment if you want to test task 10 query
        #task.task10()
    finally:
        task.close()

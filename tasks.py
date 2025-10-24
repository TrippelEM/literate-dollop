import os
from pprint import pprint
from DbConnector import DbConnector

class Q:
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
    def close(self):
        self.connection.close_connection()

class TaskProgram(Q):
    def task1_top_directors(self):
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
        return list(self.db["movies"].aggregate(pipeline))

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

    def task2_top_actors(self):
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

    def task4(self):
        pipeline = [
            {
                "$match": {
                    "collection": {"$ne": None},
                    "revenue": {"$type": "number"},
                    "vote_average": {"$type": "number"},
                    "release_date": {"$type": "date"}
                }
            },
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


if __name__ == "__main__":
    task = TaskProgram()
    try:
        # res = task.task1_top_directors()
        # task.print_task(res,1)
        task.task2_top_actors()
    finally:
        task.close()

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


if __name__ == "__main__":
    task = TaskProgram()
    try:
        # res = task.task1_top_directors()
        # task.print_task(res,1)
        task.task2_top_actors()
    finally:
        task.close()

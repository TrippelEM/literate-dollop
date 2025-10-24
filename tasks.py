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

    def print_task1(self, rows):
        for doc in rows:
            pprint(doc, sort_dicts=False, width=100)
        if not rows:
            print("No results.")

    def task2_top_actors(self):
        pass  # To be implemented

if __name__ == "__main__":
    task = TaskProgram()
    try:
        res = task.task1_top_directors()
        task.print_task1(res)
    finally:
        task.close()

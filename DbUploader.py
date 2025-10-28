from DbConnector import DbConnector
from typing import Iterable, Dict, Any
from pymongo import ASCENDING


class DbUploader:

    def __init__(self, **db_kwargs):
        self.connection = DbConnector(**db_kwargs)
        self.client = self.connection.client
        self.db = self.connection.db

    def drop_if_exists(self, collection_name: str):
        if collection_name in self.db.list_collection_names():
            self.db[collection_name].drop()

    def insert_many_chunked(self, collection_name: str, docs: Iterable[Dict[str, Any]], batch_size: int = 50_000):
        return self.connection.bulk_insert(collection_name, docs, batch_size)

    def create_assignment_indexes(self):
        spec = {
            "movies": [
                [("title", ASCENDING)],
                [("genres", ASCENDING)],
                [("cast.person_id", ASCENDING)],
                [("crew.person_id", ASCENDING)],
            ],
            "people": [
                [("name", ASCENDING)],
                [("cast.tmdb_id", ASCENDING)],
                [("crew.tmdb_id", ASCENDING)],
            ],
            "ratings": [
                [("userId", ASCENDING)],
                [("movieId", ASCENDING)],
            ],
        }
        self.connection.ensure_indexes(spec)

    def stats(self):
        return {
            "movies": self.db["movies"].estimated_document_count(),
            "people": self.db["people"].estimated_document_count(),
            "ratings": self.db["ratings"].estimated_document_count(),
        }
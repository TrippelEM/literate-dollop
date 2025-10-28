from pprint import pprint 
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
                [("links", ASCENDING)],
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
            "links": [
                [("movieId", ASCENDING)],
                [("tmdbId", ASCENDING)],
            ],
        }
        self.connection.ensure_indexes(spec)

    def stats(self):
        return {
            "movies": self.db["movies"].estimated_document_count(),
            "people": self.db["people"].estimated_document_count(),
            "ratings": self.db["ratings"].estimated_document_count(),
            "links": self.db["links"].estimated_document_count(),
        }

    def create_coll(self, collection_name):
        collection = self.db.create_collection(collection_name)    
        print('Created collection: ', collection)

    def insert_documents(self, collection_name):
        docs = [
            {
                "_id": 1,
                "name": "Bobby",
                "courses": 
                    [
                    {'code':'TDT4225', 'name': ' Very Large, Distributed Data Volumes'},
                    {'code':'BOI1001', 'name': ' How to become a boi or boierinnaa'}
                    ] 
            },
            {
                "_id": 2,
                "name": "Bobby",
                "courses": 
                    [
                    {'code':'TDT02', 'name': ' Advanced, Distributed Systems'},
                    ] 
            },
            {
                "_id": 3,
                "name": "Bobby",
            }
        ]  
        collection = self.db[collection_name]
        collection.insert_many(docs)
        
    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents: 
            pprint(doc)
        

    def drop_coll(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()

        
    def show_coll(self):
        collections = self.client['test'].list_collection_names()
        print(collections)
         


def main():
    program = None
    try:
        program = ExampleProgram()
        program.create_coll(collection_name="Person")
        program.show_coll()
        program.insert_documents(collection_name="Person")
        program.fetch_documents(collection_name="Person")
        program.drop_coll(collection_name="Person")
        # program.drop_coll(collection_name='person')
        # program.drop_coll(collection_name='users')
        # Check that the table is dropped
        program.show_coll()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == '__main__':
    main()

from pymongo import MongoClient


class DbConnectorMini:
    """
    Connects to the MongoDB server on the Ubuntu virtual machine.
    Connector needs HOST, USER and PASSWORD to connect.

    Example:
    HOST = "tdt4225-00.idi.ntnu.no" // Your server IP address/domain name
    USER = "testuser" // This is the user you created and added privileges for
    PASSWORD = "test123" // The password you set for said user
    """

    def __init__(self,
                 DATABASE='movie_db_mini',
                 HOST="localhost",
                 USER="slaver",
                 PASSWORD="root"):
        #uri = f"mongodb://{USER}:{PASSWORD}@{HOST}:27018/{DATABASE}?authSource=movie_db_mini"
        uri = "mongodb://slaver:root@localhost:27018/movie_db_mini?authSource=movie_db_mini"
        print(uri)
        # Connect to the databases
        try:
            self.client = MongoClient(uri)
            self.db = self.client[DATABASE]
        except Exception as e:
            print("ERROR: Failed to connect to db:", e)

        # get database information
        print("You are connected to the database:", self.db.name)
        print("-----------------------------------------------\n")
        print(uri)

    def close_connection(self):
        # close the cursor
        # close the DB connection
        
        self.client.close()
        print("\n-----------------------------------------------")
        print("Connection to %s-db is closed" % self.db.name)

    def collection(self, name: str):
        return self.db[name]

    def bulk_insert(self, collection_name: str, docs, batch_size: int = 50_000):
        """
        Inserts docs in chunks. Accepts list or any iterable of dicts.
        Returns total inserted count.
        """
        coll = self.collection(collection_name)
        buf, n = [], 0
        for d in docs:
            buf.append(d)
            if len(buf) >= batch_size:
                coll.insert_many(buf, ordered=False)
                n += len(buf)
                buf.clear()
        if buf:
            coll.insert_many(buf, ordered=False)
            n += len(buf)
        return n
    
    def ensure_indexes(self, spec: dict):
        """
        spec example:
        {
          "movies": [ [("title", 1)], [("genres", 1)], [("cast.person_id", 1)] ],
          "people": [ [("name", 1)], [("cast.tmdb_id", 1)], [("crew.tmdb_id",1)] ],
        }
        """
        for coll_name, idx_list in spec.items():
            coll = self.collection(coll_name)
            for keys in idx_list:
                coll.create_index(keys)
#  CRUD_Python_Module_Module4.p

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
from bson.objectid import ObjectId


class AnimalShelter:
    """CRUD operations for Animal collection in MongoDB (backward-compatible init)."""

    def __init__(self, *args, **kwargs):
        """
        Backward-compatible constructor:
        - Accepts either positional or keyword args.
        - Keyword names: username, password, host, port, database, collection, auth_source
        - Positional order (legacy): username, password, host, port, database, collection, auth_source
        """

        # Defaults
        defaults = {
            "username": "aacuser",
            "password": "Password12345",
            "host": "127.0.0.1",
            "port": 27017,
            "database": "aac",
            "collection": "animals",
            "auth_source": None,
        }

        # Map positional args if provided (legacy signatures)
        pos_keys = ["username", "password", "host", "port", "database", "collection", "auth_source"]
        for i, val in enumerate(args):
            if i < len(pos_keys):
                defaults[pos_keys[i]] = val

        # Overlay keyword args
        for k in defaults:
            if k in kwargs and kwargs[k] is not None:
                defaults[k] = kwargs[k]

        self.username = defaults["username"]
        self.password = defaults["password"]
        self.host = defaults["host"]
        self.port = int(defaults["port"])
        self.database_name = defaults["database"]
        self.collection_name = defaults["collection"]
        self.auth_source = defaults["auth_source"] or self.database_name

        try:
            self.client = MongoClient(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                authSource=self.auth_source,
                authMechanism="SCRAM-SHA-256",
                serverSelectionTimeoutMS=8000,
            )
            # Verify connection & auth fast
            self.client[self.database_name].command("ping")
            self.database = self.client[self.database_name]
            self.collection = self.database[self.collection_name]
        except (ConnectionFailure, OperationFailure) as e:
            self.client = None
            self.database = None
            self.collection = None
            raise RuntimeError(f"Mongo connection failed: {e}")

    # -------- Helpers --------
    def getNextRecordNum(self) -> int:
        """Find highest rec_num and return next integer; start at 1 if empty."""
        top = list(
            self.collection.find({}, {"rec_num": 1})
            .sort("rec_num", -1)
            .limit(1)
        )
        return int(top[0].get("rec_num", 0)) + 1 if top else 1

    # -------- CRUD --------
    def create(self, data) -> bool:
        """
        Insert one or more documents.
        - Accepts a dict or list of dicts
        - Removes '_id' and (re)assigns sequential 'rec_num'
        Returns True if all inserts succeed.
        """
        if data is None or (isinstance(data, list) and not data):
            raise ValueError("Nothing to save, data is empty")

        docs = data if isinstance(data, list) else [data]
        for doc in docs:
            if not isinstance(doc, dict):
                raise TypeError("create() expects a dict or list of dicts")

            doc.pop("_id", None)
            doc.pop("rec_num", None)
            doc["rec_num"] = self.getNextRecordNum()

            ret = self.collection.insert_one(doc)
            if not (ret.acknowledged and ret.inserted_id):
                return False
        return True

    #def read(self, filter_doc=None):
        #"""
       # Return a list of documents matching filter_doc.
        #If filter_doc is None, return [first_doc] for template compatibility.
        #"""
        #if filter_doc:
            #return list(self.collection.find(filter_doc))
        #first = self.collection.find_one()
        #return [first] if first else []
    
    def read(self, query):
        if query is not None:
            return self.collection.find(query,{"_id":False})
        else:
            return []

    def update(self, records, new_values):
        try:
            result = self.collection.update_many(records, {'$set': new_values})
            return result.modified_count
        except Exception as e:
            print(f"An error occured: {e}")
            return 0
    
    def delete(self, query):
        try:
            result = self.collection.delete_many(query)
            return result.deleted_count
        except Exception as e:
            print(f"An error occurred: {e}")
            return 0
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from bson.objectid import ObjectId
import certifi

client_url = "mongodb+srv://dev:potentia@certificate.srcge0k.mongodb.net/?retryWrites=true&w=majority&appName=certificate"

def insert_document(database_name, collection_name, document):
    try:
        client = MongoClient(client_url, tlsCAFile=certifi.where())
        db = client[database_name]
        collection = db[collection_name]
        id = collection.insert_one(document).inserted_id

        client.close()
        return {"status" : True, "data" : id}
    except Exception as e:
        print("mongo module threw except", e)
        return {"status" : False, "data" : e}

def fetch_documents(database_name, collection_name, query):
    try:
        client = MongoClient(client_url, tlsCAFile=certifi.where())
        db = client[database_name]
        collection = db[collection_name]
        documents = list(collection.find(query))
        client.close()
        for doc in documents:
            del doc["_id"]
        return {"status" : True, "data" : documents}
    except Exception as e:
        return {"status" : False, "data" : e}


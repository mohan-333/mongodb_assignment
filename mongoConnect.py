import pymongo
from pymongo import errors


def connect():
    connectionString = "mongodb://localhost:27017/?directConnection=true"

    try:
        # Establish connection to MongoDB
        client = pymongo.MongoClient(connectionString)

        # Check if the connection is successful
        client.server_info()
        print("Successfully connected to MongoDB")

    except errors.ConnectionFailure as e:
        print("Could not connect to MongoDB:", e)
    except Exception as e:
        print("An error occurred:", e)
    return client



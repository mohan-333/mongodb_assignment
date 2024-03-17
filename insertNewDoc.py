from pymongo import errors
import bson.json_util

def insert_comment(client, database_name, collection_name):
    db = client[database_name]
    collection = db[collection_name]

    # Prompt user for input for the new comment
    new_comment = {
        "name": input("Enter name: "),
        "email": input("Enter email: "),
        "movie_id": input("Enter movie_id: "),
        "text": input("Enter comment: "),
        "date": input("Enter date (in YYYY-MM-DDTHH:MM:SSZ format): ")
    }

    try:
        # Insert the new comment document into the MongoDB collection
        bson_data = bson.json_util.loads(bson.json_util.dumps(new_comment))
        result = collection.insert_one(bson_data)
        print(f"Inserted new document with _id '{result.inserted_id}' into collection '{collection_name}'")
    except errors.PyMongoError as e:
        print(f"Error inserting document into collection '{collection_name}': {e}")



def insert_movie(client, database_name, collection_name):
    db = client[database_name]
    collection = db[collection_name]

    # Prompt user for input for the new movie
    new_movie = {
        "title": input("Enter title: "),
        "year": {
            "$numberInt": input("Enter year: ")
        },
        "runtime": {
            "$numberInt": input("Enter runtime: ")
        },
        "released": {
            "$date": {
                "$numberLong": input("Enter released date (in milliseconds since UNIX epoch): ")
            }
        },
        "poster": input("Enter poster URL: "),
        "plot": input("Enter plot: "),
        "fullplot": input("Enter fullplot: "),
        "lastupdated": input("Enter last updated (in ISO 8601 format): "),
        "type": input("Enter type: "),
        "directors": input("Enter directors (comma-separated): ").split(","),
        "imdb": {
            "rating": {
                "$numberDouble": input("Enter IMDb rating: ")
            },
            "votes": {
                "$numberInt": input("Enter IMDb votes: ")
            },
            "id": {
                "$numberInt": input("Enter IMDb ID: ")
            }
        },
        "cast": input("Enter cast (comma-separated): ").split(","),
        "countries": input("Enter countries (comma-separated): ").split(","),
        "genres": input("Enter genres (comma-separated): ").split(","),
        "tomatoes": {
            "viewer": {
                "rating": {
                    "$numberDouble": input("Enter viewer rating: ")
                },
                "numReviews": {
                    "$numberInt": input("Enter number of reviews: ")
                }
            },
            "lastUpdated": {
                "$date": {
                    "$numberLong": input("Enter last tomatoes update (in milliseconds since UNIX epoch): ")
                }
            }
        },
        "num_mflix_comments": {
            "$numberInt": input("Enter number of mflix comments: ")
        }
    }

    try:
        # Insert the new movie document into the MongoDB collection
        bson_data = bson.json_util.loads(bson.json_util.dumps(new_movie))
        result = collection.insert_one(bson_data)
        print(f"Inserted new document with _id '{result.inserted_id}' into collection '{collection_name}'")
    except errors.PyMongoError as e:
        print(f"Error inserting document into collection '{collection_name}': {e}")


def insert_theatre(client, database_name, collection_name):
    db = client[database_name]
    collection = db[collection_name]

    # Prompt user for input for the new theatre
    new_theatre = {
        "theaterId": {
            "$numberInt": input("Enter theaterId: ")
        },
        "location": {
            "address": {
                "street1": input("Enter street1: "),
                "city": input("Enter city: "),
                "state": input("Enter state: "),
                "zipcode": input("Enter zipcode: ")
            },
            "geo": {
                "type": input("Enter geo type: "),
                "coordinates": [
                    {
                        "$numberDouble": input("Enter longitude: ")
                    },
                    {
                        "$numberDouble": input("Enter latitude: ")
                    }
                ]
            }
        }
    }

    try:
        # Insert the new theatre document into the MongoDB collection
        bson_data = bson.json_util.loads(bson.json_util.dumps(new_theatre))
        result = collection.insert_one(bson_data)
        print(f"Inserted new document with _id '{result.inserted_id}' into collection '{collection_name}'")
    except errors.PyMongoError as e:
        print(f"Error inserting document into collection '{collection_name}': {e}")


def insert_user(client, database_name, collection_name):
    db = client[database_name]
    collection = db[collection_name]

    # Prompt user for input for the new user
    new_user = {
        "name": input("Enter name: "),
        "email": input("Enter email: "),
        "password": input("Enter password: ")
    }

    try:
        # Insert the new user document into the MongoDB collection
        bson_data = bson.json_util.loads(bson.json_util.dumps(new_user))
        result = collection.insert_one(bson_data)
        print(f"Inserted new document with _id '{result.inserted_id}' into collection '{collection_name}'")
    except errors.PyMongoError as e:
        print(f"Error inserting document into collection '{collection_name}': {e}")
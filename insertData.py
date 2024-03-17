import os
import json
import bson.json_util
from pymongo import errors
from mongoConnect import connect

def create_database(client, database_name):
    db_list = client.list_database_names()
    if database_name not in db_list:
        print(f"Creating database '{database_name}'")
        client[database_name]
def create_theaters_index(collection):
    # Check if the 2dsphere index on the 'location' field exists
    if 'location_2dsphere' not in collection.index_information():
        # Create the 2dsphere index on the 'location' field
        collection.create_index([("location.geo", "2dsphere")])
        print("Created 2dsphere index on 'location.geo' field for 'theatres' collection")
    else:
        print("2dsphere index on 'location.geo' field already exists for 'theatres' collection")
def insert_data_from_folder(client, database_name, folder_path):
    db = client[database_name]

    # Iterate over files in the folder
    for filename in os.listdir(folder_path):
        if filename.endswith(".json"):
            collection_name = os.path.splitext(filename)[0]  # Extract collection name from filename
            collection = db[collection_name]
            total_documents = 0

            # Read JSON data from file
            with open(os.path.join(folder_path, filename), 'r') as file:
                # Iterate over each line in the file
                for line in file:
                    data = json.loads(line)
                    bson_data = bson.json_util.loads(bson.json_util.dumps(data))

                    # Insert JSON data into MongoDB collection
                    try:
                        collection.insert_one(bson_data)
                        total_documents += 1  # Increment total documents count
                    except errors.BulkWriteError as e:
                        print(f"Error inserting document into collection '{collection_name}': {e}")

                # Print the total number of documents inserted for the collection
            print(f"Inserted {total_documents} documents into collection '{collection_name}'")

            if collection_name == "theaters":
                create_theaters_index(collection)



if __name__ == "__main__":
    # Reuse MongoDB connection instance
    mongo_client = connect()

    # Define the database name and folder path
    database_name = "sample_mflix"
    folder_path = "sample_mflix"  # Replace with the actual path to the simple_mflix folder

    # Create database if it doesn't exist
    create_database(mongo_client, database_name)

    # Insert data into MongoDB collections
    insert_data_from_folder(mongo_client, database_name, folder_path)


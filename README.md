# MongoDB Assignment

This README file provides a detailed explanation of the main server file (`server.py`) and its functionalities.
## Installation

To run the server and execute queries on a local MongoDB instance, follow these steps:

1. **Clone Repository**: Clone this repository to your local machine.

2. **Install Dependencies**: Install the required Python packages listed in the `requirements.txt` file.

3. **MongoDB Installation**: Ensure MongoDB is installed on your system. You can download and install MongoDB from the [official MongoDB website](https://www.mongodb.com/try/download/community).

4. **Start MongoDB**: Start the MongoDB server on your local machine. If you're using the default configurations, MongoDB should be running on `localhost` at port `27017`.

5. **Run the Server**: Execute the `server.py` file to start the server.

6. **Interact with the Server**: Once the server is running, follow the prompts to perform various operations such as inserting data or executing queries.

By following these steps, you should be able to set up and run the server for executing MongoDB queries locally.

## Usage

1. **Unzip the data folder sample_mflix**: It contains all the data we need to work on. 

2. **Connecting to MongoDB**: The server establishes a connection to MongoDB using the `mongoConnect.py` file.

3. **Inserting Data**: Data insertion is facilitated through the `insertData.py` file.

4. **Executing Queries**: The main functionality of the server involves executing various queries on MongoDB collections. This is achieved through functions defined in files such as `fourA.py`, `fourB.py`, and `fourC.py`.

## Running the Server

1. Ensure that MongoDB is running on your system.
2. Execute the `server.py` file.

## Example

To find the top 10 users with the most comments:
1. Choose option `4Q` (Execute Queries).
2. Select sub-option `4a` (Comments collection).
3. Enter sub-option `i` (Find top 10 users who made the maximum number of comments).

## Files Structure

- `server.py`: Main server file handling user interactions and executing functionalities.
- `mongoConnect.py`: Establishes connection to MongoDB.
- `insertData.py`: Bulk Loads the data from sample_mflix folder into MongoDB database.
- `insertNewDoc.py`: Inserts new documents into MongoDB collections.
- `fourA.py`, `fourB.py`, `fourC.py`: Files containing functions for executing different types of queries.
- `requirements.txt`: Contains Python package dependencies.

## Author

#### Thota Mohan Reddy


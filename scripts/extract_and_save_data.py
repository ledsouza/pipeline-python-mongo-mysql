from dotenv import load_dotenv
import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests

# Functions

def connect_mongo(uri):
    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))

    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)

    return client

def create_connect_db(client, db_name):
    db = client[db_name]
    return db

def create_connect_collection(db, col_name):
    collection = db[col_name]
    return collection

def extract_api_data(url):
    response = requests.get(url)
    return response.json()

def insert_data(col, data):
    docs = col.insert_many(data)
    return docs

# Main

# Load environment variables
load_dotenv()

uri = f"mongodb+srv://ledsouza:{os.getenv('MONGODB_PASSWORD')}@cluster-pipeline-python.cv4ixil.mongodb.net/?retryWrites=true&w=majority"

# Connect to MongoDB
client = connect_mongo(uri)

# Connect to the database
db = create_connect_db(client, "db_produtos")

# Connect to the collection
col = create_connect_collection(db, "produtos")

# Extract data from the API
url = "https://labdados.com/produtos"

data = extract_api_data(url)

# Insert data into the collection
docs = insert_data(col, data)

client.close()
from pymongo import MongoClient
import os

MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://mongo:27017/')

# Connecting to our MongoDB Database
client = MongoClient(MONGODB_URI)
db = client.fombook

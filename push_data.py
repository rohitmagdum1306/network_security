import os
import sys
import json
import certifi
import pandas as pd
import numpy as np
import pymongo
from network_security.exception.exception import NetworkSecurityException
from network_security.logging.logger import logging

from dotenv import load_dotenv
load_dotenv()

MONGO_DB_URL = os.getenv("MONGO_DB_URL")
# print(MONGO_DB_URL)

ca = certifi.where()

class NetworkDataExtract:
    def __init__(self):
        try:
            pass
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def csv_to_json(self, file_path):
        try:
            # Read the CSV file into a pandas DataFrame
            data = pd.read_csv(file_path)

            # Reset the DataFrame index to ensure a clean 0-based index
            data.reset_index(drop=True, inplace=True)

            # Convert the DataFrame into a dictionary format (JSON-style) and then to a list of records
            # data.T.to_json() transposes the DataFrame and converts it to a JSON string
            # json.loads(...) parses that string to a Python dict
            # .values() gets the values of that dict (which are individual records)
            records = list(json.loads(data.T.to_json()).values())
            return records
        except Exception as e:
            # Raise a custom exception if any error occurs
            raise NetworkSecurityException(e, sys)

    def insert_data_mongodb(self, records, database, collection):
        try:
            # Save passed parameters to instance variables
            self.records = records
            self.database = database
            self.collection = collection

            # Create a MongoDB client using a constant URL (ensure MONGO_DB_URL is defined globally)
            self.mongo_client = pymongo.MongoClient(MONGO_DB_URL)

            # Get the target database by name
            self.database = self.mongo_client[self.database]

            # Get the collection (table equivalent in MongoDB) by name
            self.collection = self.database[self.collection]

            # Insert all records at once into the specified MongoDB collection
            self.collection.insert_many(self.records)

            # Return the number of inserted records
            return len(self.records)

        except Exception as e:
            # Raise a custom exception with traceback info
            raise NetworkSecurityException(e, sys)

if __name__== "__main__":
    FILE_PATH = "Network_Data/phisingData.csv"
    DATABASE = "NETWORKAI"
    Collection = "NetworkData"
    networkobj = NetworkDataExtract()
    records = networkobj.csv_to_json(file_path=FILE_PATH)
    print(records)
    no_of_records = networkobj.insert_data_mongodb(records, DATABASE, Collection)
    print(no_of_records)




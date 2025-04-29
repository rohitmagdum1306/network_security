# Import custom exception and logging modules
from network_security.exception.exception import NetworkSecurityException
from network_security.logging.logger import logging

# Import data ingestion configuration entity
from network_security.entity.config_entity import DataIngestionConfig
from network_security.entity.artifact_entity import DataIngestionArtifact

# Standard library and external imports
import os
import sys
import numpy as np
import pandas as pd
import pymongo
from typing import List
from sklearn.model_selection import train_test_split
from dotenv import load_dotenv

# Importing MONGO_DB_URL from another script (though it's reassigned later)
from push_data import MONGO_DB_URL
#from venv.Tools.scripts.generate_opcode_h import header  # Unused import

# Load environment variables from .env file
load_dotenv()

# Override MONGO_DB_URL with value from environment variable
MONGO_DB_URL = os.getenv("MONGO_DB_URL")

class DataIngestion:
    """
    Class for handling the data ingestion process:
    - Extract data from MongoDB
    - Store as feature CSV
    - Split into train and test datasets
    """

    def __init__(self, data_ingestion_config: DataIngestionConfig):
        """
        Constructor for initializing DataIngestion with configuration.
        """
        try:
            self.data_ingestion_config = data_ingestion_config
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def export_collection_as_dataframe(self) -> pd.DataFrame:
        """
        Connects to MongoDB, retrieves the specified collection,
        converts it to a pandas DataFrame, and returns it.
        """
        try:
            # Read database and collection names from config
            database_name = self.data_ingestion_config.database_name
            collection_name = self.data_ingestion_config.collection_name

            # Connect to MongoDB using the provided connection string
            self.mongo_client = pymongo.MongoClient(MONGO_DB_URL)
            collection = self.mongo_client[database_name][collection_name]

            # Convert the collection documents to a DataFrame
            df = pd.DataFrame(list(collection.find()))

            # Drop the MongoDB internal ID column if present
            if "_id" in df.columns.to_list():
                df = df.drop(columns=["_id"], axis=1)

            # Replace 'na' strings with actual NaN values
            df.replace({"na": np.nan}, inplace=True)
            return df
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def export_data_into_feature_store(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Saves the DataFrame to a CSV file (feature store) at the specified path.
        """
        try:
            feature_store_file_path = self.data_ingestion_config.feature_store_file_path

            # Create the directory if it does not exist
            dir_path = os.path.dirname(feature_store_file_path)
            os.makedirs(dir_path, exist_ok=True)

            # Save DataFrame as CSV
            dataframe.to_csv(feature_store_file_path, index=False, header=True)
            return dataframe
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def split_data_as_train_test(self, dataframe: pd.DataFrame):
        """
        Splits the data into training and testing sets and saves them to CSV files.
        """
        try:
            # Perform train-test split
            train_set, test_set = train_test_split(
                dataframe,
                test_size=self.data_ingestion_config.train_test_split_ratio
            )
            logging.info("Performed train test split on the dataframe")

            # Create directory for saving the split files
            dir_path = os.path.dirname(self.data_ingestion_config.training_file_path)
            os.makedirs(dir_path, exist_ok=True)

            logging.info("Exporting train and test file path")

            # Save train and test sets to their respective paths
            train_set.to_csv(
                self.data_ingestion_config.training_file_path, index=False, header=True
            )

            test_set.to_csv(
                self.data_ingestion_config.testing_file_path, index=False, header=True
            )

            logging.info("Exported train and test file path.")
        except Exception as e:
            raise NetworkSecurityException(e, sys)

    def initiate_data_ingestion(self):
        """
        Orchestrates the full data ingestion process:
        1. Extract data from MongoDB
        2. Save to feature store
        3. Split and save train/test data
        """
        try:
            dataframe = self.export_collection_as_dataframe()
            dataframe = self.export_data_into_feature_store(dataframe)
            self.split_data_as_train_test(dataframe)

            dataingestionartifact = DataIngestionArtifact(trained_file_path=self.data_ingestion_config.training_file_path,
                                                          test_file_path = self.data_ingestion_config.testing_file_path)

            return dataingestionartifact
        except Exception as e:
            raise NetworkSecurityException(e, sys)



# from network_security.exception.exception import NetworkSecurityException
# from network_security.logging.logger import logging
#
# ## Configs of Data Ingestion Config
# from network_security.entity.config_entity import DataIngestionConfig
#
# import os
# import sys
# import numpy as np
# import pandas as pd
# import pymongo
# from typing import List
# from sklearn.model_selection import train_test_split
#
# from dotenv import load_dotenv
#
# from push_data import MONGO_DB_URL
# from venv.Tools.scripts.generate_opcode_h import header
#
# load_dotenv()
#
# MONGO_DB_URL = os.getenv("MONGO_DB_URL")
#
#
# class DataIngestion:
#     def __init__(self, data_ingestion_config:DataIngestionConfig):
#         try:
#             self.data_ingestion_config = data_ingestion_config
#         except Exception as e:
#             raise NetworkSecurityException(e, sys)
#
#     def export_collection_as_dataframe(self):
#         try:
#             database_name = self.data_ingestion_config.database_name
#             collection_name = self.data_ingestion_config.collection_name
#             self.mongo_client = pymongo.MongoClient(MONGO_DB_URL)
#             collection = self.mongo_client[database_name][collection_name]
#
#             df = pd.DataFrame(list(collection.find()))
#
#             if "_id" in df.columns.to_list():
#                 df = df.drop(columns=["_id"], axis=1)
#
#             df.replace({
#                 "na": np.nan
#             }, inplace=True)
#             return df
#         except Exception as e:
#             raise NetworkSecurityException(e, sys)
#
#
#     def export_data_into_feature_store(self, dataframe:pd.DataFrame):
#         try:
#             feature_store_file_path = self.data_ingestion_config.feature_store_file_path
#             ## Creating Folder
#             dir_path = os.path.dirname(feature_store_file_path)
#             os.makedirs(dir_path, exist_ok=True)
#             dataframe.to_csv(feature_store_file_path, index=False, header=True)
#             return dataframe
#         except Exception as e:
#             raise NetworkSecurityException(e, sys)
#
#
#     def split_data_as_train_test(self, dataframe:pd.DataFrame):
#         try:
#             train_set, test_set = train_test_split(
#                 dataframe,
#                 test_size=self.data_ingestion_config.train_test_split_ratio
#             )
#             logging.info("Performed train test split on the dataframe")
#
#             dir_path = os.path.dirname(self.data_ingestion_config.training_file_path)
#
#             os.makedirs(dir_path, exist_ok=True)
#
#             logging.info("Exporting train and test file path")
#
#             train_set.to_csv(
#                 self.data_ingestion_config.training_file_path, index=False, header=True
#             )
#
#             test_set.to_csv(
#                 self.data_ingestion_config.testing_file_path, index=False, header=True
#             )
#
#             logging.info("Exported train and test file path.")
#         except Exception as e:
#             raise NetworkSecurityException(e, sys)
#
#     def initiate_data_ingestion(self):
#         try:
#             dataframe = self.export_collection_as_dataframe()
#             dataframe = self.export_data_into_feature_store(dataframe)
#             self.split_data_as_train_test(dataframe)
#         except Exception as e:
#             raise NetworkSecurityException(e, sys)
#
#
#

from datetime import datetime
import os
from network_security.constant import training_pipeline

# Print the pipeline name for debugging purposes
print(training_pipeline.PIPELINE_NAME)

# This class is responsible for configuring the training pipeline.
# It sets up the directory structure and other configurations needed for the pipeline.
class TrainingPipelineConfig:
    def __init__(self, timestamp=datetime.now()):
        # Format the current timestamp to create unique directory names
        timestamp = timestamp.strftime("%m_%d_%Y_%H_%M_%S")
        
        # Name of the pipeline (defined in the training_pipeline constants)
        self.pipeline_name = training_pipeline.PIPELINE_NAME
        
        # Base directory for storing artifacts (defined in the training_pipeline constants)
        self.artifact_name = training_pipeline.ARTIFACT_DIR
        
        # Full path to the artifact directory, including the timestamp
        self.artifact_dir = os.path.join(self.artifact_name, timestamp)
        
        # Directory to store the final trained model
        self.model_dir = os.path.join("final_model")
        
        # Store the formatted timestamp
        self.timestamp: str = timestamp

# This class is responsible for configuring the data ingestion process.
# It defines paths and parameters required to fetch, split, and store the data.
class DataIngestionConfig:
    def __init__(self, training_pipeline_config: TrainingPipelineConfig):
        # Directory for data ingestion artifacts, based on the training pipeline's artifact directory
        self.data_ingestion_dir: str = os.path.join(
            training_pipeline_config.artifact_dir, training_pipeline.DATA_INGESTION_DIR_NAME
        )
        
        # Path to the feature store file (where raw data is stored)
        self.feature_store_file_path: str = os.path.join(
            self.data_ingestion_dir, training_pipeline.DATA_INGESTION_FEATURE_STORE_DIR, training_pipeline.FILE_NAME
        )
        
        # Path to the training data file
        self.training_file_path: str = os.path.join(
            self.data_ingestion_dir, training_pipeline.DATA_INGESTION_INGESTED_DIR, training_pipeline.TRAIN_FILE_NAME
        )
        
        # Path to the testing data file
        self.testing_file_path: str = os.path.join(
            self.data_ingestion_dir, training_pipeline.DATA_INGESTION_INGESTED_DIR, training_pipeline.TEST_FILE_NAME
        )
        
        # Ratio for splitting the data into training and testing sets
        self.train_test_split_ratio: float = training_pipeline.DATA_INGESTION_TRAIN_TEST_SPLIT_RATION
        
        # Name of the collection in the database for storing data
        self.collection_name: str = training_pipeline.DATA_INGESTION_COLLECTION_NAME
        
        # Name of the database for storing data
        self.database_name: str = training_pipeline.DATA_INGESTION_DATABASE_NAME
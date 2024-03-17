import os
import logging
import threading
from multiprocessing.pool import ThreadPool
from datetime import datetime
import time
from pyspark.sql import SparkSession
from DecryptionProcessing import FileDecryptor
from HashValueProcessing import HashingGeneration
from ConfigProcessor import get_config_details
from UUIDGeneration import UUIDProcessing
from QualityCheck import QualityCheckProcessing

# Create the output directory if it doesn't exist

output_directory = get_config_details()['Paths']['log']

# Set up the logger
log_file_path = os.path.join(output_directory, f"log_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.txt")
logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class DataProcessingDriver:
    """
    A class for handling data processing operations using Apache Spark and other utility functions.
    """

    def create_spark_session(self):
        """
        Creates and returns a Spark session.

        Returns:
            pyspark.sql.SparkSession: A Spark session object.
        """
        spark = SparkSession.builder.appName("YourSparkJob").getOrCreate()
        return spark

    @staticmethod
    def initiatedriver(file_path, spark_session, lockvalue):
        """
        Initiates the data processing workflow for a Multiple files.

        Args:
        file_path (str): The path of the file to process.
        spark_session (pyspark.sql.SparkSession): The Spark session object.
        lockvalue (threading.Lock): A lock object to synchronize access to shared resources.
        """
        # Step 5: Decryption
        decrypted = FileDecryptor.decryption_entry(conf, spark, file_path)

        # Step 6: Hashing
        hashed_values = HashingGeneration.hashing_process(spark, conf, decrypted)

        # Step 7: UUID Generation
        UUIDProcessing.generate_time_uuid(spark, conf, file_path, hashed_values)
        with lockvalue:
            # Step 8: Quality Check
            QualityCheckProcessing.quality_check(spark_session, conf, file_path)


if __name__ == "__main__":
    try:
        logging.info(f"Started Preprocessing")

        pool = ThreadPool(10)

        # Step 1: Initialization and object creation
        start_time = time.time()
        data_processing = DataProcessingDriver()

        # Step 2: Get configuration details
        conf = get_config_details()
        file_paths = conf['Paths']['encrypted_file'].split(', ')

        # Step 3: Create a Spark session
        spark = data_processing.create_spark_session()

        lock = threading.Lock()

        # Step 4: Multi-threaded data processing
        # Map the function to the file_paths list using ThreadPool
        pool.map(lambda file_path: data_processing.initiatedriver(file_path, spark, lock), file_paths)

        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Execution time: {execution_time} seconds")
        logging.info(f"Completed Preprocessing")

        # Stop SparkSession
        spark.stop()

    except Exception as e:
        logging.INFO(f"An error occurred: {str(e)}")
        raise  # Re-raise the exception after logging

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from cryptography.fernet import Fernet
import json
import logging


class FileDecryptor:
    """
    A class to decrypt files and determine their types.

    Attributes:
    - key (bytes): The decryption key.
    - fernet (Fernet): Fernet object for encryption and decryption.
    """

    def __init__(self, key):
        """
        Initialize the FileDecryptor with the given key.

        Args:
        - key (bytes): The decryption key.
        """
        self.key = key
        self.fernet = Fernet(self.key)

    def decrypt_file(self, encrypted_data):
        """
        Decrypt the encrypted data.

        Args:
        - encrypted_data (str): The encrypted data to be decrypted.

        Returns:
        - str: The decrypted data.
        """

        decrypted_data = self.fernet.decrypt(encrypted_data.encode()).decode('utf-8')
        return decrypted_data

    def create_dataframe(self, decrypted_content, spark):
        """
        Create a DataFrame from the decrypted content.

        Args:
        - decrypted_content (str): The decrypted content to create the DataFrame from.

        Returns:
        - DataFrame: The DataFrame created from the decrypted content.
        """
        # Check if the content is in JSON format
        try:
            json_content = json.loads(decrypted_content)
            df = spark.read.json(spark.sparkContext.parallelize([json_content]))
            return df
        except ValueError:
            pass

        # If not JSON, assume CSV

        data = [line.split(',') for line in decrypted_content.strip().split('\n')[1:]]
        columns = decrypted_content.strip().split('\n')[0].split(',')
        df = spark.createDataFrame(data, columns)
        return df

    @staticmethod
    def decryption_entry(conf, spark, file_path):
        """
        Decrypts the content of an encrypted file and returns it as a DataFrame.

        Args:
            conf (dict): Configuration details containing encryption key and file paths.
            spark (pyspark.sql.SparkSession): The Spark session object.
            file_path (str): The path of the encrypted file to decrypt.

        Returns:
            pyspark.sql.DataFrame: The decrypted content of the file as a DataFrame.
        """
        try:
            logging.info(f"Decryption started")
            # Define the decryption key
            decryption_key = b'ZhxSRQnX_Boi0LpkIQM9Mzu8c6IeS_bt0O3S_sgzd0I='  # Replace with your decryption key

            # Initialize FileDecryptor instance
            decryptor = FileDecryptor(decryption_key)

            # Define UDF for decryption
            decrypt_udf = udf(decryptor.decrypt_file, StringType())

            # Read the encrypted file as DataFrame
            encrypted_df = spark.read.text(file_path)

            # Apply decryption UDF
            decrypted_df = encrypted_df.withColumn("decrypted_content", decrypt_udf(encrypted_df["value"]))

            # Create DataFrame from decrypted content
            decrypted_content = decrypted_df.select("decrypted_content").first()[0]
            df = decryptor.create_dataframe(decrypted_content, spark)

            # Show the decrypted content
            print(f"File {file_path}:")
            logging.info(f"Decryption is completed successfully")
            return df

        except Exception as e:
            logging.error(f"Error Occurred: {str(e)}")

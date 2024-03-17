from pyspark.sql.functions import sha2, concat, col, lit
import uuid
import logging
import os


class UUIDProcessing:
    """
    A class containing methods for generating UUIDs and processing data in Apache Spark DataFrames.
    """

    @staticmethod
    def generate_time_uuid(spark, conf, file_path, hashed_values):
        """
        Generates Time-based UUIDs and writes the results to a JSON file.

        Args:
            spark (pyspark.sql.SparkSession): The Spark session object.
            conf (dict): A dictionary containing configuration details.
            file_path (str): The path of the input file.
            hashed_values (pyspark.sql.DataFrame): The DataFrame containing hashed values.

        Returns:
            None
        """
        try:
            logging.info(f"UUID Generation started")
            hashed_value_df = hashed_values

            # Add a new column with Time-based UUIDs
            uuid_df = hashed_value_df.withColumn("UUID_Column",
                                                 sha2(concat(col(conf['Paths']['uuid_column']), lit(str(uuid.uuid4()))),
                                                      256))

            # Show the DataFrame with UUIDs
            logging.info(f"Generated UUID and merged successfully")

            filename = os.path.basename(file_path)

            json_output_path = os.path.join(conf['DEFAULT']['OutputJsonPath'], filename.replace('.enc', '_output'))

            # Write the results to a text file
            uuid_df.coalesce(1).write.json(json_output_path)
            logging.info(f"JSON file saved successfully")

            return uuid_df
        except Exception as e:
            logging.error(f"Error Occured: {str(e)}")
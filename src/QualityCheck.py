from pyspark.sql.types import StringType
from pyspark.sql.functions import col, input_file_name
import hashlib
import logging
import os


class QualityCheckProcessing:
    """
    A class for performing quality checks on JSON files and saving the results.

    """

    @staticmethod
    def quality_check(spark, conf, file_path):
        """
        Performs quality checks on JSON files and saves the results.

        Args:
            spark (pyspark.sql.SparkSession): The Spark session object.
            conf (dict): Configuration details.
            file_path (str): The path of the JSON file to perform quality checks on.

        """

        try:
            logging.info(f"Quality Check Started")

            # Path to the parent directory containing the JSON files
            parent_directory = conf["DEFAULT"]["OutputJsonPath"]
            file_path_json = file_path

            # Iterate over JSON files in the directory
            for root, dirs, files in os.walk(parent_directory):
                for file_name in files:
                    if file_name.endswith(".json"):
                        file_path = os.path.join(root, file_name)

                        # Read JSON file
                        df = spark.read.json(file_path)

                        # Add input file name as a column
                        df = df.withColumn("filename", input_file_name())

                        # Perform null checks on columns
                        null_checks = {col_name: df.where(col(col_name).isNull()).count() for col_name in df.columns}

                        # Calculate record count
                        record_count = df.count()

                        # Concatenate all rows into a single string
                        concatenated_string = "\n".join([str(row) for row in df.collect()])

                        # Compute MD5 hash
                        md5_hash = hashlib.md5(concatenated_string.encode()).hexdigest()

                        # Save results
                        output_directory = conf["DEFAULT"]['OutputQcPath']
                        source_file_name = os.path.basename(file_path_json)
                        result_file_path = os.path.join(output_directory,
                                                        source_file_name.replace(".enc", "_qc_output"))

                        results_data = [
                            f"File Name: {result_file_path} | Total Record Count: {record_count} | md5 value: {md5_hash} | Null checks: {null_checks}"
                        ]

                        results_df = spark.createDataFrame(results_data, StringType())

                        results_df.coalesce(1).write.mode("overwrite").text(result_file_path)

            logging.info(f"QC file saved successfully")
            # results_df.show()
            logging.info(f"Quality check is performed successfully")

        except Exception as e:
            logging.error(f"Error Occured: {str(e)}")
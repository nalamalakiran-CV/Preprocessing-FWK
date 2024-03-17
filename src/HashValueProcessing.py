from pyspark.sql.functions import col
import time
import requests
import logging


class HashingGeneration:
    """
    A class for generating hash values for specified columns in a Spark DataFrame.
    """

    # Define the hashing function
    def send_data_and_get_hash(self, data_batch, columns_to_hash, conf):
        """
        Sends data batch to the hashing API and retrieves the hash value.

        Args:
            data_batch (list of dict): Batch of data records to hash.
            columns_to_hash (list of str): List of column names to hash.
            conf: To get the parameters into the modules.

        Returns:
            str: The hash value computed by the API.

        Raises:
            Exception: If there is an error in the API response.
        """
        start_time = time.time()
        api_url = conf['apiurl']['api_url']
        payload = {'data_batch': data_batch, 'columns_to_hash': columns_to_hash}
        response = requests.post(str(api_url), json=payload)
        result = response.json()
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Execution time: {execution_time} seconds")
        if 'hash_value' in result:
            return result['hash_value']
        elif 'error' in result:
            raise Exception(result['error'])
        else:
            raise Exception('Unexpected response from the server')

    @staticmethod
    def hashing_process(spark, conf, decrypted_data):
        """
        Perform hashing on specified columns of the input DataFrame.

        Args:
            spark (pyspark.sql.SparkSession): Spark session object.
            conf (dict): Configuration details.
            decrypted_data (pyspark.sql.DataFrame): DataFrame containing decrypted data.

        Returns:
            pyspark.sql.DataFrame: DataFrame with hashed values joined to the original DataFrame.

        Raises:
            Exception: If an error occurs during the hashing process.
        """
        try:
            logging.info(f"Hashing started")

            hash_value_creation = HashingGeneration()

            # Define batch size

            batch_size = int(conf['apiurl']['batch'])

            # Calculate the number of batches
            num_batches = (decrypted_data.count() // batch_size) + (1 if decrypted_data.count() % batch_size != 0 else 0)
            print(num_batches)

            # Specify columns to hash
            # should come from config
            columns_to_hash = str(conf['Paths']['hashing_column_names']).split(', ')

            # Process records in batches
            for batch in range(num_batches):
                start_idx = batch * batch_size
                end_idx = min((batch + 1) * batch_size, decrypted_data.count())
                print(end_idx)

                batchid = decrypted_data.filter((col("ID") >= start_idx) & (col("ID") < end_idx))

                # Convert batch DataFrame to a list of dictionaries
                data_batch = [{column: record[column] for column in columns_to_hash + ['ID']} for record in batchid.collect()]

                # Call the hashing function
                hashed_values = hash_value_creation.send_data_and_get_hash(data_batch, columns_to_hash,conf)

                # Create a DataFrame from hashed values
                hashed_df = spark.createDataFrame(hashed_values)

                # Dropping the columns from the actual df which are sent for hashing
                for column in columns_to_hash:
                    decrypted_data = decrypted_data.drop(column)
                # Join the hashed values DataFrame with the original DataFrame
                merged_hashed_df = decrypted_data.join(hashed_df, "ID", "inner")

                # Show the joined DataFrame
                logging.info(f"Hashing is completed successfully")
                return merged_hashed_df

        except Exception as e:
            logging.error(f"Error Occured: {str(e)}")
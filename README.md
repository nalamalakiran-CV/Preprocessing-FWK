# **Data Processing Framework**

### **Overview**:

This framework is implemented by leveraging Apache Spark for handling data processing operations such as decryption, hashing, UUID generation, and quality checks. The framework utilizes multi-threading for parallel processing of multiple input files.

### **Features**:

Decrypts encrypted files using a specified algorithm.
Generates hash values for selected columns in the decrypted data.
Creates unique ID columns for each file.
Performs quality checks on the processed data.
Utilizes multi-threading for parallel processing of multiple input files.
Logs processing details and errors for debugging and monitoring.

### **Usage**:

#### Input Files: 

The framework expects multiple encrypted files as input. File paths are configured in the configuration file.

#### Decryption: 
The framework decrypts each input file using the specified decryption algorithm.

#### Hashing: 

After decryption, selected columns in the dataframe are hashed to protect sensitive information.

#### UUID Generation: 

Unique ID columns are generated for each file to give an unique identification.

#### Quality Check: 

Quality checks are performed on the processed data to ensure data integrity and accuracy.

#### Parallel Processing:

##### Multi-threading and Thread Locks

Multi-threading is utilized to process multiple files concurrently, improving overall processing speed and efficiency.

Thread locks, implemented using threading.Lock(), are used to synchronize access to shared resources, ensuring that critical sections of code are executed by only one thread at a time. This prevents potential race conditions and data corruption issues.

### Execution:

To execute the data processing framework:

Ensure all required libraries are installed (pyspark, datetime, time, threading, multiprocessing, logging).

Update the configuration file (ConfigProcessor.py) with the necessary details such as file paths, encryption algorithm, etc.

Run the main script (DataProcessingDriver.py) to start the data processing workflow.

Monitor the logs generated in the specified log directory for processing details and any errors encountered during execution.
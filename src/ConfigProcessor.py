# Importing the configparser module for reading configuration files
import configparser


# Function to retrieve configuration details from the specified INI file
def get_config_details():
    # Creating an instance of ConfigParser
    config = configparser.ConfigParser()

    # Reading the configuration file located at the specified path
    config.read('C:\\Users\\RAKI\\PycharmProjects\\VisaPreProcessing\\PreProcessingFWK\\Config\\config.ini')

    # Returning the ConfigParser object containing configuration details
    return config
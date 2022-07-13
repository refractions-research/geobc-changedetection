#-------------------------------------------------------------------------------
# Module of common function and variables for change detection script.
#
# Author: Emily Gouge
# Date: July 2022
# Copyright: (c) GeoBC 2021
#-------------------------------------------------------------------------------
import json, configparser, argparse
from osgeo import ogr
import os
import logging
from zipfile import ZipFile
import requests
import shutil
from enum import Enum
import datetime

provider_config = None
provider_db = None
log_folder = None
output_folder = None
data_staging_folder = None
args = None

# Processing status values for provider
class ProcessingStatus(Enum):
    NOT_PROCESSED = -1
    ERROR = 1
    PROCESS_OK = 2

# Types of changes supported
class ChangeType(Enum):
    REMOVED_FEATURE = 'feature-removed'
    NEW_FEATURE = 'feature-added'
    UPDATED_ATTRIBUTES = 'attribute-update'

# Change statistics tracker    
class DataStatistic(Enum):
    OLD_DATA_TABLE = 'old_data_table_name'
    NEW_DATA_TABLE = 'new_data_table_name'
    NUM_OLD_RECORDS = 'num_old_records'
    NUM_NEW_RECORDS = 'num_new_records'
    NUM_OLD_DUPLICATE_RECORDS = 'num_old_duplicate_records'
    OLD_DUPLICATE_RECORDS = 'old_duplicate_record_ids'
    NUM_NEW_DUPLICATE_RECORDS = 'num_new_duplicate_records'
    NEW_DUPLICATE_RECORDS = 'new_duplicate_record_ids'
    NUM_REMOVED_FEATURES = 'num_removed_features'
    NUM_NEW_FEATURES = 'num_new_features'
    NUM_FEATURES_ATTRIBUTE_CHANGES = 'num_feature_changed'
    TOTAL_CHANGES = 'total_changes'
    

#projection for storing all data
bc_albers_epsg = 3005

#run date and time for logging filename
rundatetime = datetime.datetime.now().strftime("%Y_%m_%d_%H%M%S")
# Strings with today's date. Note: today_date_string variable used to create new table name.
today_date_string = datetime.date.today().strftime("%Y_%m_%d")

#_logger
_logger = logging.getLogger(__name__)

#-------------------------------------------------------------------------------
# parse the command line and configuration file 
# populating various module variables
#-------------------------------------------------------------------------------
def parse_config():
    global args, provider_config, provider_db, log_folder, output_folder, data_staging_folder
    #update global variables
    parser = argparse.ArgumentParser(description='Run automated dataset change detection.')
    parser.add_argument('-c', type=str, help='the configuration file', required=False);
    parser.add_argument('args', type=str, nargs='*');
    args = parser.parse_args()
    
    #initialize configuration variables for config.ini file
    configfile = "config.ini"
    if (args.c):
        configfile = args.c
        
    configp = configparser.ConfigParser()
    configp.read(configfile)
        
    provider_config = configp['CHANGE_DETECTION']['provider_config']
    provider_db = configp['CHANGE_DETECTION']['database_file']
    log_folder = configp['CHANGE_DETECTION']['log_folder']
    output_folder = configp['CHANGE_DETECTION']['geopackage_output_folder']
    data_staging_folder = configp['CHANGE_DETECTION']['data_staging_folder']

#-------------------------------------------------------------------------------
# converts data between JSON and python objects
#-------------------------------------------------------------------------------
def load_json(jf):
    """
    Args:
        jf - full path to the json file being imported
    Returns:
        python object containing the contents of the json file (object type depends on the content of the JSON file)
    """
    with open(jf) as json_obj:
        return json.load(json_obj)
        
def dump_json(py, jf):
    """
    Args:
        py - python object to dump to json file
        jf - full path to json file to dump py object into
    Returns:
        None - dumps python object to json file
    """
    with open(jf, 'w') as json_obj:
        json.dump(py, json_obj, indent= 4)

#---------------------------------------------------------------------------------------------------
# Converts statistics to string for logging
#---------------------------------------------------------------------------------------------------    
def format_statistics(stats):
    if (stats is None):
        return ""
    
    if len(stats) == 0:
        return ""
     
    return f"""
OLD DATASET:
Source: {stats.get(DataStatistic.OLD_DATA_TABLE, "")}
Number of records: {stats.get(DataStatistic.NUM_OLD_RECORDS, "")}
Number of duplicate features: {stats.get(DataStatistic.NUM_OLD_DUPLICATE_RECORDS, "")}
Duplicate features: {stats.get(DataStatistic.OLD_DUPLICATE_RECORDS, "")}
    
NEW DATASET:
Source: {stats.get(DataStatistic.NEW_DATA_TABLE, "")}
Number of records: {stats.get(DataStatistic.NUM_NEW_RECORDS, "")}
Number of duplicate features: {stats.get(DataStatistic.NUM_NEW_DUPLICATE_RECORDS, "")}
Duplicate features: {stats.get(DataStatistic.NEW_DUPLICATE_RECORDS, "")}

CHANGE SUMMARY:
Total Change Records: {stats.get(DataStatistic.TOTAL_CHANGES, "")}
Number of Added Features: {stats.get(DataStatistic.NUM_NEW_FEATURES, "")}
Number of Removed Features: {stats.get(DataStatistic.NUM_REMOVED_FEATURES, "")}
Number of Attribute Changes: {stats.get(DataStatistic.NUM_FEATURES_ATTRIBUTE_CHANGES, "")}
"""

#---------------------------------------------------------------------------------------------------
# Find the spatial data source in the provided file
# Returns None if can not read data source
#---------------------------------------------------------------------------------------------------
def find_data_source(filename):
    try:
        #find driver
        datasource = None
        for i in range(ogr.GetDriverCount()):
            try:
                driver = ogr.GetDriver(i)
                #print ("testring:" + driver.GetName())
                data_source = driver.Open(filename)
                if (data_source is not None):
                    datasource = data_source
                    break;
            except Exception as e:
                #eat this as another driver might work
                pass
        return datasource;
    except Exception as e:
        _logger.error(e)
        return None

#---------------------------------------------------------------------------------------------------
# Finds all spatial layers in the given file.
# Returns None if could not read data source
#---------------------------------------------------------------------------------------------------
def get_layers(filename):
    datasource = find_data_source(os.path.abspath(filename))
    if (datasource is None):
        return None;
    else:
        layers = set()
        for i in range(0,datasource.GetLayerCount()):
            layers.add(datasource.GetLayerByIndex(i).GetLayerDefn().GetName())
        
        return layers;
    
    
#---------------------------------------------------------------------------------------------------
# Downloads data set from url
#---------------------------------------------------------------------------------------------------
def get_file(url, dataset_name, staging_folder):
    """Downloads dataset from url and extracts dataset if required (zip dataset)
    
        Parameters:
            - url to download the data - data download location
            - dataset_name - name of dataset
            - staging_folder - location to store downloaded data; any existing folder and data will be deleted

        Returns:
        
        Raise:
            Exception 
                - if error occrus while downloading or extracting data
    """
    
    _logger.info(f"Downloading dataset: {dataset_name}")
    _logger.debug(f"URL: {url}")
    
    # delete existing staging folder
    shutil.rmtree(staging_folder, ignore_errors=True)
    if os.path.exists(staging_folder):
        logging.error(f"Error processing {dataset_name}. Could not remove folder {staging_folder}.")
        raise Exception(f"Could not remove folder: {staging_folder}")
        return 
    
    # create staging folder
    os.mkdir(staging_folder)
        
    # determine zip status
    is_zip = False
    package_name = dataset_name
    if url[-4:].lower() == '.zip':
        is_zip = True
        package_name = package_name + ".zip"
    
    #Get a file from a URL and stream it to disk
    targetfile = os.path.join(staging_folder, package_name)
    try:
        stream = requests.get(url, timeout=10, stream=True)
        #Open file for writing
        with open(targetfile, 'wb') as file:
            file.write(stream.content)
            
    except requests.exceptions.RequestException as e:
        _logger.error("Error downloading dataset: %s", dataset_name, exc_info=e)
        raise e
    
    if is_zip:
        try:
            zipfilename = os.path.join(staging_folder, package_name);
            with ZipFile(zipfilename,mode='r') as file_zip:
                file_zip.extractall(staging_folder)
                file_zip.close()
            #then delete the zip itself
            os.remove(os.path.join(staging_folder, package_name))
        except Exception as e:
            _logger.error("Error unarchiving dataset: %s, file: %s", dataset_name, zipfilename, exc_info=e)
            raise e

    _logger.debug("Download and extraction complete.")

#---------------------------------------------------------------------------------------------------
# Writes layer statistics for given provider to log file in the log_folder_path
#---------------------------------------------------------------------------------------------------
def write_log_file(log_folder_path, provider_name, stats):
    """
    Create a log file that describes the data processing steps.

    Parameters:
        log_folder_path (string)
            - Location where log file will be stored
        provider_name (string)
            - Unique name of data provider (eg. "Mission")
        stats (dictionary)
            - Provider processing statistics to write to log file
            
    Dependencies 
        None

    Returns:
        None
    """

    log_file_name = f"Change_Detection_Processing_Log_{provider_name}_{rundatetime}.txt"
    log_file = os.path.join(log_folder_path, log_file_name)
    log_text = f"""
Change Detection Processing Log for {provider_name}, {today_date_string}
       
COMPARISON BETWEEN ORIGINAL DATA AND PREVIOUS ORIGINAL DATA
{format_statistics(stats)}  
    """

    # Create the processing log file and populate it with the log text
    _logger.debug(f"Writing log file: {log_file}")
    processing_log = open(log_file, "w")
    try:
        processing_log.write(log_text)
    finally:
        processing_log.close()
    _logger.debug(f"Writing log file written")    
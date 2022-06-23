# module of common functions and variables for 
# Change Detection Script

import json, datetime, configparser, argparse
from enum import Enum

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
    NUM_DUPLICATE_RECORDS = 'num_duplicate_records'
    DUPLICATE_RECORDS = 'duplicate_record_ids'
    NUM_REMOVED_FEATURES = 'num_removed_features'
    NUM_NEW_FEATURES = 'num_new_features'
    NUM_FEATURES_ATTRIBUTE_CHANGES = 'num_feature_changed'
    TOTAL_CHANGES = 'total_changes'
    

#projection for storing all data
bc_albers_epsg = 3005

#run date and time for logging filename
rundatetime = datetime.datetime.now().strftime("%Y_%m_%d_%H%M%S")

#initialize configuration variables for config.ini file
configfile = "config.ini"

parser = argparse.ArgumentParser(description='Run automated dataset change detection.')
parser.add_argument('-c', type=str, help='the configuration file', required=False);
parser.add_argument('args', type=str, nargs='*');
args = parser.parse_args()
if (args.c):
    configfile = args.c
    
config = configparser.ConfigParser()
config.read(configfile)
    
provider_config = config['CHANGE_DETECTION']['provider_config']
provider_db = config['CHANGE_DETECTION']['database_file']
log_folder = config['CHANGE_DETECTION']['log_folder']
output_folder = config['CHANGE_DETECTION']['geopackage_output_folder']
data_staging_folder = config['CHANGE_DETECTION']['data_staging_folder']


# converts data between JSON and python objects
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
def format_statistics(stats):
    if (stats is None):
        return ""
    
    if len(stats) == 0:
        return ""
     
    return f"""
OLD DATASET:
Source: {stats.get(DataStatistic.OLD_DATA_TABLE, "")}
Number of records: {stats.get(DataStatistic.NUM_OLD_RECORDS, "")}
    
NEW DATASET:
Source: {stats.get(DataStatistic.NEW_DATA_TABLE, "")}
Number of records: {stats.get(DataStatistic.NUM_NEW_RECORDS, "")}
Number of duplicate features: {stats.get(DataStatistic.NUM_DUPLICATE_RECORDS, "")}
Duplicate features: {stats.get(DataStatistic.DUPLICATE_RECORDS, "")}

CHANGE SUMMARY:
Total Change Records: {stats.get(DataStatistic.TOTAL_CHANGES, "")}
Number of Added Features: {stats.get(DataStatistic.NUM_NEW_FEATURES, "")}
Number of Removed Features: {stats.get(DataStatistic.NUM_REMOVED_FEATURES, "")}
Number of Attribute Changes: {stats.get(DataStatistic.NUM_FEATURES_ATTRIBUTE_CHANGES, "")}
"""
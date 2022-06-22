# module of common functions and variables for 
# Change Detection Script

import os, json, traceback, sys, csv, datetime
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
    
# configuration and reference filepaths
main_dir = os.path.dirname(os.path.abspath(__file__)) #Relative path of this file (main folder of project)
provider_config = os.path.join(main_dir, 'config', 'provider_config.json') #provider configuration file
sample_config = os.path.join(main_dir, 'config', 'sample_config.json') #sample config for testing and sample scripts

dataload_dir = os.path.abspath(os.path.join(main_dir, os.pardir, 'data'))
 
provider_db = os.path.join(dataload_dir, 'Road_Provider_Changes_PROD.db3') #Production database - use for full compare of all tracked datasets
log_folder =  os.path.join(dataload_dir,'logs')
output_folder =  os.path.join(dataload_dir, 'output')
data_staging_folder =  os.path.join(dataload_dir, 'raw')

#projection for storing all data
bc_albers_epsg = 3005

#run date and time for logging filename
rundatetime = datetime.datetime.now().strftime("%Y_%m_%d_%H%M%S")

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
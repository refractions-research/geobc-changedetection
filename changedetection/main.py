#-------------------------------------------------------------------------------
# Name:        main
# Purpose:     run process to obtain provider data from schedule or manual trigger 
#              trigger to download data to working folder from providers
#              stage data to working folder
#              call change detection
#              and log results / trigger notifications as needed
# Author:      jedharri
#
# Created:     08-21-2021
# Copyright:   (c) GeoBC 2021
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import cd
import os
import datetime
import logging
import change_detector
import get_file_from_URL

#TODO functions. 
'''
- add configuration for a provider with web service data source DONE
- add configuration for a provider with ad-hoc data source
- add new ad-hoc data for a provider and compare to previous
- download web service data for a single provider and compare to previous
- update configuration for a provider (for roads)
- run scheduled download data for a set of providers and compare to previous
- when running scheduled compare, notify the (who? - geobc info)
- when running ad-hoc compare - notify the user
- compare two versions of data without waiting (we need another table method option - currently tied to date . . . add time = or v2, v3 etc?)
- add a new provider 
- add a new top level data type in addition to roads (FUTURE)
- set up schedule to run change detection (Jenkins?)
- each top level data type should have separate changes database (needs config)
- remove top level data type as nested config item, and point to separate config file for each type
- 

'''

# ---- configure logging ----
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# console handler - info messages only
consolehandler = logging.StreamHandler()
consolehandler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
consolehandler.setFormatter(formatter)
logger.addHandler(consolehandler)

# file handler - all messages
fileloghandler = logging.FileHandler(os.path.join(cd.log_folder, "Change_Detection_Processing_" + cd.rundatetime + ".txt"), mode='a', encoding="utf-8",)
fileloghandler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fileloghandler.setFormatter(formatter)
logger.addHandler(fileloghandler)

#class for tracking provider runs 
#and associated status and statistics
class ProviderStatus:
    
    def __init__(self, provider_name):
        self.provider_name = provider_name
        self.status = cd.ProcessingStatus.NOT_PROCESSED
    
    def setStatus(self, status, message, stats=[]):
        self.status = status
        self.message = message
        self.stats = stats
        
    
#list of providers processed
pproviders = []

def runapp():
    process_all_providers()
    
    #process_provider("District of Saanich Parks")
    #process_provider("District of Saanich")
    #process_provider("District of North Vancouver")
    #process_provider("Kamloops")
    #process_provider("RD_Fraser_Fort_George")
    #process_provider("Regional District of Central Okanagan")
    
    #prints processing summary
    print_summary();

def process_all_providers():
    provider_dict = cd.load_json(cd.provider_config)
    providers = provider_dict.keys()
    
    for provider in providers:
        process_provider(provider)


def process_provider(provider_name):
    
    logger.info(f"""Processing: {provider_name}""")
    info = ProviderStatus(provider_name)
    pproviders.append(info)
    
    try:
        provider_dict = cd.load_json(cd.provider_config)
        if not provider_name in provider_dict.keys():
            logger.error(f"""No configuration for {provider_name} found.""")
            info.setStatus(cd.ProcessingStatus.NOT_PROCESSED, f"No details found for {provider_name} in configuration file.")
            return
        
        url = provider_dict[provider_name].get('url')
        if (not url):
            logger.warning(f"""No URL for {provider_name} in configuration file. Provider not processed.""")
            info.setStatus(cd.ProcessingStatus.NOT_PROCESSED, f"No URL for {provider_name} in configuration file.")
            return
            
        
        #Only get data where there is a URLcreate a folder to stage the data load
        provider_db = cd.provider_db
        log_folder_path = cd.log_folder
        output_folder_path = cd.output_folder
        
        dataset_name = provider_dict[provider_name].get('dataset_name')
        database_name = provider_dict[provider_name].get('database_name')
        data_type = provider_dict[provider_name].get('data_type')
        compare_fields = provider_dict[provider_name].get('compare_fields')
        reference_fields = [] # TODO Not currently configured - set to empty list for intitial testing
    
        date_string = str(datetime.date.today()).replace('-', '_')
        staging_folder =  os.path.join(cd.data_staging_folder, provider_name.replace(' ','_') + '_' + date_string)
        
        try:
            get_file_from_URL.getfile(url, dataset_name, staging_folder)
        except Exception as e:
            #some error occurred and we don't want to continue
            info.setStatus(cd.ProcessingStatus.ERROR, f"Data download failed: {e}")
            logger.error(f"""Could not download data for {provider_name}""")
            return
        
        stats = change_detector.detect_changes(
                provider_db,
                provider_name,
                staging_folder,
                dataset_name,
                database_name,
                log_folder_path,
                output_folder_path,
                data_type,
                compare_fields,
                reference_fields,
        )
        info.setStatus(cd.ProcessingStatus.PROCESS_OK, "", stats)
        
            
    except Exception as e:
        info.setStatus(cd.ProcessingStatus.ERROR, f"Error while processing {provider_name}: " + str(e))
        logger.error(f"Error processing {provider_name}", exc_info=e)
    
        

def print_summary():
    logstr = "------------------------------------------------------------------------\n"
    logstr += "PROCESSING SUMMARY\n"
    logstr += "------------------------------------------------------------------------\n"
    logstr += "Providers Processed: " + str(len(pproviders)) + "\n\n"

    for provider in pproviders:
        logstr += f"{provider.provider_name}: {provider.status}   {provider.message}\n"
    logstr += "------------------------------------------------------------------------\n"
    logstr += "\n\n"
    
    for provider in pproviders:
        logstr += "------------------------------------------------------------------------\n"
        logstr += f"{provider.provider_name} Statistics \n"
        logstr += cd.format_statistics(provider.stats)
        logstr += "\n\n"
        
    
    
    #print to console
    print(logstr)
    
    #write to file
    log_file_name = f"Change_Detection_Processing_SUMMARY_{cd.rundatetime}.txt"

    log_file = os.path.join(cd.log_folder, log_file_name)
    processing_log = open(log_file, "w")
    try:
        processing_log.write(logstr)
    finally:
        processing_log.close()
    
        

#to call change detection: (file name, data folder, fields, provider name, file type???)

if __name__ == '__main__':
    logger.debug(f"PROJ_LIB directory: {os.environ['PROJ_LIB']}")
    logger.debug(f"Provider Configuration: {cd.provider_config}")
    logger.debug(f"Change Log Database: {cd.provider_db}")
    logger.debug(f"Log Output: {cd.log_folder}")
    logger.debug(f"Geopackage Output Folder: {cd.output_folder}")
    logger.debug(f"Data Staging Folder: {cd.data_staging_folder}")

    runapp()




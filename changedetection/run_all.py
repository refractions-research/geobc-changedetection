#-------------------------------------------------------------------------------
# Name:        run_all
# Purpose:     run process to the reads provider data from json file,  
#              download datas for each provider then calls change
#              detection tools to computer changes and write results to output files
# Author:      jedharri
#
# Created:     08-21-2021
# Copyright:   (c) GeoBC 2021
# Licence:     <your licence>
#-------------------------------------------------------------------------------
# Updates: refactoring, logging, and layer statistics 
# Emily Gouge
# July 2022
#-------------------------------------------------------------------------------

from core import utils
import os
import datetime
import logging
from core import change_detector

# ---- configure logging ----
_logger = logging.getLogger()

#keep track of providers processed
_processed_providers = []

#-------------------------------------------------------------------------------
# Class for tracking a data provider with   
# associated status and statistics
#-------------------------------------------------------------------------------
class ProviderStatus:
    
    def __init__(self, provider_name):
        self.provider_name = provider_name
        self.status = utils.ProcessingStatus.NOT_PROCESSED
    
    def setStatus(self, status, message, stats=[]):
        self.status = status
        self.message = message
        self.stats = stats
        

#-------------------------------------------------------------------------------
# configure logging   
#-------------------------------------------------------------------------------
def configure_logging():
    _logger.setLevel(logging.DEBUG)
    # console handler - info messages only
    consolehandler = logging.StreamHandler()
    consolehandler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    consolehandler.setFormatter(formatter)
    _logger.addHandler(consolehandler)

    # file handler - all messages
    fileloghandler = logging.FileHandler(os.path.join(utils.log_folder, "Change_Detection_Processing_" + utils.rundatetime + ".txt"), mode='a', encoding="utf-8",)
    fileloghandler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fileloghandler.setFormatter(formatter)
    _logger.addHandler(fileloghandler)


#-------------------------------------------------------------------------------
# Run the application   
#-------------------------------------------------------------------------------
def runapp():
    process_all_providers()
    
    #process_provider("District of Saanich Parks")
    #process_provider("District of Saanich")
    #process_provider("District of North Vancouver")
    #process_provider("Kelowna")
    #process_provider("City of Victoria")
    #process_provider("RD_Fraser_Fort_George")
    #process_provider("Regional District of Central Okanagan")
    
    #prints processing summary
    print_summary();

#-------------------------------------------------------------------------------
# Processes all providers   
#-------------------------------------------------------------------------------
def process_all_providers():
    provider_dict = utils.load_json(utils.provider_config)
    providers = provider_dict.keys()
    
    for provider in providers:
        process_provider(provider)


#-------------------------------------------------------------------------------
# Processes individual provider   
#-------------------------------------------------------------------------------
def process_provider(provider_name):
    
    _logger.info(f"""Processing: {provider_name}""")
    info = ProviderStatus(provider_name)
    _processed_providers.append(info)
    
    try:
        provider_dict = utils.load_json(utils.provider_config)
        if not provider_name in provider_dict.keys():
            _logger.error(f"""No configuration for {provider_name} found.""")
            info.setStatus(utils.ProcessingStatus.NOT_PROCESSED, f"No details found for {provider_name} in configuration file.")
            return
        
        url = provider_dict[provider_name].get('url')
        if (not url):
            _logger.warning(f"""No URL for {provider_name} in configuration file. Provider not processed.""")
            info.setStatus(utils.ProcessingStatus.NOT_PROCESSED, f"No URL for {provider_name} in configuration file.")
            return
            
        
        #Only get data where there is a URLcreate a folder to stage the data load
        provider_db = utils.provider_db
        log_folder_path = utils.log_folder
        output_folder_path = utils.output_folder
        
        dataset_name = provider_dict[provider_name].get('dataset_name')
        database_name = provider_dict[provider_name].get('database_name')
        data_type = provider_dict[provider_name].get('data_type')
        compare_fields = provider_dict[provider_name].get('compare_fields')
        reference_fields = [] # TODO Not currently configured - set to empty list for intitial testing
    
        date_string = str(datetime.date.today()).replace('-', '_')
        staging_folder =  os.path.join(utils.data_staging_folder, provider_name.replace(' ','_') + '_' + date_string)
        
        try:
            utils.get_file(url, dataset_name, staging_folder)
        except Exception as e:
            #some error occurred and we don't want to continue
            info.setStatus(utils.ProcessingStatus.ERROR, f"Data download failed: {e}")
            _logger.error(f"""Could not download data for {provider_name}""")
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
        info.setStatus(utils.ProcessingStatus.PROCESS_OK, "", stats)
        
            
    except Exception as e:
        info.setStatus(utils.ProcessingStatus.ERROR, f"Error while processing {provider_name}: " + str(e))
        _logger.error(f"Error processing {provider_name}", exc_info=e)
    
        
#-------------------------------------------------------------------------------
# Print a summary of data processed to console   
#-------------------------------------------------------------------------------
def print_summary():
    logstr = "------------------------------------------------------------------------\n"
    logstr += "PROCESSING SUMMARY\n"
    logstr += "------------------------------------------------------------------------\n"
    logstr += "Providers Processed: " + str(len(_processed_providers)) + "\n\n"

    for provider in _processed_providers:
        logstr += f"{provider.provider_name}: {provider.status}   {provider.message}\n"
    logstr += "------------------------------------------------------------------------\n"
    logstr += "\n\n"
    
    for provider in _processed_providers:
        logstr += "------------------------------------------------------------------------\n"
        logstr += f"{provider.provider_name} Statistics \n"
        logstr += utils.format_statistics(provider.stats)
        logstr += "\n\n"
    
    #print to console
    print(logstr)
    
    #write to file
    log_file_name = f"Change_Detection_Processing_SUMMARY_{utils.rundatetime}.txt"

    log_file = os.path.join(utils.log_folder, log_file_name)
    processing_log = open(log_file, "w")
    try:
        processing_log.write(logstr)
    finally:
        processing_log.close()
    
        
#-------------------------------------------------------------------------------
# Main function
#-------------------------------------------------------------------------------
if __name__ == '__main__':
    utils.parse_config()
    configure_logging()
    
    _logger.debug(f"PROJ_LIB directory: {os.environ['PROJ_LIB']}")
    _logger.debug(f"Provider Configuration: {utils.provider_config}")
    _logger.debug(f"Change Log Database: {utils.provider_db}")
    _logger.debug(f"Log Output: {utils.log_folder}")
    _logger.debug(f"Geopackage Output Folder: {utils.output_folder}")
    _logger.debug(f"Data Staging Folder: {utils.data_staging_folder}")

    runapp()

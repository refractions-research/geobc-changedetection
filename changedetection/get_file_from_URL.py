#-------------------------------------------------------------------------------
# Name:        getFileFromURL
# Purpose:
#
# Author:      jedharri
#
# Created:     07-08-2020
# Copyright:   (c) jedharri 2020
# Licence:     <your licence>
#-------------------------------------------------------------------------------
#
# Updated: June 9, 2022
# Updated By: Refractions Research (Emily Gouge)
#
#-------------------------------------------------------------------------------

#import modules
import requests
import os
from zipfile import ZipFile
import logging
import shutil

logger = logging.getLogger(__name__)

#script parameters
def getfile(url, dataset_name, staging_folder):
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
    
    logger.info(f"Downloading dataset: {dataset_name}")
    logger.debug(f"URL: {url}")
    
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
        logger.error("Error downloading dataset: %s", dataset_name, exc_info=e)
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
            logger.error("Error unarchiving dataset: %s, file: %s", dataset_name, zipfilename, exc_info=e)
            raise e

    logger.debug("Download and extraction complete.")


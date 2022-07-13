# -------------------------------------------------------------------------------
# Original Script:
# File: road_change_tracking_orig_from_jed_20210630.py
# Location:
#   \\spatialfiles.bcgov\ilmb\vic\geobc\Workarea\njackson\ROADS_COMPARE\ARCHIVE
# Author:      Jed Harrison
# Created:     2020-07-08
# Copyright:   (c) jedharri 2020
# -------------------------------------------------------------------------------
# Version Update:
# File: change_detector.py
# Location: https://github.com/MaddiLoo/RoadIntegration
# Last Modified: 2021-11-04
# Modified By: Natalie Jackson
# Copyright (c) 2021 Natalie Jackson, GeoBC
# -------------------------------------------------------------------------------
# Version Update:
# Last Modified: 2022-06-09
# Modified By: Refractions Reasearch (Emily Gouge)
# Copyright (c) 2021 Natalie Jackson, GeoBC
# -------------------------------------------------------------------------------

# -------------------------------------------------------------------------------
# IMPORTS
# -------------------------------------------------------------------------------
import os
import re
import hashlib
from osgeo import ogr, osr
import sqlite3
import datetime
import logging
from core import utils
from enum import Enum

# -------------------------------------------------------------------------------
# GLOBAL VARIABLES
# -------------------------------------------------------------------------------
_logger = logging.getLogger(__name__)

# Field names for sqlite tables
# These field names must match between comparison dates, so edit with caution.
class FieldName(Enum):
    ID = 'has_table_id'
    CHANGE_SUMMARY = 'change_id'
    SRC_PKEY = 'source_primary_key'
    GEOM_WKT = 'geometry_wkt'
    ATTRIBUTE_HASH = 'attribute_hash'
    GEOMETRY_HASH = 'geometry_hash'
    FULL_HASH = 'full_hash'
    CHANGE_TYPE = 'change_type'
    ATTRIBUTES_MOD = "attributes_modified"

# Hash table field names as list, not including id_fieldname:
_change_detect_fields = [
    FieldName.GEOM_WKT, 
    FieldName.ATTRIBUTE_HASH,
    FieldName.GEOMETRY_HASH,
    FieldName.FULL_HASH
]

#-------------------------------------------------------------------------------
# setup logging

# Only need to call this function is using change detector in multi-threaded environment
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
    fileloghandler = logging.FileHandler(os.path.join(utils.log_folder, "Change_Detection_Manual_Compare_" + utils.rundatetime + "_2.txt"), mode='a', encoding="utf-8",)
    fileloghandler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fileloghandler.setFormatter(formatter)
    _logger.addHandler(fileloghandler)

    
# -------------------------------------------------------------------------------
# detects changes between two datasets
# using the paramenters provider
# -------------------------------------------------------------------------------
def detect_changes(
    provider_db,
    provider_name_raw,
    source_data_folder,
    source_dataset_name,
    source_database_name,
    log_folder_path,
    output_folder_path,
    source_data_type,
    provider_attribute_fields,
    provider_reference_fields=[],  # NB: Optional
):
    """
    Primary function called by py.py that detects changes between two datasets.
    Can also be called by change_detector_caller_testing_only.py.

    Parameters:
        provider_db (string)
            - Full path of sqlite database where changes are stored
        provider_name_raw (string)
            - Unique name of provider
        source_data_folder (string)
            - Location where source data is saved
        source_dataset_name (string)
            - Name of file within folder (if applicable)
        source_database_name
            - Name of database (if applicable)
        log_folder_path (string)
            - Location where log file is saved
        output_folder_path (string)
            - Location where geodatabases of changes are staged for action
        source_data_type (string)
            - OGR file type
        provider_attribute_fields (list of strings)
            - List of attribute fields to be compared between old and new versions of dataset
        provider_reference_fields (list of strings) (optional)
            - List of attribute fields that will not be compared, but maintained as reference
            values in the new dataset (for example, a persistent ID field that is not an Object ID)

    Dependencies (global variables):
        None
        
    Returns:
        Dictionary of processing statistics. See utils.DataStatistic for statistics tracked
    """
    
    providerstats = {}
    
    # Calculate and display the start time for the detect_changes function
    start_time = datetime.datetime.now()
    _logger.info(f"Change Detection Start: {provider_name_raw} Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Connect to sqlite database for the specified provider
    db_connection = sqlite3.connect(provider_db)

    # Provider-specific parameters:
    src_name = source_dataset_name
    if source_database_name:
        src_name = source_database_name
    
    #source dataset path
    source_data_path = os.path.join(source_data_folder, src_name)
    
    #if not found, look through subfolders
    if (not os.path.exists(source_data_path)):
        _logger.debug(f"""File {source_data_path} not found. Searching {source_data_folder} for filename.""")
        
        #search source data folder for filename
        #this deals with case where zip files are hidden within folders
        for root, dir, files in os.walk(source_data_folder):
            if src_name in files:
                source_data_path = os.path.join(root, src_name)
                break
        
        #check exists still
        if (not os.path.exists(source_data_path)):
            _logger.debug(f"""File {source_data_path} not found in {source_data_folder}.""")
            raise Exception(f"""File {source_data_path} not found in {source_data_folder}.""")
        
    

    # Remove non-alphanumeric-and-underscore characters from provider ID
    # to prevent SQL injection errors/attacks
    provider_name = scrub(provider_name_raw)

    # Add new data as table to sqlite database with hash attributes,
    # or identify the table if it already exists with today's data
    
    new_table = f"{provider_name}_{utils.today_date_string}"
    load_data_and_compute_hash(
        db_connection,
        new_table,
        source_data_path,
        None,
        source_data_type,
        provider_attribute_fields,
        provider_reference_fields,
    )
    providerstats[utils.DataStatistic.NEW_DATA_TABLE] = new_table
    
    # Identify features with duplicates (same geometry and attributes)
    # in the new table
    duplicate_features = find_duplicate_features(db_connection, new_table)
    providerstats[utils.DataStatistic.NUM_NEW_DUPLICATE_RECORDS] = len(duplicate_features[0])
    providerstats[utils.DataStatistic.NEW_DUPLICATE_RECORDS] = duplicate_features[1]
    _logger.debug(duplicate_features[1])

    # Identify most recent existing version of data to compare with new version,
    # or return null value if no other versions exist
    old_table = identify_old_table(db_connection, provider_name)
    providerstats[utils.DataStatistic.OLD_DATA_TABLE] = old_table
    
    
    # Compare old and new versions of data
    if old_table:
        
        duplicate_features = find_duplicate_features(db_connection, old_table)
        providerstats[utils.DataStatistic.NUM_OLD_DUPLICATE_RECORDS] = len(duplicate_features[0])
        providerstats[utils.DataStatistic.OLD_DUPLICATE_RECORDS] = duplicate_features[1]
    
        # Compare the two tables for changes
        #comparison_object = compare_tables(db_connection, new_table, old_table)
        _logger.info(f"Creating and populating change table for {provider_name}")
    
        # Extract date of each table, format "YYYYMMDD"
        old_table_date = old_table[-10:].replace("_", "")
        new_table_date = new_table[-10:].replace("_", "")

        # Define name of change summary table
        # Format: ProviderName_fromYYYYMMDD_toYYYYMMDD
        # eg. Mission_from20210312_to20211017
        change_summary_table_name = f"{provider_name}_from{old_table_date}_to{new_table_date}"
        
        # Create and populate change summary table
        change_table = create_and_populate_change_table(
            db_connection,
            new_table,
            new_table_date,
            old_table,
            old_table_date,
            change_summary_table_name,
            provider_attribute_fields,
            provider_reference_fields,
        )
        
        #compute stats
        compute_stats(db_connection, new_table, old_table, change_table, providerstats)
            
        
        _logger.info(f"Exporting change table for {provider_name}")
        gpkg_file_name = os.path.join(output_folder_path, provider_name + "_" + utils.today_date_string + '_Changes.gpkg')
        export_change_table(change_table, db_connection, gpkg_file_name)
        

    else:
        _logger.info(f"Only one table for {provider_name} in database; nothing to compare!")

    # Create log file recording actions taken by this script
    utils.write_log_file(log_folder_path, provider_name, providerstats)

    # TODO: drop any unnecessary tables from database to avoid storing large amounts of
    # TODO: obsolete/unnecessary data. (Which tables to drop?)
    # TODO: See tidy_database.py in this repository for an example of table-dropping.

    # Calculate and display the run time of the detect_changes function
    end_time = datetime.datetime.now()
    run_duration = end_time - start_time
    _logger.info(f"Change Detection End: {provider_name_raw} Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    _logger.info(f"Change Detection Duration: {provider_name_raw} Duration: {run_duration}")
    
    return providerstats

#-------------------------------------------------------------------------------
# Compute statistic for data sets and changes
#-------------------------------------------------------------------------------
def compute_stats(db_connection, new_table, old_table, change_table, providerstats):
    """
    Primary function called by py.py that detects changes between two datasets.
    Can also be called by change_detector_caller_testing_only.py.

    Parameters:
        db_connection
            - database connection
        new_table
            - name of new data table
        old_table
            - name of original data table
        change_Table
            - name of table containing data changes
        providerstats
            - dictionary to be populated with various statistics 
            
        
    Returns: Nothing
        
    """
    
    cursor = db_connection.cursor()
    try:
        providerstats[utils.DataStatistic.NUM_NEW_RECORDS] = cursor.execute(f"SELECT count(*) FROM {new_table}").fetchone()[0]
        providerstats[utils.DataStatistic.NUM_OLD_RECORDS] = cursor.execute(f"SELECT count(*) FROM {old_table}").fetchone()[0]
            
        counts = cursor.execute(f"SELECT count(*), {FieldName.CHANGE_TYPE.value} FROM {change_table} GROUP BY {FieldName.CHANGE_TYPE.value}").fetchall()
        cnt = 0;
        for field in counts:
            cnt = cnt + field[0]
            if (field[1].lower() == utils.ChangeType.NEW_FEATURE.value):
                providerstats[utils.DataStatistic.NUM_NEW_FEATURES] = field[0]
            elif (field[1].lower() == utils.ChangeType.REMOVED_FEATURE.value):
                providerstats[utils.DataStatistic.NUM_REMOVED_FEATURES] = field[0]
            elif (field[1].lower() == utils.ChangeType.UPDATED_ATTRIBUTES.value):
                providerstats[utils.DataStatistic.NUM_FEATURES_ATTRIBUTE_CHANGES] = field[0]
        providerstats[utils.DataStatistic.TOTAL_CHANGES] = cnt
            
    finally:
        cursor.close()

#-------------------------------------------------------------------------------
# cleans a string for database use
#-------------------------------------------------------------------------------
def scrub(dirty_string):
    """
    Takes a string and removes non-alphanumeric-and-underscore characters.

     Parameters:
        dirty_string (string)
            -String containing any characters.

    Dependencies (global variables):
        n/a

    Returns:
        clean_string (string)
            -String with spaces replaced with underscores and all other punctuation removed.
    """
    # Strip whitespace from beginning and end of string
    clean_string = dirty_string.strip()

    # Replace spaces with underscores
    clean_string = clean_string.replace(" ", "_")

    # Remove all non alpha-numeric and underscore characters
    clean_string = re.sub(r"\W+", "", clean_string)

    return clean_string

#-------------------------------------------------------------------------------
# Loads a dataset into the database and computes various hash fields
#-------------------------------------------------------------------------------
def load_data_and_compute_hash(
    db_connection,
    table_name,
    source_data_path,
    source_data_layer,
    source_data_type,
    provider_attribute_fields,
    provider_reference_fields,
):   
    """
    Add new data as table to sqlite database with hash attributes and load data from
    data source into this table

    Parameters:
        db_connection (connection object)
            - Connection to sqlite database
        table_name
            - Name of database table to load data inot
        source_data_path (string)
            - Full path to data (eg. shapefile) or database
        source_data_layer (string)
            - Name of the dataset (only used by function when dataset is in a database)
        source_data_type (string)
            - Can be None or OGR file type
        provider_attribute_fields (list of strings)
            - List of attribute fields used to create attribute hash
        provider_reference_Fields (list of strings)
            - List of attribute fields that will not be compared, but maintained as reference
            values in the new dataset (for example, a persistent ID field that is not an Object ID)

    Dependencies (global variables):
        date_string (string)
            - Today's date with format _YYYY_MM_DD
        id_fieldname (string)
            - Name of field to auto increment with primary key
        change_detect_fields (list of strings)
            - List of fields used for change detection (defined in runapp())
        bc_albers_epsg
            - EPSG code for BC Albers (defined in utils)

    Returns:
        new_table (string)
            - Name of new table with hashed data
                - Format: ProviderName_YYYY_MM_DD, where the date is today
                - eg. "Mission_2021_07_20"
                - If a table for today all ready exists, return the name
                    of that table without re-generating rows
    Raises:
        Exception
            - When error occurs reading data or loading data
    """

    _logger.info("Loading raw data for dataset %s into table %s", source_data_path, table_name)
    _logger.debug("Source Path: %s", source_data_path)
    _logger.debug("Source Layer: %s", source_data_layer)
    _logger.debug("Source Data Type: %s", source_data_type)
    
    
    # Check if a table exists with today's date
    _logger.debug("checking for existing table: %s", table_name)
    cursor = db_connection.cursor()
    try:
        table_check = cursor.execute(f"""
            SELECT name 
            FROM sqlite_master
            WHERE type='table' AND name='{table_name}'"""
        ).fetchone()
    finally:
        cursor.close()
        
    # Create table already exists; remove it and reload data
    if table_check:
        #drop existing table before loading new data
        _logger.debug("table %s will be dropped and reloaded", table_name)
        
        cursor = db_connection.cursor()
        try:
            cursor.execute(f"drop table {table_name}")
        finally:
            cursor.close
        db_connection.commit()
        
    #if not table_check:
    _logger.debug("creating table: %s ", table_name)
    create_dataset_table(
        db_connection, 
        table_name, 
        provider_attribute_fields, 
        provider_reference_fields
    )

    # Get provider data from source
    data_source = None
    if (source_data_type is None):
        data_source = utils.find_data_source(source_data_path)
    else:
        driver = ogr.GetDriverByName(source_data_type) 
        data_source = driver.Open(source_data_path)
        
    if data_source is None:
        msg = f"""Unable to open dataset (type: {source_data_type}, location: {source_data_path})"""
        _logger.error(msg)
        raise Exception(msg)
    else:
        _logger.debug("connected to data source %s", source_data_path)
        

    # Concatenate all provider attribute fields to single list
    all_provider_fields = provider_reference_fields + provider_attribute_fields
    
    # Generate well-known-text and hash values and populate table
    # If the datasource is in a database - specify layer name,
    # for a file there will be only one layer
    if source_data_layer is not None:
        layer = data_source.GetLayer(source_data_layer)
        if (layer is None):
            raise Exception (f"The layer {source_data_layer} not found in file {source_data_path}")
    else:
        layer = data_source.GetLayer()
    
    #validate that schema has expected attributes
    layer_def = layer.GetLayerDefn()
    notfound = []
    for field in all_provider_fields:
        index = layer_def.GetFieldIndex(field)
        if index < 0:
            #field not found record message and throw exception
            notfound.append(field)
    
    if len(notfound) > 0:
        raise Exception(f"The following fields specified in the configuration file do not exist in the data source: {', '.join(notfound)}")
    
    #target BC Albers    
    crs_target = osr.SpatialReference()
    crs_target.ImportFromEPSG(utils.bc_albers_epsg)
    
    crs_source = layer.GetSpatialRef()
    
    _logger.debug(f"Source CRS: {crs_source}")
    _logger.debug(f"Target CRS: {crs_target}")
    
    transform = osr.CoordinateTransformation(crs_source, crs_target)
    
    _logger.debug(f"transform: {transform}")
    
    if transform is None:
        raise Exception(f"Could not find transform to reproject between {crs_source} and {crs_target}.")
    
    feature = layer.GetNextFeature()
    _logger.debug(
        "Generating well-known-text and hash values, and populating table with unique IDs, "
        "provider attribute fields, well-known-text, and hash values. "
        "This step can take several minutes."
    )
        
    sql_insert = f"INSERT INTO {table_name} VALUES (?, ?,"
    for field in all_provider_fields:
        sql_insert += "?, "  # Add placeholder to sql insert statement for each field
    # Add placeholders for new fields (Geometry_WKT, Attribute Hash, Geom_Hash, Full_Hash)
    sql_insert += "?, ?, ?, ?)"  
        
    while feature:
        # Identify the value of the unique FID in the source data
        provider_Primary_Key_value = feature.GetFID()

        # Initiate list of values to replace ? placeholders in sql_insert statement.
        # First value is None; as the primary key for the table, it will auto-increment
        # Second value is the primary key used by the provider
        values_list = [None, provider_Primary_Key_value]

        # Initiate string of attribute values to be hashed
        attribute_text = ""

        # Add attributes to values list; add non-reference attributes to text for hashing
        for field in all_provider_fields:
            attribute_value = feature.GetFieldAsString(field)
            if attribute_value:
                values_list.append(attribute_value)
                if field in provider_attribute_fields:
                    attribute_text += attribute_value
            else:
                values_list.append(None)
                #TODO: what to do with null values 
                #as it stands now null will be considered the same as empty string
                #if field in provider_attribute_fields:
                #    attribute_text += attribute_value

        # Get geometry as WKT
        geometry = feature.geometry()
        geometry.Transform(transform)
        
        geom_text = geometry.ExportToWkt()
        # TODO: Need to add method call here to reduce precision - may prevent detection
        # TODO: of unintentional changes from differing export and conversion processes
        values_list.append(geom_text)

        # Create hash for attributes
        attribute_hash = hashlib.sha256((attribute_text.encode("utf-8"))).hexdigest()
        values_list.append(attribute_hash)

        # Create hash for geometry
        geom_hash = hashlib.sha256((geom_text.encode("utf-8"))).hexdigest()
        values_list.append(geom_hash)

        # Create hash for combined attributes and geometry
        full_hash = hashlib.sha256(
            ((f"{attribute_text}{geom_text}").encode("utf-8"))
        ).hexdigest()
        values_list.append(full_hash)

        # Convert list to tuple to pass to cursor execute method
        values_tuple = tuple(values_list)

        # Create cursor and execute sql statement to insert row (feature) into table
        cursor = db_connection.cursor()
        try:
            cursor.execute(sql_insert, values_tuple)
        finally:
            cursor.close()
        # Destroy the current GetNextFeature object
        feature.Destroy()
    
        # Create the next GetNextFeature object to iterate through features
        feature = layer.GetNextFeature()
        
    db_connection.commit()
    _logger.debug("Done generating wkt, hashes, and populating table.")
    
    return table_name

#-------------------------------------------------------------------------------
# Creates a table for dataset
#-------------------------------------------------------------------------------
def create_dataset_table(
    db_connection, 
    table_name, 
    provider_attribute_fields, 
    provider_reference_fields
):
    """
    Create provider data table.

    Parameters:
        db_connection (connection object)
            - Connection to sqlite database
        table_name (string)
            - Name of table to be created
        provider_attribute_fields (list of strings)
            - List of attribute fields used to create attribute hash
        provider_reference_fields (list of strings)
            - List of attribute fields that will not be compared, but maintained as reference
        values in the new dataset (for example, a persistent ID field that is not an Object ID)


    Dependencies (global variables):
        FieldName (enum)
            - names for various additional fields added to table
        
    Returns:
        n/a
    """
    all_text_fields_list = (
        provider_reference_fields + provider_attribute_fields
    )
    for field in _change_detect_fields:
        all_text_fields_list.append(field.value)

    create_sqlite_table(db_connection, table_name, FieldName.ID.value, 1, all_text_fields_list)


#-------------------------------------------------------------------------------
# converts list to string
#-------------------------------------------------------------------------------
def list_to_string_with_type(field_names_list, field_type="text"):
    """
    Create a string from a list with commas and field type descriptor (default is 'text')
    Intended use: to define the fields for a sqlite table based on a list of field names
    eg:
        Input: [Street, Surface, Speed_Limit], "text"
        Output: "Street text, Surface text, Speed_Limit text"

    Parameters:
        field_names_list (list of strings)
        field_type (string) (optional)
            - Intended input: valid sqlite data type (capitalization flexible)
            - Default: "text"

    Dependencies (global variables):
        n/a

    Returns:
        fields_as_string_with_type (string)
            - Fields as string with sqlite data type,
                ready to input in sql statement
    """
    # Initialize return variable
    fields_as_string_with_type = ""

    # Iterate through each item in list
    for field in field_names_list:

        # Remove non-sqlite-friendly characters from field name to prevent SQL injection attack
        clean_field = scrub(field)

        # Add clean field name and field type to string
        fields_as_string_with_type += f"{clean_field} {field_type}"

        # If there are more items in the list, add a comma and a space to the string
        if field != field_names_list[-1]:
            fields_as_string_with_type += ", "

    # Return the populated string
    return fields_as_string_with_type

#-------------------------------------------------------------------------------
# create database table
#-------------------------------------------------------------------------------
def create_sqlite_table(
    db_connection,
    table_name,
    primary_key_field,
    table_type,
    text_fields_list=[],
    numeric_fields_list=[],
    blob_fields_list=[],
):
    """
    Create table in sqlite database.

    Parameters:
        db_connection (connection object)
            - Connection to sqlite database
        table_name (string)
            - Name of table to create
        primary_key_field (string)
            - Name of primary key field for new table
        table_type (int)
            - Type 1: WKT, hashed data table
            - Type 2: Change Summary Table
        text_fields_list (string) (optional)
            - List of field names with TEXT data type
        numeric_fields_list (string) (optional)
            - List of field names with NUMERIC data type (can be int or real)
        blob_fields_list (string) (optional)
            - List of field names with BLOB data type

    Dependencies (global variables):
        source_primary_key_fieldname (string)
            - Local name for source primary key field

    Returns:
        n/a
    """
    if not text_fields_list and not numeric_fields_list and not blob_fields_list:
        raise Exception("Please specify schema for new table; see create_sqlite_table function")
        
    
    # Remove non-alphanumeric characters and replace spaces with underscores from
    # primary key field name and source primary field key name.
    # (They're hardcoded into this script, but since we have the technology...)
    primary_key_field_clean = scrub(primary_key_field)
    if table_type >= 1:
        source_primary_key_fieldname_clean = scrub(FieldName.SRC_PKEY.value)

    # Define sql schema statement
    schema_statement = (
        f"CREATE TABLE {table_name} ({primary_key_field_clean} integer PRIMARY KEY, "
    )
    if table_type == 1:  # Source Primary Key of new data
        schema_statement += f"{source_primary_key_fieldname_clean} integer, "
    elif table_type == 2:  # Source Primary Keys of old and new data
        change_type_fieldname_clean = scrub(FieldName.CHANGE_TYPE.value)
        schema_statement += (
            f"{change_type_fieldname_clean} text, "
            f"{source_primary_key_fieldname_clean}_{table_name[-19:-11]} integer, "
            f"{source_primary_key_fieldname_clean}_{table_name[-8:]} integer, "
        )
    if text_fields_list:
        schema_statement += list_to_string_with_type(text_fields_list, "text")
        if numeric_fields_list or blob_fields_list:
            schema_statement += ", "
    if numeric_fields_list:
        schema_statement += list_to_string_with_type(numeric_fields_list, "numeric")
        if blob_fields_list:
            schema_statement += ", "
    if blob_fields_list:
        schema_statement += list_to_string_with_type(blob_fields_list, "blob")
    schema_statement += ")"

    _logger.debug(f"Schema of {table_name}: {schema_statement}")

    # Execute commands to create table in database
    cursor = db_connection.cursor()
    try:
        cursor.execute(schema_statement)
    finally:
        cursor.close()
        
    db_connection.commit()

#-------------------------------------------------------------------------------
# find features with the same geometry and attributes in given table
#-------------------------------------------------------------------------------
def find_duplicate_features(db_connection, table_name):
    """
    Identify duplicate records in sqlite table (same geometry and attributes)

    Parameters:
        db_connection (connection object)
            - Connection to sqlite database
        table_name (string)
            - Name of sqlite table in database with full hash attribute

    Dependencies (global variables):
        source_primary_key_fieldname (string)
            - Local name for primary key from provider data
        full_hash_fieldname (string)
            - Fieldname for full hash attribute

    Returns:
        tuple
            duplicates_set (set string)
                - List of unique duplicate ids
            duplicates_message (string)
                - Descriptive message that identifies duplicate features
    """
    
    # duplicates don't affect change detection logic,
    # but someone might care about redundant records
    # duplicates are added to output statistics

    sql_statement = f"""
        SELECT {FieldName.SRC_PKEY.value} 
        FROM {table_name} 
        GROUP BY {FieldName.FULL_HASH.value} 
        HAVING COUNT(*) >1
    """
    
    cursor = db_connection.cursor()
    try:
        cursor.execute(sql_statement)
        all_duplicates = cursor.fetchall()
    finally:
        cursor.close()
        
    ids = set()
    for row in all_duplicates:
        ids.add(str(row[0]))
    
    if len(ids) > 0:
        duplicates_message = f"Primary key values from original data of features with duplicates in {table_name}: {', '.join(ids)}" 
    else:
        duplicates_message = f"No duplicate features in {table_name}."

    return (ids, duplicates_message)

#-------------------------------------------------------------------------------
# Return name of most recent table, e.g., "Mission_2021_06_14",
# or None if no previous versions exist
#-------------------------------------------------------------------------------
def identify_old_table(db_connection, provider_name):
    """
    Search sqlite database and identify most recent
    existing version of data (before today).

    Parameters:
        db_connection (connection object)
            - Connection to sqlite database
        provider_name (string)
            - Unique name of data provider
            - eg. "Mission"

    Dependencies (global variables):
        n/a

    Returns:
        old_table (string or None)
            - Name of table of hashed data from most recent existing version,
            or,
            - None if there are no existing versions.
    """
    _logger.debug(f"""Searching for previous data for {provider_name}""")
    
    # Sort hash tables for specified provider in database from newest to oldest
    # Exclude change summary tables ("Mission_from20210922_to20211103")
    
    sql_query = f"""
        SELECT name FROM sqlite_master WHERE type='table' 
        AND name LIKE '{provider_name}\_____\___\___' ESCAPE '\\' 
        AND name NOT LIKE '{provider_name}_from%' 
        ORDER BY name DESC
    """
    
    cursor = db_connection.cursor()
    try:
        cursor.execute(sql_query)
        # Store the sorted table names in a tuple
        all_tables = cursor.fetchall()
    finally:
        cursor.close()

    # Count the number of tables in the sorted tuple
    table_count = len(all_tables)

    # If there is more than one table in the tuple,
    # set old_table to the name of the second table in the sorted tuple.
    if table_count > 1:
        old_table = all_tables[1][0]
    # If there is only one table in the tuple,
    # set old_table to None
    else:
        old_table = None

    _logger.debug(f"""Previous data table for {provider_name}: {old_table}""")

    # Return name of most recent table, e.g., "Mission_2021_06_14",
    # or None if no previous versions exist
    return old_table

#-------------------------------------------------------------------------------
# creates an populates change table
#-------------------------------------------------------------------------------
def create_and_populate_change_table(
    db_connection,
    new_table,
    new_table_field_suffix,
    old_table,
    old_table_field_suffix,
    change_table,
    provider_attribute_fields,
    provider_reference_fields,
):
    """
    Create new table within SQLite databse detailing the differences
    between the old and new tables and populates this table with the dataset differences

    Parameters:
        db_connection (connection object)
            - Connection to sqlite database
        new table
            - Name of table with new data
        new_table_field_suffix
            - Attribute field suffix for new data attribute
        old_table
            - Name of old data table
        old_table_field_suffix
            -Attribute field suffix for old data attribute
        chage_table
            - Name of output change table
        provider_attribute_fields (list of strings) (optional)
            - List of attribute fields used to create attribute hash
        provider_reference_fields (list of strings)
            - List of attribute fields that will not be compared, but maintained as reference
            values in the new dataset (for example, a persistent ID field that is not an Object ID)

    Dependencies (global variables):
        source_primary_key_fieldname (string)
            - Local name for primary key from provider data

    Returns:
        n/a
    """
    
    _logger.info(f"Creating and populating table for {change_table}")

    # Check if change summary table exists in database.
    # If it exists, rename the existing copy with _backup# suffix.
    cursor = db_connection.cursor()
    try:
        table_check = cursor.execute(
            """SELECT name FROM sqlite_master
            WHERE type='table' AND name=?""",
            ([change_table]),
        ).fetchone()
    finally:
        cursor.close()
    
    if table_check:
        backup_table_name = ""
        backup_table_check = True
        i = 1
        while backup_table_check:
            backup_table_name = f"{change_table}_backup{str(i)}"
            
            cursor = db_connection.cursor()
            try:    
                backup_table_check = cursor.execute(
                    """SELECT name FROM sqlite_master
                    WHERE type='table' AND name=?""",
                    ([backup_table_name]),
                ).fetchone()
                i += 1
            finally:
                cursor.close()
        sql_alter = f"ALTER TABLE {change_table} " f"RENAME TO {backup_table_name}"
        
        cursor = db_connection.cursor()
        try:
            cursor.execute(sql_alter)
        finally:
            cursor.close()
        db_connection.commit()

    # Create list of text fields for change summary table
    table_text_fields = provider_reference_fields + provider_attribute_fields
    old_data_fields = [f"{fieldname}_{old_table_field_suffix}" for fieldname in table_text_fields]
    new_data_fields = [f"{fieldname}_{new_table_field_suffix}" for fieldname in table_text_fields]
    
    change_summary_table_text_fields = old_data_fields + new_data_fields
    change_summary_table_text_fields.append(FieldName.GEOM_WKT.value)

    # Create the change summary table
    create_sqlite_table(
        db_connection,
        change_table,
        FieldName.ID.value,
        2,
        change_summary_table_text_fields,
    )
    
    
    # do change detection with database queries
    changetypefield = scrub(FieldName.CHANGE_TYPE.value)
    oldchangefields = ','.join(old_data_fields)
    
    atable_text_fields = [f"a.{fieldname}" for fieldname in table_text_fields]
    arawfields = ','.join(atable_text_fields)
    
    btable_text_fields = [f"b.{fieldname}" for fieldname in table_text_fields]
    brawfields = ','.join(btable_text_fields)
    newchangefields = ','.join(new_data_fields)
    
    # find features that have been removed
    query = f"""
        insert into {change_table} 
        ({changetypefield},{oldchangefields},{FieldName.GEOM_WKT.value})
        SELECT '{utils.ChangeType.REMOVED_FEATURE.value}', {arawfields}, a.{FieldName.GEOM_WKT.value}
        FROM {old_table} a
        WHERE a.{FieldName.GEOMETRY_HASH.value} NOT IN (
        select b.{FieldName.GEOMETRY_HASH.value} FROM {new_table} b) 
    """
    cursor = db_connection.cursor()
    try:
        cursor.execute(query)
    finally:
        cursor.close()
    
    # find new features 
    query = f"""
        insert into {change_table} 
        ({changetypefield},{newchangefields},{FieldName.GEOM_WKT.value})
        SELECT '{utils.ChangeType.NEW_FEATURE.value}', {brawfields}, b.{FieldName.GEOM_WKT.value}
        FROM {new_table} b
        WHERE b.{FieldName.GEOMETRY_HASH.value} NOT IN (
        select a.{FieldName.GEOMETRY_HASH.value} FROM {old_table} a) 
    """
    cursor = db_connection.cursor()
    try:
        cursor.execute(query)
    finally:
        cursor.close()    

    #same geometry difference attributes
    query = f"""
        insert into {change_table} 
        ({changetypefield},{oldchangefields},{newchangefields},{FieldName.GEOM_WKT.value})
        SELECT '{utils.ChangeType.UPDATED_ATTRIBUTES.value}', {arawfields}, {brawfields}, a.{FieldName.GEOM_WKT.value}
        FROM {new_table} a join {old_table} b on a.{FieldName.GEOMETRY_HASH.value} = b.{FieldName.GEOMETRY_HASH.value}
        WHERE a.{FieldName.ATTRIBUTE_HASH.value} != b.{FieldName.ATTRIBUTE_HASH.value}    
    """
    cursor = db_connection.cursor()
    try:
        cursor.execute(query)
    finally:
        cursor.close()
    
    #add a field for changed attributes
    query = f"alter table {change_table} add column {FieldName.ATTRIBUTES_MOD.value} varchar"
    cursor = db_connection.cursor()
    try:
        cursor.execute(query)
    finally:
        cursor.close()
        
    #find fields which have changed
    query = f"UPDATE {change_table} set {FieldName.ATTRIBUTES_MOD.value} = "
    query += "substr("
    for field in provider_attribute_fields:
        query += f"case when {field}_{old_table_field_suffix} is not {field}_{new_table_field_suffix} then ',{field}' else '' end || "
    
    query = query[:-4]
    query += ", 2)"
    query += f" WHERE {changetypefield} = '{utils.ChangeType.UPDATED_ATTRIBUTES.value}'"
    
    _logger.debug(f"Attribute change query: {query}")
    cursor = db_connection.cursor()
    try:
        cursor.execute(query)
    finally:
        cursor.close()
    
    db_connection.commit()


    return change_table;

#-------------------------------------------------------------------------------
# export change table to geopackage file
#-------------------------------------------------------------------------------
def export_change_table(change_table, db_connection, gpkg_file_name):
    """
    Export Changes to geopackage file

    Parameters:
        change_table (string)
            - Name of change table to be exported for review            
        db_connection (connection object)
            - Connection to sqlite database
        gpkg_file_name(string)
            - Output file name

    Dependencies (global variables):
        date_string (string)
            - string used to create output folder
        bc_albers_epsg
            - BC Albers EPSG code (defined in utils)
    Returns:
        
    """
    
    _logger.info(f"exporting changes to {gpkg_file_name}")
    
    rowcount = 0
    cursor = db_connection.cursor()
    try:
        cursor.execute(f"SELECT count(*) FROM {change_table}")
        #First determine if there are any rows in the table - the change detector code currently creates a table regardless
        rowcount = cursor.fetchone()[0]
    finally:
        cursor.close()
        
    if rowcount > 0:
        #Create new empty geopackage with today's date
        #and export data

        _logger.debug(f"export file: {gpkg_file_name}")
        
        if os.path.exists(gpkg_file_name):
            _logger.debug(f"file {gpkg_file_name} exists and will be replace with new version")
            os.remove(gpkg_file_name)
        
        #make folder 
        outdir = os.path.dirname(gpkg_file_name)
        if not os.path.exists(outdir):
            os.makedirs(outdir)
                 
                 
        gis_output = ogr.GetDriverByName('GPKG').CreateDataSource(gpkg_file_name)
        if gis_output is None:
            raise Exception(f"Unable to create output geopackage file {gpkg_file_name}. Ensure parent directory exists.")
        #everything gets written as BC Albers
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(utils.bc_albers_epsg)
            
        #layer per geometry type
        #merge all single types into their multitypes
        layer_by_geom_type = {}
        
    
        #Iterate the schema and created dictionary for geopackage output fields
        schema = {}
        query_fields = '' #string placeholder for query
       
        #TODO: NOTE the PRAGMA function is SQLite specific
        cursor = db_connection.cursor()
        try:
            cursor.execute(f"PRAGMA table_info({change_table})")
            for row in cursor.fetchall():                
                schema[row[1]] = row[2] #map column name to datatype
        finally:
            cursor.close()
        
        has_multi = False    
        query = f"SELECT {FieldName.GEOM_WKT.value} FROM {change_table}"
        cursor = db_connection.cursor()
        try:
            cursor.execute(query)
            for row in cursor.fetchall():
                geom = ogr.CreateGeometryFromWkt(row[0])
            
                geomtype = geom.GetGeometryType()
                if (geomtype == ogr.wkbMultiPoint or
                    geomtype == ogr.wkbMultiLineString or
                    geomtype == ogr.wkbMultiPolygon or
                    geomtype == ogr.wkbMultiCurve or
                    geomtype == ogr.wkbMultiSurface or
                    geomtype == ogr.wkbMultiCurveZ or
                    geomtype == ogr.wkbMultiSurfaceZ or
                    geomtype == ogr.wkbMultiPointM or
                    geomtype == ogr.wkbMultiLineStringM or
                    geomtype == ogr.wkbMultiPolygonM or
                    geomtype == ogr.wkbMultiCurveM or
                    geomtype == ogr.wkbMultiSurfaceM or
                    geomtype == ogr.wkbMultiPointZM or
                    geomtype == ogr.wkbMultiLineStringZM or
                    geomtype == ogr.wkbMultiPolygonZM or
                    geomtype == ogr.wkbMultiCurveZM or
                    geomtype == ogr.wkbMultiSurfaceZM or
                    geomtype == ogr.wkbMultiPoint25D or
                    geomtype == ogr.wkbMultiLineString25D or
                    geomtype == ogr.wkbMultiPolygon25D):
                    
                    has_multi = True
                    break                
        finally:
            cursor.close()
       
        if has_multi:
            _logger.info("Data contains both multi and single geometries. All output will be converted to multi geometries.")
       
        for key in sorted(schema):
            query_fields += (key + ',')
        query_fields = query_fields[:-1] #remove trailing comma
        query = f"SELECT {query_fields} FROM {change_table}"

        cursor = db_connection.cursor()
        try:
            cursor.execute(query)
            for row in cursor.fetchall():
                
                incrementor = 0 #increment a field index in same order as the query
                geom = None
                fields = {}
                for key in sorted(schema):
                    if key.lower() == FieldName.GEOM_WKT.value.lower():
                        geom = ogr.CreateGeometryFromWkt(row[incrementor])
                        
                        if (has_multi):
                            #convert single to multi
                            if (geom.GetGeometryType() == ogr.wkbLineString or
                                geom.GetGeometryType() == ogr.wkbLineString25D or
                                geom.GetGeometryType() == ogr.wkbLineStringM or
                                geom.GetGeometryType() == ogr.wkbLineStringZM):
                                geom = ogr.ForceToMultiLineString(geom) 
                            if (geom.GetGeometryType == ogr.wkbPolygon25D or
                                geom.GetGeometryType == ogr.wkbPolygonM or
                                geom.GetGeometryType == ogr.wkbPolygonZM or
                                geom.GetGeometryType == ogr.wkbPolygon):
                                geom = ogr.ForceToMultiPolygon(geom)     
                            if (geom.GetGeometryType == ogr.wkbPoint25D or
                                geom.GetGeometryType == ogr.wkbPointM or
                                geom.GetGeometryType == ogr.wkbPointZM or
                                geom.GetGeometryType == ogr.wkbPoint):
                                geom = ogr.ForceToMultiPoint(geom)    
                    else:
                        if row[incrementor]:
                            fields[key] = row[incrementor]
                    incrementor += 1
                    
                #get layer based on geometry type
                if geom.GetGeometryType() in layer_by_geom_type:
                    layer = layer_by_geom_type[geom.GetGeometryType()]
                else:
                    _logger.debug(f"Create layer in output dataset for geometry type: {geom.GetGeometryType()}")
                    layer = gis_output.CreateLayer(change_table + "_" + geom.GetGeometryName(), srs, geom.GetGeometryType())
                    #sort the schema by field name then add the integer and text fields
                    for key, value in sorted(schema.items()):
                        if not key.lower() == FieldName.GEOM_WKT.value.lower():
                            ftype = ogr.OFTString
                            if value.lower() == 'integer':
                                ftype = ogr.OFTInteger;
                            elif value.lower() == 'real':
                                ftype = ogr.OFTReal;
                            
                            layer.CreateField(ogr.FieldDefn(key,ftype))
                            
                    layer_by_geom_type[geom.GetGeometryType()] = layer
                    
                feature = ogr.Feature(layer.GetLayerDefn())                    
                feature.SetGeometry(geom)
                for entry in fields:
                    feature.SetField(entry,fields[entry])
                layer.CreateFeature(feature)
                
                
                feature = None
        finally:
            cursor.close()
        _logger.debug(f"""The table {change_table} successfully exported to {gpkg_file_name}""")
    else:
        _logger.debug(f"""The table {change_table} is empty - no output geopackage created""")

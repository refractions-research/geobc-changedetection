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

# TODO: set security permissions on sqlite database to prevent lock problems?

# -------------------------------------------------------------------------------
# IMPORTS
# -------------------------------------------------------------------------------

# Import standard library modules
import os
import re
import hashlib
from osgeo import ogr, osr
import sqlite3
import datetime
import logging
import cd


# -------------------------------------------------------------------------------
# GLOBAL VARIABLES
# -------------------------------------------------------------------------------
logger = logging.getLogger(__name__)

# Strings with today's date. Note: date_string variable used to create new table name.
date_string = datetime.date.today().strftime("%Y_%m_%d")

# Field names for sqlite tables
# These field names must match between comparison dates, so edit with caution.
id_fieldname = "Hash_Table_ID"
change_summary_id_fieldname = "Change_ID"
source_primary_key_fieldname = "Source_Primary_Key"
geom_wkt_fieldname = "Geometry_WKT"
attribute_hash_fieldname = "Attribute_Hash"
geom_hash_fieldname = "Geometry_Hash"
full_hash_fieldname = "Full_Hash"
change_type_fieldname = "Change_Type"

# Hash table field names as list, not including id_fieldname:
change_detect_fields = [
    geom_wkt_fieldname,
    attribute_hash_fieldname,
    geom_hash_fieldname,
    full_hash_fieldname,
]


    
# -------------------------------------------------------------------------------
# PRIMARY FUNCTION detect_changes(), CALLED BY MAIN.PY
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
    Primary function called by main.py that detects changes between two datasets.
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
        Dictionary of processing statistics. See cd.DataStatistic for statistics tracked
    """
    
    providerstats = {}
    
    # Calculate and display the start time for the detect_changes function
    start_time = datetime.datetime.now()
    logger.info(f"Change Detection Start: {provider_name_raw} Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

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
        logger.debug(f"""File {source_data_path} not found. Searching {source_data_folder} for filename.""")
        
        #search source data folder for filename
        #this deals with case where zip files are hidden within folders
        for root, dir, files in os.walk(source_data_folder):
            if src_name in files:
                source_data_path = os.path.join(root, src_name)
                break
        
        #check exists still
        if (not os.path.exists(source_data_path)):
            logger.debug(f"""File {source_data_path} not found in {source_data_folder}.""")
            raise Exception(f"""File {source_data_path} not found in {source_data_folder}.""")
        
    

    # Remove non-alphanumeric-and-underscore characters from provider ID
    # to prevent SQL injection errors/attacks
    provider_name = scrub(provider_name_raw)

    # Add new data as table to sqlite database with hash attributes,
    # or identify the table if it already exists with today's data
    new_table = load_data_and_compute_hash(
        db_connection,
        provider_name,
        source_data_path,
        source_database_name,
        source_dataset_name,
        source_data_type,
        provider_attribute_fields,
        provider_reference_fields,
    )
    providerstats[cd.DataStatistic.NEW_DATA_TABLE] = new_table
    
    # Identify features with duplicates (same geometry and attributes)
    # in the new table
    duplicate_features = find_duplicate_features(db_connection, new_table)
    providerstats[cd.DataStatistic.NUM_DUPLICATE_RECORDS] = len(duplicate_features[0])
    providerstats[cd.DataStatistic.DUPLICATE_RECORDS] = duplicate_features[1]
    logger.debug(duplicate_features[1])

    # Identify most recent existing version of data to compare with new version,
    # or return null value if no other versions exist
    old_table = identify_old_table(db_connection, provider_name)
    providerstats[cd.DataStatistic.OLD_DATA_TABLE] = old_table
    
    # Compare old and new versions of data
    if old_table:
        # Compare the two tables for changes
        #comparison_object = compare_tables(db_connection, new_table, old_table)

        # Create and populate change summary table
        change_table = create_and_populate_change_summary_table(
            db_connection,
            provider_name,
            new_table,
            old_table,
            provider_attribute_fields,
            provider_reference_fields,
        )
        
        #compute stats
        cursor = db_connection.cursor()
        try:
            providerstats[cd.DataStatistic.NUM_NEW_RECORDS] = cursor.execute(f"SELECT count(*) FROM {new_table}").fetchone()[0]
            providerstats[cd.DataStatistic.NUM_OLD_RECORDS] = cursor.execute(f"SELECT count(*) FROM {old_table}").fetchone()[0]
            
            counts = cursor.execute(f"SELECT count(*), {change_type_fieldname} FROM {change_table} GROUP BY {change_type_fieldname}").fetchall()
            cnt = 0;
            for field in counts:
                cnt = cnt + field[0]
                if (field[1].lower() == cd.ChangeType.NEW_FEATURE.value):
                    providerstats[cd.DataStatistic.NUM_NEW_FEATURES] = field[0]
                elif (field[1].lower() == cd.ChangeType.REMOVED_FEATURE.value):
                    providerstats[cd.DataStatistic.NUM_REMOVED_FEATURES] = field[0]
                elif (field[1].lower() == cd.ChangeType.UPDATED_ATTRIBUTES.value):
                    providerstats[cd.DataStatistic.NUM_FEATURES_ATTRIBUTE_CHANGES] = field[0]
            providerstats[cd.DataStatistic.TOTAL_CHANGES] = cnt
            
        finally:
            cursor.close()
            
            
        export_change_table(change_table,provider_name, db_connection,output_folder_path)
        

    else:
        logger.info(f"Only one table for {provider_name} in database; nothing to compare!")

    # Create log file recording actions taken by this script
    write_log_file(log_folder_path, provider_name, providerstats)

    # TODO: drop any unnecessary tables from database to avoid storing large amounts of
    # TODO: obsolete/unnecessary data. (Which tables to drop?)
    # TODO: See tidy_database.py in this repository for an example of table-dropping.

    # Calculate and display the run time of the detect_changes function
    end_time = datetime.datetime.now()
    run_duration = end_time - start_time
    logger.info(f"Change Detection End: {provider_name_raw} Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Change Detection Duration: {provider_name_raw} Duration: {run_duration}")
    
    return providerstats

# -------------------------------------------------------------------------------
# FUNCTIONS
# -------------------------------------------------------------------------------


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


def load_data_and_compute_hash(
    db_connection,
    provider_name,
    source_data_path,
    source_database_name,
    source_dataset_name,
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
        provider_name (string)
            - Unique name of data provider (eg. "Mission")
        source_data_path (string)
            - Full path to data (eg. shapefile) or database
        source_database_name (string)
            - Name of the database - used only to determine how to create the ogr layer
        source_dataset_name (string)
            - Name of the dataset (only used by function when dataset is in a database)
        source_data_type (string)
            - OGR file type
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
            - EPSG code for BC Albers (defined in cd)

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

    logger.info("Loading raw data for dataset %s", source_dataset_name)
    logger.debug("Source Data Type: %s", source_data_type)
    logger.debug("Source Path: %s", source_data_path)
    logger.debug("Source Database: %s", source_database_name)
    
    # Check if a table exists with today's date
    new_table = f"{provider_name}_{date_string}"
    logger.debug("checking for existing table: %s", new_table)
    
    cursor = db_connection.cursor()
    try:
        table_check = cursor.execute(f"""
            SELECT name 
            FROM sqlite_master
            WHERE type='table' AND name='{new_table}'"""
        ).fetchone()
    finally:
        cursor.close()
        
    # Create table already exists; remove it and reload data
    if table_check:
        #drop existing table before loading new data
        logger.debug("table %s will be dropped and reloaded", new_table)
        
        cursor = db_connection.cursor()
        try:
            cursor.execute(f"drop table {new_table}")
        finally:
            cursor.close
        db_connection.commit()
        
    #if not table_check:
    logger.debug("creating table: %s ", new_table)
    create_provider_data_table(
        db_connection, 
        new_table, 
        provider_attribute_fields, 
        provider_reference_fields
    )

    # Get provider data from source
    driver = ogr.GetDriverByName(source_data_type) 
    data_source = driver.Open(source_data_path)
    if data_source is None:
        msg = f"""Unable to open dataset (type: {source_data_type}, location: {source_data_path})"""
        logger.error(msg)
        raise Exception(msg)
    else:
        logger.debug("connected to data source %s", source_data_path)
        

    # Concatenate all provider attribute fields to single list
    all_provider_fields = provider_reference_fields + provider_attribute_fields
    
    # Generate well-known-text and hash values and populate table
    # If the datasource is in a database - specify layer name,
    # for a file there will be only one layer
    if source_database_name:
        layer = data_source.GetLayer(source_dataset_name)
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
    crs_target.ImportFromEPSG(cd.bc_albers_epsg)
    
    crs_source = layer.GetSpatialRef()
    
    logger.debug(f"Source CRS: {crs_source}")
    logger.debug(f"Target CRS: {crs_target}")
    
    transform = osr.CoordinateTransformation(crs_source, crs_target)
    
    logger.debug(f"transform: {transform}")
    
    if transform is None:
        raise Exception(f"Could not find transform to reproject between {crs_source} and {crs_target}.")
    
    feature = layer.GetNextFeature()
    logger.debug(
        "Generating well-known-text and hash values, and populating table with unique IDs, "
        "provider attribute fields, well-known-text, and hash values. "
        "This step can take several minutes."
    )
    
    
        
    sql_insert = f"INSERT INTO {new_table} VALUES (?, ?,"
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
    logger.debug("Done generating wkt, hashes, and populating table.")
    
    return new_table


def create_provider_data_table(
    db_connection, table_name, provider_attribute_fields, provider_reference_fields
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
        id_fieldname (string)
            - Name of field in new table to contain primary key
        source_primary_key_fieldname (string)
            - Name of field in new table that contains primary key from original data source
        change_detect_fields (list of strings)
            - List of fields used for change detection (defined in runapp())

    Returns:
        n/a
    """
    all_text_fields_list = (
        provider_reference_fields + provider_attribute_fields + change_detect_fields
    )

    create_sqlite_table(db_connection, table_name, id_fieldname, 1, all_text_fields_list)



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
        source_primary_key_fieldname_clean = scrub(source_primary_key_fieldname)

    # Define sql schema statement
    schema_statement = (
        f"CREATE TABLE {table_name} ({primary_key_field_clean} integer PRIMARY KEY, "
    )
    if table_type == 1:  # Source Primary Key of new data
        schema_statement += f"{source_primary_key_fieldname_clean} integer, "
    elif table_type == 2:  # Source Primary Keys of old and new data
        change_type_fieldname_clean = scrub(change_type_fieldname)
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

    logger.debug(f"Schema of {table_name}: {schema_statement}")

    # Execute commands to create table in database
    cursor = db_connection.cursor()
    try:
        cursor.execute(schema_statement)
    finally:
        cursor.close()
        
    db_connection.commit()


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
        SELECT {source_primary_key_fieldname} 
        FROM {table_name} 
        GROUP BY {full_hash_fieldname} 
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


# Return name of most recent table, e.g., "Mission_2021_06_14",
# or None if no previous versions exist
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
    logger.debug(f"""Searching for previous data for {provider_name}""")
    
    # Sort hash tables for specified provider in database from newest to oldest
    # Exclude change summary tables ("Mission_from20210922_to20211103")
    sql_query = f"""
        SELECT name FROM sqlite_master WHERE type='table' 
        AND name LIKE '{provider_name}%' 
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

    logger.debug(f"""Previous data table for {provider_name}: {old_table}""")

    # Return name of most recent table, e.g., "Mission_2021_06_14",
    # or None if no previous versions exist
    return old_table

def create_and_populate_change_summary_table(
    db_connection,
    provider_name,
    new_table,
    old_table,
    provider_attribute_fields,
    provider_reference_fields,
):
    """
    Create new table within SQLite databse detailing the differences
    between the old and new tables and populates this table with the dataset differences

    Parameters:
        db_connection (connection object)
            - Connection to sqlite database
        provider_name (string)
            - Unique name of data provider (eg. "Mission")
        new_table (string)
            - Name of new table
        old_table (string)
            - Name of the old table
        comparison_object (tuple of sets)
            - Full hashes of features in new/old tables with attribute-only/geometry changes
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
    
    logger.info(f"Creating and populating change table for {provider_name}")

    # Extract date of each table, format "YYYYMMDD"
    old_table_date = old_table[-10:].replace("_", "")
    new_table_date = new_table[-10:].replace("_", "")

    # Define name of change summary table
    # Format: ProviderName_fromYYYYMMDD_toYYYYMMDD
    # eg. Mission_from20210312_to20211017
    change_summary_table_name = f"{provider_name}_from{old_table_date}_to{new_table_date}"

    # Check if change summary table exists in database.
    # If it exists, rename the existing copy with _backup# suffix.
    cursor = db_connection.cursor()
    try:
        table_check = cursor.execute(
            """SELECT name FROM sqlite_master
            WHERE type='table' AND name=?""",
            ([change_summary_table_name]),
        ).fetchone()
    finally:
        cursor.close()
    
    if table_check:
        backup_table_name = ""
        backup_table_check = True
        i = 1
        while backup_table_check:
            backup_table_name = f"{change_summary_table_name}_backup{str(i)}"
            
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
        sql_alter = f"ALTER TABLE {change_summary_table_name} " f"RENAME TO {backup_table_name}"
        
        cursor = db_connection.cursor()
        try:
            cursor.execute(sql_alter)
        finally:
            cursor.close()
        db_connection.commit()

    # Create list of text fields for change summary table
    table_text_fields = provider_reference_fields + provider_attribute_fields
    old_data_fields = [f"{fieldname}_{old_table_date}" for fieldname in table_text_fields]
    new_data_fields = [f"{fieldname}_{new_table_date}" for fieldname in table_text_fields]
    
    change_summary_table_text_fields = old_data_fields + new_data_fields
    change_summary_table_text_fields.append(geom_wkt_fieldname)

    # Create the change summary table
    create_sqlite_table(
        db_connection,
        change_summary_table_name,
        change_summary_id_fieldname,
        2,
        change_summary_table_text_fields,
    )
    
    
    # do change detection with database queries
    type = scrub(change_type_fieldname)
    oldchangefields = ','.join(old_data_fields)
    
    atable_text_fields = [f"a.{fieldname}" for fieldname in table_text_fields]
    arawfields = ','.join(atable_text_fields)
    
    btable_text_fields = [f"b.{fieldname}" for fieldname in table_text_fields]
    brawfields = ','.join(btable_text_fields)
    newchangefields = ','.join(new_data_fields)
    
    # find features that have been removed
    query = f"""
        insert into {change_summary_table_name} 
        ({type},{oldchangefields},{geom_wkt_fieldname})
        SELECT '{cd.ChangeType.REMOVED_FEATURE.value}', {arawfields}, a.{geom_wkt_fieldname}
        FROM {old_table} a
        WHERE a.{geom_hash_fieldname} NOT IN (
        select b.{geom_hash_fieldname} FROM {new_table} b) 
    """
    cursor = db_connection.cursor()
    try:
        cursor.execute(query)
    finally:
        cursor.close()
    
    # find new features 
    query = f"""
        insert into {change_summary_table_name} 
        ({type},{newchangefields},{geom_wkt_fieldname})
        SELECT '{cd.ChangeType.NEW_FEATURE.value}', {brawfields}, b.{geom_wkt_fieldname}
        FROM {new_table} b
        WHERE b.{geom_hash_fieldname} NOT IN (
        select a.{geom_hash_fieldname} FROM {old_table} a) 
    """
    cursor = db_connection.cursor()
    try:
        cursor.execute(query)
    finally:
        cursor.close()    

    #same geometry difference attributes
    query = f"""
        insert into {change_summary_table_name} 
        ({type},{oldchangefields},{newchangefields},{geom_wkt_fieldname})
        SELECT '{cd.ChangeType.UPDATED_ATTRIBUTES.value}', {arawfields}, {brawfields}, a.{geom_wkt_fieldname}
        FROM {new_table} a join {old_table} b on a.{geom_hash_fieldname} = b.{geom_hash_fieldname}
        WHERE a.{attribute_hash_fieldname} != b.{attribute_hash_fieldname}    
    """
    cursor = db_connection.cursor()
    try:
        cursor.execute(query)
    finally:
        cursor.close()
        
    db_connection.commit()


    return change_summary_table_name;


def export_change_table(change_table, provider_name, db_connection, output_folder_path):
    """
    Export Changes to geopackage file

    Parameters:
        change_table (string)
            - Name of change table to be exported for review
        provider_name(string)
            - string used to create output folder and GIS datasets            
        db_connection (connection object)
            - Connection to sqlite database
        output_folder_path(string)
            - Root location for output

    Dependencies (global variables):
        date_string (string)
            - string used to create output folder
        bc_albers_epsg
            - BC Albers EPSG code (defined in cd)
    Returns:
        
    """
    
    logger.debug(f"exporting changes for {provider_name} to {output_folder_path}")
    
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
        #TODO: Get geometry type from data - currently hard coded to MULTILINESTRING
        gpkg_file_name = os.path.join(output_folder_path, provider_name + "_" + date_string + '_Changes.gpkg') 
        logger.debug(f"export file: {gpkg_file_name}")
        
        if os.path.exists(gpkg_file_name):
            logger.debug(f"file {gpkg_file_name} exists and will be replace with new version")
            os.remove(gpkg_file_name)
                
    
        gis_output = ogr.GetDriverByName('GPKG').CreateDataSource(gpkg_file_name)
            
        #everything gets written as BC Albers
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(cd.bc_albers_epsg)
            
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
                    if key.lower() == geom_wkt_fieldname.lower():
                        geom = ogr.CreateGeometryFromWkt(row[incrementor])
                    else:
                        if row[incrementor]:
                            fields[key] = row[incrementor]
                    incrementor += 1
                    
                #get layer based on geometry type
                if geom.GetGeometryType() in layer_by_geom_type:
                    layer = layer_by_geom_type[geom.GetGeometryType()]
                else:
                    logger.debug(f"Create layer in output dataset for geometry type: {geom.GetGeometryType()}")
                    layer = gis_output.CreateLayer(change_table + "_" + geom.GetGeometryName(), srs, geom.GetGeometryType())
                    #sort the schema by field name then add the integer and text fields
                    for key, value in sorted(schema.items()):
                        if not key.lower() == geom_wkt_fieldname.lower():
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
        logger.debug(f"""The table {change_table} successfully exported to {gpkg_file_name}""")
    else:
        logger.debug(f"""The table {change_table} is empty - no output geopackage created for {provider_name}""")



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

    log_file_name = f"Change_Detection_Processing_Log_{provider_name}_{cd.rundatetime}.txt"
    log_file = os.path.join(log_folder_path, log_file_name)
    log_text = f"""
Change Detection Processing Log for {provider_name}, {date_string}
       
COMPARISON BETWEEN ORIGINAL DATA AND PREVIOUS ORIGINAL DATA
{cd.format_statistics(stats)}  
    """

    # Create the processing log file and populate it with the log text
    logger.debug(f"Writing log file: {log_file}")
    processing_log = open(log_file, "w")
    try:
        processing_log.write(log_text)
    finally:
        processing_log.close()
    logger.debug(f"Writing log file written")


# -------------------------------------------------------------------------------
# END OF FILE
# -------------------------------------------------------------------------------

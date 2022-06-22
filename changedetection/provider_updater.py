# Edits existing or adds new fields to provider config JSON
# Maddison Lussin
# August 23 - 2021

import easygui
import shutil
import os
import datetime
from osgeo import ogr
import get_file_from_URL
import cd

#* CHECKS FOR VALID URL AND RETURNS LIST OF FIELD NAMES FOR DATASET
def confirm_url_get_schema(url, dataset_name, database_name, data_type, is_rest):
    """Tests that a URL returns a valid file. If file is available, returns a list of field names
        Parameters:
        - url to download the data
        - dataset_name
        - database_name (only required if dataset is within a database)
        - data type: a string that matches the OGC driver string required 
        - is_rest: boolean to determine if file is streamed from a REST service

        Returns list of: [Boolean success], [list of field names], [message string] 
    """
    datestamp = str(datetime.date.today()).replace('-', '_')
    temp_folder =  os.path.join(cd.dataload_dir, f'url_test_{datestamp}')
    if os.path.exists(temp_folder):
        shutil.rmtree(temp_folder)
    os.mkdir(temp_folder)

    #Attempt to download package to temp folder
    #TODO NEED TO ADD error/success messages returned from get_file_from_url function
    response, response_code, request_error = get_file_from_URL.getfile(url,is_rest,dataset_name,temp_folder)
    
    print(f"            [{response_code}]{response}")
    if not request_error is None:
        print(f"            Request Error: {request_error}")
    
    #Checks for a successful response code before continuing
    if response_code in range(200, 300):
        driver = ogr.GetDriverByName(data_type)
        field_names = None
        #Open connection to data source - path will be different if in a database
        if database_name is None:
            data_source = driver.Open(os.path.join(temp_folder,dataset_name))
        else:
            data_source = driver.Open(os.path.join(temp_folder,database_name)) 

        if data_source is None:
            success = False
            message = 'Unable to connect to data after download'
            # Adds request error to output message if any
            if not request_error is None:
                message = f"{message}\nRequest Error: {request_error}"
        else:
            message = 'Connected to data source'
            success = True

            #Enumerate field and add to list
            if database_name is None:
                layer = data_source.GetLayer()
            else:
                layer = data_source.GetLayer(dataset_name)

            field_names = [field.name for field in layer.schema]
            del layer
            del data_source

        shutil.rmtree(temp_folder)
    else:
        success = False
        field_names = None
        message = f"Download Failed\nResponse:[{response_code}]{response}\nRequest Error:{request_error}"

    return[success,field_names,message]

#* CHECKS FOR EXISTING PROVIDER WITH SAME NAME AS NEW
def check_provider(prv, prv_lst):
    """
    Params: prv_list - List of existing provider records
    Return: Boolean value to restart provider selection

    Checks user added new provider for matches in existing provider list. 
    if a match is found user either edits existing record or adds new
    """
    #Checks if provider is already in system
    prv_lwr = prv.replace(" ", "").lower()
    for p in prv_lst:
        p_lwr = p.replace(" ", "").lower()
        if p_lwr == prv_lwr:
            print(f"    Existing configuration found for {prv}, cannot add!\n")
            msg = f"A configuration for {prv} already exists. \nEdit existing configuration?"
            ttl = "Duplicate provider found!"
            sel_prov = easygui.boolbox(msg, ttl, ["OK", "Re-Select"])
            
            # Updates existing configuration
            if sel_prov:
                print(f"    Updating {prv}\n")
                return False
            # Restarts dialogue to select new configuration
            else:
                print("     Selecting another provider\n")
                return True
    return False

#* BUILDS DEFAULT DICTIONARY WITH NONE VALUES FOR NEW PROVIDER
def build_dict(prv, prv_dict):
    """
    Params: prv - Current Provider Name
            prv_dict - Dictionary for all providers
    Return: Data dictionary for selected provider

    looks for provider dict and returns provider dict,
    or creates a new empty dictionary for new provider
    """
    if prv in prv_dict.keys():
        return prv_dict.get(prv)
    else:
        return {
                "dataset_name": None,
                "url": None,
                "data_type": None,
                "database_name": None,
                "is_rest": None,
                "compare_fields": [],
                "Schedule": None                    
                }   

#* SETS/CHECKS GDAL DATA TYPE
def select_gdal(prv_dict):
    gdal_types = [
                    None,'AmigoCloud','AO','ARCGEN','AVCBIN','AVCE00','CAD','CARTO','Cloudant','CouchDB','CSV','CSW','DB2','DGN','DGNv8','DODS','DWG',
                    'DXF','EDIGEO','EEDA','Elasticsearch','ESRIJSON','ESRI Shapefile','FileGDB','FlatGeobuf','FME','Geoconcept','GeoJSON','GeoJSONSeq',
                    'Geomedia', 'GeoRSS','GML','GMLAS','GMT','GPKG','GPSBabel','GPX','GRASS','GTM','IDB','IDRISI','INTERLIS 1','INTERLIS 2','INGRES',
                    'JML', 'KML','LIBKML','LVBAG','MapML','MDB','Memory','MITAB','MongoDB','MongoDBv3','MSSQLSpatial','MVT','MySQL','NAS','netCDF','NGW',
                    'UK .NTF','OAPIF','OCI','ODBC','ODS','OGDI','OpenFileGDB','OSM','PDF','PDS','PostgreSQL','PGDump','PGeo','PLScenes','S57','SDTS',
                    'Selafin','SOSI','SQLite','SVG','SXF','TIGER','TopoJSON','VDV','VFK','VRT','Walk','WAsP','WFS','XLS','XLSX'
                ]
    prv_type = prv_dict.get("data_type")

    # Sets preselect value to None for new configuration
    gdal_index = 0
    if prv_type is None:
        msg = "Select the GDAL data type of your data?"

    # Preselects existing data type for existing configuration
    else:
        msg = f"The GDAL type of your data is set to {prv_type}\n Select new type or OK to continue"
        for gdal_type in gdal_types:
            if prv_type == gdal_type:
                gdal_index = gdal_types.index(gdal_type)

        #! Sets type to none for invalid gdal types and alerts user in gui dialogue
        if gdal_index == 0:
            msg = f"The GDAL type of your data is set to {prv_type} which is not a valid GDAL file type\n Select a new type."
    return easygui.choicebox(msg, "Select GDAL File Type", gdal_types, gdal_index)

#* SETS/CHECKS BOOLEAN PARAMETER VALUES (REST SERVICE & SCHEDULER)
def set_bool(in_val, val_type):
    """
    Params: in_val - Current value of parameter from dict (True, False, None)
            val_type - Name of parameter being set (for easygui message)
    Return: Boolean value for parameter being set

    Returns a boolean value selected by user input
    """
    # Initial setting of value if None type (default)
    if in_val is None:
        msg = f"Set provider as a {val_type} service?"
        buttons = [val_type, f"non-{val_type}"]
    
    # Modify message/buttons depending on if value is true or false
    elif in_val is True:
        msg = f"Current provider is set as a {val_type} service.\nIs this correct?"
        buttons = [f"Keep as {val_type}", f"Change to non-{val_type}"]
    elif in_val is False: 
        msg = f"Current provider is set as a non-{val_type} service. \nIs this correct?"
        buttons = [f"Keep as non-{val_type}", f"Change to {val_type}"]
    
    #! Alerts user if value is not a valid boolean value in gui dialogue
    else:
        msg = f"{val_type} must be a boolean value. {in_val} is not a valid.\nPlease select service type"
        buttons = [val_type, f"non-{val_type}"]
    
    out_val = easygui.boolbox(msg, f"Set {val_type} service type", buttons)
    if in_val is False:
        return not out_val
    else:
        return out_val

#* CHECKS USER INPUT VALUES FOR DEFAULT/NONE VALUES UNTIL VALIDATED
def validate_inputs(value_list):
    """
    Params: Value List - list containing values for dataset name, url, and database name (optional, may be none-type)
    Return: Values for dataset name, url, database name

    Creates gui dialogue with default helper values OR existing values (if any) for dataset name , url, and database name.
    Checks for default values, or empty strings and flags for user to enter a valid value (or sets to None type for optional database name)
    """
    def check_nt(param_val, default_val):
        """Checks for none type and replaces with default"""
        if param_val is None:
            param_val = default_val
        return param_val

    param_list = ["Dataset Name", "URL", "Database Name (Optional)"]
    defaults = ["i.e. roads.shp, roads.json, etc", "i.e. https://www.provider.ca/roads.zip", "for files in a geodatabase i.e. SampleCityData.gdb", 
                "Please enter a valid dataset name", "Please enter a valid URL", "No Geodatabase", "", " ", None]
    prefill_list = []

    # Sets default values for dataset name, url, and database name if current type is None
    prefill_list.append(check_nt(value_list[0], defaults[0]))
    prefill_list.append(check_nt(value_list[1], defaults[1]))
    prefill_list.append(check_nt(value_list[2], defaults[2]))

    #! Checks that all user input values are valid
    qa = True
    msg = "Enter/confirm dataset parameters"
    while qa:
        qa = False
        value_list = easygui.multenterbox(msg, "dataset parameters", param_list, prefill_list)

        if value_list[0] in defaults:
            prefill_list[0] = "Please enter a valid dataset name"
            qa = True # Flags for QA repeat if no dataset name is provided
        else:
            value_list[0] = value_list[0].strip()

        if value_list[1] in defaults:
            prefill_list[1] = "Please enter a valid URL"
            qa = True # Flags for QA repeat if no URL is provided
        else:
            value_list[1] = value_list[1].strip()
            
        # Sets geodatabase to None if user deleted default or left default (parameter is optional, no QA)
        if value_list[2] in defaults:
            value_list[2] = None
            prefill_list[2] = "No Geodatabase"
        else:
            value_list[2] = value_list[2].strip()

        msg = "Some required parameters were not filled out correctly, please review and update inputs"

    return value_list[0], value_list[1], value_list[2]

#* CHECKS IF LIST ITEMS ARE IN OR NOT IN ANOTHER LIST
def list_in_list(compare_list, in_list, reverse_compare = False):
    """
    Params: compare_list    - Checks if values in this list are in the other
            in_list         - list to be compared with compare_list
            reverse_compare - boolean value if true checks for values not in list
    Return: List of items in (or not in) other list
    """
    return_list = []
    for item in compare_list:
        if reverse_compare:
            if not item in in_list:
                return_list.append(item)
        else:
            if item in in_list:
                return_list.append(item)
    return return_list

#* SETS/CHECKS FIELDS FOR CHANGE DETECTION
def set_comparison_fields(comp_fields, all_fields):
    """
    Params: comp_fields - current list of selected comparison fields (empty list if new provider)
            all_fields - list of all fields in dataset
    Return: updated list of comparison fields

    Loads existing comparison fields into a multi-select gui dialogue populated with all field names for dataset
    Alerts user if they have fields selected for comparison that do not exist in the dataset 
    """
    if len(comp_fields)>0:
        #Checks list of comparison fields against all fields, removes values (if any) that are not found in all fields
        preselect_list = list_in_list(comp_fields, all_fields)

        # Creates list of index numbers for all fields that correspond to comparison field pre-select values
        preselect_index = []
        for item in preselect_list:
            preselect_index.append(all_fields.index(item))

        #! Alerts user that some previously-selected comparison fields are not valid field names
        not_in_list = list_in_list(comp_fields, all_fields, True)
        if len(not_in_list)>0:
            msg = f"Select fields to use in change detection.\nWARNING! You have pre-selected the followinng comparison fields that are NOT in the data source:\n{not_in_list}"
        else:
            msg = "Review selected comparison fields to use in change detection."
    else:
        msg = "Select fields to use in change detection."
        preselect_index = None

    #! Continues selection dialogue until user has selected at least 1 valid field
    check_compare = True
    while check_compare:
        comp_fields = easygui.multchoicebox(msg, "Select Comparison Fields", all_fields, preselect_index)
        
        if comp_fields is None:
            check_compare = True
        # None-type check seperated from len() check to prevent error caused by len() check on None type object
        elif len(comp_fields) == 0:
            check_compare = True
        else: 
            check_compare = False
        
        #! Alerts user if they haven't selected any comparison fields
        if check_compare:
            easygui.msgbox("You must select at least one field for comparison, please select comparison fields", "No fields selected!")
    return comp_fields

def confirm_delete(prov):
    """User enters password and confirms that they want to delete the selected provider"""
    password = "DeleteConfig"
    msg = f"Enter the security code to delete configuration for {prov}"
    if prov is None:
        return False
    else:
        val = False
        while val is False:
            user_pwd = easygui.passwordbox(msg, "Enter Password")
            if user_pwd is None:
                return False #>Returns False value for delete provider if user exits password dialogue
            elif password == user_pwd:
                val = True
            else:
                msg = "Sorry wrong password, try again!"
                print("     Password incorrect, cannot confirm delete!")
        
        #>User must enter provider name to confirm deletion
        user_prov = easygui.enterbox(f"CAUTION: THIS CANNOT BE UNDONE\nEnter {prov} below to confirm delete", f"Confirm Delete: {prov}")
        if user_prov == prov:
            return True
        else:
            return False
    

#* -----------------------------------MAIN FUNCTION-----------------------------------
def main():
    print("\n\n\n")

    #>Loads existing Provider Dictionary
    #TODO: switch to environment passed to main then select correct config
    
    config_json = cd.provider_config
    pd = cd.load_json(config_json) 
    # User selects new provider or selects to add a new provider
    add_new = True
    while add_new:
        sel_list = list(pd.keys()) #Existing Provider Names
        sel_list.insert(0, "ADD NEW") #Adds "ADD NEW" option for easygui selection
        sel_list.insert(1, "REMOVE PROVIDER") #Adds option to remove a configuration

        msg = "Select the Provider Configuration you want to Update"
        ttl = "Update Config"
        provider = easygui.choicebox(msg, ttl, sel_list)
        provider_list = list(pd.keys())
        # User input of new provider
        if provider == "ADD NEW":
            provider = easygui.enterbox("Enter new provider name", "Provider Name")
            print(f"Adding configuration for new provider: {provider}\n")
            add_new = check_provider(provider, provider_list)
        
        #User input for delete provider
        elif provider == "REMOVE PROVIDER":
            provider = easygui.choicebox("Pick a provider to remove", "Delete Provider", provider_list)
            #If confirm delete test successful, deletes provider config
            if confirm_delete(provider):
                print(f"DELETING CONFIGURATION FOR {provider}")
                del pd[provider]
                print(f"     Configuration deleted for {provider}\n")
                add_new = False

                #! Exits loop after deleting provider
                cd.dump_json(pd, config_json)
                msg = f"Provider configuration deleted for {provider}, update another provider?"
                run_prog = easygui.boolbox(msg, "Continue Updates", ["Continue", "Exit"])
                return run_prog

        # Uses user selected existing provider
        else:
            add_new = False
        
    #! Checks if user canceled instead of selecting a provider
    if provider is None:
        msg = "No provider selected, would you like to try again?"
        run_prog = easygui.boolbox(msg, "Try again?", ["Continue", "Exit"])
        return run_prog

    
    #> Sets Data Dictionary
    data_dict = build_dict(provider, pd)
    print(f"Updating configuration for {provider}:\n")

    #> Sets GDAL Data Type 
    data_type = select_gdal(data_dict)
    # will not validate on None type
    while data_type is None:
        easygui.msgbox("You must select a valid gdal data type", "No data type selected")
        data_type = select_gdal(data_dict)
    
    #> Checks/sets REST service status
    is_rest = set_bool(data_dict.get("is_rest"), "REST")
    print(f"        Set rest service status for {provider} to {is_rest}")

    #> Checks/sets schedule status
    is_sched = set_bool(data_dict.get("Schedule"), "Scheduled")
    print(f"        Set scheduler status for {provider} to {is_sched}")

    # Sets/checks values for dataset name, url, and geodatabase name (optional)
    dataset_name = data_dict.get("dataset_name")
    url = data_dict.get("url")
    database_name = data_dict.get("database_name")

    #> Sets dataset name, url, database name
    dataset_name, url, database_name = validate_inputs([dataset_name, url, database_name])

    # Tests url validity by downloading and checking data and returns list of fields in dataset
    print("        Testing URL")
    url_valid, fields, msg = confirm_url_get_schema(url, dataset_name, database_name, data_type, is_rest)
    while url_valid is False:
        print(f"            URL invalid for {provider}:\n{msg}\n")
        url = easygui.enterbox(f'{msg}\n\nEdit URL and try again?', "INVALID URL", url)
        if url is None:
            url_valid = None
        else:
            url_valid, fields, msg = confirm_url_get_schema(url, dataset_name, database_name, data_type, is_rest)

    # Cancels configuration and restarts dialogue for new provider if user cancels URL re-entry
    if url_valid is None:
        print(f"            Cancelling configuration for {provider}")
    
    #> Sets comparison field list
    else:
        print(f"\n        {msg}")

        compare_fields = set_comparison_fields(data_dict.get("compare_fields"), fields)
        
        if compare_fields is None:
            print(f"        No fields selected for comparison for {provider}\n")
            compare_fields = []
        else:
            print(f"        Set comparison field values for {provider} to:")
            for fld in compare_fields:
                print(f"            {fld}")
            print("")

        #> Updates config file
        pd.update({
            provider:{
                "dataset_name": dataset_name,
                "data_type": data_type,
                "url": url,
                "database_name": database_name,
                "is_rest": is_rest,
                "compare_fields": compare_fields,
                "Schedule": is_sched}
            })
        cd.dump_json(pd, config_json)

    # User input indicating program exit or re-run on another provider
    msg = f"Provider configuration finished for {provider}, update another provider?"
    run_prog = easygui.boolbox(msg, "Continue Updates", ["Continue", "Exit"])
    return run_prog


#* RUN MAIN
if __name__ == '__main__':
    run_updater = True
    while run_updater:
        run_updater = main()
    msg = "\nUpdates complete, goodbye!\n\nฅ(＾・ω・＾ฅ)\n\n"
    print(msg)
    easygui.msgbox(msg, "Exiting Updater")

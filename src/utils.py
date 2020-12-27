#==============================================================================#
#                                                                              #
#    Title: utils                                                              #
#    Purpose: Provide the utilities to be used by other modules                #
#    Notes: ...                                                                #
#    Author: chrimaho                                                          #
#    Created: Created                                                          #
#    References: 26/Dec/2020                                                   #
#    Sources: ...                                                              #
#    Edited: ...                                                               #
#                                                                              #
#==============================================================================#



#------------------------------------------------------------------------------#
# Packages                                                                  ####
#------------------------------------------------------------------------------#

from io import IncrementalNewlineDecoder
from pathlib import Path
from urllib.parse import urlparse
import os
import sys
from numpy.lib.arraysetops import isin
from requests.models import HTTPError
import requests
import json
import pandas as pd


#------------------------------------------------------------------------------#
# Import sources                                                            ####
#------------------------------------------------------------------------------#


# Set root directory ----
project_dir = Path(__file__).resolve().parents[2]


# Add directory to Sys path ----
try:
    # The directory "." is added to the Path environment so modules can easily be called between files.
    if not os.path.abspath(project_dir) in sys.path:
        sys.path.append(os.path.abspath(project_dir))
except:
    raise ModuleNotFoundError("The custom modules were not able to be loaded.")


# Import modules ----
from src import utils
from src import sources


#------------------------------------------------------------------------------#
# Define Custom Functions                                                   ####
#------------------------------------------------------------------------------#

# Project root ----
def get_ProjectRoot() -> Path:
    return Path(__file__).parent.parent


# Check list in list ----
def all_in(sequence1, sequence2):
    log_return = all(elem in sequence2 for elem in sequence1)
    return log_return


# Check valid url ----
def valid_url(url):
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc, result.path])
    except:
        return False
    
    
#------------------------------------------------------------------------------#
# Get Raw Data                                                              ####
#------------------------------------------------------------------------------#

# Get the raw data ----
def get_RawData(url=sources.PostalAreaCode):
    """
    Get raw data from the ABS website.
    For API formatting & error handling tips, see: https://realpython.com/python-requests/#status-codes

    Args:
        url (str, optional): The url that should be called to get the raw data from. Defaults to sources.PostalAreaCode.

    Raises:
        ImportError: If the URL is invalid or if the API returns a bad status code.
        ImportError: [description]

    Returns:
        dict: The JSON output from the API response
    """
    
    # Assertions
    assert isinstance(url, str)
    assert utils.valid_url(url)
    
    # Call the Api & handle the response
    try:
        response = requests.get(url)
        response.raise_for_status()
    except HTTPError as http_err:
        # logger.error("HTTP error occurred: {}".format(http_err))
        raise ImportError("Cannot connect to API.")
    except Exception as err:
        # logger.error("Unknown error occurred\nStatus code: {}\nMessage: {}".format(response.status_code, err))
        raise ImportError("Unknown error occurred")
    else:
        call = response.json()
        # logger.info('successfully imported file')
    
    # Return
    return call


# Dump the data ----
def let_DumpData(Data, TargetFilePath=os.path.join(project_dir,"data/raw"), TargetFileName="RawData.json"):
    # Assertions
    assert isinstance(Data, (dict, pd.DataFrame))
    assert isinstance(TargetFileName, str)
    assert os.path.exists(TargetFilePath)
    
    # Declare fullname
    FullName = os.path.join(os.path.abspath(TargetFilePath), TargetFileName)
    
    # Try the dump
    try:
        if isinstance(Data, dict):
            with open(FullName, "w") as fp:
                json.dump(Data, fp, indent=4)
            # logger.info('successfully dumped json data')
        if isinstance(Data, pd.DataFrame):
            Data.to_csv(FullName)
            # logger.info('successfully dumped csv data')
        result = True
    except:
        result = False
        # logger.error("failed to dump the data")
        
    # Return the result
    return result


# Get Json file ----
def get_LoadJson(TargetFileFullName):
    
    # Assertions
    assert isinstance(TargetFileFullName, str)
    assert os.path.exists(TargetFileFullName)
    
    # Try to read
    try:
        with open(TargetFileFullName, "r") as file:
            data = json.loads(file.read())
    except:
        raise ImportError("There was an issue importing the data.")
    
    # Return the result
    return data
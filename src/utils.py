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
        response = requests.get(url, timeout=240)
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



#------------------------------------------------------------------------------#
# Import/Export the Data                                                    ####
#------------------------------------------------------------------------------#


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



#------------------------------------------------------------------------------#
# Data Processing                                                           ####
#------------------------------------------------------------------------------#


# Get the data labels ----
def get_DataLabels(raw, label="POA", element="id"):
    """
    Extract the data labels from a given dictionary file.
        Note: The `raw` element is the json data as extracted from the ABS website using the `get_RawData()` function.

    Args:
        raw (dict): The raw data, from which the data labels will be extracted.
        label (str, optional): The name or index of the label to be extracted. Defaults to "POA".
        element (str, optional): The element of the label to be extracted (either `id` or `name`). Defaults to "id".

    Returns:
        dict: A dictionary containin the index (as keys) and labels (as values).
    """
    
    # Declare which labels are okay
    OkayLabels = \
        { "POA": 0
        , "LGA_2016": 0
        , "SSC": 0
        , "SEIFA_SA1_7DIGIT": 0
        , "ASGS_2016": 0
        , "SEIFAINDEXTYPE": 1
        , "SEIFA_MEASURE": 2
        , "TIME_PERIOD": 3
        , 1: 0
        , 2: 1
        , 3: 2
        , 4: 3
        }

    # Assertions
    assert isinstance(raw, dict)
    assert list(raw.keys())==['header', 'dataSets', 'structure']
    assert isinstance(label, (str, int))
    assert utils.all_in([label["id"] for label in raw["structure"]["dimensions"]["observation"]], list(OkayLabels) + list(range(4)))
    assert element in ["id","name"]

    # Get Index
    KeyIndex = OkayLabels[label]

    # Get list
    LabelsList = raw['structure']['dimensions']['observation'][KeyIndex]['values']

    # Make dict
    dic_Labels = {str(lst_Index):lst_Label[element] for lst_Index, lst_Label in enumerate(LabelsList)}
    
    # Return
    return dic_Labels


# Extract the data ----
def set_DataFrame(raw):
    
    # Assertions
    assert isinstance(raw, dict)
    assert list(raw)==['header','dataSets','structure']

    # Get data
    data = raw['dataSets'][0]['observations']

    # Coerce to DataFrame
    data = pd.DataFrame(data)

    # Return
    return data

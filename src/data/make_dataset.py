#%%
#==============================================================================#
#                                                                              #
#    Title: Make Dataset                                                       #
#    Purpose: To download and process the data for the App                     #
#    Notes: ...                                                                #
#    Author: chrimaho                                                          #
#    Created: 26/Dec/2020                                                      #
#    References: ...                                                           #
#    Sources: ...                                                              #
#    Edited: ...                                                               #
#                                                                              #
#==============================================================================#



#------------------------------------------------------------------------------#
#                                                                              #
#    Set Up                                                                 ####
#                                                                              #
#------------------------------------------------------------------------------#



#------------------------------------------------------------------------------#
# Import packages                                                           ####
#------------------------------------------------------------------------------#

# -*- coding: utf-8 -*- #
# import click                                 #<-- Interactivity
import logging                               #<-- For ease of debugging
from pathlib import Path                     #<-- Because we need a path forward
from dotenv import find_dotenv, load_dotenv
from pandas.core.algorithms import isin
from requests.models import HTTPError, Response  #<-- It's nice to have an environment
import streamlit as st                       #<-- For app deployment
import pandas as pd                          #<-- Frame your Data
import requests
import json
from pprint import pprint
import os
import sys


#------------------------------------------------------------------------------#
# Set root directory                                                        ####
#------------------------------------------------------------------------------#

project_dir = Path(__file__).resolve().parents[2]


#------------------------------------------------------------------------------#
# Import Configs                                                            ####
#------------------------------------------------------------------------------#

# Add directory to Sys path ----
try:
    # The directory "." is added to the Path environment so modules can easily be called between files.
    if not os.path.abspath(project_dir) in sys.path:
        sys.path.append(os.path.abspath(project_dir))
    # from src.modules import GlobalFunctions as fun
    # from src.config import params
except:
    raise ModuleNotFoundError("The custom modules were not able to be loaded.")


# Import modules ----
from src import utils
from src import config



#------------------------------------------------------------------------------#
# Declare common functions                                                  ####
#------------------------------------------------------------------------------#


# Dump the data ----
def let_DumpData(Data, TargetFilePath=os.path.join(project_dir,"data/raw"), TargetFileName="RawData.json"):
    assert isinstance(Data, (dict, pd.DataFrame))
    assert isinstance(TargetFileName, str)
    assert os.path.exists(TargetFilePath)
    try:
        if isinstance(Data, dict):
            with open(os.path.join(os.path.abspath(TargetFilePath), TargetFileName), "w") as fp:
                json.dump(Data, fp)
            logger.info('successfully dumped json data')
        if isinstance(Data, pd.DataFrame):
            Data.to_csv(os.path.join(os.path.abspath(TargetFilePath), TargetFileName))
            logger.info('successfully dumped csv data')
        result = True
    except:
        result = False
        logger.error("failed to dump the data")
    return result



#------------------------------------------------------------------------------#
#                                                                              #
#    Main Part                                                              ####
#                                                                              #
#------------------------------------------------------------------------------#


#------------------------------------------------------------------------------#
# Get Raw Data                                                              ####
#------------------------------------------------------------------------------#

# Get the raw data ----
def get_RawData(url=config.PostCode):
    """
    Get raw data from the ABS website.
    For API formatting & error handling tips, see: https://realpython.com/python-requests/#status-codes

    Args:
        url (str, optional): The url that should be called to get the raw data from. Defaults to config.PostCode.

    Raises:
        ImportError: If the URL is invalid or if the API returns a bad status code.
        ImportError: [description]

    Returns:
        dict: The JSON output from the API response
    """
    
    # Assertions
    assert isinstance(url, str)
    assert utils.valid_url(url)
    
    # Call the Api
    try:
        response = requests.get(url)
        response.raise_for_status()
    except HTTPError as http_err:
        logger.error("HTTP error occurred: {}".format(http_err))
        raise ImportError("Cannot connect to API.")
    except Exception as err:
        logger.error("Unknown error occurred\nStatus code: {}\nMessage: {}".format(response.status_code, err))
        raise ImportError("Unknown error occurred")
    else:
        call = response.json()
        logger.info('successfully imported file')
    
    # Return
    return call
    

# Get the data ----
def set_RawData():
    raw = get_RawData(config.PostCode)
    let_DumpData(raw, os.path.join(project_dir, "data/raw"), "RawData.json")
    return raw
    
    
    
#------------------------------------------------------------------------------#
# Process Data                                                              ####
#------------------------------------------------------------------------------#


# Get the data labels ----
def get_DataLabels(raw, label="POA", element="id"):
    """
    Get the labels from a given object.

    Args:
        object (dict): The dictionary, from which the labels will be derrived.
        label (str, optional): Which of the data elements should be exported. Defaults to "POA".
        element (str, optional): Should the 'id' or the 'name' be exported. Defaults to "id".

    Returns:
        dict: A dictionary containing the index and label for each data element.
    """

    # Assertions
    assert isinstance(raw, dict)
    assert list(raw.keys())==['header', 'dataSets', 'structure']
    assert isinstance(label, (str, int))
    OkayLabels = ['POA', 'SEIFAINDEXTYPE', 'SEIFA_MEASURE', 'TIME_PERIOD']
    assert utils.all_in([label["id"] for label in raw["structure"]["dimensions"]["observation"]], OkayLabels + list(range(4)))
    assert element in ["id","name"]

    # Get Index
    if isinstance(label, str):
        KeyIndex = OkayLabels.index(label)
    else:
        KeyIndex = label

    # Get list
    LabelsList = raw['structure']['dimensions']['observation'][KeyIndex]['values']

    # Declare empty list
    lst_Labels = list()
    lst_Indexes = list()

    # Itterate
    for index, label in enumerate(LabelsList):
        lst_Labels.append(label[element])
        lst_Indexes.append(str(index))

    # Make dict
    dic_Labels = dict(zip(lst_Indexes,lst_Labels))

    # Return
    return dic_Labels


# Extract the data ----
def get_DataFrame(raw):
    
    # Assertions
    assert isinstance(raw, dict)
    assert list(raw.keys())==['header','dataSets','structure']

    # Get data
    data = raw['dataSets'][0]['observations']

    # Coerce to DataFrame
    data = pd.DataFrame(data)

    # Return
    return data


# Fix the data frame ----
def let_FixData(DataFrame, raw):
    
    # Assertions
    assert isinstance(DataFrame, pd.DataFrame)

    # Melt the frame
    data = DataFrame.melt()

    # Split column
    data[[1,2,3,4]] = data['variable'].str.split(':',expand=True)

    # Duplicate columns
    data[[5,6,7,8]] = data[[1,2,3,4]]

    # Convert data
    data.iloc[:,2] = data.iloc[:,2].replace(get_DataLabels(raw, "POA", "id"))
    data.iloc[:,3] = data.iloc[:,3].replace(get_DataLabels(raw, "SEIFAINDEXTYPE", "id"))
    data.iloc[:,4] = data.iloc[:,4].replace(get_DataLabels(raw, "SEIFA_MEASURE", "id"))
    data.iloc[:,5] = data.iloc[:,5].replace(get_DataLabels(raw, "TIME_PERIOD", "id"))
    data.iloc[:,6] = data.iloc[:,6].replace(get_DataLabels(raw, "POA", "name"))
    data.iloc[:,7] = data.iloc[:,7].replace(get_DataLabels(raw, "SEIFAINDEXTYPE", "name"))
    data.iloc[:,8] = data.iloc[:,8].replace(get_DataLabels(raw, "SEIFA_MEASURE", "name"))
    data.iloc[:,9] = data.iloc[:,9].replace(get_DataLabels(raw, "TIME_PERIOD", "name"))

    # Rename columns
    data = data.rename(columns={
        1:raw["structure"]["dimensions"]["observation"][0]["name"].replace(" ",""),
        2:raw["structure"]["dimensions"]["observation"][1]["name"].replace(" ",""),
        3:raw["structure"]["dimensions"]["observation"][2]["name"].replace(" ",""),
        4:raw["structure"]["dimensions"]["observation"][3]["name"].replace(" ",""),
        5:raw["structure"]["dimensions"]["observation"][0]["name"].replace(" ","") + "Long",
        6:raw["structure"]["dimensions"]["observation"][1]["name"].replace(" ","") + "Long",
        7:raw["structure"]["dimensions"]["observation"][2]["name"].replace(" ","") + "Long",
        8:raw["structure"]["dimensions"]["observation"][3]["name"].replace(" ","") + "Long",
        })
    
    # Drop the unnecessary column
    del data["variable"]

    # Return
    return data


# Process the data ----
def set_ProcessData(raw):
    """
    Run the combined scripts to process the raw data.

    Args:
        raw (dict): The raw data, as extracted from the ABS.

    Returns:
        pd.DataFrame: The processed dataframe.
    """
    data = get_DataFrame(raw)
    data = let_FixData(data, raw)
    let_DumpData(data, os.path.join(project_dir, "data/processed"), TargetFileName="ProcessedData.csv")
    return data



#------------------------------------------------------------------------------#
#                                                                              #
#    Define & Run the Main                                                  ####
#                                                                              #
#------------------------------------------------------------------------------#


# Main Function ----
# @click.command()
# @click.argument('input_filepath', type=click.Path(exists=True))
# @click.argument('output_filepath', type=click.Path())
def main():
    """
    Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    
    # Run logger
    logger.info('making final data set from raw data')
    
    # Get data
    raw = set_RawData()
    
    # Dump data
    dat = set_ProcessData(raw)
    display(dat)
    
    return(dat)
    

# Run ----
if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()

# %%

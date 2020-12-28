#%%
#==============================================================================#
#                                                                              #
#    Title: Make PostCodes Dataset                                             #
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
from dotenv import find_dotenv, load_dotenv  #<-- It's nice to have an environment
import pandas as pd                          #<-- Frame your Data
from pprint import pprint
import os
import sys



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
#                                                                              #
#    Main Part                                                              ####
#                                                                              #
#------------------------------------------------------------------------------#


#------------------------------------------------------------------------------#
# Process Data                                                              ####
#------------------------------------------------------------------------------#


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


# Fix the data frame ----
def set_FixData(DataFrame, raw):
    """
    Fix the data and make it manageable and logical

    Args:
        DataFrame (pd.DataFrame): The DataFrame to be processed
        raw (dict): The dictionary containing the raw information, as extracted from the ABS.

    Returns:
        pd.DataFrame: The processed DataFrame
    """
    
    # Assertions
    assert isinstance(DataFrame, pd.DataFrame)
    assert isinstance(raw, dict)

    # Melt the frame
    data = DataFrame.melt()

    # Split column
    data[[1,2,3,4]] = data['variable'].str.split(':',expand=True)

    # Duplicate columns
    data[[5,6,7,8]] = data[[1,2,3,4]]
    
    # Drop the unnecessary column
    del data["variable"]

    # Convert data
    data.iloc[:,1] = data.iloc[:,1].replace(utils.get_DataLabels(raw, 1, "id"))
    data.iloc[:,2] = data.iloc[:,2].replace(utils.get_DataLabels(raw, "SEIFAINDEXTYPE", "id"))
    data.iloc[:,3] = data.iloc[:,3].replace(utils.get_DataLabels(raw, "SEIFA_MEASURE", "id"))
    data.iloc[:,4] = data.iloc[:,4].replace(utils.get_DataLabels(raw, "TIME_PERIOD", "id"))
    data.iloc[:,5] = data.iloc[:,5].replace(utils.get_DataLabels(raw, 1, "name"))
    data.iloc[:,6] = data.iloc[:,6].replace(utils.get_DataLabels(raw, "SEIFAINDEXTYPE", "name"))
    data.iloc[:,7] = data.iloc[:,7].replace(utils.get_DataLabels(raw, "SEIFA_MEASURE", "name"))
    data.iloc[:,8] = data.iloc[:,8].replace(utils.get_DataLabels(raw, "TIME_PERIOD", "name"))

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

    # Return
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
    raw = utils.get_RawData(sources.PostalAreaCode)
    utils.let_DumpData(raw, os.path.join(project_dir, "data/raw"), "Seifa2016_POA_Raw.json")
    
    # Process data
    data = set_DataFrame(raw)
    utils.let_DumpData(data, os.path.join(project_dir, "data/raw"), "Seifa2016_POA_Raw.csv")
    data = set_FixData(data, raw)
    utils.let_DumpData(data, os.path.join(project_dir, "data/processed"), TargetFileName="Seifa2016_POA_Processed.csv")
    print(data)
    
    return(data)
    

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

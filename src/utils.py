#==============================================================================#
#                                                                              #
#    Title: Title                                                              #
#    Purpose: Purpose                                                          #
#    Notes: Notes                                                              #
#    Author: chrimaho                                                          #
#    Created: Created                                                          #
#    References: References                                                    #
#    Sources: Sources                                                          #
#    Edited: Edited                                                            #
#                                                                              #
#==============================================================================#



#------------------------------------------------------------------------------#
# Packages                                                                  ####
#------------------------------------------------------------------------------#

from pathlib import Path
from urllib.parse import urlparse


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

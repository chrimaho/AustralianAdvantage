# Import modules ----
from src import sources

# Settings per data set ----
DataSets = \
    [ { "name": "POA"
      , "index": 0
      , "source": sources.PostalAreaCode
      }
    , { "name": "LGA"
      , "index": 1
      , "source": sources.LocalGovernmentArea
      }
    , { "name": "SSC"
      , "index": 2
      , "source": sources.StateSuburbCode
      }
    , { "name": "SA1"
      , "index": 3
      , "source": sources.StatisticalAreaOne
      }
    , { "name": "SA2"
      , "index": 4
      , "source": sources.StatisticalAreaOne
      }
    ]
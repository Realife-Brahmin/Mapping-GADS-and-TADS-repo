# %%
# pylint: disable=undefined-variable line-too-long invalid-name missing-function-docstring f-string-without-interpolation

import os
from collections import defaultdict
import re
import pandas as pd
import us

from src.housekeeping import (
    # filter_tlines_by_latest_reported_year,  # Forward Declaration
    get_latest_entries, # Forward Declaration
    get_matched_entries, # Forward Declaration
    get_reduced_df, # Forward Declaration
    sort_and_shift_columns, # Forward Declaration
    sort_and_shift_columns_dfVelo, # Forward Declaration
)

from src.housekeeping_gads import (
    filter_states,  # Forward Declaration
    filter_non_empty_eia_id,  # Forward Declaration
    filter_by_eia_code,  # Forward Declaration
)

try:
    fileAddr = __vsc_ipynb_file__
    wd = os.path.dirname(fileAddr)
    print("We seem to be working in a JuPyteR Notebook")
except ImportError:
    wd = os.getcwd()
    print("We seem to be working in a regular .py file")


rawDataFolder = os.path.join(wd, "rawData")
processedDataFolder = os.path.join(wd, "processedData/")
# %% Input the entire GADS Data and get some preliminary information about it

gadsFileAddr = os.path.join(rawDataFolder, "GADS inventory 2024.csv")
dfGads0 = pd.read_csv(gadsFileAddr)
sizeGads0 = dfGads0.shape
print(f"Size of GADS db before filtering: {sizeGads0[0]}, {sizeGads0[1]}")
companyNamesGads0 = set(dfGads0.CompanyName)
numCompaniesGads0 = len(companyNamesGads0)
print(f"There are {numCompaniesGads0} unique companies owning tlines in the entire GADS database.")
# display(dfgads)

# %% Input Velocity Suite Data for the specific configuration and get some preliminary information about it
location = "chicago-ohare"
components1 = "genUnits"
components2 = "genPlants"
ext = ".xlsx"
filenameVeloGads = components1 + "-near-" + location + ext
veloFileAddr = os.path.join(rawDataFolder, filenameVeloGads) # gen units which are <= 50miles from `Chicago/Ohare` weather station
print(veloFileAddr)
dfVelo0 = pd.read_excel(veloFileAddr, engine='openpyxl')
sizeVelo0 = dfVelo0.shape
print(f"Size of velocity suite db before any filtering: {sizeVelo0[0]}, {sizeVelo0[1]}")
# dfVelo0
# %% Housekeeping on dfVelo (remove empty EIA_ID rows)
dfVelo = dfVelo0.copy()

sizeVelo = dfVelo.shape

dfVeloSorted = dfVelo.sort_values(by=['Plant Name', 'Plant Operator Name'])
veloSortedAddr = os.path.join(processedDataFolder, "dfVelo-"+components1+"-"+location+"-Sorted"+ext)
dfVeloSorted.to_excel(veloSortedAddr, index=False)

dfVeloEIA = filter_non_empty_eia_id(dfVeloSorted)
veloValidEIAAddr = os.path.join(processedDataFolder, "dfVelo-"+components1+"-"+location+"-validEIA"+ext)
dfVeloEIA.to_excel(veloValidEIAAddr, index=False)

veloStates = set(dfVeloEIA['State'])
# %% Housekeeping on dfGads
dfGads = dfGads0.copy()

dfGadsFilt = filter_states(dfGads0, veloStates)
sizeGadsFilt = dfGadsFilt.shape
print(f"Size of GADS db after filtering: {sizeGadsFilt[0]}, {sizeGadsFilt[1]}")

gadsFiltStatesAddr = os.path.join(
    processedDataFolder, "dfGads-" + components1 + "-" + location + "-filteredStates" + ext
)
dfGadsFilt.to_excel(gadsFiltStatesAddr, index=False)

# %% Match
# dfMatch = filter_by_eia_code(dfVelo, dfGads)

matchAddr = os.path.join(
    processedDataFolder, "dfGads-" + components1 + "-" + location + "-Matched" + ext
)
dfMatch.to_excel(matchAddr, index=False)
# %%

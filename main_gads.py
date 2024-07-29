# %%
# pylint: disable=undefined-variable line-too-long invalid-name missing-function-docstring f-string-without-interpolation

import os
from collections import defaultdict
import re
import pandas as pd
import us # for mapping US state names and their acronyms

from src.housekeeping_gads import (
    filter_states,  # Forward Declaration
    filter_non_empty_eia_id,  # Forward Declaration
    filter_by_eia_code,  # Forward Declaration
    sort_and_reorder_columns,  # Forward Declaration
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
filenameVeloGenPlants = components2 + "-near-" + location + "-raw" + ext
veloFileGenPlantsAddr = os.path.join(rawDataFolder, filenameVeloGenPlants) # gen units which are <= 50miles from `Chicago/Ohare` weather station
print(veloFileGenPlantsAddr)
dfVeloPlants0 = pd.read_excel(veloFileGenPlantsAddr, engine='openpyxl')
sizeVeloPlants0 = dfVeloPlants0.shape
print(f"Size of velocity suite Gen Plants db before any filtering: {sizeVeloPlants0[0]}, {sizeVeloPlants0[1]}")
# %% Housekeeping on dfVelo (remove empty EIA_ID rows)
dfVeloP = dfVeloPlants0.copy()

# dfVeloP = filter_non_empty_eia_id(dfVeloP)
sizeVeloP = dfVeloP.shape

dfVeloPSorted = dfVeloP.sort_values(by=['Plant Name', 'Plant Operator Name'])
veloPSortedAddr = os.path.join(processedDataFolder, "dfVelo-"+components2+"-"+location+"-Sorted"+ext)
dfVeloPSorted.to_excel(veloPSortedAddr, index=False)

dfVeloPEIA = filter_non_empty_eia_id(dfVeloPSorted)
veloPValidEIAAddr = os.path.join(processedDataFolder, "dfVelo-"+components2+"-"+location+"-validEIA"+ext)

# Table 1: All Gen Plants from Velocity Suite which are 50 mi from location, have valid EIA, sorted by 'Plant Name' and then 'Plant Operator Name'.
dfVeloPEIA.to_excel(veloPValidEIAAddr, index=False)

veloPStates = set(dfVeloPEIA['State'])
# %% Housekeeping on dfGads
dfGads = dfGads0.copy()

dfGadsFilt = filter_states(dfGads0, veloPStates)
sizeGadsFilt = dfGadsFilt.shape
print(f"Size of GADS db after filtering for States Related to our Region: {sizeGadsFilt[0]}, {sizeGadsFilt[1]}")

gadsFiltStatesAddr = os.path.join(
    processedDataFolder, "dfGads-" + components1 + "-" + location + "-filteredStates" + ext
)

dfGadsFilt = sort_and_reorder_columns(dfGadsFilt, sort_columns=["UnitName", "UtilityName"])
# dfGadsFilt = dfGadsFilt.sort_values(by=["UnitName", "UtilityName"])

dfGadsFilt.to_excel(gadsFiltStatesAddr, index=False)

# %% Match all Gen Units from GADS to Gen Plants from Velocity Suite based on EIA
dfMatchGads = filter_by_eia_code(dfVeloPEIA, dfGadsFilt)

gadsMatchAddr = os.path.join(
    processedDataFolder, "dfGads-" + components1 + "-" + location + "-Matched" + ext
)

# Table 2: All Gen Units from GADS which were matched with Gen Plants from Velocity Suite on the basis of EIA. Addtionally the rows are sorted by Unit Name and Utility Name and those columns are brought to the front.
dfMatchGads.to_excel(gadsMatchAddr, index=False)
# %% Importing Gen Units from Velocity Suite and Housekeeping
filenameVeloGenUnits = components1 + "-near-" + location + "-raw" + ext
veloFileGenUnitsAddr = os.path.join(
    rawDataFolder, filenameVeloGenUnits
)  # gen units which are <= 50miles from `Chicago/Ohare` weather station
print(veloFileGenUnitsAddr)
dfVeloUnits0 = pd.read_excel(veloFileGenUnitsAddr, engine="openpyxl")
sizeVeloUnits0 = dfVeloUnits0.shape
print(
    f"Size of velocity suite Gen Units db before any filtering: {sizeVeloUnits0[0]}, {sizeVeloUnits0[1]}"
)
# %% Housekeeping on dfVelo (remove empty EIA_ID rows)

dfVeloU = dfVeloUnits0.copy()

# dfVeloP = filter_non_empty_eia_id(dfVeloP)
sizeVeloU = dfVeloU.shape

dfVeloUSorted = sort_and_reorder_columns(dfVeloU, sort_columns=["Plant Name", "Unit"])
# dfVeloUSorted = dfVeloU.sort_values(by=["Plant Name", "Unit"])
veloPSortedAddr = os.path.join(
    processedDataFolder, "dfVelo-" + components1 + "-" + location + "-Sorted" + ext
)
dfVeloUSorted.to_excel(veloPSortedAddr, index=False)
# %%

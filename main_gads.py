# %%
# pylint: disable=undefined-variable line-too-long invalid-name missing-function-docstring f-string-without-interpolation

import os
from collections import defaultdict
import re
import pandas as pd
import us # for mapping US state names and their acronyms

from src.housekeeping_gads import (
    filter_states,  # Forward Declaration
    filter_non_empty_column,  # Forward Declaration
    match_by_eia_code,  # Forward Declaration
    match_by_plant_name_and_add_eia,  # Forward Declaration
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

dfVeloPEIA = filter_non_empty_column(dfVeloPSorted, column_name="EIA ID")
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

dfGadsFiltEIA = filter_non_empty_column(dfGadsFilt, column_name="EIACode")

gadsFiltEIAAddr = os.path.join(
    processedDataFolder,
    "dfGads-" + components1 + "-" + location + "-filteredStates-validEIA" + ext,
)

dfGadsFiltEIA.to_excel(gadsFiltEIAAddr, index=False)
# %% Match all Gen Units from GADS to Gen Plants from Velocity Suite based on EIA
dfMatchGads_with_VSPlants = match_by_eia_code(dfVeloPEIA, dfGadsFilt)

gadsMatch_with_VSPlants_Addr = os.path.join(
    processedDataFolder, "dfGads-" + components1 + "-" + location + "-Matched-with-VSPlants" + ext
)

# Table 2: All Gen Units from GADS which were matched with Gen Plants from Velocity Suite on the basis of EIA. Addtionally the rows are sorted by Unit Name and Utility Name and those columns are brought to the front.
dfMatchGads_with_VSPlants.to_excel(gadsMatch_with_VSPlants_Addr, index=False)
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
# %% Match all Gen Units from Velocity Suite to Gen Plants from Velocity Suite based on Plant Name (Gen Units from VS don't have an EIA Code)

dfMatchVeloUAllEIA = match_by_plant_name_and_add_eia(dfVeloP, dfVeloUSorted)

veloUMatchAllEIAAddr = os.path.join(
    processedDataFolder, "dfVelo-" + components1 + "-" + location + "-Matched-with-VSPlants-allEIA" + ext
)

dfMatchVeloUAllEIA.to_excel(veloUMatchAllEIAAddr, index=False)

dfMatchVeloUEIA = filter_non_empty_column(dfMatchVeloUAllEIA, column_name="EIA ID")

veloUMatchEIAAddr = os.path.join(
    processedDataFolder,
    "dfVelo-" + components1 + "-" + location + "-Matched-with-VSPlants-validEIA" + ext,
)

# Table 3: All Gen Units from Velocity Suite which were matched with Gen Plants from Velocity Suite on the basis of Plant Name. Addtionally the rows are sorted by 'Plant Name' and 'Unit' and those columns are brought to the front. Lastly, EIA ID's from Gen Plants have been added to the corresponding rows in Gen Units and for this table rows with empty EIA values have been dropped.
dfMatchVeloUEIA.to_excel(veloUMatchEIAAddr, index=False)

# %% Now let's match rows from Table 2 (GADS Gen Units) and Table 3 (Velocity Suite Gen Units) based on EIA Codes
dfMatchGads_with_VSUnits = match_by_eia_code(dfMatchVeloUEIA, dfGadsFilt)

gadsMatch_with_VSUnits_Addr = os.path.join(
    processedDataFolder,
    "dfGads-" + components1 + "-" + location + "-Matched-with-VSUnits" + ext,
)

# Table 4: All Gen Units from GADS which were matched with Gen Units from Velocity Suite on the basis of EIA (note that VS Gen Units didn't originally have EIA, they were mapped from VS Gen Plants). Addtionally the rows are sorted by Unit Name and Utility Name and those columns are brought to the front.
dfMatchGads_with_VSUnits.to_excel(gadsMatch_with_VSUnits_Addr, index=False)

# %%

# %%
# pylint: disable=undefined-variable line-too-long invalid-name missing-function-docstring f-string-without-interpolation wrong-import-position

import os
import importlib
import pandas as pd

try:
    fileAddr = __vsc_ipynb_file__ #pylint: disable=reportUndefinedVariable
    wd = os.path.dirname(fileAddr)
    print("We seem to be working in a JuPyteR Notebook")
except ImportError:
    wd = os.path.dirname(__file__)
    print("We seem to be working in a regular .py file")

# Forward declarations
from src.housekeeping_gads import (
    eia_filtering,  # Forward Declaration
    filter_states,  # Forward Declaration
    filterRetiredPlants, # Forward Declaration
    computeCombinedMWRating, #Forward Declaration
    # filter_non_empty_column,  # Forward Declaration
    match_by_eia_code_and_add_recid,  # Forward Declaration
    match_by_plant_name_and_add_eia_recid,  # Forward Declaration
    sort_and_reorder_columns,  # Forward Declaration
)

# Function to reload the module
def reload_housekeeping():
    importlib.reload(src.housekeeping_gads)

analysisCategory = "generator_data"
rawDataFolder = os.path.join(wd, "rawData", analysisCategory)
processedDataFolder = os.path.join(wd, "processedData", analysisCategory)
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
# location = "newYork-jfk"
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

# dfVeloPEIA = filter_non_empty_column(dfVeloPSorted, column_name="EIA ID")
dfVeloPEIA = eia_filtering(dfVeloPSorted, column_name="EIA ID")

sizeVeloPEIA = dfVeloPEIA.shape
print(
    f"Size of velocity suite Gen Plants db after filtering for EIA Codes: {sizeVeloPEIA[0]}, {sizeVeloPEIA[1]}"
)

veloPValidEIAAddr = os.path.join(processedDataFolder, "dfVelo-"+components2+"-"+location+"-validEIA"+ext)

# Table 1: All Gen Plants from Velocity Suite which are 50 mi from location, have valid EIA, sorted by 'Plant Name' and then 'Plant Operator Name'.
dfVeloPEIA.to_excel(veloPValidEIAAddr, index=False)

# For reference (but not actually used for making matches), computing how many plants from VS are eligible to be in GADS (GADS has a cutoff rating of 75MW for its Plants)
dfVeloPEIA_comb = computeCombinedMWRating(dfVeloPEIA)

dfVeloPEIA_75 = dfVeloPEIA_comb[dfVeloPEIA_comb['Combined Cap MW'] >= 75]

size = dfVeloPEIA_75.shape
print(
    f"Size of velocity suite Gen Plants db after filtering for plants having rating less than 75MW: {size[0]}, {size[1]}"
)

veloPStates = set(dfVeloPEIA['State'])

# Housekeeping on dfGads
dfGads = dfGads0.copy()

# Filter GADS data to contain only the US States present in the Velocity Suite Data
dfGadsFilt = filter_states(dfGads0, veloPStates)
sizeGadsFilt = dfGadsFilt.shape
print(f"Size of GADS db after filtering for States Related to our Region: {sizeGadsFilt[0]}, {sizeGadsFilt[1]}")

gadsFiltStatesAddr = os.path.join(
    processedDataFolder, "dfGads-" + components1 + "-" + location + "-filteredStates" + ext
)

dfGadsFilt = sort_and_reorder_columns(dfGadsFilt, sort_columns=["UnitName", "UtilityName"])
# dfGadsFilt = dfGadsFilt.sort_values(by=["UnitName", "UtilityName"])

dfGadsFilt.to_excel(gadsFiltStatesAddr, index=False)

# dfGadsFiltEIA = filter_non_empty_column(dfGadsFilt, column_name="EIACode")
dfGadsFiltEIA = eia_filtering(dfGadsFilt, column_name="EIACode")

gadsFiltEIAAddr = os.path.join(
    processedDataFolder,
    "dfGads-" + components1 + "-" + location + "-filteredStates-validEIA" + ext,
)

dfGadsFiltEIA.to_excel(gadsFiltEIAAddr, index=False)
# %% Match all Gen Units from GADS to Gen Plants from Velocity Suite based on EIA
dfMatchGads_with_VSPlants = match_by_eia_code_and_add_recid(dfVeloPEIA, dfGadsFilt)

sizeMatchGads_with_VSPlants = dfMatchGads_with_VSPlants.shape
print(
    f"Size of GADS db after matching EIA Codes with Velocity Suite Plants: {sizeMatchGads_with_VSPlants[0]}, {sizeMatchGads_with_VSPlants[1]}"
)


numUniqueMatchedPlants = len(set(dfMatchGads_with_VSPlants['Rec_ID']))

print(
    f"These matched rows between GADS and Velocity Suite Plants represent {numUniqueMatchedPlants} unique plants (EIA Codes as well as Rec IDs)"
)

gadsMatch_with_VSPlants_Addr = os.path.join(
    processedDataFolder, "dfGads-" + components1 + "-" + location + "-Matched-with-VSPlants" + ext
)

# Table 2: All Gen Units from GADS which were matched with Gen Plants from Velocity Suite on the basis of EIA, and attach Rec IDs too. Addtionally the rows are sorted by Unit Name and Utility Name and those columns are brought to the front.
dfMatchGads_with_VSPlants.to_excel(gadsMatch_with_VSPlants_Addr, index=False)
# %% Importing Gen Units from Velocity Suite and Housekeeping
filenameVeloGenUnits = components1 + "-near-" + location + "-raw" + ext
veloFileGenUnitsAddr = os.path.join(
    rawDataFolder, filenameVeloGenUnits
)  # gen units which are <= 50miles from `Chicago/Ohare` weather station
print(veloFileGenUnitsAddr)

# Note that dfVeloUnits have neither EIA Codes nor Rec_ID
dfVeloUnits0 = pd.read_excel(veloFileGenUnitsAddr, engine="openpyxl")
sizeVeloUnits0 = dfVeloUnits0.shape
print(
    f"Size of velocity suite Gen Units db before any filtering: {sizeVeloUnits0[0]}, {sizeVeloUnits0[1]}"
)

dfVeloU = dfVeloUnits0.copy()

sizeVeloU = dfVeloU.shape

dfVeloUSorted = sort_and_reorder_columns(dfVeloU, sort_columns=["Plant Name", "Unit"])
# dfVeloUSorted = dfVeloU.sort_values(by=["Plant Name", "Unit"])
veloPSortedAddr = os.path.join(
    processedDataFolder, "dfVelo-" + components1 + "-" + location + "-Sorted" + ext
)
dfVeloUSorted.to_excel(veloPSortedAddr, index=False)
# %% Match all Gen Units from Velocity Suite to Gen Plants from Velocity Suite based on Plant Name (Gen Units from VS don't have an EIA Code)

dfMatchVeloUAllEIA = match_by_plant_name_and_add_eia_recid(dfVeloP, dfVeloUSorted)

sizeMatchVSUnits_with_VSPlants = dfMatchVeloUAllEIA.shape
print(
    f"Size of Velocity Suite Units db after matching Plant Names with Velocity Suite Plants: {sizeMatchVSUnits_with_VSPlants[0]}, {sizeMatchVSUnits_with_VSPlants[1]}"
)

veloUMatchAllEIAAddr = os.path.join(
    processedDataFolder, "dfVelo-" + components1 + "-" + location + "-Matched-with-VSPlants-allEIA" + ext
)

dfMatchVeloUAllEIA.to_excel(veloUMatchAllEIAAddr, index=False)

dfMatchVeloUEIA = eia_filtering(dfMatchVeloUAllEIA, column_name="EIA ID")

sizeMatchVSUnits_with_VSPlants_EIA = dfMatchVeloUEIA.shape
print(
    f"Size of Velocity Suite Units db after matching Plant Names with Velocity Suite Plants after removing rows with empty EIAs: {sizeMatchVSUnits_with_VSPlants_EIA[0]}, {sizeMatchVSUnits_with_VSPlants_EIA[1]}"
)

veloUMatchEIAAddr = os.path.join(
    processedDataFolder,
    "dfVelo-" + components1 + "-" + location + "-Matched-with-VSPlants-validEIA" + ext,
)

# Table 3: All Gen Units from Velocity Suite which were matched with Gen Plants from Velocity Suite on the basis of Plant Name. Addtionally the rows are sorted by 'Plant Name' and 'Unit' and those columns are brought to the front. Lastly, EIA ID's as well as Rec IDs from Gen Plants have been added to the corresponding rows in Gen Units and for this table rows with empty EIA values have been dropped.
dfMatchVeloUEIA.to_excel(veloUMatchEIAAddr, index=False)

# %% Now let's match rows from Table 2 (GADS Gen Units) and Table 3 (Velocity Suite Gen Units) based on EIA Codes
dfMatchGads_with_VSUnits = match_by_eia_code_and_add_recid(dfMatchVeloUEIA, dfGadsFilt)

sizeMatchGads_with_VSUnits = dfMatchGads_with_VSUnits.shape
print(
    f"Size of GADS db after matching EIA Codes with Velocity Suite Units: {sizeMatchGads_with_VSUnits[0]}, {sizeMatchGads_with_VSUnits[1]}"
)

numUniqueMatchedPlants_via_VSUnits = len(set(dfMatchGads_with_VSUnits["Rec_ID"]))

print(
    f"These matched rows between GADS and Velocity Suite Units represent {numUniqueMatchedPlants_via_VSUnits} unique plants (EIA Codes as well as Rec IDs)"
)

gadsMatch_with_VSUnits_Addr = os.path.join(
    processedDataFolder,
    "dfGads-" + components1 + "-" + location + "-Matched-with-VSUnits" + ext,
)

# Table 4: All Gen Units from GADS which were matched with Gen Units from Velocity Suite on the basis of EIA and attach Rec IDs too (note that VS Gen Units didn't originally have EIA, they were mapped from VS Gen Plants). Addtionally the rows are sorted by Unit Name and Utility Name and those columns are brought to the front.
dfMatchGads_with_VSUnits.to_excel(gadsMatch_with_VSUnits_Addr, index=False)

# %%

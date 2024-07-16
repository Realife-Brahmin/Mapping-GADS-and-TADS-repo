# %%
import os
from collections import defaultdict
import re
import pandas as pd

from src.housekeeping import (
    # filter_tlines_by_latest_reported_year,  # Forward Declaration
    get_latest_entries, # Forward Declaration
    get_matched_entries, # Forward Declaration
    get_reduced_df, # Forward Declaration
    sort_and_shift_columns, # Forward Declaration
    sort_and_shift_columns_dfVelo, # Forward Declaration
)

# %%
# pylint: disable=undefined-variable line-too-long invalid-name missing-function-docstring f-string-without-interpolation

try:
    fileAddr = __vsc_ipynb_file__
    wd = os.path.dirname(fileAddr)
    print("We seem to be working in a JuPyteR Notebook")
except ImportError:
    wd = os.getcwd()
    print("We seem to be working in a regular .py file")


rawDataFolder = os.path.join(wd, "rawData")
processedDataFolder = os.path.join(wd, "processedData/")
# %%
gadsFileAddr = os.path.join(rawDataFolder, "GADS 2024 AC Inventory.csv")
dfGads0 = pd.read_csv(gadsFileAddr)
sizeGads0 = dfGads0.shape
print(f"Size of GADS db before filtering: {sizeGads0[0]}, {sizeGads0[1]}")
companyNamesGads0 = set(dfGads0.CompanyName)
numCompaniesGads0 = len(companyNamesGads0)
print(f"There are {numCompaniesGads0} unique companies owning tlines in the entire GADS database.")
# display(dfgads)

# %%
location = "chicago-ohare"
veloFileAddr = os.path.join(rawDataFolder, "tlines-near-chicago-ohare-raw.xlsx") # tlines which are <= 50miles from `Chicago/Ohare` weather station
print(veloFileAddr)
dfVelo0 = pd.read_excel(veloFileAddr, engine='openpyxl')
sizeVelo0 = dfVelo0.shape
print(f"Size of velocity suite db before any filtering: {sizeVelo0[0]}, {sizeVelo0[1]}")
# dfVelo0

# %%
# Filter rows with 'Undetermined Company`
# dfVelo = dfVelo0[ dfVelo0['Company Name'] != 'Undetermined Company' ]
# Filter tlines with less than 100kV voltage
dfVelo = dfVelo0.copy()
dfVelo = dfVelo[ dfVelo['Voltage kV'] >= 100 ]
# Filter tlines not currently in service
dfVelo = dfVelo[ dfVelo['Proposed'] == 'In Service']

sizeVelo = dfVelo.shape
print(f"Size of velocity suite db after filtering for Company Names, Voltage [kV] and 'Proposed': {sizeVelo[0]}, {sizeVelo[1]}")
companyNamesVelo = set(dfVelo['Company Name'])
numCompaniesVelo = len(companyNamesVelo)
print(f"There are {numCompaniesVelo} named companies owning the tlines near {location}")
print(f"Their names are:")
print(companyNamesVelo)
# dfVelo

# %%
print(f"Now let's see how many tlines are owned by these {numCompaniesVelo} "       "companies in the entire GADS database:")

print(""f"But first I'll need to rename some companies in vs db to match with the exact strings of the GADS db.")

companyNamesVelo2Gads = companyNamesVelo.copy()  # Create a copy to avoid modifying the original

# Replace the element using the 'discard' method (more efficient for sets)
companyNamesVelo2Gads.discard("Commonwealth Edison Co")
companyNamesVelo2Gads.add("Commonwealth Edison Company")
companyNamesVelo2Gads.discard("AmerenIP")
companyNamesVelo2Gads.add("Ameren Services Company")
companyNamesVelo2Gads.discard("American Transmission Co LLC")
companyNamesVelo2Gads.add("American Transmission Company")
companyNamesVelo2Gads.discard("Northern Indiana Public Service Co LLC")
companyNamesVelo2Gads.add("Northern Indiana Public Service Company [BA")
companyNamesVelo2Gads.discard("Northern Municipal Power Agency")
companyNamesVelo2Gads.add("Northern Indiana Public Service Company [BA")
companyNamesVelo2Gads.discard("Undetermined Company")
companyNamesVelo2Gads.add("Commonwealth Edison Company")
print(companyNamesVelo2Gads)

dfVeloSorted = sort_and_shift_columns_dfVelo(dfVelo)

veloSortedAddr = os.path.join(processedDataFolder, "dfVelo-Chicago-Ohare-Sorted.xlsx")
dfVeloSorted.to_excel(veloSortedAddr)

# %%
dfGads = dfGads0.copy()
dfGads = dfGads[dfGads['CompanyName'].isin(companyNamesVelo2Gads)]
voltageClassesGads0 = set(dfGads['VoltageClassCodeName'])
print(voltageClassesGads0)
voltageClassesAllowedGads = voltageClassesGads0.copy()
voltageClassesAllowedGads.discard("0-99 kV")

dfGads = dfGads[dfGads['VoltageClassCodeName'].isin(voltageClassesAllowedGads)]

sizeGads = dfGads.shape
print(f"Size of GADS db after filtering: {sizeGads[0]}, {sizeGads[1]}")

dfGadsSorted = sort_and_shift_columns(dfGads)

gadsSortedAddr = os.path.join(processedDataFolder, "dfGads-Chicago-Ohare-Sorted.xlsx")

dfGadsSorted.to_excel(gadsSortedAddr, index=False)

# dfGadsLatest = filter_tlines_by_latest_reported_year(dfGadsSorted)
dfGadsLatest = get_latest_entries(dfGadsSorted)

sizeGadsLatest = dfGadsLatest.shape

print(f"Size of GADS db after filtering for only latest reported year: {sizeGadsLatest[0]}, {sizeGadsLatest[1]}")

gadsLatestAddr = os.path.join(processedDataFolder, "dfGads-Chicago-Ohare-Latest.xlsx")

dfGadsLatest.to_excel(gadsLatestAddr)
# %%
dfMatch = get_matched_entries(dfVeloSorted, dfGadsLatest)
matchAddr = os.path.join(processedDataFolder, "dfGads-Chicago-Ohare-Matched.xlsx")
dfMatch.to_excel(matchAddr)

# %%
dfMatchReduced = get_reduced_df(dfMatch)
matchReducedAddr = os.path.join(processedDataFolder, "chicago-ohare-lines.xlsx")
dfMatchReduced.to_excel(matchReducedAddr, index=False)
# %%

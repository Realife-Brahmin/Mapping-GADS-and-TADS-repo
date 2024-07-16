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
try:
    # pylint: disable=undefined-variable line-too-long invalid-name
    fileAddr = __vsc_ipynb_file__
    wd = os.path.dirname(fileAddr)
    print("We seem to be working in a JuPyteR Notebook")
except ImportError:
    wd = os.getcwd()
    print("We seem to be working in a regular .py file")


rawDataFolder = os.path.join(wd, "rawData")
processedDataFolder = os.path.join(wd, "processedData/")
# %%
tadsFileAddr = os.path.join(rawDataFolder, "TADS 2024 AC Inventory.csv")
dfTads0 = pd.read_csv(tadsFileAddr)
sizeTads0 = dfTads0.shape
print(f"Size of TADS db before filtering: {sizeTads0[0]}, {sizeTads0[1]}")
companyNamesTads0 = set(dfTads0.CompanyName)
numCompaniesTads0 = len(companyNamesTads0)
print(f"There are {numCompaniesTads0} unique companies owning tlines in the entire TADS database.")
# display(dftads)

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
print(f"Now let's see how many tlines are owned by these {numCompaniesVelo} "       "companies in the entire TADS database:")

print(""f"But first I'll need to rename some companies in vs db to match with the exact strings of the TADS db.")

companyNamesVelo2Tads = companyNamesVelo.copy()  # Create a copy to avoid modifying the original

# Replace the element using the 'discard' method (more efficient for sets)
companyNamesVelo2Tads.discard("Commonwealth Edison Co")
companyNamesVelo2Tads.add("Commonwealth Edison Company")
companyNamesVelo2Tads.discard("AmerenIP")
companyNamesVelo2Tads.add("Ameren Services Company")
companyNamesVelo2Tads.discard("American Transmission Co LLC")
companyNamesVelo2Tads.add("American Transmission Company")
companyNamesVelo2Tads.discard("Northern Indiana Public Service Co LLC")
companyNamesVelo2Tads.add("Northern Indiana Public Service Company [BA")
companyNamesVelo2Tads.discard("Northern Municipal Power Agency")
companyNamesVelo2Tads.add("Northern Indiana Public Service Company [BA")
companyNamesVelo2Tads.discard("Undetermined Company")
companyNamesVelo2Tads.add("Commonwealth Edison Company")
print(companyNamesVelo2Tads)

dfVeloSorted = sort_and_shift_columns_dfVelo(dfVelo)

veloSortedAddr = os.path.join(processedDataFolder, "dfVelo-Chicago-Ohare-Sorted.xlsx")
dfVeloSorted.to_excel(veloSortedAddr)

# %%
dfTads = dfTads0.copy()
dfTads = dfTads[dfTads['CompanyName'].isin(companyNamesVelo2Tads)]
voltageClassesTads0 = set(dfTads['VoltageClassCodeName'])
print(voltageClassesTads0)
voltageClassesAllowedTads = voltageClassesTads0.copy()
voltageClassesAllowedTads.discard("0-99 kV")

dfTads = dfTads[dfTads['VoltageClassCodeName'].isin(voltageClassesAllowedTads)]

sizeTads = dfTads.shape
print(f"Size of TADS db after filtering: {sizeTads[0]}, {sizeTads[1]}")

dfTadsSorted = sort_and_shift_columns(dfTads)

tadsSortedAddr = os.path.join(processedDataFolder, "dfTads-Chicago-Ohare-Sorted.xlsx")

dfTadsSorted.to_excel(tadsSortedAddr, index=False)

# dfTadsLatest = filter_tlines_by_latest_reported_year(dfTadsSorted)
dfTadsLatest = get_latest_entries(dfTadsSorted)

sizeTadsLatest = dfTadsLatest.shape

print(f"Size of TADS db after filtering for only latest reported year: {sizeTadsLatest[0]}, {sizeTadsLatest[1]}")

tadsLatestAddr = os.path.join(processedDataFolder, "dfTads-Chicago-Ohare-Latest.xlsx")

dfTadsLatest.to_excel(tadsLatestAddr)
# %%
dfMatch = get_matched_entries(dfVeloSorted, dfTadsLatest)
matchAddr = os.path.join(processedDataFolder, "dfTads-Chicago-Ohare-Matched.xlsx")
dfMatch.to_excel(matchAddr)

# %%
dfMatchReduced = get_reduced_df(dfMatch)
matchReducedAddr = os.path.join(processedDataFolder, "chicago-ohare-lines.xlsx")
dfMatchReduced.to_excel(matchReducedAddr)
# %%

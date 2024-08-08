# %%
# pylint: disable=undefined-variable line-too-long invalid-name missing-function-docstring f-string-without-interpolation wrong-import-position

import os
import importlib
import pandas as pd

try:
    fileAddr = __vsc_ipynb_file__  # pylint: disable=reportUndefinedVariable
    wd = os.path.dirname(fileAddr)
    print("We seem to be working in a JuPyteR Notebook")
except ImportError:
    wd = os.path.dirname(__file__)
    print("We seem to be working in a regular .py file")

from src.housekeeping_tads import (
    # filter_tlines_by_latest_reported_year,  # Forward Declaration
    get_latest_entries, # Forward Declaration
    get_matched_entries, # Forward Declaration
    get_reduced_df, # Forward Declaration
    sort_and_shift_columns, # Forward Declaration
    sort_and_shift_columns_dfVelo, # Forward Declaration
)

# Function to reload the module
def reload_housekeeping():
    importlib.reload(src.housekeeping_tads)

analysisCategory = "transmission_data"
rawDataFolder = os.path.join(wd, "rawData", analysisCategory)
processedDataFolder = os.path.join(wd, "processedData", analysisCategory)

# rawDataFolder = os.path.join(wd, "rawData")
# processedDataFolder = os.path.join(wd, "processedData/")
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
# location = "newYork-jfk"
components1 = "tlines"
ext = ".xlsx"
# location = "chicago-ohare"
filenameVeloTlines = components1 + "-near-" + location + "-raw" + ext
veloFileTlinesAddr = os.path.join(
    rawDataFolder, filenameVeloTlines
)  # tlines units which are <= 50miles from `Chicago/Ohare` weather station
print(veloFileTlinesAddr)
# veloFileTlinesAddr = os.path.join(rawDataFolder, "tlines-near-chicago-ohare-raw.xlsx") # tlines which are <= 50miles from `Chicago/Ohare` weather station
# print(veloFileTlinesAddr)
dfVeloTlines0 = pd.read_excel(veloFileTlinesAddr, engine='openpyxl')
sizeVelo0 = dfVeloTlines0.shape
print(f"Size of velocity suite db before any filtering: {sizeVelo0[0]}, {sizeVelo0[1]}")
# dfVeloTlines0

# %%
# Filter rows with 'Undetermined Company`
# dfVeloTlines = dfVeloTlines0[ dfVeloTlines0['Company Name'] != 'Undetermined Company' ]
# Filter tlines with less than 100kV voltage
dfVeloTlines = dfVeloTlines0.copy()
dfVeloTlines = dfVeloTlines[ dfVeloTlines['Voltage kV'] >= 100 ]
# Filter tlines not currently in service
dfVeloTlines = dfVeloTlines[ dfVeloTlines['Proposed'] == 'In Service']

sizeVelo = dfVeloTlines.shape
print(f"Size of velocity suite db after filtering for Company Names, Voltage [kV] and 'Proposed': {sizeVelo[0]}, {sizeVelo[1]}")
companyNamesVelo = set(dfVeloTlines['Company Name'])
numCompaniesVelo = len(companyNamesVelo)
print(f"There are {numCompaniesVelo} named companies owning the tlines near {location}")
print(f"Their names are:")
print(companyNamesVelo)
# dfVeloTlines

# %%
print(f"Now let's see how many tlines are owned by these {numCompaniesVelo} "       "companies in the entire TADS database:")

dfVeloTlinesSorted = sort_and_shift_columns_dfVelo(dfVeloTlines)

veloTlinesSortedAddr = os.path.join(
    processedDataFolder, "dfVelo-" + components1 + "-" + location + "-Sorted" + ext
)
dfVeloTlinesSorted.to_excel(veloTlinesSortedAddr, index=False)

# veloSortedAddr = os.path.join(processedDataFolder, "dfVeloTlines-Chicago-Ohare-Sorted.xlsx")
# dfVeloTlinesSorted.to_excel(veloSortedAddr, index=False)

print(""f"But first I'll need to rename some companies in vs db to match with the exact strings of the TADS db.")

companyNamesVelo2Tads = companyNamesVelo.copy()  # Create a copy to avoid modifying the original

if location == "chicago-ohare" :
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

# tadsSortedAddr = os.path.join(processedDataFolder, "dfTads-Chicago-Ohare-Sorted.xlsx")

tadsSortedAddr = os.path.join(
    processedDataFolder,
    "dfTads-" + components1 + "-" + location + "-Sorted" + ext,
)

dfTadsSorted.to_excel(tadsSortedAddr, index=False)

# dfTadsLatest = filter_tlines_by_latest_reported_year(dfTadsSorted)
dfTadsLatest = get_latest_entries(dfTadsSorted)

sizeTadsLatest = dfTadsLatest.shape

print(f"Size of TADS db after filtering for only latest reported year: {sizeTadsLatest[0]}, {sizeTadsLatest[1]}")

# tadsLatestAddr = os.path.join(processedDataFolder, "dfTads-Chicago-Ohare-Latest.xlsx")

tadsLatestAddr = os.path.join(
    processedDataFolder,
    "dfTads-" + components1 + "-" + location + "-Latest" + ext,
)

dfTadsLatest.to_excel(tadsLatestAddr, index=False)
# %%
dfMatchTads_with_VSTlines = get_matched_entries(dfVeloTlinesSorted, dfTadsLatest)

size = dfMatchTads_with_VSTlines.shape
print(
    f"Size of TADS db after matching (from bus, to bus) with Velocity Suite Tlines: {size[0]}, {size[1]}"
)

tadsMatch_with_VSTlines_Addr = os.path.join(
    processedDataFolder,
    "dfTads-" + components1 + "-" + location + "-Matched-with-VSTlines" + ext,
)

# matchAddr = os.path.join(processedDataFolder, "dfTads-Chicago-Ohare-Matched.xlsx")
dfMatchTads_with_VSTlines.to_excel(tadsMatch_with_VSTlines_Addr, index=False)

# %%
dfMatchTads_with_VSTlines_Reduced = get_reduced_df(dfMatchTads_with_VSTlines)
# matchReducedAddr = os.path.join(processedDataFolder, "chicago-ohare-lines.xlsx")
tadsMatch_with_VSTlines_Reduced_Addr = os.path.join(
    processedDataFolder,
    "dfTads-" + components1 + "-" + location + "-Matched-with-VSTlines-Reduced" + ext,
)
dfMatchTads_with_VSTlines_Reduced.to_excel(
    tadsMatch_with_VSTlines_Reduced_Addr, index=False
)

size = dfMatchTads_with_VSTlines_Reduced.shape
print(
    f"Size of matched TADS db entries after formatting them based on desired final format: {size[0]}, {size[1]}"
)
# %%

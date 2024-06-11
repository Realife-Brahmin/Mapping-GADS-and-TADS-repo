# %%
import pandas as pd
import os
import sys
import IPython

# %%
try:
    fileAddr = __vsc_ipynb_file__ # type: ignore # vscode only, Pylance seems to have an issue with this macro, so ignoring the warning
    wd = os.path.dirname(fileAddr)
    print("We seem to be working in a JuPyteR Notebook")
except:
    wd = os.getcwd()
    print("We seem to be working in a regular .py file")
    

rawDataFolder = os.path.join(wd, "rawData")
processedDataFolder = os.path.join(wd, "processedData/")

# %%
matchFileAddr = os.path.join(rawDataFolder, "Match_file.csv")
dfMatch = pd.read_csv(matchFileAddr)
colNamesMatch = dfMatch.columns.tolist()
colNamesMatch
matchedIndices = dfMatch['Rec_ID'].notna()
num = sum(matchedIndices)
dfMatched = dfMatch[matchedIndices]
dfMatched.to_excel("dfMatched.xlsx")
sizeMatched0 = dfMatched.shape
print(f"Size of Match_file csv before any filtering: {sizeMatched0[0]}, {sizeMatched0[1]}")

# %%
tadsFileAddr = os.path.join(rawDataFolder, "TADS 2024 AC Inventory.csv")
dfTads = pd.read_csv(tadsFileAddr)
sizeTads0 = dfTads.shape
print(f"Size of TADS db before filtering: {sizeTads0[0]}, {sizeTads0[1]}")
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
dfVelo = dfVelo0[ dfVelo0['Company Name'] != 'Undetermined Company' ] 
# Filter tlines with less than 100kV voltage
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
dfVeloMatched = dfVelo[dfVelo['Rec_ID'].isin(dfMatched['Rec_ID'])]
dfVeloMatched
veloMatchedAddr = os.path.join(processedDataFolder, "dfVeloMatched.xlsx")
veloMatchedAddr
dfVeloMatched.to_excel("dfVeloMatched.xlsx")
# sizeVelo = dfVelo.shape
# print(f"Size of velocity suite db before any filtering: {sizeVelo[0]}, {sizeVelo[1]}")
# dfVeloMatched

# %%




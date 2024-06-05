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

# %%
tadsFileAddr = os.path.join(rawDataFolder, "TADS 2024 AC Inventory.csv")
dfTads = pd.read_csv(tadsFileAddr)
# display(dftads)

# %%
veloFileAddr = os.path.join(rawDataFolder, "tlines-near-chicago-ohare-raw.xlsx") # tlines which are <= 50miles from `Chicago/Ohare` weather station
print(veloFileAddr)
dfVelo0 = pd.read_excel(veloFileAddr, engine='openpyxl')
# dfVelo0

# %%
# Filter rows with 'Undetermined Company`
dfVelo = dfVelo0[ dfVelo0['Company Name'] != 'Undetermined Company' ] 
# Filter tlines with less than 100kV voltage
dfVelo = dfVelo[ dfVelo['Voltage kV'] >= 100 ]
# Filter tlines not currently in service
dfVelo = dfVelo[ dfVelo['Proposed'] == 'In Service']
dfVelo


# %% [markdown]
# 

# %% [markdown]
# 

# %%
dfVeloMatched = dfVelo[dfVelo['Rec_ID'].isin(dfMatched['Rec_ID'])]
dfVeloMatched
veloMatchedAddr = os.path.join(processedDataFolder, "dfVeloMatched.xlsx")
veloMatchedAddr
dfVeloMatched.to_excel("dfVeloMatched.xlsx")

# %%




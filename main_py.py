# %%
import os
# import sys
# import IPython
import pandas as pd

# %%
try:
    fileAddr = __vsc_ipynb_file__ # type: ignore # vscode only, Pylance seems to have an issue with this macro, so ignoring the warning
    wd = os.path.dirname(fileAddr)
    print("We seem to be working in a JuPyteR Notebook")
except ImportError:
    wd = os.getcwd()
    print("We seem to be working in a regular .py file")
    

# pylint: disable=f-string-without-interpolation line-too-long pointless-statement invalid-name

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

# %%
dfVeloMatched = dfVelo[dfVelo['Rec_ID'].isin(dfMatched['Rec_ID'])]
# dfVeloMatched
veloMatchedAddr = os.path.join(processedDataFolder, "dfVeloMatched.xlsx")
dfVeloMatched.to_excel("dfVeloMatched.xlsx")
# sizeVelo = dfVelo.shape
# print(f"Size of velocity suite db before any filtering: {sizeVelo[0]}, {sizeVelo[1]}")
# dfVeloMatched

# %%
# def find_partial_matches(df1, col1, df2, col2):
#     return df1[df1[col1].str.contains('|'.join(df2[col2].dropna()), na=False)]

# # Finding partial matches
# matchV1T1 = find_partial_matches(dfVelo, 'From Sub', dfTads, 'FromBus')
# matchV1T2 = find_partial_matches(dfVelo, 'From Sub', dfTads, 'ToBus')
# matchV2T1 = find_partial_matches(dfVelo, 'To Sub', dfTads, 'FromBus')
# matchV2T2 = find_partial_matches(dfVelo, 'To Sub', dfTads, 'ToBus')

# # Display results
# print("Matching rows from 'From Sub' to 'FromBus':")
# print(matchV1T1)

# print("\nMatching rows from 'From Sub' to 'ToBus':")
# print(matchV1T2)

# print("\nMatching rows from 'To Sub' to 'FromBus':")
# print(matchV2T1)

# print("\nMatching rows from 'To Sub' to 'ToBus':")
# print(matchV2T2)



# # %%
# # Assuming matchV1T1, matchV2T1, matchV1T2, and matchV2T2 are DataFrames with an index

# intersection_V1T1_V2T2 = matchV1T1[matchV1T1.index.isin(matchV2T2.index)]

# # Print the intersection DataFrame
# print("Intersection of matchV1T1 and matchV2T2:")
# print(intersection_V1T1_V2T2)

# # %%
# intersection_V1T2_V2T1 = matchV1T2[matchV1T2.index.isin(matchV2T1.index)]

# # Print the intersection DataFrame
# print("Intersection of matchV1T2 and matchV2T1:")
# print(intersection_V1T2_V2T1)

# %%
# Function to find and display matches with verbosity
def find_and_display_matches(df1, df2, max_matches=5):
    matches = []
    count = 0

    for idx1, row1 in df1.iterrows():
        match_found = False
        for idx2, row2 in df2.iterrows():
            if (row1['From Sub'] == row2['FromBus'] and row1['To Sub'] == row2['ToBus']) or \
               (row1['From Sub'] == row2['ToBus'] and row1['To Sub'] == row2['FromBus']):
                matches.append((idx1, idx2))
                count += 1
                if row1['From Sub'] == row2['FromBus'] and row1['To Sub'] == row2['ToBus']:
                    print(f"Match {count}:")
                    print(f"dfVelo row {idx1} 'From Sub': {row1['From Sub']}, 'To Sub': {row1['To Sub']}")
                    print(f"dfTads row {idx2} 'FromBus': {row2['FromBus']}, 'ToBus': {row2['ToBus']}\n")
                elif row1['From Sub'] == row2['ToBus'] and row1['To Sub'] == row2['FromBus']:
                    print(f"Match {count}:")
                    print(f"dfVelo row {idx1} 'From Sub': {row1['From Sub']}, 'To Sub': {row1['To Sub']}")
                    print(f"dfTads row {idx2} 'FromBus': {row2['ToBus']}, 'ToBus': {row2['FromBus']}\n")
                match_found = True
                if count >= max_matches:
                    return matches
                break
        if match_found:
            continue
    return matches

# Set the maximum number of matches to display
max_matches = 500

# Finding exact matches with verbose output
print("Finding exact matches with verbose output:\n")
exact_matches = find_and_display_matches(dfVelo,  dfTads, max_matches)
# %%
# VS: "Voltage Class kV" is among "100-161", "345", "735 and Above", 
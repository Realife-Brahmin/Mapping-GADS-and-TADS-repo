# %%
import os
import pandas as pd
import re
from collections import defaultdict
from src.helperFunctions import find_tline_by_buses # Forward Declaration

# pylint: disable=f-string-without-interpolation line-too-long pointless-statement invalid-name

# %%
try:
    # pylint: disable=undefined-variable
    fileAddr = __vsc_ipynb_file__
    wd = os.path.dirname(fileAddr)
    print("We seem to be working in a JuPyteR Notebook")
except ImportError:
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
dfMatchedAddr = os.path.join(processedDataFolder, "dfMatched.xlsx")
dfMatched.to_excel(dfMatchedAddr)
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
dfVeloMatched.to_excel(veloMatchedAddr)
# sizeVelo = dfVelo.shape
# print(f"Size of velocity suite db before any filtering: {sizeVelo[0]}, {sizeVelo[1]}")
# dfVeloMatched

# %% The matching function
def findMatchingLinesRepeated(
    df1,
    df2,
    max_matches=5,
    output_file="matches.txt", 
    output_dir="./processedData"
):
    matches = []

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)  # Create directory if needed

    with open(os.path.join(output_dir, output_file), "w", encoding="utf-8") as f:
        for idx1, row1 in df1.iterrows():
            match_found = False

            for idx2, row2 in df2.iterrows():
                if (
                    row1["From Sub"] == row2["FromBus"]
                    and row1["To Sub"] == row2["ToBus"]
                ) or (
                    row1["From Sub"] == row2["ToBus"]
                    and row1["To Sub"] == row2["FromBus"]
                ):
                    matches.append((idx1, idx2))
                    match_found = True  # Set match found once a match is identified

                    # Print/write details for all matches (up to max_matches)
                    if match_found and len(matches) <= max_matches:
                        print(f"\nMatch {len(matches)}:")
                        print(
                            f"dfVelo row {idx1} 'From Sub': {row1['From Sub']}, 'To Sub': {row1['To Sub']}"
                        )
                        print(
                            f"dfTads row {idx2} 'FromBus': {row2['FromBus']}, 'ToBus': {row2['ToBus']}"
                        )

                        f.write(f"\nMatch {len(matches)}:\n")
                        f.write(
                            f"dfVelo row {idx1} 'From Sub': {row1['From Sub']}, 'To Sub': {row1['To Sub']}\n"
                        )
                        f.write(
                            f"dfTads row {idx2} 'FromBus': {row2['FromBus']}, 'ToBus': {row2['ToBus']}\n"
                        )

                        # Include ReportedYearNbr if it exists
                        if 'ReportingYearNbr' in row2:
                            reporting_year_nbr = row2['ReportingYearNbr']
                            print(f"dfTads row {idx2}: Reporting Year Number: {reporting_year_nbr}")
                            f.write(f"dfTads row {idx2}: Reporting Year Number: {reporting_year_nbr}\n")
                        
                            # Check for retirement date and print/write
                        if not pd.isna(row2["RetirementDate"]):
                            retirement_date = row2["RetirementDate"]
                            print(f"dfTads row {idx2}: Retired on {retirement_date}")
                            f.write(f"dfTads row {idx2}: Retired on {retirement_date}\n")

    return matches


# %% Calling the matching function
# Set the maximum number of matches to display
max_matches = 5000


# Finding exact matches with verbose output
print("Finding exact matches with verbose output:\n")
exact_matches = findMatchingLinesRepeated(dfVelo,  dfTads, max_matches=max_matches)
# %%
def getLatestReportedEntries(input_file, output_file):

    # Regular expression to extract match data including the optional "Retired on" line
    match_pattern = re.compile(
        r"Match (?P<match_num>\d+):\ndfVelo row (?P<dfVelo_row>\d+) 'From Sub': (?P<from_sub>.+), 'To Sub': (?P<to_sub>.+)\ndfTads row (?P<dfTads_row>\d+) 'FromBus': (?P<from_bus>.+), 'ToBus': (?P<to_bus>.+)\ndfTads row \d+: Reporting Year Number: (?P<year>\d+)(?:\n(?P<retired>dfTads row \d+: Retired on .+))?"
    )

    matches = defaultdict(list)

    # Read the input file and parse matches
    with open(input_file, "r") as infile:
        content = infile.read()

    for match in match_pattern.finditer(content):
        from_bus = match.group("from_bus")
        to_bus = match.group("to_bus")
        year = int(match.group("year"))
        match_text = match.group(0)
        retired_line = match.group("retired")
        if retired_line:
            match_text += f"\n{retired_line}"
        matches[(from_bus, to_bus)].append((year, match_text))

    # Filter to keep only the latest year for each distinct (from_bus, to_bus) pair
    latest_matches = []
    for bus_pair, year_matches in matches.items():
        latest_match = max(year_matches, key=lambda x: x[0])[1]
        latest_matches.append(latest_match)

    # Sort matches based on the new numbering
    latest_matches.sort()

    # Write the latest matches to the output file with renumbered matches
    with open(output_file, "w") as outfile:
        for i, match in enumerate(latest_matches, start=1):
            renumbered_match = re.sub(r"Match \d+:", f"Match {i}:", match)
            outfile.write(renumbered_match + "\n")


# %%
# Call the function with the input and output file paths
getLatestReportedEntries(
    "processedData/matches.txt", "processedData/matches-latest.txt"
)
# %%

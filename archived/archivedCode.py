# %%
# Function to find and display matches with verbosity
# def find_and_display_matches(df1, df2, max_matches=5):
#     matches = []
#     count = 0

#     for idx1, row1 in df1.iterrows():
#         match_found = False
#         for idx2, row2 in df2.iterrows():
#             if (row1['From Sub'] == row2['FromBus'] and row1['To Sub'] == row2['ToBus']) or \
#                (row1['From Sub'] == row2['ToBus'] and row1['To Sub'] == row2['FromBus']):
#                 matches.append((idx1, idx2))
#                 count += 1
#                 if row1['From Sub'] == row2['FromBus'] and row1['To Sub'] == row2['ToBus']:
#                     print(f"Match {count}:")
#                     print(f"dfVelo row {idx1} 'From Sub': {row1['From Sub']}, 'To Sub': {row1['To Sub']}")
#                     print(f"dfTads row {idx2} 'FromBus': {row2['FromBus']}, 'ToBus': {row2['ToBus']}\n")
#                 elif row1['From Sub'] == row2['ToBus'] and row1['To Sub'] == row2['FromBus']:
#                     print(f"Match {count}:")
#                     print(f"dfVelo row {idx1} 'From Sub': {row1['From Sub']}, 'To Sub': {row1['To Sub']}")
#                     print(f"dfTads row {idx2} 'FromBus': {row2['ToBus']}, 'ToBus': {row2['FromBus']}\n")
#                 match_found = True
#                 if count >= max_matches:
#                     return matches
#                 break
#         if match_found:
#             continue
#     return matches

# %%
# matchFileAddr = os.path.join(rawDataFolder, "Match_file.csv")
# dfMatch = pd.read_csv(matchFileAddr)
# colNamesMatch = dfMatch.columns.tolist()
# colNamesMatch
# matchedIndices = dfMatch['Rec_ID'].notna()
# num = sum(matchedIndices)
# dfMatched = dfMatch[matchedIndices]
# dfMatchedAddr = os.path.join(processedDataFolder, "dfMatched.xlsx")
# dfMatched.to_excel(dfMatchedAddr)
# sizeMatched0 = dfMatched.shape
# print(f"Size of Match_file csv before any filtering: {sizeMatched0[0]}, {sizeMatched0[1]}")

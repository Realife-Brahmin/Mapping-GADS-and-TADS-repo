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

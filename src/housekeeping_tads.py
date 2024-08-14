# %%
import pandas as pd
import os

# pylint: disable=undefined-variable line-too-long invalid-name missing-function-docstring f-string-without-interpolation

def get_reduced_df(dfMatch):
    """
    This function takes a pandas DataFrame (dfMatch) and returns a new DataFrame containing specific columns
    with a dynamic 'combo' column as the first column, with FromBus always preceding ToBus and sorted.

    Args:
        dfMatch: The input pandas DataFrame.

    Returns:
        A new DataFrame containing the following columns:
            - 'combo' (filled with a string combining CircuitTypeCode, FromBus-ToBus, and ElementIdentifierName) - This column becomes the first column in the output.
            - 'ElementIdentifierName'
            - 'CompanyName'
            - ... (other desired columns) - Include any other columns you want in the output DataFrame.
            - 'RetirementDate' (added)
            - 'Rec_ID' (added)
    """

    # Select desired columns from the input DataFrame
    desired_cols = [
        "ElementIdentifierName",
        "CompanyName",
        "RegionCode",
        "FromBus",
        "ToBus",
        "TertiaryBus",
        "Miles",
        "BESExemptedFlag",
        "NumberOfTerminals",
        "CircuitTypeCode",
        "VoltageClassCodeName",
        "ParentCode",
        "ConductorsPerPhaseCode",
        "OverheadGroundWireCode",
        "InsulatorTypeCode",
        "CableTypeCode",
        "StructureMaterialCode",
        "StructureTypeCode",
        "CircuitsPerStructureCode",
        "TerrainCode",
        "ElevationCode",
        "InServiceDate",
        "RetirementDate",
        "Rec_ID",
    ]

    df_reduced = dfMatch[desired_cols]

    df_reduced = rearrangeColumns(df_reduced)
    # Create a copy of the DataFrame to avoid modifying the original
    df_reduced_copy = df_reduced.copy()

    # Extract the first word from CircuitTypeCode
    df_reduced_copy["CircuitTypeCode_FirstWord"] = (
        df_reduced_copy["CircuitTypeCode"].str.split().str[0]
    )

    # Temporary column to store the sorted Bus combination
    df_reduced_copy["SortedBus"] = df_reduced_copy[["FromBus", "ToBus"]].apply(
        lambda x: "-".join(sorted(x)), axis=1
    )

    # Create a dynamic combo option string using the first word and sorted FromBus-ToBus
    df_reduced_copy["combo"] = df_reduced_copy.apply(
        lambda row: f"{row['CircuitTypeCode_FirstWord']} {row['SortedBus']} {row['ElementIdentifierName']}",
        axis=1,
    )

    df_reduced_copy.pop("CircuitTypeCode_FirstWord")  # No longer needed
    df_reduced_copy.pop("SortedBus")  # No longer needed

    # Sort the DataFrame by FromBus and ToBus
    df_reduced_copy = df_reduced_copy.sort_values(by=["FromBus", "ToBus"])

    # Make 'combo' the first column
    col = df_reduced_copy.pop("combo")
    df_reduced_copy.insert(loc=0, column="combo", value=col)

    return df_reduced_copy


def filter_tlines_by_latest_reported_year(df):
    """
    Filters a DataFrame to include only the first row for each unique combination of 'FromBus' and 'ToBus' columns,
    assuming 'FromBus', 'ToBus', and 'ReportingYearNbr' are already sorted in descending order by 'ReportingYearNbr'.

    Args:
        df: A pandas DataFrame containing columns 'FromBus', 'ToBus', and 'ReportingYearNbr' (sorted by 'ReportingYearNbr' descending).

    Returns:
        A new DataFrame containing the first row for each unique combination of 'FromBus' and 'ToBus' columns.
    """

    # Initialize variables to track current and previous values
    current_frombus = None
    current_tobus = None
    filtered_df = pd.DataFrame(
        columns=df.columns
    )  # Create empty DataFrame to store filtered rows

    # Iterate through each row
    for index, row in df.iterrows():
        frombus, tobus, _ = row["FromBus"], row["ToBus"], row["ReportingYearNbr"]

        # Check if new unique combination of 'FromBus' and 'ToBus' is encountered
        if (current_frombus != frombus) or (current_tobus != tobus):
            # Add previous row (if it exists) to the filtered DataFrame
            if current_frombus is not None and current_tobus is not None:
                try:
                    # Attempt to add the previous row using loc
                    filtered_df = pd.concat(
                        [filtered_df, df.loc[(current_frombus, current_tobus)]],
                        ignore_index=True,
                    )
                except KeyError:
                    # Handle potential KeyError (e.g., missing value in previous combination)
                    # You can choose a strategy like logging the error or skipping the row
                    print(
                        f"KeyError encountered for ({current_frombus}, {current_tobus}). Skipping row."
                    )

            # Update current values
            current_frombus = frombus
            current_tobus = tobus

        # Always append the current row (might be the first or subsequent for the same 'FromBus' and 'ToBus')
        filtered_df = pd.concat([filtered_df, row], ignore_index=True)

    return filtered_df

def get_latest_entries(dfTadsSorted):
    # Drop duplicates, keeping the first occurrence
    dfTadsLatest = dfTadsSorted.drop_duplicates(
        subset=["FromBus", "ToBus"], keep="first"
    )

    return dfTadsLatest

def sort_and_shift_columns(df):
    """
    Sorts a DataFrame by 'FromBus', 'ToBus', 'ReportingYearNbr' and rearranges those columns to be first.

    Args:
        df: A pandas DataFrame containing columns 'FromBus', 'ToBus', and 'ReportingYearNbr'.

    Returns:
        A new pandas DataFrame with all columns sorted by 'FromBus', 'ToBus', 'ReportingYearNbr'
        with those three columns positioned at the beginning.
    """

    # Sort by 'FromBus', 'ToBus', 'ReportingYearNbr' (descending order for ReportingYearNbr)
    sorted_df = df.sort_values(
        by=["FromBus", "ToBus", "ReportingYearNbr"], ascending=[True, True, False]
    )

    # Define desired column order (efficient approach)
    desired_column_order = ["FromBus", "ToBus", "ReportingYearNbr"] + [
        col
        for col in sorted_df.columns
        if col not in ["FromBus", "ToBus", "ReportingYearNbr"]
    ]

    # Reorder columns using `.loc` indexing
    shifted_df = sorted_df.loc[:, desired_column_order]


    return shifted_df

def sort_and_shift_columns_dfVelo(df):
    """
    Sorts a DataFrame by 'From Sub', 'To Sub' and rearranges those columns to be first.

    Args:
        df: A pandas DataFrame containing columns 'From Sub', 'To Sub' apart from any other columns.

    Returns:
        A new pandas DataFrame with all columns sorted by 'FromBus', 'ToBus
        with those two columns positioned at the beginning.
    """

    # Sort by 'FromBus', 'ToBus', 'ReportingYearNbr' (descending order for ReportingYearNbr)
    sorted_df = df.sort_values(
        by=["From Sub", "To Sub"], ascending=[True, True]
    )

    # Define desired column order (efficient approach)
    desired_column_order = ["From Sub", "To Sub"] + [
        col for col in sorted_df.columns if col not in ["From Sub", "To Sub"]
    ]

    # Reorder columns using `.loc` indexing
    shifted_df = sorted_df.loc[:, desired_column_order]

    return shifted_df


def get_matched_entries(dfVeloSorted, dfTadsLatest, getMatchVeloTlines=True):
    """
    Match entries between dfVeloSorted and dfTadsLatest based on 'From Sub'/'To Sub' and 'FromBus'/'ToBus' pairs.

    This function iterates through `dfVeloSorted` and `dfTadsLatest`, matching rows where the
    'From Sub'/'To Sub' pair from `dfVeloSorted` matches the 'FromBus'/'ToBus' pair from `dfTadsLatest`,
    either in the same order or reversed. The matching rows from `dfTadsLatest` are returned in
    `dfTadsMatched`, with the corresponding 'Rec_ID' from `dfVeloSorted` appended. Optionally,
    it can also return `dfVeloMatched`, which contains the matched rows from `dfVeloSorted`.

    Parameters
    ----------
    - `dfVeloSorted` : pandas.DataFrame
        A DataFrame sorted by 'From Sub' and 'To Sub', containing transmission line data
        from the Velocity Suite. It includes columns such as 'From Sub', 'To Sub', and 'Rec_ID'.

    - `dfTadsLatest` : pandas.DataFrame
        A DataFrame containing transmission line data from the Transmission Availability
        Database System (TADS). It includes columns such as 'FromBus' and 'ToBus'.

    - `getMatchVeloTlines` : bool, optional (default=True)
        If set to True, the function will also return a DataFrame containing the rows
        from `dfVeloSorted` that were matched with `dfTadsLatest`.

    Returns
    ----------
    If `getMatchVeloTlines` is False:
        `dfTadsMatched` : pandas.DataFrame
            A DataFrame that contains the matched rows from `dfTadsLatest` with an added
            'Rec_ID' column from `dfVeloSorted`.

    If `getMatchVeloTlines` is True:
        `dfTadsMatched`, `dfVeloMatched` : pandas.DataFrame, pandas.DataFrame
            `dfTadsMatched` is as described above.
            `dfVeloMatched` contains all rows from `dfVeloSorted` that were matched
            with `dfTadsLatest`.

    Example
    ----------
    >>> dfVeloSorted = pd.DataFrame({
    ...     'From Sub': ['SubA', 'SubB', 'SubC'],
    ...     'To Sub': ['SubD', 'SubE', 'SubF'],
    ...     'Rec_ID': ['R1', 'R2', 'R3']
    ... })
    >>> dfTadsLatest = pd.DataFrame({
    ...     'FromBus': ['SubA', 'SubE', 'SubF'],
    ...     'ToBus': ['SubD', 'SubB', 'SubC']
    ... })
    >>> dfTadsMatched, dfVeloMatched = get_matched_entries(dfVeloSorted, dfTadsLatest, getMatchVeloTlines=True)
    >>> print(dfTadsMatched)
        FromBus  ToBus Rec_ID
    0    SubA   SubD     R1
    1    SubE   SubB     R2
    2    SubF   SubC     R3
    >>> print(dfVeloMatched)
        From Sub  To Sub Rec_ID
    0     SubA    SubD     R1
    1     SubB    SubE     R2
    2     SubC    SubF     R3
    """
    matched_rows = []
    matched_velo_rows = []

    # Iterate through both DataFrames
    for i in range(len(dfVeloSorted)):
        from_sub, to_sub = str(dfVeloSorted.iloc[i]["From Sub"]), str(
            dfVeloSorted.iloc[i]["To Sub"]
        )
        rec_id = dfVeloSorted.iloc[i]["Rec_ID"]
        matched = False

        for j in range(len(dfTadsLatest)):
            from_bus, to_bus = str(dfTadsLatest.iloc[j]["FromBus"]), str(
                dfTadsLatest.iloc[j]["ToBus"]
            )

            if (from_sub == from_bus and to_sub == to_bus) or (
                from_sub == to_bus and to_sub == from_bus
            ):
                matched_row = dfTadsLatest.iloc[j].copy()
                matched_row["Rec_ID"] = rec_id
                matched_rows.append(matched_row)
                matched = True

        if matched:
            matched_velo_rows.append(dfVeloSorted.iloc[i])

    dfTadsMatched = pd.DataFrame(matched_rows)

    if getMatchVeloTlines:
        dfVeloMatched = pd.DataFrame(matched_velo_rows)
        return dfTadsMatched, dfVeloMatched

    return dfTadsMatched



# def get_matched_entries0(dfVeloSorted, dfTadsLatest):
#     matched_rows = []

#     # Iterate through both DataFrames
#     for i in range(len(dfVeloSorted)):
#         from_sub, to_sub = str(dfVeloSorted.iloc[i]["From Sub"]), str(
#             dfVeloSorted.iloc[i]["To Sub"]
#         )
#         rec_id = dfVeloSorted.iloc[i]["Rec_ID"]
#         for j in range(len(dfTadsLatest)):
#             from_bus, to_bus = str(dfTadsLatest.iloc[j]["FromBus"]), str(
#                 dfTadsLatest.iloc[j]["ToBus"]
#             )

#             if (from_sub == from_bus and to_sub == to_bus) or (
#                 from_sub == to_bus and to_sub == from_bus
#             ):
#                 matched_row = dfTadsLatest.iloc[j].copy()
#                 matched_row["Rec_ID"] = rec_id
#                 matched_rows.append(matched_row)

#     dfTadsMatched = pd.DataFrame(matched_rows)

#     return dfTadsMatched


def rearrangeColumns(df, col1="FromBus", col2="ToBus"):
    # Make a copy of the DataFrame to avoid modifying the original
    df = df.copy()

    # Iterate through each row and swap col1 and col2 if necessary
    for index, row in df.iterrows():
        value1 = str(row[col1])
        value2 = str(row[col2])

        if value1 > value2:
            df.at[index, col1] = value2
            df.at[index, col2] = value1

    return df


# %%

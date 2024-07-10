# %%
import pandas as pd
import os


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
# %%
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


# %%


def get_matched_entries(dfVeloSorted, dfTadsLatest):
    matched_indices = []

    # Iterate through both DataFrames
    for i in range(len(dfVeloSorted)):
        from_sub, to_sub = str(dfVeloSorted.iloc[i]["From Sub"]), str(
            dfVeloSorted.iloc[i]["To Sub"]
        )
        for j in range(len(dfTadsLatest)):
            from_bus, to_bus = str(dfTadsLatest.iloc[j]["FromBus"]), str(
                dfTadsLatest.iloc[j]["ToBus"]
            )

            if (from_sub == from_bus and to_sub == to_bus) or (
                from_sub == to_bus and to_sub == from_bus
            ):
                matched_indices.append(j)

    dfTadsMatched = dfTadsLatest.iloc[matched_indices].copy()

    return dfTadsMatched


# %%

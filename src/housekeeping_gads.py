# %%
# pylint: disable=undefined-variable line-too-long invalid-name missing-function-docstring f-string-without-interpolation
import pandas as pd
import us

def match_by_eia_code(dfVeloP, dfGads):
    # Get the unique 'EIA ID' values from dfVeloP
    eia_ids = dfVeloP["EIA ID"].unique()

    # Filter dfGads to include only rows where 'EIACode' is in the list of 'EIA ID' values
    dfGadsFiltered = dfGads[dfGads["EIACode"].isin(eia_ids)]

    return dfGadsFiltered


# def match_by_eia_code_and_add_recid(dfVeloP, dfGads):
#     # Merge dfVeloP and dfGads on 'EIA ID' and 'EIACode' columns to add 'Rec_ID' from dfVeloP to dfGads
#     dfMerged = pd.merge(
#         dfGads,
#         dfVeloP[["EIA ID", "Rec_ID"]],
#         left_on="EIACode",
#         right_on="EIA ID",
#         how="left",
#     )

#     # Drop rows where 'EIA ID' or 'Rec_ID' is NaN
#     dfGadsFiltered = dfMerged.dropna(subset=["EIA ID", "Rec_ID"])

#     # Drop the duplicate 'EIA ID' column from the merge
#     dfGadsFiltered = dfGadsFiltered.drop(columns=["EIA ID"])

#     return dfGadsFiltered


def match_by_eia_code_and_add_recid(dfVeloP, dfGads):
    # Drop duplicates in dfVeloP to avoid creating extra rows in the merge
    dfVeloP_unique = dfVeloP[["EIA ID", "Rec_ID"]].drop_duplicates(subset=["EIA ID"])

    # Merge dfVeloP and dfGads on 'EIA ID' and 'EIACode' columns to add 'Rec_ID' from dfVeloP to dfGads
    dfMerged = pd.merge(
        dfGads,
        dfVeloP_unique,
        left_on="EIACode",
        right_on="EIA ID",
        how="left",
    )

    # Drop rows where 'Rec_ID' is NaN (implying no matching EIA ID)
    dfGadsFiltered = dfMerged.dropna(subset=["Rec_ID"])

    # Drop the duplicate 'EIA ID' column from the merge
    dfGadsFiltered = dfGadsFiltered.drop(columns=["EIA ID"])

    return dfGadsFiltered


def match_by_plant_name_and_add_eia_recid(dfVeloP, dfVeloU):
    # Merge dfVeloP and dfVeloU on 'Plant Name' to add 'EIA ID' from dfVeloP to dfVeloU
    dfMerged = pd.merge(
        dfVeloU, dfVeloP[["Plant Name", "EIA ID", "Rec_ID"]], on="Plant Name", how="left"
    )

    return dfMerged


def eia_filtering(df, column_name="EIA ID"):
    # Drop rows where the specified column is NaN or 0
    df_filtered = df.dropna(subset=[column_name]).copy()
    df_filtered = df_filtered[df_filtered[column_name] != 0]

    # Function to clean up the EIA ID
    def clean_eia_id(value):
        if isinstance(value, str):
            # Handle weird numbers separated by ':' or ','
            if ":" in value or "," in value:
                value = value.split(":")[0].split(",")[0]
            # Remove leading zeros
            value = value.lstrip("0")
        return value

    # Apply the cleaning function to the specified column
    df_filtered[column_name] = df_filtered[column_name].apply(clean_eia_id)

    return df_filtered


def filter_non_empty_column(df, column_name="EIA ID"):
    # Drop rows where the specified column is NaN
    df_filtered = df.dropna(subset=[column_name]).copy()

    return df_filtered


def filterRetiredPlants(dfVeloP):
    # Columns to check for non-zero values
    capacity_columns = [
        "Operating Cap MW",
        "Planned Cap MW",
        "Canceled Cap MW",
        "Mothballed Cap MW",
    ]

    # Filter out plants with no non-zero values in the specified columns
    dfVeloP_filtered = dfVeloP[(dfVeloP[capacity_columns] != 0).any(axis=1)]

    return dfVeloP_filtered


def computeCombinedMWRating(dfVeloP):
    # Columns to sum up
    capacity_columns = [
        "Operating Cap MW",
        "Planned Cap MW",
        # "Canceled Cap MW",
        "Mothballed Cap MW",
        # "Retired Cap MW",
    ]

    # Compute the sum of the specified columns
    dfVeloP["Combined Cap MW"] = dfVeloP[capacity_columns].sum(axis=1)

    return dfVeloP


def filter_states(dfGads, veloStates):
    # Create a mapping of full state names to their abbreviations using the us package
    state_abbreviations = {state.name: state.abbr for state in us.states.STATES}

    # Print the state abbreviations mapping for debugging
    print("State Abbreviations Mapping:")
    print(state_abbreviations)

    # Map StateName to state abbreviations
    dfGads["StateAbbreviation"] = dfGads["StateName"].map(state_abbreviations)

    # Print a sample of the DataFrame to debug the mapping
    print("Sample of dfGads with StateAbbreviation:")
    print(dfGads[["StateName", "StateAbbreviation"]].head(10))

    # Filter dfGads based on the StateAbbreviation being in veloStates
    dfGadsFilt = dfGads[dfGads["StateAbbreviation"].isin(veloStates)]

    # Drop the temporary 'StateAbbreviation' column
    dfGadsFilt = dfGadsFilt.drop(columns=["StateAbbreviation"])

    return dfGadsFilt

def sort_and_reorder_columns(df, sort_columns=None):
    if sort_columns is None:
        sort_columns = ["UnitName", "UtilityName"]

    # Sort the DataFrame by the specified columns
    df_sorted = df.sort_values(by=sort_columns)

    # Move the sort columns to the front
    cols = sort_columns + [col for col in df.columns if col not in sort_columns]
    df_reordered = df_sorted[cols]

    return df_reordered


# %%

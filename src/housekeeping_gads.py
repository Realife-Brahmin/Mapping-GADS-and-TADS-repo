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


def match_by_eia_code_and_add_recid(dfVeloP, dfGads):
    # Merge dfVeloP and dfGads on 'EIA ID' and 'EIACode' columns to add 'Rec_ID' from dfVeloP to dfGads
    dfMerged = pd.merge(
        dfGads,
        dfVeloP[["EIA ID", "Rec_ID"]],
        left_on="EIACode",
        right_on="EIA ID",
        how="left",
    )

    # Drop the duplicate 'EIA ID' column from the merge
    dfGadsFiltered = dfMerged.drop(columns=["EIA ID"])

    return dfGadsFiltered


def match_by_plant_name_and_add_eia_recid(dfVeloP, dfVeloU):
    # Merge dfVeloP and dfVeloU on 'Plant Name' to add 'EIA ID' from dfVeloP to dfVeloU
    dfMerged = pd.merge(
        dfVeloU, dfVeloP[["Plant Name", "EIA ID", "Rec_ID"]], on="Plant Name", how="left"
    )

    return dfMerged

def filter_non_empty_column(df, column_name="EIA ID"):
    # Drop rows where the specified column is NaN
    df_filtered = df.dropna(subset=[column_name]).copy()

    return df_filtered

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

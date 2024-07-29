# %%
# pylint: disable=undefined-variable line-too-long invalid-name missing-function-docstring f-string-without-interpolation
import pandas as pd
import us

# def filter_states(dfGads, states_to_keep=["Illinois", "Indiana", "Wisconsin"]):
#     # Define the list of state names to filter
#     # states_to_keep = ["IL", "IN", "WI"]

#     # Filter the DataFrame
#     dfFiltered = dfGads[dfGads["StateName"].isin(states_to_keep)]

#     return dfFiltered

def filter_by_eia_code(dfVelo, dfGads):
    # Get the unique 'EIA ID' values from dfVelo
    eia_ids = dfVelo["EIA ID"].unique()

    # Filter dfGads to include only rows where 'EIACode' is in the list of 'EIA ID' values
    dfFiltered = dfGads[dfGads["EIACode"].isin(eia_ids)]

    return dfFiltered

def filter_non_empty_eia_id(dfVeloSorted):
    # Drop rows where 'EIA ID' is NaN
    dfVeloEIA = dfVeloSorted.dropna(subset=["EIA ID"]).copy()

    return dfVeloEIA


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

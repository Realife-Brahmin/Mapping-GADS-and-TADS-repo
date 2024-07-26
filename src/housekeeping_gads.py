# %%
# pylint: disable=undefined-variable line-too-long invalid-name missing-function-docstring f-string-without-interpolation
import pandas as pd

def filter_states(dfGads, states_to_keep=["Illinois", "Indiana", "Wisconsin"]):
    # Define the list of state names to filter
    # states_to_keep = ["IL", "IN", "WI"]

    # Filter the DataFrame
    dfFiltered = dfGads[dfGads["StateName"].isin(states_to_keep)]

    return dfFiltered

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

# %%

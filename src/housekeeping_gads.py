# %%
import pandas as pd

def filter_states(dfGads, states_to_keep=["Illinois", "Indiana", "Wisconsin"]):
    # Define the list of state names to filter
    # states_to_keep = ["IL", "IN", "WI"]

    # Filter the DataFrame
    dfFiltered = dfGads[dfGads["StateName"].isin(states_to_keep)]

    return dfFiltered

def filter_by_eia_code0(dfVelo, dfGads):
    # Get the unique 'EIA ID' values from dfVelo
    eia_ids = dfVelo["EIA ID"].unique()

    # Filter dfGads to include only rows where 'EIACode' is in the list of 'EIA ID' values
    dfFiltered = dfGads[dfGads["EIACode"].isin(eia_ids)]

    return dfFiltered


def filter_by_eia_code(dfVelo, dfGads):
    import pandas as pd
    # Merge the two DataFrames on 'EIA ID' and 'EIACode' columns
    dfMerged = pd.merge(
        dfGads,
        dfVelo[["EIA ID", "Rec_ID"]],
        left_on="EIACode",
        right_on="EIA ID",
        how="inner",
    )

    # Drop the duplicate 'EIA ID' column from the merge
    dfFiltered = dfMerged.drop(columns=["EIA ID"])

    return dfFiltered

# %%

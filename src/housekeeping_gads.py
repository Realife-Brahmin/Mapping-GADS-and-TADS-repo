# %%
# pylint: disable=undefined-variable line-too-long invalid-name missing-function-docstring f-string-without-interpolation
import pandas as pd
import us

def match_by_eia_code(dfVeloP, dfGads):
    """
    Filters `dfGads` at places with matching `EIACode` in `dfVeloP`
    
    Filters the `dfGads` DataFrame to include only rows where the `'EIACode'` matches
    any `'EIA ID'` found in the `dfVeloP` DataFrame.
    
    Parameters
    ----------
    - `dfVeloP` : pandas.DataFrame
        A DataFrame from Velocity Suite containing at least the `'EIA ID'` column, which represents
        unique identifiers for plants in the Velocity suite dataset.
    - `dfGads` : pandas.DataFrame
        A DataFrame from GADS containing at least the `'EIACode'` column, which represents
        unique identifiers for units in the GADS dataset.

    Returns
    ----------
    dfGadsFiltered : pandas.DataFrame
        A filtered GADS DataFrame that includes only the rows from dfGads where
        `'EIACode'` matches any `'EIA ID'` from `dfVeloP`.

    Example
    ----------
    ```
    >>> dfVeloP = pd.DataFrame({'EIA ID': [10474, 10521, 10552]})
    >>> dfGads = pd.DataFrame({'EIACode': [10474, 12345, 10552, 67890]})
    >>> dfGadsFiltered = match_by_eia_code(dfVeloP, dfGads)
    >>> print(dfGadsFiltered)
        EIACode
    0    10474
    2    10552
    ```
    """
    # Get the unique 'EIA ID' values from dfVeloP
    eia_ids = dfVeloP["EIA ID"].unique()

    # Filter dfGads to include only rows where 'EIACode' is in the list of 'EIA ID' values
    dfGadsFiltered = dfGads[dfGads["EIACode"].isin(eia_ids)]

    return dfGadsFiltered


def match_by_eia_code_and_add_recid(dfVeloP, dfGads):
    """
    Filters dfGads at places where it has a matching EIA Code with `dfVeloP`
    
    Filters the `dfGads` DataFrame to include only rows where the `'EIACode'` matches any `'EIA ID'` found in the `dfVeloP` DataFrame, and appends the corresponding `'Rec_ID'` from `dfVeloP` to the filtered `dfGads`.

    Parameters
    ----------
    - `dfVeloP` : pandas.DataFrame
        A DataFrame from the Velocity Suite containing at least the `'EIA ID'`
        and `'Rec_ID'` columns. `'EIA ID'` represents unique identifiers for
        plants, and `'Rec_ID'` is the corresponding record identifier unique to Velocity Suite.
    - `dfGads` : pandas.DataFrame
        A DataFrame from the Generation Availability Data System (GADS)
        containing at least the `'EIACode'` column, which represents unique
        identifiers for units.

    Returns
    ----------
    `dfGadsFiltered` : pandas.DataFrame
        A filtered DataFrame that includes only the rows from `dfGads` where
        `'EIACode'` matches any `'EIA ID'` from `dfVeloP`, with an additional
        `'Rec_ID'` column from `dfVeloP`.

    Example
    ----------
    ```
    >>> dfVeloP = pd.DataFrame({
    ...     'EIA ID': [10474, 10521, 10552],
    ...     'Rec_ID': ['R1', 'R2', 'R3']
    ... })
    >>> dfGads = pd.DataFrame({
    ...     'EIACode': [10474, 12345, 10552, 67890]
    ... })
    >>> dfGadsFiltered = match_by_eia_code_and_add_recid(dfVeloP, dfGads)
    >>> print(dfGadsFiltered)
        EIACode Rec_ID
    0    10474     R1
    2    10552     R3
    ```
    """
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
    """
    Merge dfVeloP and dfVeloU on 'Plant Name' to add 'EIA ID' and 'Rec_ID'.

    This function merges dfVeloP and dfVeloU DataFrames based on the 'Plant Name'
    column, appending the corresponding 'EIA ID' and 'Rec_ID' from dfVeloP
    to the matched rows in dfVeloU.

    Parameters
    ----------
    - `dfVeloP` : pandas.DataFrame
        A DataFrame from the Velocity Suite containing at least the `'Plant Name'`,
        `'EIA ID'`, and `'Rec_ID'` columns. `'Plant Name'` represents the name
        of the plant, `'EIA ID'` represents the unique identifier for plants,
        and `'Rec_ID'` is the corresponding record identifier unique to the Velocity Suite.

    - `dfVeloU` : pandas.DataFrame
        A DataFrame from the Velocity Suite or another related dataset containing
        at least the `'Plant Name'` column. This DataFrame will be enriched with
        the `'EIA ID'` and `'Rec_ID'` columns from `dfVeloP`.

    Returns
    ----------
    `dfMerged` : pandas.DataFrame
        A DataFrame that includes all rows from `dfVeloU` with additional columns
        `'EIA ID'` and `'Rec_ID'` from `dfVeloP` where the `'Plant Name'` matches.

    Example
    ----------
    >>> dfVeloP = pd.DataFrame({
    ...     'Plant Name': ['Plant A', 'Plant B', 'Plant C'],
    ...     'EIA ID': [101, 102, 103],
    ...     'Rec_ID': ['R1', 'R2', 'R3']
    ... })
    >>> dfVeloU = pd.DataFrame({
    ...     'Plant Name': ['Plant A', 'Plant D', 'Plant C']
    ... })
    >>> dfMerged = match_by_plant_name_and_add_eia_recid(dfVeloP, dfVeloU)
    >>> print(dfMerged)
        Plant Name  EIA ID Rec_ID
    0    Plant A     101     R1
    1    Plant D     NaN    NaN
    2    Plant C     103     R3
    """
    # Merge dfVeloP and dfVeloU on 'Plant Name' to add 'EIA ID' from dfVeloP to dfVeloU
    dfMerged = pd.merge(
        dfVeloU, dfVeloP[["Plant Name", "EIA ID", "Rec_ID"]], on="Plant Name", how="left"
    )

    return dfMerged


def eia_filtering(df, column_name="EIA ID"):
    """
    Filter and clean EIA ID values in the specified column of a DataFrame.

    This function filters out rows where the specified column (`column_name`)
    contains NaN or 0 values. It also cleans up the values in the column by
    removing leading zeros and handling cases where the values are formatted
    as lists or ranges (e.g., "12345:67890" or "12345,67890") by keeping only
    the first number.

    Parameters
    ----------
    - `df` : pandas.DataFrame
        The DataFrame containing the column to be filtered and cleaned.

    - `column_name` : str, optional (default="EIA ID")
        The name of the column in `df` that contains the EIA IDs to be filtered
        and cleaned.

    Returns
    ----------
    `df_filtered` : pandas.DataFrame
        A DataFrame where the specified column has been filtered for NaN or 0
        values and cleaned of leading zeros and unwanted characters.

    Example
    ----------
    >>> df = pd.DataFrame({
    ...     'EIA ID': ['00235', '02341', '0', None, '12345:67890', '12345,67890']
    ... })
    >>> df_filtered = eia_filtering(df, column_name="EIA ID")
    >>> print(df_filtered)
        EIA ID
    0    235
    1   2341
    4  12345
    5  12345
    """
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
        "Canceled Cap MW",
        "Mothballed Cap MW",
        "Retired Cap MW",
    ]

    # Compute the sum of the specified columns
    dfVeloP["Combined Cap MW"] = dfVeloP[capacity_columns].sum(axis=1)

    return dfVeloP


def filter_states(dfGads, veloStates):
    # Create a mapping of full state names to their abbreviations using the us package
    state_abbreviations = {state.name: state.abbr for state in us.states.STATES}

    # Map StateName to state abbreviations
    dfGads["StateAbbreviation"] = dfGads["StateName"].map(state_abbreviations)

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

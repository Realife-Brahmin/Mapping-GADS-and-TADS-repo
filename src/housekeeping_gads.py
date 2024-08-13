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


def match_by_eia_code_and_add_recid(dfVeloP, dfGads, getMatchVeloP=False):
    """
    Filters dfGads at places where it has a matching EIA Code with `dfVeloP`
    and optionally returns the matching rows from dfVeloP.

    Filters the `dfGads` DataFrame to include only rows where the `'EIACode'`
    matches any `'EIA ID'` found in the `dfVeloP` DataFrame, and appends the
    corresponding `'Rec_ID'` from `dfVeloP` to the filtered `dfGads`. Optionally,
    returns the matching rows from `dfVeloP` as well.

    Parameters
    ----------
    - `dfVeloP` : pandas.DataFrame
        A DataFrame from the Velocity Suite containing at least the `'EIA ID'`
        and `'Rec_ID'` columns. `'EIA ID'` represents unique identifiers for
        plants, and `'Rec_ID'` is the corresponding record identifier unique
        to Velocity Suite.

    - `dfGads` : pandas.DataFrame
        A DataFrame from the Generation Availability Data System (GADS)
        containing at least the `'EIACode'` column, which represents unique
        identifiers for units.

    - `getMatchVeloP` : bool, optional (default=False)
        If set to True, the function will also return a DataFrame containing
        the rows from `dfVeloP` that were matched with `dfGads`.

    Returns
    ----------
    If `getMatchVeloP` is False:
        `dfGadsFiltered` : pandas.DataFrame
            A filtered DataFrame that includes only the rows from `dfGads` where
            `'EIACode'` matches any `'EIA ID'` from `dfVeloP`, with an additional
            `'Rec_ID'` column from `dfVeloP`.

    If `getMatchVeloP` is True:
        `dfGadsFiltered`, `dfVeloPFiltered` : pandas.DataFrame, pandas.DataFrame
            `dfGadsFiltered` is as described above.
            `dfVeloPFiltered` contains all rows from `dfVeloP` that were matched
            into `dfGads`.

    Example
    ----------
    >>> dfVeloP = pd.DataFrame({
    ...     'EIA ID': [10474, 10521, 10552],
    ...     'Rec_ID': ['R1', 'R2', 'R3']
    ... })
    >>> dfGads = pd.DataFrame({
    ...     'EIACode': [10474, 12345, 10552, 67890]
    ... })
    >>> dfGadsFiltered, dfVeloPFiltered = match_by_eia_code_and_add_recid(dfVeloP, dfGads, getMatchVeloP=True)
    >>> print(dfGadsFiltered)
        EIACode Rec_ID
    0    10474     R1
    2    10552     R3
    >>> print(dfVeloPFiltered)
        EIA ID Rec_ID
    0   10474     R1
    2   10552     R3
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

    if getMatchVeloP:
        # Filter dfVeloP to include only the rows that were matched with dfGads
        dfVeloPFiltered = dfVeloP[dfVeloP["EIA ID"].isin(dfGadsFiltered["EIACode"])]
        return dfGadsFiltered, dfVeloPFiltered

    return dfGadsFiltered


# def match_by_eia_code_and_add_recid(dfVeloP, dfGads):
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
    """
    Filter out rows with NaN values in the specified column.

    This function filters the `df` DataFrame to exclude rows where the specified
    column (`column_name`) contains NaN values.

    Parameters
    ----------
    - `df` : pandas.DataFrame
        The DataFrame to be filtered.

    - `column_name` : str, optional (default="EIA ID")
        The name of the column in `df` to check for NaN values. Rows with NaN
        values in this column will be dropped.

    Returns
    ----------
    `df_filtered` : pandas.DataFrame
        A DataFrame that excludes rows where the specified column contains NaN values.

    Example
    ----------
    >>> df = pd.DataFrame({
    ...     'EIA ID': [12345, None, 67890, None],
    ...     'Plant Name': ['Plant A', 'Plant B', 'Plant C', 'Plant D']
    ... })
    >>> df_filtered = filter_non_empty_column(df, column_name="EIA ID")
    >>> print(df_filtered)
        EIA ID Plant Name
    0  12345    Plant A
    2  67890    Plant C
    """
    # Drop rows where the specified column is NaN
    df_filtered = df.dropna(subset=[column_name]).copy()

    return df_filtered


def filterRetiredPlants(dfVeloP):
    """
    Filter out plants that have no non-zero values in specified capacity columns.

    This function filters the `dfVeloP` DataFrame to exclude rows (plants) that
    have zero values in all of the specified capacity columns: "Operating Cap MW",
    "Planned Cap MW", "Canceled Cap MW", and "Mothballed Cap MW".

    Parameters
    ----------
    - `dfVeloP` : pandas.DataFrame
        A DataFrame from the Velocity Suite containing columns that represent
        various capacity measures such as "Operating Cap MW", "Planned Cap MW",
        "Canceled Cap MW", and "Mothballed Cap MW".

    Returns
    ----------
    `dfVeloP_filtered` : pandas.DataFrame
        A DataFrame that includes only the plants from `dfVeloP` that have
        non-zero values in at least one of the specified capacity columns.

    Example
    ----------
    >>> dfVeloP = pd.DataFrame({
    ...     'Operating Cap MW': [100, 0, 0],
    ...     'Planned Cap MW': [0, 0, 0],
    ...     'Canceled Cap MW': [0, 0, 10],
    ...     'Mothballed Cap MW': [0, 0, 0],
    ...     'Retired Cap MW': [50, 100, 200]
    ... })
    >>> dfVeloP_filtered = filterRetiredPlants(dfVeloP)
    >>> print(dfVeloP_filtered)
        Operating Cap MW  Planned Cap MW  Canceled Cap MW  Mothballed Cap MW  Retired Cap MW
    0               100               0                0                  0              50
    2                 0               0               10                  0             200
    """
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
    """
    Compute and add a 'Combined Cap MW' column by summing capacity columns.

    This function computes the sum of specified capacity columns in the `dfVeloP`
    DataFrame and adds the result as a new column named 'Combined Cap MW'.

    Parameters
    ----------
    - `dfVeloP` : pandas.DataFrame
        A DataFrame from the Velocity Suite containing columns for various
        capacity measures, such as "Operating Cap MW", "Planned Cap MW",
        "Canceled Cap MW", "Mothballed Cap MW", and "Retired Cap MW".

    Returns
    ----------
    `dfVeloP` : pandas.DataFrame
        The input DataFrame with an additional column 'Combined Cap MW' that
        represents the sum of the specified capacity columns for each row.

    Example
    ----------
    >>> dfVeloP = pd.DataFrame({
    ...     'Operating Cap MW': [100, 200, 150],
    ...     'Planned Cap MW': [50, 30, 20],
    ...     'Canceled Cap MW': [10, 5, 0],
    ...     'Mothballed Cap MW': [0, 0, 0],
    ...     'Retired Cap MW': [0, 50, 100]
    ... })
    >>> dfVeloP = computeCombinedMWRating(dfVeloP)
    >>> print(dfVeloP)
        Operating Cap MW  Planned Cap MW  Canceled Cap MW  Mothballed Cap MW  Retired Cap MW  Combined Cap MW
    0               100              50               10                  0               0              160
    1               200              30                5                  0              50              285
    2               150              20                0                  0             100              270
    """
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
    """
    Filter rows in dfGads based on state abbreviations in veloStates.

    This function filters the `dfGads` DataFrame to include only rows where
    the state name in `StateName` matches any state abbreviation in the
    `veloStates` set. The function temporarily maps full state names to their
    abbreviations using the `us` package and then filters based on those
    abbreviations.

    Parameters
    ----------
    - `dfGads` : pandas.DataFrame
        A DataFrame from the Generation Availability Data System (GADS) containing
        at least the `StateName` column, which holds the full names of U.S. states.

    - `veloStates` : set
        A set of state abbreviations (e.g., `{"IL", "IN", "WI"}`) against which
        the DataFrame will be filtered.

    Returns
    ----------
    `dfGadsFilt` : pandas.DataFrame
        A filtered DataFrame that includes only the rows from `dfGads` where
        the `StateName` corresponds to a state abbreviation in `veloStates`.

    Example
    ----------
    >>> import us
    >>> dfGads = pd.DataFrame({
    ...     'StateName': ['Illinois', 'Indiana', 'Wisconsin', 'Ohio', 'Michigan']
    ... })
    >>> veloStates = {'IL', 'IN', 'WI'}
    >>> dfGadsFilt = filter_states(dfGads, veloStates)
    >>> print(dfGadsFilt)
        StateName
    0  Illinois
    1   Indiana
    2 Wisconsin
    """
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
    """
    Sort and reorder columns in a DataFrame.

    This function sorts the DataFrame by the specified columns and then reorders
    the columns so that the sorted columns appear at the front of the DataFrame.

    Parameters
    ----------
    - `df` : pandas.DataFrame
        The DataFrame to be sorted and reordered.

    - `sort_columns` : list, optional (default=["UnitName", "UtilityName"])
        A list of columns by which to sort the DataFrame. If not provided,
        the DataFrame is sorted by "UnitName" and "UtilityName" by default.

    Returns
    ----------
    `df_reordered` : pandas.DataFrame
        A DataFrame that has been sorted by the specified columns and reordered
        so that the sorted columns appear at the front.

    Example
    ----------
    >>> df = pd.DataFrame({
    ...     'UtilityName': ['Utility A', 'Utility B', 'Utility C'],
    ...     'UnitName': ['Unit 2', 'Unit 1', 'Unit 3'],
    ...     'Capacity': [100, 200, 150]
    ... })
    >>> df_reordered = sort_and_reorder_columns(df)
    >>> print(df_reordered)
        UnitName UtilityName  Capacity
    1   Unit 1   Utility B       200
    0   Unit 2   Utility A       100
    2   Unit 3   Utility C       150
    """
    if sort_columns is None:
        sort_columns = ["UnitName", "UtilityName"]

    # Sort the DataFrame by the specified columns
    df_sorted = df.sort_values(by=sort_columns)

    # Move the sort columns to the front
    cols = sort_columns + [col for col in df.columns if col not in sort_columns]
    df_reordered = df_sorted[cols]

    return df_reordered


# %%

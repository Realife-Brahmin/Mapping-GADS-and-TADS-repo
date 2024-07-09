# %%
def find_tline_by_buses(
    df,
    value1,
    value2,
    col1_name='FromBus',
    col2_name='ToBus',
):
    """
    Finds rows in a DataFrame where a tuple of values matches two columns.

    Args:
        df (pandas.DataFrame): The DataFrame to search.
        col1_name (str): The name of the first column.
        col2_name (str): The name of the second column.
        value1 (str): The first value to match.
        value2 (str): The second value to match.

    Returns:
        pandas.DataFrame: A new DataFrame containing matching rows.
    """

    return df[
        (df[col1_name] == value1) & (df[col2_name] == value2)
        | (df[col1_name] == value2) & (df[col2_name] == value1)
    ]


# def filter_tlines_by_latest_reported_year(df):
#     """
#     Filters a DataFrame to only include the latest year for each unique combination of 'FromBus' and 'ToBus' columns.

#     Args:
#         df: A pandas DataFrame containing columns 'FromBus', 'ToBus', and 'ReportingYearNbr'.

#     Returns:
#         A new DataFrame containing only the rows with the latest 'ReportingYearNbr' for each unique combination of 'FromBus' and 'ToBus' columns.
#     """

#     # Sort the DataFrame by 'FromBus', 'ToBus', and 'ReportingYearNbr' (descending order)
#     sorted_df = df.sort_values(
#         by=["FromBus", "ToBus", "ReportingYearNbr"], ascending=[True, True, False]
#     )

#     # Get the groupby object based on 'FromBus' and 'ToBus' columns
#     grouped_df = sorted_df.groupby(["FromBus", "ToBus"])

#     # Apply a function to select the last row (i.e., the row with the latest year) from each group
#     filtered_df = grouped_df.apply(lambda x: x.iloc[-1:])

#     return filtered_df

# %%

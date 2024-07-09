# %%
def filter_tlines_by_latest_reported_year(df):
    """
    Filters a DataFrame to only include the latest year for each unique combination of 'FromBus' and 'ToBus' columns.

    Args:
        df: A pandas DataFrame containing columns 'FromBus', 'ToBus', and 'ReportingYearNbr'.

    Returns:
        A new DataFrame containing only the rows with the latest 'ReportingYearNbr' for each unique combination of 'FromBus' and 'ToBus' columns.
    """

    # Sort the DataFrame by 'FromBus', 'ToBus', and 'ReportingYearNbr' (descending order)
    sorted_df = df.sort_values(
        by=["FromBus", "ToBus", "ReportingYearNbr"], ascending=[True, True, False]
    )

    # Get the groupby object based on 'FromBus' and 'ToBus' columns
    grouped_df = sorted_df.groupby(["FromBus", "ToBus"])

    # Apply a function to select the last row (i.e., the row with the latest year) from each group
    filtered_df = grouped_df.apply(lambda x: x.iloc[-1:])

    return filtered_df

# %%

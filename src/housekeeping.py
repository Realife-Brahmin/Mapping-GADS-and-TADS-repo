# %%
import pandas as pd
import os

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
        by=["FromBus", "ToBus", "ReportingYearNbr"], ascending=[True, True, True]
    )

    # Get the groupby object based on 'FromBus' and 'ToBus' columns
    grouped_df = sorted_df.groupby(["FromBus", "ToBus"])

    # Apply a function to select the last row (i.e., the row with the latest year) from each group
    filtered_df = grouped_df.apply(lambda x: x.iloc[-1:])

    return filtered_df


def group_dfTads_by_frombus(df):
    """
    Sorts and groups dfTads by 'FromBus', keeping the three columns on the left in the resulting DataFrame.

    Args:
        df: A pandas DataFrame containing columns 'FromBus', 'ToBus', and 'ReportingYearNbr'.

    Returns:
        A new pandas DataFrame sorted by 'FromBus', 'ToBus', and 'ReportingYearNbr' with the latest year for each unique combination.
    """

    # Sort by 'FromBus', 'ToBus', 'ReportingYearNbr' (descending order)
    sorted_df = df.sort_values(
        by=["FromBus", "ToBus", "ReportingYearNbr"], ascending=[True, True, False]
    )

    # Apply filter function to get the latest year for each group (alternative approach)
    def get_latest_row(group):
        # Use a list to select specific columns
        return group[["ToBus", "ReportingYearNbr"]].iloc[
            -1:
        ]  # Select the last row (latest year)

    # Get the latest row (using get function with apply)
    latest_df = sorted_df.groupby("FromBus").apply(get_latest_row)

    # Set column names explicitly using a list
    latest_df.columns = ["ToBus", "ReportingYearNbr"]

    # Sort DataFrame by 'FromBus' (optional)
    latest_df = latest_df.sort_values(by="FromBus")

    # Return the filtered and potentially sorted DataFrame
    return latest_df


# Rest of your code using grouped_dfTads

# %%

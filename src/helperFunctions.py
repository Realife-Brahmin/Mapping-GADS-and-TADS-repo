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


# # Example usage
# df = pd.DataFrame(
#     {
#         "FromBus": ["Arcadian", "Stardust", "Zion", "Zephyr"],
#         "ToBus": ["Zion", "Arcadian", "Zephyr", "Arcadian"],
#     }
# )
# matched_df = find_tline_by_buses(df, "Arcadian", "Zion")
# print(matched_df)

# %%

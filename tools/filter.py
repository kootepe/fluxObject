import pandas as pd


def date_filter_list(data_to_filter, filter_tuple_list):
    dflist = []
    for tuple in filter_tuple_list:
        start_time = data_to_filter.index.searchsorted(tuple[0])
        end_time = data_to_filter.index.searchsorted(tuple[1])
        df = data_to_filter.iloc[start_time:end_time]
        dflist.append(df)
    df = pd.concat(dflist)
    df.sort_index(inplace=True)
    return df


def date_filter(data_to_filter, filter_tuple):
    """
    Filters dataframes with two dates provided in a tuple

    args:
    ---
    data_to_filter -- pandas.dataframe
        The data we want to filter
    filter_tuple -- tuple
        (YYYY-MM-DD HH:MM:SS, YYYY-MM-DD HH:MM:SS)

    returns:
    ---
    Data that is between these timestamps
    """
    if not data_to_filter.index.is_monotonic_increasing:
        data_to_filter.sort_index(inplace=True)
    start_time = data_to_filter.index.searchsorted(filter_tuple[0])
    end_time = data_to_filter.index.searchsorted(filter_tuple[1])
    df = data_to_filter.iloc[start_time:end_time]
    return df


def create_filter_tuple(df):
    filter_tuple = list(zip(df["close_time"], df["open_time"], df["chamber"]))
    return filter_tuple

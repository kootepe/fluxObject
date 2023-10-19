def date_filter(data_to_filter, filter_tuple):
    start_time = data_to_filter.index.searchsorted(filter_tuple[0])
    end_time = data_to_filter.index.searchsorted(filter_tuple[1])
    df = data_to_filter.iloc[start_time:end_time]
    return df

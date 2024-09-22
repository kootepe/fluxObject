#!/usr/bin/env python3

import pandas as pd
import logging

logger = logging.getLogger("defaultLogger")


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


def get_datetime_index(df, filter_tuple):
    start = df.index.searchsorted(filter_tuple[0], side="left")
    end = df.index.searchsorted(filter_tuple[1], side="left")
    return start, end


def date_filter(data_to_filter, filter_tuple):
    """
    Filters dataframes with two dates provided in a tuple
    Equivalent to using a boolean mask but considerably faster.

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
    # NOTE: this check should probably come before, and it should already be sorted
    # before getting to this point...
    if not data_to_filter.index.is_monotonic_increasing:
        data_to_filter.sort_index(inplace=True)
    start, end = get_datetime_index(data_to_filter, filter_tuple)
    df = data_to_filter.iloc[start:end]
    return df


def mk_fltr_tuple(df, st="start_time", ct="close_time", ot="open_time", et="end_time"):
    """create filter tuple from given column names"""
    filter_tuple = list(zip(df[ct], df[ot], df[st], df[et], df["chamber"]))
    return filter_tuple


def subs_from_fltr_tuple(filter_tuple, percentage):
    """
    'Remove' percentage from both ends of the filter tuple, eg. shorten the
    time between the timestamps
    """
    time_difference = (filter_tuple[1] - filter_tuple[0]).seconds
    time_to_remove = pd.to_timedelta(time_difference * (percentage / 100), "s")
    start = filter_tuple[0] + time_to_remove
    end = filter_tuple[1] - time_to_remove
    time_tuple = (start, end, filter_tuple[2])
    return time_tuple


def add_to_fltr_tuple(filter_tuple, percentage):
    """Add percentage to both ends of the filter tuple, eg. lengthen the time
    between the timestamps"""
    time_difference = (filter_tuple[1] - filter_tuple[0]).seconds
    time_to_remove = pd.to_timedelta(time_difference * (percentage / 100), "s")
    start = filter_tuple[0] - time_to_remove
    end = filter_tuple[1] + time_to_remove
    time_tuple = (start, end, filter_tuple[2])
    return time_tuple


def add_min_to_fltr_tuple(filter_tuple):
    """Add percentage to both ends of the filter tuple, eg. lengthen the time
    between the timestamps"""
    # time_difference = (filter_tuple[1] - filter_tuple[0]).seconds
    time_to_remove = pd.to_timedelta(1, "min")
    start = filter_tuple[0] - time_to_remove
    end = filter_tuple[1] + time_to_remove
    time_tuple = (start, end, filter_tuple[2])
    return time_tuple

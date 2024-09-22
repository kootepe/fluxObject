#!/usr/bin/env python3

import datetime
import re
import logging
from numpy import array
from pandas.api.types import is_datetime64_any_dtype

logger = logging.getLogger("defaultLogger")


def rm_tz(df):
    """
    Localizes datetime columns in a pandas DataFrame by removing timezone information.

    Parameters
    ----------
    - df: A pandas DataFrame.

    Returns
    -------
    - The DataFrame with timezone information removed from datetime columns.
    """

    for col in df.columns:
        # Check if the column is a datetime type
        if is_datetime64_any_dtype(df[col]):
            # Check if the column has timezone information
            if df[col].dt.tz is not None:
                # logger.debug(f"Removed tz info from colum: {col}")
                # Remove timezone information
                df[col] = df[col].dt.tz_localize(None)
    return df


def time_to_numeric(time):
    """
    Helper function to calculate ordinal time from HH:MM:SS

    args:
    ---
    time -- numpy.array
        Array of HH:MM:SS string timestamps

    returns:
    ---
    time -- numpy.array
        Array of float timestamps
    """
    # Split the HH:MM:SS strings; this creates a list of lists
    split_times = [time.split(":") for time in time]
    # Convert split times to hours, minutes, and seconds, and calculate the fractional day
    numeric_times = array(
        [(int(h) * 3600 + int(m) * 60 + int(s)) for h, m, s in split_times]
    ).round(10)

    return numeric_times


def strftime_to_regex(file_timestamp_format):
    """
    Changes strftime timestamp to regex format

    args:
    ---

    returns:
    ---
    file_timestamp_format in regex

    """
    conversion_dict = {
        "%a": r"(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)",
        "%A": r"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)",
        "%b": r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)",
        "%B": r"(?:January|February|March|April|May|June|July|August|September|October|November|December)",
        "%d": r"(?P<day>\d{2})",
        "%H": r"(?P<hour>\d{2})",
        "%I": r"(?P<hour>\d{2})",
        "%m": r"(?P<month>\d{2})",
        "%M": r"(?P<minute>\d{2})",
        "%p": r"(?:AM|PM)",
        "%S": r"(?P<second>\d{2})",
        "%Y": r"(?P<year>\d{4})",
        "%y": r"(?P<year>\d{2})",
        "%%": r"%",
    }
    regex_pattern = re.sub(
        r"%[aAbBdHIImMpPSYy%]",
        lambda m: conversion_dict.get(m.group(), m.group()),
        file_timestamp_format,
    )

    return regex_pattern


def check_timestamp(start_timestamp, end_timestamp):
    """
    Compare the start and end timestamps, if start timestamp is
    older than end timestamp, terminate script as then there's no
    new data

    args:
    ---

    returns:
    ---

    """
    if start_timestamp > end_timestamp:
        return True
    else:
        return False


def extract_date(file_timestamp_format, datestring):
    """
    Extracts the date from the filename

    args:
    ---
    datestring -- str
        The format of the timestamp in the filename

    returns:
    ---
    datetime.datetime
        timestamp in datetime.datetime format
    """
    # try:
    #    date = re.search(strftime_to_regex(), datestring).group(0)
    # except AttributeError:
    #    print('Files are found in folder but no matching file found, is the format of the timestamp correct?')
    #    return None
    if file_timestamp_format == strftime_to_regex(file_timestamp_format):
        logger.info("No strftime formatting in filename, returning current date")
        return datetime.datetime.today()
    date = re.search(strftime_to_regex(file_timestamp_format), datestring).group(0)
    # class chamber_cycle calls this method and using an instance
    # variable here might cause issues if the timestamp formats
    # should be different
    return datetime.datetime.strptime(date, file_timestamp_format)


def get_time_diff(start, stop):
    return (stop - start).total_seconds()


def convert_seconds(time_in_sec):
    if time_in_sec < 60:
        return f"{round(time_in_sec)} seconds"
    elif time_in_sec < 3600:
        minutes = round(time_in_sec // 60)
        seconds = round(time_in_sec % 60)
        return f"{minutes} minutes, {seconds} seconds"
    else:
        hours = round(time_in_sec // 3600)
        remainder = round(time_in_sec % 3600)
        minutes = round(remainder // 60)
        seconds = round(remainder % 60)
        return f"{hours} hours, {minutes} minutes, {seconds} seconds"


def convert_timestamp_format(
    timestamp_str, output_format, input_format="%Y-%m-%d %H:%M:%S"
):
    """
    Convert string timestamps

    Parameters
    ----------
    timestamp_str : str
        String timestamp

    output_format : str
        Format which you want the output timestamp to be

    input_format : str
        Format of the input string


    Returns
    -------
    converted_timestamp : str
        timestamp_str converted to the format of output_format
    """
    # Parse the input timestamp string to a datetime object
    dt_obj = datetime.datetime.strptime(timestamp_str, input_format)

    # Format the datetime object to the desired output format
    converted_timestamp = dt_obj.strftime(output_format)

    return converted_timestamp

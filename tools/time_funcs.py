import datetime
import re
import logging


def ordinal_timer(time):
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
    h, m, s = map(int, time.split(":"))
    sec = 60
    secondsInDay = 86400
    ordinal_time = round((sec * (sec * h) + sec * m + s) / secondsInDay, 10)
    return ordinal_time


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
        logging.info("No strftime formatting in filename, returning current date")
        return datetime.datetime.today()
    date = re.search(strftime_to_regex(file_timestamp_format), datestring).group(0)
    # class chamber_cycle calls this method and using an instance
    # variable here might cause issues if the timestamp formats
    # should be different
    return datetime.datetime.strptime(date, file_timestamp_format)

#!/usr/bin/env python3

from pathlib import Path
from re import search
from tools.time_funcs import strftime_to_regex
from datetime import datetime
import logging
import os
import sys

logger = logging.getLogger("defaultLogger")


def get_files(dict, start_ts, end_ts):
    path = dict.get("path")
    ts_fmt = dict.get("file_timestamp_format")
    fls = find_files(path)
    file_date_dict = mk_date_dict(fls, ts_fmt)
    filtered_files = filter_between_dates(start_ts, end_ts, file_date_dict)
    return filtered_files


def find_files(path):
    files = list(Path(path).glob("*"))
    files = [f for f in files if "~" not in f.name]
    return files


def filter_between_dates(sd, ed, date_dict):
    """
    Drops files from date dict if they don't fall between given dates


    Parameters
    ----------
    sd : datetime
    starting date

    ed : datetime
    ending date

    date_dict : dict
    dictionary of filename:date


    Returns
    -------



    """
    filtered_files = {
        key: value
        for key, value in date_dict.items()
        if (sd is None or value >= sd) and (ed is None or value <= ed)
    }
    return list(filtered_files.keys())


def get_newest(path: str, file_extension: str):
    """
    Fetchest name of the newest file in a folder

    args:
    ---

    returns:
    ---
    newest_file -- str
        Name of the newest file in a folder

    """
    logger.info(f"Getting ts of last modified file from {path}")
    files = list(Path(path).rglob(f"*{file_extension}*"))
    if not files:
        logger.info(f"No files found in {path}")
        logger.warning("EXITING")
        # BUG: NEED A BETTER WAY OF EXITING THE FUNCTION BECAUSE THIS
        # STOPS THE LOOPING THROUGH FILES
        sys.exit(0)

    # linux only
    # newest_file = str(max([f for f in files], key=lambda item: item.stat().st_ctime))
    # cross platform
    newest_file = str(max(files, key=os.path.getmtime))
    return newest_file


def mk_date_dict(files, ts_fmt):
    """
    Creates a dictionary out of a list of files and the timestamps in the
    filenames. Used for filtering the files to specific timeframe.

    Parameters
    ----------
    files :

    ts_fmt :


    Returns
    -------


    """
    file_date_dict = {
        key: datetime.strptime(match.group(), ts_fmt)
        for i, key in enumerate(files)
        if (
            match := search(
                strftime_to_regex(ts_fmt),
                str(key),
            )
        )
    }
    return file_date_dict

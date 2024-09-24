#!/usr/bin/env python3

from pathlib import Path
from re import search
from tools.time_funcs import strftime_to_regex
from datetime import datetime
import pandas as pd
import logging
import os
import sys

from tools.validation import overlap_test

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


def read_man_meas_f(measurement_files, chamber_cycle):
    from tools.fluxer import timestamps

    # NOTE: the format of the manual measurement is hardcoded
    tmp = []
    for f in measurement_files:
        # with open(f) as f:
        #    first_line = f.read_line()
        # date = first_line
        logger.debug(f"Reading measurement {f.name}.")
        ts_reader = timestamps()
        df = ts_reader.read_file(f)
        # NOTE: for the sake of consisteny, even though the manual
        # measurement doesn't really have a closing time, the
        # variable is named like this
        df["start_time"] = df["datetime"]
        # NOTE: move to function in future
        df["close_time"] = df["datetime"] + pd.to_timedelta(chamber_cycle[0], unit="s")
        df["open_time"] = df["datetime"] + pd.to_timedelta(chamber_cycle[1], unit="s")
        df["end_time"] = df["datetime"] + pd.to_timedelta(chamber_cycle[2], unit="s")
        df["snowdepth"] = df["snowdepth"].fillna(0)
        df["ts_file"] = str(f.name)
        tmp.append(df)
    dfs = pd.concat(tmp)
    dfs.set_index("datetime", inplace=True)
    dfs["notes"] = dfs["notes"].fillna("")
    dfs = overlap_test(dfs)
    dfs.sort_index(inplace=True)
    return dfs

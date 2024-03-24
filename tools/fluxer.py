import os
import re
import functools
import glob

import sys
import shutil
import logging
import datetime
import configparser
import timeit
import numpy as np
import pandas as pd
import zipfile as zf
from zipfile import BadZipFile
from pathlib import Path
from traceback import format_exc

# modules from this repo
from tools.filter import (
    date_filter,
    date_filter_list,
    mk_fltr_tuple,
    create_filter_tuple_extra,
    subs_from_filter_tuple,
    add_to_filter_tuple,
)
from tools.time_funcs import (
    ordinal_timer,
    strftime_to_regex,
    check_timestamp,
    extract_date,
)
from tools.influxdb_funcs import influx_push, check_last_db_timestamp
from tools.file_tools import get_newest
from tools.gas_funcs import calculate_gas_flux, calculate_pearsons_r, calculate_slope
import tools.snow_height
from tools.merging import (
    merge_by_dtx,
    merge_by_id,
    merge_by_dtx_and_id,
    is_df_valid,
)

from tools.create_excel import (
    create_excel,
    create_excel2,
    create_sparkline,
    create_sparkline2,
    create_fig,
    create_rects,
)


logger = logging.getLogger("defaultLogger")


class pusher:
    """
    Pushes pandas dataframes to InfluxDB

    Attributes
    ---
    influxdb_dict -- dictionary
        dictionary with the influxdb login information from .ini
    data -- pandas dataframe
        data that will be pushed to influxdb
    tag_columns -- list
        List of columns that will be used as tags in influxdb

    Methods
    ---
    influxPush(df)
        Pushes data to influxdb
    read_tag_columns()
        Sets tag columns for pushing to influxdb


    """

    def __init__(self, data, influxdb_dict):
        self.influxdb_dict = influxdb_dict
        self.data = data
        logger.info("Pushing data to DB")
        self.tag_columns = self.read_tag_columns()
        # only push one day of data at once
        for date, group in self.data.groupby(pd.Grouper(freq="D")):
            logger.debug(f"Running groupby on {date}")
            # if there was a day when all measurements had errors, the
            # dataframe can be empty
            if group.empty:
                logger.debug(f"Dataframe on {date} is empty")
                continue
            logger.debug(f"Pushing data from {date} to db.")
            influx_push(group, self.influxdb_dict, self.tag_columns)

    def read_tag_columns(self):
        """
        Reads tag columns from .ini and checks that they are in the
        dataframe, and returns them as list

        args:
        ---

        returns:
        ---
        tag_columns -- list
            list of tag_columns, as defined in .ini
        """

        tag_columns = self.influxdb_dict.get("tag_columns").split(",")
        measurement_columns = self.data.columns
        # checks that all items in list tag_colums exist in list
        # measurement_columns, if so return true, else return false
        check = any(item in tag_columns for item in measurement_columns)
        has_items = tag_columns[0]
        if not has_items:
            logger.warning("No tag columns defined")
            return []
        else:
            if check is True:
                return tag_columns
            else:
                logger.info(
                    "Columns labeled for tagging don't exist in dataframe"
                )
                logger.info("EXITING")
                sys.exit(0)


class snowdepth_parser:
    """
    Class for parsing the snowdepth measurement for automatic chambers

    Attributes
    ---
    snowdepth_measurement -- xlsx file
        .xlsx file for with snowdepth for each chamber
    snowdepth_df -- pandas dataframe
        dataframe version of the .xlsx file.

    Methods
    ---
    add_snowdepth()
        Reads snowdepth measurement and turns it into a dataframe, if it exists. Or creates a dummy one.
    """

    def __init__(self, snowdepth_measurement):
        self.snowdepth_measurement = snowdepth_measurement
        # NOTE: does doing this little require it's own class?
        self.snowdepth_df, self.set_to_zero = (
            tools.snow_height.read_snow_measurement(self.snowdepth_measurement)
        )


class calculated_data:
    """
    Calculates gas flux and pearsonsR of each flux measurement.

    Attributes
    ---
    get_temp_and_pressure_from_file -- bool
        1 to get temp and pressure from files, 0 to use defaults
    chamber_height -- int
        Height of the measurement chamber, for volume, in mm
    chamber_width -- int
        Width of the measurement chamber, for volume, in mm
    chamber_length -- int
        Length of the measurement chamber, for volume, in mm
    filter_tuple -- tuple
        (start_time, end_time, chamber_num), measurement is calculated
        from values between these timestamps,
    default_pressure -- int
        Default pressure value
    default_temp -- int
        Default temperature value
    calculated_data -- df
        Data with gas flux and pearsonsR calculated
    upload_ready_data -- df
        Data with unnecessary columns removed

    Methods
    ---
    calculate_slope_pearsons_r(df, measurement_name)
        Calculates slope and pearsonsR
    calculate_gas_flux(df, measurement_name)
        Calculates gas flux
    summarize(data)
        Grabs first row from each measurement, since flux and r are the
        same per measurement, also removes unnecesary columns

    """

    def __init__(
        self, measured_data, measuring_chamber_dict, filter_tuple, defaults_dict
    ):
        self.filter_tuple = filter_tuple
        self.measured_data = measured_data
        self.get_temp_and_pressure_from_file = defaults_dict.get(
            "get_temp_and_pressure_from_file"
        )

        self.measurement_perc = defaults_dict.get("measurement_perc")
        if self.measurement_perc is None:
            self.measurement_perc = 20
        else:
            self.measurement_perc = int(self.measurement_perc)

        self.chamber_height = float(
            measuring_chamber_dict.get("chamber_height")
        )
        self.def_press = float(defaults_dict.get("default_pressure"))
        self.def_temp = float(defaults_dict.get("default_temperature"))
        self.check_valid()

        gases = ["ch4", "co2"]
        for gas in gases:
            self.measured_data = self.calc_slope_pearsR(self.measured_data, gas)
        self.upload_ready_data = self.summarize(self.measured_data)

    def check_valid(self):
        dfa = pd.DataFrame()
        for date in self.filter_tuple:
            measurement_df = date_filter(self.measured_data, date)
            if measurement_df["error_code"].sum() != 0:
                measurement_df["checks"] += "has errors,"
                measurement_df["is_valid"] = False

            if measurement_df["air_temperature"].isna().all():
                measurement_df["checks"] += "no air temp,"
                measurement_df["is_valid"] = False

            if measurement_df["air_pressure"].isna().all():
                measurement_df["checks"] += "no air press,"
                measurement_df["is_valid"] = False
            dfa = pd.concat([dfa, measurement_df])
        self.measured_data = dfa

    def calc_slope_pearsR(self, data, measurement_name):
        """
        Calculates Pearsons R (correlation) and the slope of
        the CH4 flux.

        args:
        ---
        df -- pandas.dataframe
            dataframe of the gas flux
        measurement_name -- str
            name of the gas that slope, pearsons_r and flux is
            going to be calculated for

        returns:
        ---
        all_measurements_df -- pandas.dataframe
            same dataframe with additional slope, pearsons_r and
            flux columns
        """
        # TODO: Clean this mess up
        meas_name = measurement_name
        measurement_list = []
        slope_times_list = []
        pearsons_r_times_list = []
        df = data.copy()

        for date in self.filter_tuple:
            is_valid = None
            measurement_df = date_filter(df, date).copy()
            if measurement_df.empty:
                continue

            slope_dates = subs_from_filter_tuple(date, self.measurement_perc)
            slope = calculate_slope(df, slope_dates, meas_name)
            if slope is None:
                continue
            measurement_df[f"{meas_name}_slope"] = slope
            pearsons_dates = subs_from_filter_tuple(date, self.measurement_perc)
            pearsons = calculate_pearsons_r(df, pearsons_dates, meas_name)
            if pearsons is None:
                continue
            measurement_df[f"{meas_name}_pearsons_r"] = pearsons

            flux, is_valid = calculate_gas_flux(
                measurement_df,
                meas_name,
                self.chamber_height,
                self.def_temp,
                self.def_press,
            )
            measurement_df[f"{meas_name}_flux"] = flux

            # MOVE THIS TO A DIFFERENT FUNCTION, THIS FUNCTION RUNS
            # MULTIPLE TIMES TO THE VALUES ARE DUPLICATED
            # if measurement_df["error_code"].sum() != 0:
            #     measurement_df["checks"] += "has errors,"
            # if is_valid is False:
            #     measurement_df["checks"] += "no temp or pressure,"
            # if measurement_df["error_code"].sum() != 0  or is_valid is False:
            #     measurement_df["is_valid"] = False
            # else:
            #     measurement_df["is_valid"] = True
            # if is_valid is False:
            # print(measurement_df["checks"])
            measurement_list.append(measurement_df)
            slope_times_list.append(slope_dates)
            pearsons_r_times_list.append(pearsons_dates)
        all_measurements_df = pd.concat(measurement_list)
        self.pearsons_r_times_list = pearsons_r_times_list
        self.slope_times_list = slope_times_list
        return all_measurements_df

    def summarize(self, data):
        """
        Drops most columns as from here they will be pushed to influxdb

        args:
        ---
        data -- pandas.dataframe

        returns:
        ---
        summary -- pandas.dataframe


        """
        dfList = []
        # data = data[
        #     ["ch4_flux", "co2_flux", "ch4_pearsons_r", "co2_pearsons_r", "chamber"]
        # ]
        for date in self.filter_tuple:
            dfa = date_filter(data, date)
            dfList.append(dfa.iloc[:1])
        summary = pd.concat(dfList)
        summary["is_valid"] = summary["is_valid"] * 1
        # try:
        #     summary.loc[summary['has_errors'] == 1, 'is_valid'] = 0
        # except Exception:
        #     pass

        return summary


class merge_data:
    """
    Merges "auxiliary" data to the main gas measurement. Auxiliary data
    being air pressure, air temperature and snowdepth most of the time.

    Attributes
    ---
    merged_data -- pandas.dataframe
        The gas measurement dataframe
    aux_data -- pandas.dataframe
        The "auxiliary" dataframe, can be any data that has a
        datetimeindex

    Methods
    ---
    merge_aux_data
        Merges the two dataframes if they are sorted by datetimeindex
    is_df_sorted_by_dt_idx
        Checks if input is a dataframe, if it has a datetimeindex and
        that the index is ascending.
    """

    def __init__(self, measurement_df, aux_df, snowdepth=None):
        self.measurement_df = measurement_df
        self.aux_df = aux_df
        self.snowdepth = snowdepth
        if self.aux_df is not None:
            if self.snowdepth is not None:
                self.merged_data = merge_by_dtx_and_id(measurement_df, aux_df)
                # self.merged_data = merge_aux_by_column(measurement_df, aux_df)
            else:
                self.merged_data = self.merge_aux_data(measurement_df, aux_df)
        else:
            self.merged_data = self.measurement_df
            pass

    def merge_aux_by_column2(self, measurement_df, aux_df):
        """
        This sole use of this function is to merge AC chamber snowdepth
        measurement into the main dataframe.
        Its a bit of a mess.
        """
        main_df = measurement_df
        other_df = aux_df
        dflist = []
        for _, group in other_df.groupby("chamber"):
            if self.is_df_sorted_by_dt_idx(
                measurement_df
            ) and self.is_df_sorted_by_dt_idx(aux_df):
                main_df = main_df.copy()
                other_df = group.copy()
                chamberNum = other_df.chamber.mean()
                mask = main_df["chamber"] == chamberNum
                other_df = other_df.drop(columns=["chamber"])
                df = pd.merge_asof(
                    main_df[mask],
                    other_df,
                    left_on="datetime",
                    right_on="datetime",
                    tolerance=pd.Timedelta("1000d"),
                    direction="nearest",
                    suffixes=("", "_y"),
                )
                df.drop(df.filter(regex="_y$").columns, axis=1, inplace=True)
                df.set_index("datetime", inplace=True)
                df["snowdepth"] = df["snowdepth"].fillna(0)
                dflist.append(df)
            else:
                logger.info(
                    "Dataframes are not properly sorted by datetimeindex"
                )
                sys.exit(0)
        df = pd.concat(dflist)
        return df

    def merge_aux_data(self, measurement_df, aux_df):
        """
        Merges 'auxiliary' data to the dataframe, mainly air temperature,
        air pressure and snowdepth.

        args:
        ---
        measurement_df -- pandas.dataframe
            gas measurement dataframe
        aux_df -- pandas.dataframe
            "aux" data dataframe

        returns:
        ---
        df -- pandas.dataframe
            dataframe with aux_data merged into the main gas measurement dataframe
        """
        # BUG: IF THERE'S A NA VALUES THE SCRIPT WILL CRASH
        aux_df.dropna(inplace=True, axis=1)
        if is_df_valid(measurement_df) and is_df_valid(aux_df):
            measurement_df["temp_index"] = measurement_df.index
            df = pd.merge_asof(
                measurement_df,
                aux_df,
                left_on="datetime",
                right_on="datetime",
                tolerance=pd.Timedelta("60m"),
                direction="nearest",
                suffixes=("", "_y"),
            )
            df.drop("temp_index", axis=1, inplace=True)
            df.drop(df.filter(regex="_y$").columns, axis=1, inplace=True)
            df.set_index("datetime", inplace=True)
        else:
            logger.info("Dataframes are not properly sorted by datetimeindex")
            sys.exit(0)
        return df


class filterer:
    """
    Takes in a large dataframe and two timestamps, and removes all data
    that isn't between those two timestamps. Also drops dataframes if
    there's no data at all between the timestamps, and if there's a
    column called error_codes (LICOR data) with errors, those will be
    dropped.

    Attributes
    ---
    clean_filter_tuple -- list of tuples
        Tuples which are not empty and have no errors in the data will
        be stored here.
    filter_tuple -- list of tuples
        "Raw" tuples generated from chamber cycle or read from files
        [(YYYY-MM-DD HH:MM:SS, YYYY-MM-DD HH:MM:SS, chamberNum),
         (YYYY-MM-DD HH:MM:SS, YYYY-MM-DD HH:MM:SS, chamberNum)]
    df -- pandas.dataframe
        Any dataframe

    Methods
    ---
    filter_data_by_dateime
        Removes data that isn't between two timestamps

    """

    # TODO: solution for what to do when there's a gas measurement and
    # there's supposed to be temperature and pressure measurement but for
    # whatever reason they are empty. Currently that data is just
    # dropped.

    # TODO:
    #     there's lots of old deprecated stuff in this function, needs
    #     cleaning up

    def __init__(self, filter_tuple, df):
        self.clean_filter_tuple = []
        self.filter_tuple = filter_tuple
        self.filtered_data = self.filter_data_by_datetime(df)

    def filter_data_by_datetime(self, data_to_filter):
        """
        Takes in the dataframe and two timestamps from each tuple, drops
        data that isn't between the timestamps.

        args:
        ---
        data_to_filter -- pandas.dataframe
            Large dataframe that can be thinned

        returns:
        ---
        clean_df -- pandas.dataframe
            Dataframe with data outside the timestamps removed

        """
        # data with no errors
        clean_data = []
        # timestamps for the data with no errors
        clean_timestamps = []
        # datafrrames which have errors
        error_data = []
        # timestamps for the data with errors
        error_timestamps = []
        # timestamps with no data
        empty_timestamps = []
        # the loop below raises a false positive warning, disable it
        pd.options.mode.chained_assignment = None
        empty_count = 0
        # logger.info( f"Filtering data with columns:\n{list(data_to_filter.columns)}")
        for date in self.filter_tuple:
            df = date_filter(data_to_filter, date)
            # drop measurements with no data
            if df.empty:
                logger.info(f"No data between {date[0]} and {date[1]}")
                empty_timestamps.append(date)
                empty_count += 1
                continue
            # drop all measurements that have any error codes
            if "error_code" in df.columns:
                if df["error_code"].sum() != 0:
                    # errors = ", ".join(
                    #     [str(x) for x in df["error_code"].unique()]
                    # )
                    diff = get_time_diff(date[0], date[1])
                    logger.info(f"Measuring errors at {date[0]} +{diff}sec.")
                    # df["has_errors"] = 1
                    error_data.append(df)
                    error_timestamps.append(date)
                    # continue
            # chamber number is the third value in filter_tuple
            df["chamber"] = date[2]
            df["chamber_close"] = date[0]
            df["chamber_open"] = date[1]
            clean_data.append(df)
            clean_timestamps.append(date)
        if len(self.filter_tuple) == empty_count:
            logger.info(
                "No data found for any timestamp, is there data in the files?"
            )
            sys.exit(0)
        clean_df = pd.concat(clean_data)
        # save tuples that have data and no error codes
        self.clean_filter_tuple = clean_timestamps
        # dataframes with errors
        if len(error_data) != 0:
            self.dirty_df = pd.concat(error_data)
        # the timestamps which cover data with errors
        self.dirty_timestamps = error_timestamps
        self.empty_timestamps = empty_timestamps
        # loop raises false positive error which was turned off before,
        # turn it on
        pd.options.mode.chained_assignment = "warn"
        return clean_df


class aux_data_reader:
    """
    Reads "auxiliary" as defined in the .ini, aux data being air
    temperature and air pressure data

    Attributes
    ---
    aux_data_dict -- dictionary
        The part of the .ini file which defines air_pressure and
        air_temperature data
    files -- list
        List of the filenames that will be read into pandas dataframe

    Methods
    ---
    aux_data_ini_parser
        Parsers the .ini dictionary into values
    add_aux_data
        Reads the .csv to pandas.dataframe
    """

    def __init__(self, aux_data_dict, files):
        self.aux_data_dict = aux_data_dict
        self.files = files
        self.aux_data_df = self.add_aux_data(self.aux_data_dict)

    def aux_data_ini_parser(self, ini_dict):
        """
        Parses the .ini dictionary into values.

        args:
        ---
        ini_dict -- dictionary
            The part of the .ini which defines how to read the .csv
            file

        returns:
        ---
        path -- str
            path to the file
        delimiter -- str
            .csv delimiter
        skiprows -- int
            how many rows to skip at beginning of file
        timestamp_format -- str
            timestamp format of the .csv timestamp column
        columns -- list of ints
            list of the column numbers which will be read from the
            .csv
        names -- list of strings
            names for the columns that will be read
        dtypes -- dict
            columns and names paired into a dict
        """
        dict = ini_dict
        path = dict.get("path")
        # file = (dict.get('file'))
        # unnecessary?
        # file_timestamp_format = dict.get('file_timestamp_format')
        # if file_timestamp_format == '':
        #    files = [p for p in Path(path).glob(f'*{file}*') if '~' not in str(p)]

        delimiter = dict.get("delimiter")
        skiprows = int(dict.get("skiprows"))
        timestamp_format = dict.get("timestamp_format")
        columns = [
            int(dict.get("timestamp_column")),
            int(dict.get("measurement_column")),
        ]
        names = [
            dict.get("timestamp_column_name"),
            dict.get("measurement_column_name"),
        ]
        # dtypes needs to passed to pandas as a dict
        dtypes = {
            dict.get("timestamp_column_name"): dict.get(
                "timestamp_column_dtype"
            ),
            dict.get("measurement_column_name"): dict.get(
                "measurement_column_dtype"
            ),
        }
        return (
            path,
            delimiter,
            skiprows,
            timestamp_format,
            columns,
            names,
            dtypes,
        )

    def add_aux_data(self, ini_dict):
        """
        Reads the .csv file into a pandas.dataframe

        args:
        ---
        ini_dict -- dictionary
            The part of the .ini that defines the .csv file

        returns:
        ---
        dfs -- pandas.dataframe
            The .csv file read into a pandas.dataframe

        """
        (
            path,
            delimiter,
            skiprows,
            timestamp_format,
            columns,
            names,
            dtypes,
        ) = self.aux_data_ini_parser(ini_dict)
        if os.path.exists(path):
            pass
        else:
            sys.exit(
                f"Path {path} doesn't exist.\nIf you want to read"
                " auxiliary data, fix the path, if you want to use"
                " default pressure and temperature set"
                " get_temp_and_pressure_from_file to 0"
            )
        tmp = []
        for f in self.files:
            try:
                df = pd.read_csv(
                    f,
                    skiprows=skiprows,
                    delimiter=delimiter,
                    usecols=columns,
                    names=names,
                    dtype=dtypes,
                )
            except FileNotFoundError:
                logger.info(
                    f"File not found at {Path(path) / f}, make sure the .ini is correct"
                )
                sys.exit()
            df["datetime"] = pd.to_datetime(
                df["datetime"], format=timestamp_format)
            df.set_index("datetime", inplace=True)
            tmp.append(df)
        try:
            dfs = pd.concat(tmp)
        except ValueError:
            logger.info(
                "None of the auxiliary data is in the same time range as gas measurement data."
            )
            sys.exit(0)
        return dfs


class measurement_reader:
    """
    Reads the LI-COR gas measurement.

    Attributes
    ---
    measurement_dict -- dictionary
        Defines how to read the gas measurement file
    measurement_files -- list
        list of the measurement file names
    measurement_df -- pandas.dataframe
        The measurement .csv files read into a pandas.dataframe
    Methods
    ---
    read_measurement
        Reads the .ini and then reads the .csv into a pandas dataframe
    ordinal_timer
        Calculates ordinal time from the timestamps, required for slope
        calculations

    """

    def __init__(self, measurement_dict, measurement_files):
        logger.info("Reading measurements.")
        logger.debug("Reading measurements.")
        self.measurement_dict = measurement_dict
        self.measurement_files = measurement_files
        self.measurement_df = self.read_measurement(self.measurement_dict)

    def read_measurement(self, dict):
        """
        Reads the measurement file into a pandas.dataframe

        args:
        ---
        dict -- dictionary
            The part of the .ini that defines that measurement .csv

        returns:
        ---
        dfs -- pandas.dataframe
            All of the .csv in self.measurement_files read into one
            big dataframe
        """
        skiprows = int(dict.get("skiprows"))
        names = dict.get("names").split(",")
        columns = list(map(int, dict.get("columns").split(",")))
        # oulanka has data from old licor software version which has no
        # remark column
        columns_alternative = list(
            map(int, dict.get("columns_alternative").split(",")))
        dtypes = dict.get("dtypes").split(",")
        # NOTE: ADD ERROR WHEN THERE'S NO FILES TO READ
        dtypes = {names[i]: dtypes[i] for i in range(len(names))}

        # initiate list where all read dataframes will be stored
        tmp = []
        for f in self.measurement_files:
            try:
                df = pd.read_csv(
                    f,
                    skiprows=skiprows,
                    delimiter="\t",
                    usecols=columns,
                    names=names,
                    dtype=dtypes,
                )
            # there's old LICOR files which have an extra column,
            # this handles those
            except ValueError:
                df = pd.read_csv(
                    f,
                    skiprows=skiprows,
                    delimiter="\t",
                    usecols=columns_alternative,
                    names=names,
                    dtype=dtypes,
                )
            tmp.append(df)
        # concatenate all stored dataframes into one big one
        dfs = pd.concat(tmp)
        # combine individual date and time columns into datetime
        # column
        dfs["datetime"] = pd.to_datetime(
            dfs["date"].apply(str) + " " + dfs["time"], format="%Y-%m-%d %H:%M:%S"
        )
        dfs["ordinal_date"] = pd.to_datetime(dfs["datetime"]).map(
            datetime.datetime.toordinal
        )
        dfs["ordinal_time"] = dfs.apply(
            lambda row: ordinal_timer(row["time"]), axis=1)
        dfs["ordinal_datetime"] = dfs["ordinal_time"] + dfs["ordinal_date"]
        dfs.set_index("datetime", inplace=True)
        dfs.sort_index(inplace=True)
        return dfs


class chamber_cycle:
    """
    Creates the chamber cycle dataframe which will be used to select
    data that will be calculated

    Attributes
    ---
    file_timestamp_format -- str
        Format of the timestamp in the measurement files
    chamber_cycle_file -- str
        Path to the chamber cycle file
    chamber_measurement_start_sec -- int
        How many seconds past the opening of the chamber to start the
        actual measurement
    chamber_measurement_end_sec -- int
        How many seconds past the opening of the chamber to end the
        actual measurement
    measurement_files -- list
        List of the measurement file paths

    Methods
    ---
    create_chamber_cycle
        Reads date from each filename, and creates dataframe with
        starting and ending times for each measurement

    """

    def __init__(
        self, measurement_dict, defaults_dict, measurement_time_dict, measurement_files
    ):
        self.file_timestamp_format = measurement_dict.get(
            "file_timestamp_format")
        self.chamber_cycle_file = defaults_dict.get("chamber_cycle_file")
        self.chamber_measurement_start_sec = int(
            measurement_time_dict.get("start_of_measurement")
        )
        self.chamber_measurement_end_sec = int(
            measurement_time_dict.get("end_of_measurement")
        )
        self.end_of_cycle = int(measurement_time_dict.get("end_of_cycle"))
        self.measurement_files = measurement_files
        self.chamber_cycle_df = self.create_chamber_cycle()
        self.filter_tuple = create_filter_tuple(self.chamber_cycle_df)
        self.whole_cycle_tuple = create_filter_tuple_extra(
            self.chamber_cycle_df, "cycle_start", "cycle_end"
        )

    def create_chamber_cycle(self):
        """
        Reads a csv file with the chamber cycles

        args:
        ---

        returns:
        ---
        dfs -- pandas.dataframe
            dataframe with chamber opening times, measurement
            starting times and measurement ending times.
        """
        tmp = []
        # loop through files, read them into a pandas dataframe and
        # create timestamps
        for file in self.measurement_files:
            df = pd.read_csv(self.chamber_cycle_file,
                             names=["time", "chamber"])
            date = extract_date(self.file_timestamp_format,
                                os.path.splitext(file)[0])
            df["date"] = date
            df["datetime"] = pd.to_datetime(
                df["date"].astype(str) + " " + df["time"].astype(str)
            )
            df["close_time"] = df["datetime"] + pd.to_timedelta(
                self.chamber_measurement_start_sec, unit="s"
            )
            df["open_time"] = df["datetime"] + pd.to_timedelta(
                self.chamber_measurement_end_sec, unit="s"
            )
            df["cycle_start"] = df["datetime"]
            df["cycle_end"] = df["datetime"] + pd.to_timedelta(
                self.end_of_cycle, unit="s"
            )
            tmp.append(df)
        dfs = pd.concat(tmp)
        dfs.set_index("datetime", inplace=True)
        return dfs


class file_finder:
    """
    Generates lists of filenames and then finds them.

    Attributes
    ---
    path -- str
        path to the file
    file_timestamp_format -- str
        format of the timestamp in the filename, in strftime format
    file_timestep -- int
        How many seconds is between each file
    start_timestamp -- str
        The timestamp where the file name generation will start, either
        from influxdb or season_start in .ini
    end_timestamp -- str
        The timestamp where the file name generation will end, from
        the newest file in the input data
    scan_or_generate -- bool
        To generate filenames or just scan them from the folder
    measurement_files -- list
        List of the final measurement files that will be read

    Methods
    ---
    generate_filenames
        Generates the list of filenames
    match_files
        Checks that all of the generated filenames can be found
    """

    def __init__(self, measurement_dict, defaults_dict, start_timestamp, end_timestamp):
        self.path = measurement_dict.get("path")
        self.file_timestamp_format = measurement_dict.get(
            "file_timestamp_format")
        self.defaults_dict = defaults_dict
        self.file_timestep = int(self.defaults_dict.get("file_timestep"))
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        self.scan_or_generate = int(measurement_dict.get("scan_or_generate"))
        self.measurement_files = self.match_files(self.generate_filenames())

    def generate_filenames(self):
        """
        Generates filenames by adding file_timestep to the timestamp in
        the filename

        args:
        ---

        returns:
        ---
        filenames -- list
            list of the filenames
        """
        start_date = self.start_timestamp
        end_date = self.end_timestamp

        filenames = []
        current_date = start_date

        # just initiate this variable for later use
        new_filename = "init"
        # TODO:
        #     Should generate filenames at least one timestep past the
        #     end_date to make sure the whole timeframe is covered.
        while current_date <= end_date:
            filename = current_date.strftime(self.file_timestamp_format)
            # if the filename in current loop is the same as the one
            # generated in previous loop, it means there's no timestamp
            # in the filename, assumdely there's only one file that
            # needs to be read.
            if filename == new_filename:
                return [filename]
            filenames.append(filename)
            current_date += datetime.timedelta(seconds=self.file_timestep)
            new_filename = filename

        filenames = sorted(filenames)
        return filenames

    def match_files(self, patterns):
        """
        Finds the files listed in the list of filenames and returns a
        list of the filenames that were found

        args:
        ---
        patterns -- list
            This is the list of filenames

        returns:
        ---
        files -- list
            List of filepaths
        """
        files = []
        p = Path(self.path)
        if self.scan_or_generate == 1:
            for filestr in patterns:
                filestr = f"*{filestr}*"
                try:
                    file = p.rglob(filestr)
                    file = [x for x in file if x.is_file()]
                    file = file[0]
                except IndexError:
                    continue
                files.append(file)
        # if scan or generate is 0, file_timestamp_format should generate
        # complete filenames
        if self.scan_or_generate == 0:
            return patterns
        return files


class get_start_and_end_time:
    """
    Gets timestamps between which the files that will be calculated
    / uploaded are selected

    Attributes
    ---
    influxdb_dict -- dict
        Influxdb part of the .ini
    season_start -- str
        This date will be used as starting date if there's no data in
        the influxdb bucket
    path -- str
        Path where to look for files
    file_extension -- str
        file extension of the file that will be scanned for
    file_timestamp_format -- str
        format of the timestamp in the filename, in strftime
    start_timestamp -- str
        Start date for the file name generation
    end_timestamp -- str
        End date for the file name generation

    Methods
    ---
    strftime_to_regex
        Converts strftime to regex, for file matching
    get_newest
        Gets newest file in a folder
    extract_date
        Extracts the date that is given in file_timestamp_format from
        the filename
    check_last_db_timestamp
        Checks the last timestamp in influxdb
    check_timestamp
        Terminates the script if start_timestamp is older than the
        end_timestamp, it means that there's no new data to push to db
    """

    def __init__(self, influxdb_dict, measurement_dict, defaults_dict):
        self.influxdb_dict = influxdb_dict
        self.defaults_dict = defaults_dict
        self.season_start = self.defaults_dict.get("season_start")
        self.path = measurement_dict.get("path")
        self.file_extension = measurement_dict.get("file_extension")
        self.file_timestamp_format = measurement_dict.get(
            "file_timestamp_format")
        self.start_timestamp = self.get_last_timestamp()
        self.used_ini_date = 0
        self.end_timestamp = self.extract_date(
            get_newest(self.path, self.file_extension)
        )
        if self.defaults_dict.get("limit_data"):
            if int(self.defaults_dict.get("limit_data")) > 0:
                to_add = int(self.defaults_dict.get("limit_data"))
                self.end_timestamp = self.start_timestamp + \
                    datetime.timedelta(days=to_add)

        if check_timestamp(self.start_timestamp, self.end_timestamp):
            if self.used_ini_date == 1:
                logger.info(
                    "Timestamp from .ini is older than the oldest file"
                    " timestamp, is the date you have in the .ini"
                    " correct?"
                )
            else:
                logger.info(
                    "Timestamp in db is older than the oldest file"
                    " timestamp, all data is already in db"
                )
                logger.info("Exiting.")
            sys.exit(0)
        else:
            pass

    def extract_date(self, datestring):
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
        #    date = re.search(self.strftime_to_regex(), datestring).group(0)
        # except AttributeError:
        # logger.info('Files are found in folder but no matching file found, is the format of the timestamp correct?')
        #    return None
        if self.file_timestamp_format == strftime_to_regex(self.file_timestamp_format):
            logger.info(
                "No strftime formatting in filename, returning current date")
            return datetime.datetime.today()
        date = re.search(
            strftime_to_regex(self.file_timestamp_format), datestring
        ).group(0)
        # class chamber_cycle calls this method and using an instance
        # variable here might cause issues if the timestamp formats
        # should be different
        return datetime.datetime.strptime(date, self.file_timestamp_format)

    def get_last_timestamp(self):
        """
        Extract latest date from influxDB

        args:
        ---

        returns:
        ---
        lastTs -- datetime.datetime
            Either the last timestamp in influxdb or season_start from .ini
        """

        if not self.influxdb_dict.get("url"):
            # if self.influxdb_dict.get("url") is "":
            last_ts = datetime.datetime.strptime(
                self.season_start, self.influxdb_dict.get(
                    "influxdb_timestamp_format")
            )
            self.used_ini_date = 1
        else:
            last_ts = check_last_db_timestamp(self.influxdb_dict)
        if last_ts is None:
            # logger.warning(
            #     "Couldn't get timestamp from influxdb," " using season_start from .ini"
            # )
            last_ts = datetime.datetime.strptime(
                self.season_start, self.influxdb_dict.get(
                    "influxdb_timestamp_format")
            )
        return last_ts


class handle_eddypro:
    """
    Opens eddypro .zip files and reads the .csv file inside them

    Attributes
    ---
    zip_files -- list
        List of the .zip_files to read
    measurement_dict -- dict
        measurement file part from the .ini
    path -- str
        file path
    names -- list
        list of the new names for the columns to be read
    columns -- list
        List of columns to be read
    delimiter -- str
        Delimiter of the .csv file
    skiprows -- int
        How many rows to skip from the beginning of file
    data -- pandas.dataframe
        Dataframe of the .csv files that are read from inside the
        eddypro zips

    Methods
    ---
    read_eddypro
        opens zipfiles and reads .csv files inside them

    """

    def __init__(self, zip_files, measurement_dict):
        self.zip_files = zip_files
        self.measurement_dict = measurement_dict
        self.path = self.measurement_dict.get("path")
        self.names = self.measurement_dict.get("names").split(",")
        self.columns = list(
            map(int, self.measurement_dict.get("columns").split(",")))
        self.dtypes = self.measurement_dict.get("dtypes").split(",")
        self.dtypes = {self.names[i]: self.dtypes[i]
                       for i in range(len(self.names))}
        # self.columns = self.measurement_dict.get('columns')
        # columns = list(map(int, dict.get('columns').split(',')))
        self.delimiter = self.measurement_dict.get("delimiter")
        self.skiprows = int(self.measurement_dict.get("skiprows"))

        self.data = self.read_eddypro()

    def read_eddypro(self):
        """
        Opens eddypro zips and reads the .csv files inside them into
        pandas.dataframes

        args:
        ---

        returns:
        ---
        ready_data -- pandas.dataframe
            the .csv files from inside the zip files read into a
            pandas.dataframe
        """
        dfList = []
        for i in self.zip_files:
            try:
                zip_file = Path(i)
                archive = zf.ZipFile(zip_file, "r")
            # skip zips that have errors
            except BadZipFile:
                continue
            # get list of all files and folders inside .zip
            files = archive.namelist()
            # remove all files that don't have 'eddypro_exp_full' in
            # them, this should leave only one file
            file = [x for x in files if "eddypro_exp_full" in x]
            if len(file) == 1:
                # pop returns current item in loop and removes it from list
                filename = file.pop()
            else:
                continue
            csv = archive.open(filename)
            ec = pd.read_csv(
                csv,
                usecols=self.columns,
                skiprows=self.skiprows,
                names=self.names,
                dtype=self.dtypes,
                na_values="NaN",
            )
            # this file should only have one row, skip if there's more
            if len(ec) != 1:
                continue
            dfList.append(ec)
        ready_data = pd.concat(dfList)
        ready_data["datetime"] = pd.to_datetime(
            ready_data["date"].apply(str) + " " + ready_data["time"],
            format="%Y-%m-%d %H:%M",
        )
        ready_data = ready_data.set_index(ready_data["datetime"])
        ready_data = ready_data.drop(columns=["date", "time", "datetime"])
        return ready_data


class csv_reader:
    """
    Reads .csv files into pandas.dataframes

    Attributes
    ---
    files -- list
        list of the .csv files to be read
    measurement_dict -- dict
        part of the .ini which defines the .csv file
    data -- pandas.dataframe
        the .csv file read into a pandas.dataframe

    Methods
    ---
    csv_ini_parser
        parses the .ini dictionary
    add_csv_data
        Reads the .csv defined in the .ini into a pandas.dataframe

    """

    def __init__(self, csv_files, measurement_dict):
        self.files = csv_files
        self.measurement_dict = measurement_dict
        self.data = self.add_csv_data(self.measurement_dict)

    def csv_ini_parser(self, ini_dict):
        """
        Parser the .ini to variables

        args:
        ---
        ini_dict -- dictionary
            The part of the .ini which defines the .csv file
        returns:
        ---
        path -- str
            path to the file
        delimiter -- str
            .csv delimiter
        skiprows -- int
            how many rows to skip at beginning of file
        timestamp_format -- str
            timestamp format of the .csv timestamp column
        file_extension -- str
            file extension of the file to be read
        columns -- list of ints
            list of the column numbers which will be read from the .csv
        names -- list of strings
            names for the columns that will be read
        dtypes -- dict
            columns and names paired into a dict
        """
        dict = ini_dict
        path = dict.get("path")
        # unnecessary?
        # file_timestamp_format = dict.get('file_timestamp_format')
        # if file_timestamp_format == '':
        #    files = [p for p in Path(path).glob(f'*{file}*') if '~' not in str(p)]

        delimiter = dict.get("delimiter")
        file_extension = dict.get("file_extension")
        skiprows = int(dict.get("skiprows"))
        timestamp_format = dict.get("timestamp_format")
        columns = list(map(int, dict.get("columns").split(",")))
        names = dict.get("names").split(",")
        dtypes = dict.get("dtypes").split(",")
        dtypes = {names[i]: dtypes[i] for i in range(len(names))}
        return (
            path,
            delimiter,
            skiprows,
            timestamp_format,
            file_extension,
            columns,
            names,
            dtypes,
        )

    def add_csv_data(self, ini_dict):
        """
        Reads the .csv files into pandas.dataframes as defined in the .ini

        args:
        ---
        ini_dict -- dictionary
            The part of the .ini file that defines the .csv file
        returns:
        ---
        dfs -- pandas.dataframe
            All of the .csv files read into a pandas.dataframe

        """
        (
            path,
            delimiter,
            skiprows,
            timestamp_format,
            file_extension,
            columns,
            names,
            dtypes,
        ) = self.csv_ini_parser(ini_dict)
        tmp = []
        date_and_time = ["date", "time"]
        for f in self.files:
            try:
                f = f + file_extension
                df = pd.read_csv(
                    Path(path) / f,
                    skiprows=skiprows,
                    delimiter=delimiter,
                    usecols=columns,
                    names=names,
                    dtype=dtypes,
                )
            except FileNotFoundError:
                logger.info(
                    f"File not found at {Path(path) / f}, make sure the .ini is correct"
                )
                sys.exit()
            tmp.append(df)
        dfs = pd.concat(tmp)
        # check if date and time are separate columns
        check = any(item in date_and_time for item in names)
        # if date and time are separate, combine them to datetime
        if check is True:
            dfs["datetime"] = pd.to_datetime(
                dfs["date"].apply(str) + " " + dfs["time"], format=timestamp_format
            )
        # if they are not separate, there should be a datetime column already
        else:
            dfs["datetime"] = pd.to_datetime(
                dfs["datetime"], format=timestamp_format)
        dfs.set_index("datetime", inplace=True)
        return dfs


class read_manual_measurement_timestamps:
    """
    Class for creating manual measurement timestamps from the manual
    measurement text files. Formatted as .csv files.
    For the sake of consistency, measurement starting times are named
    chamber_close and chamber_open even though that doesn't really
    happen with manual measurement.

    Manual measurement text file format:
        filename: YYMMDD.txt
            ----------------
            Date,211019
            Name,ek
            Sky,cloudy
            Temp,-1
            Wind,no wind
            Precipitation,none
            ACSnowdepth,
            FluxAvg,
            AcFluxAvg,
            Plot Number,Start Time,Notes,Chamber height
            71,1014
            67,1025
            65,1034
            ----------------

    Attributes
    ---
    measurement_dict -- dict
        Part of the .ini defining the measurement times
    meaurement_path -- str
        The path to the manual measurement time files
    chamber_close_time -- int
        Seconds since placing down the chamber
    chamber_open_time -- int
        Seconds since placing down the chamber
    measurement_end_time -- int
        manual measurement are 5 minutes
    measurement_files -- list
        List of the manual measurement time files
    filter_tuple -- list of tuples
        List of tuples generated from the measurement timestamps

        [(chamber_close_time, chamber_open_time, chamber_num), ..., ]
        [(YYYY-MM-DD HH:MM:SS, YYYY-MM-DD HH:MM:SS, chamberNum), ..., ]

    manual_measurement_df -- pandas.dataframe

    Methods
    ---
    """

    def __init__(
        self, measurement_time_data_dict, measurement_files, chamber_cycle_dict
    ):
        self.chamber_cycle_dict = chamber_cycle_dict
        self.measurement_time_data_dict = measurement_time_data_dict
        self.measurement_path = self.measurement_time_data_dict.get("path")
        self.measurement_files = measurement_files

        self.chamber_close_time = int(
            self.chamber_cycle_dict.get("start_of_measurement")
        )
        self.chamber_open_time = int(
            self.chamber_cycle_dict.get("end_of_measurement"))
        self.measurement_end_time = int(
            self.chamber_cycle_dict.get("end_of_cycle"))
        self.manual_measurement_df = self.read_manual_measurement_file()
        self.filter_tuple = create_filter_tuple(self.manual_measurement_df)

    def read_manual_measurement_file(self):
        # NOTE: the format of the manual measurement is hardcoded
        tmp = []
        for f in self.measurement_files:
            # with open(f) as f:
            #    first_line = f.read_line()
            # date = first_line
            date = re.search("\d{6}", str(f.name))[0]
            df = pd.read_csv(
                f,
                skiprows=10,
                names=["chamber", "start", "notes", "snowdepth", "validity"],
                dtype={
                    "chamber": "int",
                    "start": "str",
                    "notes": "str",
                    "snowdepth": "float",
                    "validity": "str",
                },
            )
            df["date"] = date
            df["datetime"] = df["date"] + " " + df["start"]
            df["datetime"] = pd.to_datetime(
                df["datetime"], format="%y%m%d %H%M")
            # for the sake of consisteny, even though the manual
            # measurement doesn't really have a closing time, the
            # variable is named like this
            df["start_time"] = df["datetime"]
            df["close_time"] = df["datetime"] + pd.to_timedelta(
                self.chamber_close_time, unit="s"
            )
            df["open_time"] = df["datetime"] + pd.to_timedelta(
                self.chamber_open_time, unit="s"
            )
            df["end_time"] = df["datetime"] + pd.to_timedelta(
                self.measurement_end_time, unit="s"
            )
            df["snowdepth"] = df["snowdepth"].fillna(0)
            tmp.append(df)
        dfs = pd.concat(tmp)
        dfs.set_index("datetime", inplace=True)
        dfs["notes"] = dfs["notes"].fillna("")
        dfs["validity"] = dfs["notes"].fillna("")
        dfs.sort_index(inplace=True)
        return dfs


# class grapher:
#     def __init__(
#         self, measurement_data, cycle_timestamps, slope_timestamps, flux_timestamps
#     ):
#         self.measurement_data = measurement_data
#         self.cycle_timestamps = cycle_timestamps
#         self.slope_timestamps = slope_timestamps
#         self.flux_timestamps = flux_timestamps
#         generate_graphs(measurement_data, slope_timestamps)


class measurement_tagger:
class excel_creator:
    def __init__(
        self, all_data, summarized_data, filter_tuple, excel_path, sort
    ):
        self.all_data = all_data
        self.sum_data = summarized_data
        self.fil_tup = filter_tuple
        self.excel_path = excel_path
        self.create_xlsx()

        # files = glob.glob("figs")

    def create_xlsx(self):
        # create a list of days for creating outputs
        daylist = []
        # initiate sparkline
        fig, ax = create_fig()
        times = self.fil_tup

        m_times = [subs_from_filter_tuple(time, 20) for time in times]
        w_times = [add_to_filter_tuple(time, 20) for time in times]

        for i, date in enumerate(w_times):
            data = date_filter(self.all_data, date).copy()
            if data.empty:
                continue
            day = date[0].date()
            smask = self.sum_data.index == times[i][0]
            if day not in daylist:
                daylist.append(day)
            try:
                gases = ["ch4", "co2"]
                day = str(date[0].date())
                name = date[0].strftime("%Y%m%d%H%M%S")
                for gas in gases:
                    fig_root = "figs"
                    path = Path(f"{fig_root}/{gas}/{day}/")
                    plotname = f"{name}.png"
                    fig_path = str(path / plotname)
                    if not path.exists():
                        path.mkdir(parents=True)
                    self.sum_data.loc[smask, f"fig_dir_{gas}"] = fig_path
                    y = data[gas]
                    rects = create_rects(y, times[i], m_times[i])
                    create_sparkline2(
                        data[[gas]], fig_path, gas, fig, ax, rects
                    )
            except Exception as e:
                logger.error(
                    f"Error when creating graph with matplotlib, "
                    f"most likely not enough memory. Error: {e}"
                )
        for day in daylist:
            data = self.sum_data[self.sum_data.index.date == day]
            sort = None
            create_excel2(data, self.excel_path, sort)
        create_excel2(self.sum_data, self.excel_path, sort, "all_data")
        shutil.rmtree(fig_root)


class parse_aux_data:
    def __init__(self, config):
        self.cfg = config
        self.parse_aux_cfg()
        self.read_aux_data()

    def parse_skiprows(self, skiprows):
        def parser(x):
            return [
                int(item) if item.isdigit() else item for item in x.split(",")
            ]

        value = parser(skiprows)
        if len(value) == 1:
            value = value.pop()
        return value

    def parse_read_csv_args(self, args_dict):
        # Define converters for specific arguments known to require non-string types
        converters = {
            "header": lambda x: int(x)
            if x.isdigit()
            else None
            if x.lower() == "none"
            else x,
            "index_col": lambda x: int(x)
            if x.isdigit()
            else None
            if x.lower() == "none"
            else x,
            "usecols": lambda x: [
                int(item) if item.isdigit() else item for item in x.split(",")
            ],
            "skiprows": self.parse_skiprows,
            "parse_dates": lambda x: [item for item in x.split(",")],
            "names": lambda x: [
                int(item) if item.isdigit() else item for item in x.split(",")
            ],
            "na_values": lambda x: x.split(",") if "," in x else x,
        }

        parsed_args = {}
        for key, value in args_dict.items():
            if key in converters:
                try:
                    # Apply the converter if one is defined for this key
                    parsed_args[key] = converters[key](value)
                except ValueError:
                    print(
                        f"Warning: Could not convert argument { key} with value {value}"
                    )
            else:
                # Copy over any arguments without a specific converter
                parsed_args[key] = value

        return parsed_args

    def parse_aux_cfg(self):
        """
        Creates a list of dictionaries out of .ini sections with have
        "aux_data_" in them.
        """
        # list all config sections with aux_data_ in them
        cfg_names = [s for s in self.cfg.sections() if "aux_data_" in s]
        # config sections to dictionaries
        aux_cfgs = [dict(self.cfg.items(c)) for c in cfg_names]
        # initiate list for the parsed configs
        self.aux_cfgs = []
        for cfg in aux_cfgs:
            name_key = cfg["name"]
            path = Path(cfg.get("path"))
            files = list(path.rglob(cfg.get("file_name")))
            merge_method = cfg.get("merge_method")
            direction = cfg.get("direction")
            tolerance = cfg.get("tolerance")
            # possible values in the .ini that we don't want passed to pandas
            # read_csv
            excluded = [
                "name",
                "path",
                "file_name",
                "merge_method",
                "direction",
                "tolerance",
            ]

            # create dict with pandas read_csv compatible args
            pd_args = {k: v for k, v in cfg.items() if k not in excluded}
            pd_args = self.parse_read_csv_args(pd_args)

            new_dict = {
                "name": name_key,
                "merge_method": merge_method,
                "files": files,
                "args": pd_args,
                "direction": direction,
                "tolerance": tolerance,
            }
            self.aux_cfgs.append(new_dict)

    def read_aux_data(self):
        self.aux_dfs = []
        for f in self.aux_cfgs:
            dfs = pd.DataFrame()
            for file in f.get("files"):
                args = f.get("args")
                df = pd.read_csv(file, **args)
                dfs = pd.concat([dfs, df])
            # NOTE: add sort
            dfs.sort_index(inplace=True)
            f["df"] = dfs


class merge_data2:
    """
    Merges auxiliary data to the main gas measurement.

    Attributes
    ----------
    measurement_df : pandas.DataFrame
        main gas measurement
    aux_cfg : dictionary
        parsed with `parse_aux_cfg`
    merged_data : pandas.DataFrame
        `measurement_df` with auxiliary data merged into it
    """

    def __init__(self, measurement_df, aux_cfg):
        self.measurement_df = measurement_df
        self.aux_cfg = aux_cfg
        self.merged_data = measurement_df
        for cfg in self.aux_cfg:
            merge_method = cfg.get("merge_method")
            logger.debug(f"Merge method for {cfg.get('name')}: {merge_method}")
            if merge_method == "timeid":
                logger.debug(f"merging {cfg.get('name')} with {merge_method}")
                merged = merge_by_dtx_and_id(self.merged_data, cfg)
                if merged is not None:
                    self.merged_data = merged
            if merge_method == "id":
                logger.debug(f"merging {cfg.get('name')} with {merge_method}")
                merged = merge_by_id(self.merged_data, cfg)
                if merged is not None:
                    self.merged_data = merged
            if merge_method == "time":
                logger.debug(f"merging {cfg.get('name')} with {merge_method}")
                merged = merge_by_dtx(self.merged_data, cfg)
                if merged is not None:
                    self.merged_data = merged

        if len(aux_cfg) == 0:
            self.merged_data = self.measurement_df
            pass

#!/usr/bin/env python3

import sys
import logging
import datetime
import numpy as np
import pandas as pd
from pathlib import Path
from traceback import format_exc
from re import search

# modules from this repo
from tools.filter import (
    date_filter,
    mk_fltr_tuples,
    add_min_to_cycle,
    add_min_to_calc,
    get_datetime_index,
)
from tools.file_tools import (
    get_newest,
    mk_date_dict,
    find_files,
    filter_between_dates,
    read_man_meas_f,
    get_files,
)
from tools.time_funcs import (
    time_to_numeric,
    strftime_to_regex,
    extract_date,
    convert_seconds,
    get_time_diff,
)
from tools.influxdb_funcs import (
    check_oldest_db_ts,
    check_newest_db_ts,
    read_ifdb,
)
from tools.gas_funcs import (
    calculate_gas_flux,
    calculate_pearsons_r,
    calculate_slope,
)
from tools.merging import (
    merge_by_dtx,
    merge_by_id,
    merge_by_dtx_and_id,
)

from tools.create_excel import (
    create_excel,
    create_sparkline,
    create_fig,
    create_rects,
)

from tools.parse_ini import iniHandler

from tools.aux_cfg_parser import parse_aux_cfg
from tools.aux_data_reader import read_aux_data
from tools.validation import check_valid, overlap_test, use_defaults

from tools.instruments import li7810

logger = logging.getLogger("defaultLogger")


# BUG: Feeding one big measurement file instead of several smaller ones causes
# the script to not honor the starting and ending times
class fluxCalculator:
    def __init__(
        self, inifile, env_vars, instrument_class=None, measurement_class=None
    ):
        self.inifile = inifile
        self.ini_handler = iniHandler(self.inifile, env_vars)
        self.instrument_class = instrument_class
        self.measurement_class = measurement_class
        self.use_defaults = self.ini_handler.use_defaults
        self.data_path = self.ini_handler.data_path
        self.data_ext = self.ini_handler.data_ext
        self.mode = self.ini_handler.mode
        self.aux_cfgs = self.ini_handler.aux_cfgs
        self.init_meas_reader(self.instrument_class, self.measurement_class)

        # start_ts and end_ts define the timeframe from which data will be
        # processed
        self.start_ts, self.end_ts = self.get_start_end()

        if self.mode == "man":
            self.create_dfs_man()
        if self.mode == "ac":
            self.create_dfs_ac()
        # self.aux_cfgs = parse_aux_cfg(self.cfg)
        self.aux_cfgs = read_aux_data(self.aux_cfgs, self.start_ts, self.end_ts)
        self.merge_aux()
        self.merged = check_valid(
            self.merged, self.measurement_list, self.device, self.ini_handler.meas_et
        )

        self.merged = self.calc_slope_pearsR(self.merged)
        # BUG: datetime is now the chamber close time instead of the measurement
        # start time since what self.merged gets filtered down to.
        self.ready_data = self.summarize()

        if self.ini_handler.get("defaults", "create_excel") == "1":
            logger.info("Excel creation enabled.")
            self.create_xlsx()
        else:
            logger.info("Excel creation disabled in .ini, skipping")
        logger.info("Run completed.")

    def init_meas_reader(self, instrument_class, measurement_class):
        if self.instrument_class is None:
            self.device = li7810()
        else:
            self.device = instrument_class()

        if self.measurement_class is None:
            self.timestamp_file = timestamps()
        else:
            self.timestamp_file = measurement_class()
        pass

    def get_start_end(self):
        s_ts = None
        e_ts = None
        if self.ini_handler.use_ini_dates == "1":
            logger.info("Using dates define in .ini to calculate data")
            s_ts = self.ini_handler.get("defaults", "start_ts")
            if s_ts is not None:
                s_ts = datetime.datetime.strptime(s_ts, "%Y-%m-%d %H:%M:%S")
            e_ts = self.ini_handler.get("defaults", "end_ts")
            if e_ts is not None:
                e_ts = datetime.datetime.strptime(e_ts, "%Y-%m-%d %H:%M:%S")
            logger.info(f"Start date: {s_ts}, End date: {e_ts}")
        else:
            s_ts = self.get_start_ts()
            # if measurement dict defines a path, get the ending timestamp from the
            # last modified file
            if self.ini_handler.get("measurement_data", "path"):
                ts_fmt = self.ini_handler.file_ts_fmt
                e_ts = extract_date(ts_fmt, get_newest(self.data_path, self.data_ext))
            # measurement defines the name of the influxdb measurement

        if s_ts:
            limit = self.ini_handler.get("defaults", "limit_data")
            if limit:
                if int(limit) > 0:
                    logger.info(f"Limiting data from {s_ts} + {limit} days")
                    e_ts = s_ts + datetime.timedelta(days=int(limit))
        if not s_ts and e_ts:
            logger.debug(f"No start ts, processing all data until {e_ts}")
        if not s_ts and not e_ts:
            logger.debug("No start or end timestamps, processing all data.")

        return s_ts, e_ts

    def create_dfs_ac(self):
        # NOTE: clean this mess
        if self.mode == "ac":
            if self.ini_handler.get("measurement_data", "path"):
                self.meas_files = get_files(
                    self.ini_handler.measurement_dict, self.start_ts, self.end_ts
                )
                logger.debug(
                    f"Found {len(self.meas_files)} in folder {self.data_path}."
                )
            else:
                pass
            if self.ini_handler.measurement_dict.get("path"):
                self.data = self.read_meas()
                self.time_data = self.mk_cham_cycle2()
            else:
                self.data = read_ifdb(
                    self.ifdb_dict, self.meas_dict, self.start_ts, self.end_ts
                )
                if self.data is None:
                    logger.info(
                        "No data returned from db. Check your dates and fields."
                    )
                    sys.exit(0)
                self.time_data = self.mk_cham_cycle2()

            # measurement times dataframe
            self.w_merged = self.data
            self.measurement_list = mk_fltr_tuples(self.time_data)
            self.merged = self.merge_main_and_time()

            # self.fltr_tuple = mk_fltr_tuple(self.time_data)

    def create_dfs_man(self):
        """Create dataframes with gas measurements and the chamber rotation"""
        # NOTE: clean this mess
        if self.mode == "man":
            # if meas_dict has a path, look for files
            if self.ini_handler.get("measurement_data", "path"):
                self.meas_files = get_files(
                    self.ini_handler.measurement_dict, self.start_ts, self.end_ts
                )
                if not self.meas_files:
                    logger.info(
                        f"No files found from {self.start_ts.date()} to {self.end_ts.date()} in {self.data_path}."
                    )
                    logger.info("Exiting.")
                    sys.exit()
                else:
                    logger.debug(
                        f"Found {len(self.meas_files)} in folder {self.data_path}."
                    )
                self.meas_t_files = get_files(
                    self.ini_handler.measurement_time_dict,
                    self.start_ts,
                    self.end_ts,
                )
                self.data = self.read_meas()
                self.time_data = read_man_meas_f(
                    self.meas_t_files, self.ini_handler.get_chamber_settings()
                )
            else:
                self.data = read_ifdb(
                    self.ini_handler.influxdb_dict,
                    self.meas_dict,
                    self.start_ts,
                    self.end_ts,
                )
                if self.data is None:
                    logger.info("No data returned from db.")
                    sys.exit()
                self.time_data = read_ifdb(
                    self.ini_handler.influxdb_dict,
                    self.meas_t_dict,
                    self.start_ts,
                    self.end_ts,
                )
                self.time_data["chamber"] = self.time_data["chamber"].astype(int)
            # measurement times dataframe
            self.w_merged = self.data
            self.measurement_list = mk_fltr_tuples(self.time_data)
            self.merged = self.merge_main_and_time()

    def mk_cham_cycle2(self):
        tmp = []
        # loop through files, read them into a pandas dataframe and
        # create timestamps
        dates = [str(date) for date in pd.unique(self.data.index.date)]
        df = pd.read_csv(
            self.ini_handler.get("defaults", "chamber_cycle_file"),
            names=["time", "chamber"],
        )
        for date in dates:
            dfa = df.copy()
            dfa["date"] = date
            # dfa["datetime"] = pd.to_datetime(
            #     dfa["date"] + " " + dfa["time"].astype(str)
            # ).tz_localize("UTC")
            dfa["datetime"] = pd.to_datetime(
                dfa["date"] + " " + dfa["time"].astype(str)
            )
            # dfa["datetime"] = dfa["datetime"].dt.tz_localize("UTC")
            # dfa["datetime"] = dfa["datetime"].tz_convert(self.data.index.tz)
            dfa["start_time"] = dfa["datetime"]
            dfa["close_time"] = dfa["datetime"] + pd.to_timedelta(
                self.ini_handler.ch_ct, unit="s"
            )
            dfa["open_time"] = dfa["datetime"] + pd.to_timedelta(
                self.ini_handler.ch_ot, unit="s"
            )
            dfa["end_time"] = dfa["datetime"] + pd.to_timedelta(
                self.ini_handler.meas_et, unit="s"
            )
            tmp.append(dfa)

        dfs = pd.concat(tmp)
        dfs = overlap_test(dfs)
        dfs.set_index("datetime", inplace=True)
        return dfs

    def get_start_ts(self):
        """
        Extract first date from influxDB measurement

        args:
        ---

        returns:
        ---
        first_ts -- datetime.datetime
            Either the oldest timestamp in influxdb or season_start from .ini
        """
        start_ts = self.ini_handler.get("defaults", "start_ts")
        ifdb_ts_format = "%Y-%m-%d %H:%M:%S"

        if not self.ini_handler.get("influxDB", "url"):
            first_ts = datetime.datetime.strptime(start_ts, ifdb_ts_format)
        else:
            logger.info("Checking latest ts from DB.")
            first_ts = check_oldest_db_ts(
                self.ini_handler.influxdb_dict,
                self.ini_handler.measurement_dict,
                self.device.gas_cols,
            )
            logger.info(f"Oldest ts in db: {first_ts}")
        if first_ts is None:
            first_ts = datetime.datetime.strptime(start_ts, ifdb_ts_format)
        return first_ts

    def gen_files(self, config):
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
        start_date = self.start_ts
        end_date = self.end_ts
        logger.debug(f"Creating file between {self.start_ts} and {self.end_ts}")

        filenames = []
        current_date = start_date

        # just initiate this variable for later use
        new_filename = "init"
        # TODO:
        #     Should generate filenames at least one timestep past the
        #     end_date to make sure the whole timeframe is covered.
        while current_date <= end_date:
            filename = current_date.strftime(config.get("file_timestamp_format"))
            # if the filename in current loop is the same as the one
            # generated in previous loop, it means there's no timestamp
            # in the filename, assumsely there's only one file that
            # needs to be read.
            if filename == new_filename:
                return [filename]
            filenames.append(filename)
            current_date += datetime.timedelta(days=1)
            new_filename = filename

        filenames = sorted(filenames)
        return filenames

    def match_files(self, patterns, file_dict):
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
        data_path = file_dict.get("path")
        scan_or_gen = int(file_dict.get("scan_or_generate"))
        p = Path(data_path)
        if scan_or_gen == 1:
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
        if self.scan_or_gen == 0:
            return patterns
        return files

    def read_meas(self):
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
        # initiate list where all read dataframes will be stored
        tmp = []
        for f in self.meas_files:
            try:
                df = self.device.read_file(f)
            except Exception as e:
                logger.warning(f"Read fail: {f.name}")
                logger.debug(f"Error: {e}")
                logger.debug(format_exc())
                continue
            logger.info(f"read success: {f.name}")
            df["gas_file"] = str(f.name)
            tmp.append(df)
        # concatenate all stored dataframes into one big one
        dfs = pd.concat(tmp)
        # combine individual date and time columns into datetime
        # column
        # logger.debug("Calculating ordinal times.")
        # dfs["numeric_date"] = pd.to_datetime(dfs["datetime"]).map(
        #     datetime.datetime.toordinal
        # )
        # dfs["numeric_time"] = numeric_timer(dfs[self.device.time_col].values)
        # dfs["numeric_datetime"] = dfs["numeric_time"] + dfs["numeric_date"]
        dfs.set_index("datetime", inplace=True)
        dfs.sort_index(inplace=True)
        dfs["month"] = dfs.index.month
        dfs["day"] = dfs.index.day
        dfs["doy"] = dfs.index.dayofyear
        dfs["checks"] = ""
        dfs["is_valid"] = True

        return dfs

    # def read_man_meas_f(self):
    #     # note: the format of the manual measurement is hardcoded
    #     tmp = []
    #     for f in self.meas_t_files:
    #         # with open(f) as f:
    #         #    first_line = f.read_line()
    #         # date = first_line
    #         logger.debug(f"reading measurement {f.name}.")
    #         df = self.timestamp_file.read_file(f)
    #         # note: for the sake of consisteny, even though the manual
    #         # measurement doesn't really have a closing time, the
    #         # variable is named like this
    #         df["start_time"] = df["datetime"]
    #         df["end_time"] = df["datetime"] + pd.to_timedelta(
    #             self.ini_handler.meas_et, unit="s"
    #         )
    #         df["close_time"] = df["datetime"] + pd.to_timedelta(
    #             self.ini_handler.ch_ct, unit="s"
    #         )
    #         df["open_time"] = df["datetime"] + pd.to_timedelta(
    #             self.ini_handler.ch_ot, unit="s"
    #         )
    #         df["snowdepth"] = df["snowdepth"].fillna(0)
    #         df["ts_file"] = str(f.name)
    #         tmp.append(df)
    #     dfs = pd.concat(tmp)
    #     dfs.set_index("datetime", inplace=true)
    #     dfs["notes"] = dfs["notes"].fillna("")
    #     dfs = overlap_test(dfs)
    #     dfs.sort_index(inplace=true)
    #     return dfs

    def merge_main_and_time(self):
        """
        Merges the measurement files into the main gas measurement dataframe row
        by row
        """
        logger.debug("Attaching measurement times to gas measurement.")
        df = self.data.copy()
        self.time_data.dropna(inplace=True, axis=1)
        time_df = self.time_data.copy()
        for filter in self.measurement_list:
            st, et = get_datetime_index(df, filter)
            stt, ett = get_datetime_index(time_df, filter)
            times = time_df.iloc[stt:ett]
            for idx, row in times.iterrows():
                for col, value in row.items():
                    if col not in df.columns:
                        df[col] = pd.NA
                    df.iloc[st:et, df.columns.get_loc(col)] = value
        return df

    def merge_aux(self):
        for cfg in self.aux_cfgs:
            merge_met = cfg.get("merge_method")
            name = cfg.get("name")
            logger.info(f"merging {name} with method {merge_met}")
            msg = f"Merged {name} with method {merge_met}"

            if merge_met == "timeid":
                merged = merge_by_dtx_and_id(self.merged, cfg)
                if merged is not None:
                    self.merged = merged
                    logger.debug(msg)

            if merge_met == "id":
                merged = merge_by_id(self.merged, cfg)
                if merged is not None:
                    self.merged = merged
                    logger.debug(msg)

            if merge_met == "time":
                merged = merge_by_dtx(self.merged, cfg)
                if merged is not None:
                    self.merged = merged
                    logger.debug(msg)

        # if there's no aux cfgs, return original
        if len(self.aux_cfgs) == 0:
            self.merged = self.merged
        logger.info(f"Completed merging {len(self.aux_cfgs)} auxiliary datasets.")

    def calc_slope_pearsR(self, data):
        """
        Calculates Pearson's R (correlation) and the slope of the CH4 flux.

        Args:
        ---
        data -- pandas.DataFrame
            DataFrame of the gas flux

        Returns:
        ---
        all_measurements_df -- pandas.DataFrame
            DataFrame with additional slope, Pearson's R, and flux columns
        """
        measurement_list = []

        def append_df_with_logging(df, message, measurement):
            logger.warning(message + f" at {measurement.start}")
            measurement_list.append(df)

        def check_conditions_and_continue(mdf, df, measurement):
            if mdf.empty:
                append_df_with_logging(df, "DataFrame empty", measurement)
                return True
            if "has errors" in mdf.iloc[0]["checks"]:
                append_df_with_logging(
                    df, "Skipping flux calculation due to diagnostic flags", measurement
                )
                return True
            if df["overlap"].any():
                append_df_with_logging(
                    df, "Overlapping measurement, skipping", measurement
                )
                return True
            return False

        logger.info("Starting gas flux calculations.")
        for measurement in self.measurement_list:
            df = date_filter(data, measurement).copy()
            mdf = date_filter(df, measurement).copy()

            logger.info(
                f"Calculating flux from {measurement.close} to {measurement.open}"
            )

            # Skip iteration if conditions are met
            if check_conditions_and_continue(mdf, df, measurement):
                continue

            # Ensure snowdepth column exists
            df["snowdepth"] = df.get("snowdepth", 0)
            mdf["snowdepth"] = mdf.get("snowdepth", 0)

            # Calculate height
            mm_to_m = 1000
            cm_to_m = 100
            cham_h = round(self.ini_handler.chamber_h / mm_to_m, 2)
            snow_h = round(df.iloc[1]["snowdepth"] / cm_to_m, 2)
            height = round(cham_h - snow_h, 2)
            df["calc_height"] = height

            # Process each gas
            for gas in self.device.gas_cols:
                slope, pearsons, flux = self.calculate_gas_properties(
                    mdf, df, gas, height
                )
                df[f"{gas}_slope"] = slope
                df[f"{gas}_pearsons_r"] = pearsons
                df[f"{gas}_flux"] = flux

                if use_defaults(df, self.use_defaults):
                    # NOTE: figure out a better way of using default temp and
                    # pressure
                    df["air_pressure"] = self.ini_handler.def_press
                    df["air_temperature"] = self.ini_handler.def_temp
                    mdf["air_pressure"] = self.ini_handler.def_press
                    mdf["air_temperature"] = self.ini_handler.def_temp
                    flux = calculate_gas_flux(
                        mdf,
                        gas,
                        slope,
                        height,
                    )
                else:
                    flux = calculate_gas_flux(
                        mdf,
                        gas,
                        slope,
                        height,
                    )
                    df[f"{gas}_flux"] = flux

            measurement_list.append(df)
        all_measurements_df = pd.concat(measurement_list)
        return all_measurements_df

    def calculate_gas_properties(self, mdf, df, gas, height):
        """
        Helper function to calculate slope, Pearson's R, and flux for a specific gas.
        """
        slope = calculate_slope(mdf["numeric_datetime"], mdf[gas])
        pearsons = calculate_pearsons_r(mdf["numeric_datetime"], mdf[gas])

        # Use default temperature and pressure if necessary
        if use_defaults(df, self.use_defaults):
            df["air_pressure"] = self.ini_handler.def_press
            df["air_temperature"] = self.ini_handler.def_temp
            mdf["air_pressure"] = self.ini_handler.def_press
            mdf["air_temperature"] = self.ini_handler.def_temp

        flux = calculate_gas_flux(mdf, gas, slope, height)
        return slope, pearsons, flux

    def summarize(self):
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
        measurement_cols = self.device.usecols
        drop_cols = [
            # "numeric_date",
            # "numeric_time",
            # "numeric_datetime",
        ]
        drop_cols = (
            measurement_cols
            + drop_cols
            + [col for col in self.merged.columns if "idx_cp" in col]
        )
        for date in self.measurement_list:
            dfa = date_filter(self.merged, date)
            dfList.append(dfa.iloc[:1])
        summary = pd.concat(dfList)
        if "test" not in self.ini_handler.ini_name:
            summary.drop(labels=drop_cols, axis=1, inplace=True)
        # convert True/False to 1/0
        summary["is_valid"] = summary["is_valid"] * 1

        return summary

    def create_xlsx(self):
        # create a list of days for creating outputs
        daylist = []
        # initiate sparkline
        fig, ax = create_fig()
        times = self.measurement_list.copy()

        # NOTE: these need to be as class attribute
        w_times = [add_min_to_calc(time) for time in times]

        m_times = times

        gases = self.device.gas_cols
        logger.info(f"Creating {len(times) * len(gases)} sparklines.")
        logger.info(
            f"Time estimate: {convert_seconds(len(times) * (0.05 * len(gases)))}."
        )
        for i, date in enumerate(self.measurement_list):
            data = date_filter(self.w_merged, date).copy()
            day = date.date
            if data.empty:
                daylist.append(day)
                continue
            smask = self.ready_data.index == w_times[i][0]
            daylist.append(day)
            try:
                day = str(date.date)
                name = date.date.strftime("%Y%m%d%H%M%S")
                for gas in gases:
                    fig_root = "figs"
                    path = Path(f"{fig_root}/{gas}/{day}/")
                    plotname = f"{name}.png"
                    fig_path = str(path / plotname)
                    if not path.exists():
                        path.mkdir(parents=True)
                    self.ready_data.loc[smask, f"fig_dir_{gas}"] = fig_path
                    y = data[gas]
                    rects = create_rects(y, date)
                    create_sparkline(data[[gas]], fig_path, gas, fig, ax, rects)
            except Exception as e:
                logger.warning("Failed sparkline creation.")
                logger.warning(e)
        # converting list to dict and then to list again removes duplicate items
        daylist = list(dict.fromkeys(daylist))
        sort = None
        for day in daylist:
            data = self.ready_data[self.ready_data.index.date == day]
            if data.empty:
                continue
            logger.debug(f"Columns in data passed to create_excel: {data.columns}")
            logger.debug(f"{data.head()}")
            create_excel(data, self.ini_handler.excel_path, sort)
        create_excel(self.ready_data, self.ini_handler.excel_path, sort, "all_data")
        logger.info(f"Saved output .xlsx in {self.ini_handler.excel_path}")


class timestamps:
    def __init__(self):
        self.skiprows = 10
        self.names = ["chamber", "start", "notes", "snowdepth", "validity"]
        self.dtypes = {
            "chamber": "int",
            "start": "str",
            "notes": "str",
            "snowdepth": "float",
            "validity": "str",
        }
        self.time_col = "start"
        self.dt_fmt = "%y%m%d%H%M"

    def read_file(self, f):
        date = search(r"\d{6}", str(f.name)).group(0)
        df = pd.read_csv(
            f,
            skiprows=self.skiprows,
            names=self.names,
            dtype=self.dtypes,
        )
        df["snowdepth"] = df["snowdepth"].fillna(0)
        df["date"] = date
        df["datetime"] = df["date"] + df[self.time_col]
        df["datetime"] = pd.to_datetime(
            df["datetime"],
            format=self.dt_fmt,
            # ).dt.tz_localize("UTC")
        )
        return df

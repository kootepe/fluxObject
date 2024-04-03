import os
import re

import sys
import shutil
import logging
import datetime
import configparser as cfgparser
import numpy as np
import pandas as pd
from pathlib import Path
from traceback import format_exc

# modules from this repo
from tools.filter import (
    date_filter,
    mk_fltr_tuple,
    subs_from_fltr_tuple,
    add_to_fltr_tuple,
)
from tools.time_funcs import (
    ordinal_timer,
    strftime_to_regex,
    extract_date,
    convert_seconds,
)
from tools.influxdb_funcs import ifdb_push, check_oldest_db_ts, read_ifdb
from tools.gas_funcs import (
    calculate_gas_flux,
    calculate_pearsons_r,
    calculate_slope,
)
from tools.merging import (
    merge_by_dtx,
    merge_by_id,
    merge_by_dtx_and_id,
    is_df_valid,
)

from tools.create_excel import (
    create_excel,
    create_sparkline,
    create_fig,
    create_rects,
)

logger = logging.getLogger("defaultLogger")


class gas_flux_calculator:
    def __init__(
        self, inifile, env_vars, instrument_class=None, measurement_class=None
    ):
        self.inifile = inifile
        self.env_vars = env_vars
        self.instrument_class = instrument_class
        self.measurement_class = measurement_class
        self.read_ini()
        self.init_meas_reader(self.instrument_class, self.measurement_class)

        # self.device = instrument_class
        # self.timestamp_file = measurement_class
        # these two variables define from what timeframe files be read from
        self.start_ts = self.get_last_ts()
        # self.end_ts = self.extract_date(
        #     get_newest(self.data_path, self.data_ext)
        self.start_ts = self.get_start_ts()
        # )
        self.end_ts = datetime.datetime.strptime(
            "2024-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"
        )
        # if self.defs.get("limit_data"):
        #     if int(self.defs.get("limit_data")) > 0:
        #         to_add = int(self.defs.get("limit_data"))
        #         self.end_ts = self.start_ts + datetime.timedelta(days=to_add)

        self.create_dfs()
        self.parse_aux_cfg()
        self.read_aux_data()
        self.merge_aux()
        self.w_merged = self.merged
        self.check_valid()
        gases = self.device.gas_cols
        for gas in gases:
            self.merged = self.calc_slope_pearsR(self.merged, gas)
        self.ready_data = self.summarize()

        if self.defs.get("create_excel") == "1":
            logger.info("Excel creation enabled.")
            self.create_xlsx()
        else:
            logger.info("Excel creation disabled in .ini, skipping")

    def init_meas_reader(self, instrument_class, measurement_class):
        if self.instrument_class is None:
            self.device = li7200()
        else:
            self.device = instrument_class()

        if self.measurement_class is None:
            self.timestamp_file = timestamps()
        else:
            self.timestamp_file = measurement_class()
        pass

    def create_dfs(self):
        """Create dataframes with gas measurements and the chamber rotation"""
        if self.mode == "man":
            # if meas_dict has a path, look for files
            if self.meas_dict.get("path"):
                print("no path")
                self.meas_files = self.match_files(
                    self.gen_files(self.meas_dict),
                    self.meas_dict,
                )
                logger.debug(
                    f"Found {len(self.meas_files)} in folder {self.data_path}."
                )
            else:
                pass

            # list of measurement_time files
            self.meas_t_files = self.match_files(
                self.gen_files(self.meas_t_dict),
                self.meas_t_dict,
            )
            logger.debug(
                f"Found {len(self.meas_t_files)} in folder {self.data_path}."
            )
            # gas measurement dataframe
            if self.meas_dict.get("path") is not None:
                self.data = self.read_meas()
            else:
                self.data = read_ifdb(
                    self.ifdb_dict, self.meas_dict, self.start_ts, self.end_ts
                )
            # measurement times dataframe
            self.time_data = self.read_man_meas_f()
            self.merged = self.merge_chamber_ts()
            self.fltr_tuple = mk_fltr_tuple(
                self.time_data, close="start_time", open="end_time"
            )
            # self.fltr_tuple = mk_fltr_tuple(self.time_data)

        if self.mode == "ac":
            self.meas_files = self.match_files(
                self.gen_files(self.meas_dict),
                self.meas_dict,
            )
            logger.debug(
                f"Found {len(self.meas_files)} in folder {self.data_path}."
            )
            # gas measurement dataframe
            self.data = self.read_meas()
            # self.data = ifdb_read(self.ifdb_dict)
            # measurement times dataframe
            self.time_data = self.mk_cham_cycle()
            self.merged = self.merge_chamber_ts()

            self.w_fltr_tuple = mk_fltr_tuple(
                self.time_data, close="start_time", open="end_time"
            )
            self.fltr_tuple = mk_fltr_tuple(self.time_data)

    def read_ini(self):
        # config = cfgparser.ConfigParser(self.env_vars, allow_no_value=True)
        config = cfgparser.ConfigParser(allow_no_value=True)
        config.read(self.inifile)
        self.cfg = config
        self.defs = dict(config.items("defaults"))
        self.ifdb_dict = dict(config.items("influxDB"))
        self.mode = self.defs.get("mode")
        self.ifdb_dict = dict(config.items("influxDB"))
        self.cham_cycle = dict(config.items("chamber_start_stop"))
        self.chamber_dict = dict(config.items("measuring_chamber"))
        self.meas_dict = dict(config.items("measurement_data"))
        # this defines chamber close and open time for manual mode
        self.meas_t_dict = dict(config.items("manual_measurement_time_data"))
        # this defines chamber close and  open times for ac mode
        self.chamber_cycle_file = self.defs.get("chamber_cycle_file")

        self.data_path = self.meas_dict.get("path")
        self.data_ext = self.meas_dict.get("file_extension")
        self.file_ts_fmt = self.meas_dict.get("file_timestamp_format")
        self.scan_or_gen = int(self.meas_dict.get("scan_or_generate"))
        self.ch_ct = int(self.cham_cycle.get("start_of_measurement"))
        self.ch_ot = int(self.cham_cycle.get("end_of_measurement"))
        self.meas_et = int(self.cham_cycle.get("end_of_cycle"))
        self.meas_perc = int(self.defs.get("measurement_perc"))
        if self.meas_perc is None:
            self.meas_perc = 20
        else:
            self.meas_perc = int(self.meas_perc)
        self.chamber_h = float(self.chamber_dict.get("chamber_height"))
        self.use_def_t_p = int(self.defs.get("use_def_t_p"))
        self.def_press = float(self.defs.get("default_pressure"))
        self.def_temp = float(self.defs.get("default_temperature"))
        self.excel_path = self.defs.get("excel_directory")

    def mk_cham_cycle(self):
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
        for file in self.meas_files:
            df = pd.read_csv(self.chamber_cycle_file, names=["time", "chamber"])
            date = extract_date(self.file_ts_fmt, os.path.splitext(file)[0])
            df["date"] = date
            df["datetime"] = pd.to_datetime(
                df["date"].astype(str) + " " + df["time"].astype(str)
            )
            df["start_time"] = df["datetime"]
            df["close_time"] = df["datetime"] + pd.to_timedelta(
                self.ch_ct, unit="s"
            )
            df["open_time"] = df["datetime"] + pd.to_timedelta(
                self.ch_ot, unit="s"
            )
            df["end_time"] = df["datetime"] + pd.to_timedelta(
                self.meas_et, unit="s"
            )
            tmp.append(df)
        dfs = pd.concat(tmp)
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
        season_start = self.defs.get("season_start")
        ifdb_ts_format = self.ifdb_dict.get("influxdb_timestamp_format")

        if not self.ifdb_dict.get("url"):
            first_ts = datetime.datetime.strptime(season_start, ifdb_ts_format)
        else:
            logging.debug("Checking latest ts from DB.")
            first_ts = check_oldest_db_ts(self.ifdb_dict)
            logging.debug(f"Newest ts in db: {first_ts}")
        if first_ts is None:
            first_ts = datetime.datetime.strptime(season_start, ifdb_ts_format)
        return first_ts

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
        if self.file_ts_fmt == strftime_to_regex(self.file_ts_fmt):
            logger.info(
                "No strftime formatting in filename, returning current date"
            )
            return datetime.datetime.today()
        date = re.search(strftime_to_regex(self.file_ts_fmt), datestring).group(
            0
        )
        return datetime.datetime.strptime(date, self.file_ts_fmt)

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
            filename = current_date.strftime(
                config.get("file_timestamp_format")
            )
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
                logger.info(f"Read fail: {f.name}")
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
        logger.debug("Calculating ordinal times.")
        dfs["ordinal_date"] = pd.to_datetime(dfs["datetime"]).map(
            datetime.datetime.toordinal
        )
        dfs["ordinal_time"] = ordinal_timer(dfs[self.device.time_col].values)
        dfs["ordinal_datetime"] = dfs["ordinal_time"] + dfs["ordinal_date"]
        dfs.set_index("datetime", inplace=True)
        dfs.sort_index(inplace=True)
        dfs["month"] = dfs.index.month
        dfs["day"] = dfs.index.day
        dfs["doy"] = dfs.index.dayofyear
        dfs["checks"] = ""
        dfs["is_valid"] = True

        return dfs

    def read_man_meas_f(self):
        # NOTE: the format of the manual measurement is hardcoded
        tmp = []
        for f in self.meas_t_files:
            # with open(f) as f:
            #    first_line = f.read_line()
            # date = first_line
            df = self.timestamp_file.read_file(f)
            # NOTE: for the sake of consisteny, even though the manual
            # measurement doesn't really have a closing time, the
            # variable is named like this
            df["start_time"] = df["datetime"]
            df["close_time"] = df["datetime"] + pd.to_timedelta(
                self.ch_ct, unit="s"
            )
            df["open_time"] = df["datetime"] + pd.to_timedelta(
                self.ch_ot, unit="s"
            )
            df["end_time"] = df["datetime"] + pd.to_timedelta(
                self.meas_et, unit="s"
            )
            df["snowdepth"] = df["snowdepth"].fillna(0)
            df["ts_file"] = str(f.name)
            tmp.append(df)
        dfs = pd.concat(tmp)
        dfs.set_index("datetime", inplace=True)
        dfs["notes"] = dfs["notes"].fillna("")
        # dfs["validity"] = dfs["notes"].fillna(1)
        dfs.sort_index(inplace=True)
        return dfs

    def merge_chamber_ts(self):
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
        self.time_data.dropna(inplace=True, axis=1)
        if is_df_valid(self.data) and is_df_valid(self.time_data):
            self.data["temp_index"] = self.data.index
            df = pd.merge_asof(
                self.data,
                self.time_data,
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
                    logger.debug(
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
            if len(files) == 0:
                logger.debug(
                    f"No files found for aux_data {name_key}, skipped."
                )
                continue
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
            dfs = []
            for file in f.get("files"):
                argss = f.get("args")
                df = pd.read_csv(file, **argss)
                dfs.append(df)
            if len(dfs) == 0:
                logger.debug(
                    f"No data returned by files found for aux_data {f.get('name')}"
                )
                continue
            dfs = pd.concat(dfs)
            dfs.sort_index(inplace=True)
            f["df"] = dfs

    def merge_aux(self):
        for cfg in self.aux_cfgs:
            merge_met = cfg.get("merge_method")
            name = cfg.get("name")
            logger.info(f"merging {name} with {merge_met}")

            if merge_met == "timeid":
                merged = merge_by_dtx_and_id(self.merged, cfg)
                if merged is not None:
                    self.merged = merged
                    logger.debug(f"Merged {name} with {merge_met}")

            if merge_met == "id":
                merged = merge_by_id(self.merged, cfg)
                if merged is not None:
                    self.merged = merged
                    logger.debug(f"Merged {name} with {merge_met}")

            if merge_met == "time":
                merged = merge_by_dtx(self.merged, cfg)
                if merged is not None:
                    self.merged = merged
                    logger.debug(f"Merged {name} with {merge_met}")

        if len(self.aux_cfgs) == 0:
            self.merged = self.merged
        logger.info(
            f"Completed merging {len(self.aux_cfgs)} auxiliary datasets."
        )

    def check_valid(self):
        # NOTE: Should this be moved inside one of the existing loops?
        logger.debug("Checking validity")
        dfa = []
        for date in self.fltr_tuple:
            df = date_filter(self.merged, date)
            has_errors = df[self.device.diag_col].sum() != 0
            if "air_temperature" not in df.columns:
                no_air_temp = True
            else:
                no_air_temp = df["air_temperature"].isna().all()
            if "air_pressure" not in df.columns:
                no_air_pressure = True
            else:
                no_air_pressure = df["air_pressure"].isna().all()
            is_empty = df.empty
            if has_errors or no_air_temp or no_air_pressure or is_empty:
                checks = []
                if has_errors:
                    checks.append("has errors,")
                if no_air_temp:
                    checks.append("no air temp,")
                if no_air_pressure:
                    checks.append("no air pressure,")
                if is_empty:
                    checks.append("no data,")
                checks_str = "".join(checks)
                df.loc[:, "checks"] += checks_str
                df.loc[:, "is_valid"] = False

            dfa.append(df)
        dfa = pd.concat(dfa)
        self.merged = dfa

    def check_valid2(self, df):
        check_df = df
        has_errors = check_df["error_code"].sum() != 0
        no_air_temp = check_df["air_temperature"].isna().all()
        no_air_pressure = check_df["air_pressure"].isna().all()
        if has_errors or no_air_temp or no_air_pressure:
            checks = []
            if has_errors:
                checks.append("has errors,")
            if no_air_temp:
                checks.append("no air temp,")
            if no_air_pressure:
                checks.append("no air pressure,")
            checks_str = "".join(checks)
            check_df.loc[:, "checks"] += checks_str
            check_df.loc[:, "is_valid"] = False
        return check_df

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
        logger.debug(f"Calculating gas flux for {measurement_name}.")
        # TODO: Clean this mess up
        meas_name = measurement_name
        measurement_list = []
        # self.calc_tuple = [
        #     subs_from_fltr_tuple(t, self.meas_perc) for t in self.fltr_tuple
        # ]

        # NOTE: not using filter_tuple here will cause issuess in excel creation
        for date in self.fltr_tuple:
            mdf = date_filter(data, date).copy()
            if mdf.empty:
                measurement_list.append(mdf)
                logger.debug(f"Df empty at {date[0]}")
                continue
            if "has errors" in mdf.iloc[0]["checks"]:
                logger.debug(
                    f"Skipping flux calculation at {date[0]} because of diagnostic flags"
                )
                measurement_list.append(mdf)
                continue

            slope = calculate_slope(mdf, date, meas_name)
            if np.isnan(slope):
                logger.debug(f"slope returned as NaN at {date[0]}")
                continue
            mdf[f"{meas_name}_slope"] = slope

            pearsons = calculate_pearsons_r(mdf, date, meas_name)
            if np.isnan(pearsons):
                logger.debug(f"pearsonsR returned as NaN at {date[0]}")
                continue
            mdf[f"{meas_name}_pearsons_r"] = pearsons

            flux = calculate_gas_flux(
                mdf,
                meas_name,
                self.chamber_h,
                self.def_temp,
                self.def_press,
                self.use_def_t_p,
            )
            mdf[f"{meas_name}_flux"] = flux

            measurement_list.append(mdf)
        all_measurements_df = pd.concat(measurement_list)
        return all_measurements_df

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
            "ordinal_date",
            "ordinal_time",
            "ordinal_datetime",
        ]
        drop_cols = (
            measurement_cols
            + drop_cols
            + [col for col in self.merged.columns if "idx_cp" in col]
        )
        for date in self.fltr_tuple:
            dfa = date_filter(self.merged, date)
            dfList.append(dfa.iloc[:1])
        summary = pd.concat(dfList)
        summary.drop(labels=drop_cols, axis=1, inplace=True)
        # convert True/False to 1/0
        summary["is_valid"] = summary["is_valid"] * 1

        return summary

    def create_xlsx(self):
        # create a list of days for creating outputs
        daylist = []
        # initiate sparkline
        fig, ax = create_fig()
        times = self.fltr_tuple

        # tuples with that cover the whole measurement / cycle
        w_times = [add_to_fltr_tuple(time, self.meas_perc) for time in times]
        # tuples that cover the time where the calculations are from
        m_times = [subs_from_fltr_tuple(time, self.meas_perc) for time in times]

        gases = self.device.gas_cols
        logger.info(f"Creating {len(times) * len(gases)} sparklines.")
        logger.info(
            f"Time estimate: {convert_seconds(len(times) * (0.05 * len(gases)))}."
        )
        for i, date in enumerate(w_times):
            data = date_filter(self.w_merged, date).copy()
            day = date[0].date()
            if data.empty:
                daylist.append(day)
                continue
            # logger.debug(self.fltr_tuple[i][0])
            # logger.debug(self.ready_data.index[i])
            smask = self.ready_data.index == times[i][0]
            daylist.append(day)
            try:
                day = str(date[0].date())
                name = date[0].strftime("%Y%m%d%H%M%S")
                for gas in gases:
                    fig_root = "figs"
                    path = Path(f"{fig_root}/{gas}/{day}/")
                    plotname = f"{name}.png"
                    fig_path = str(path / plotname)
                    if not path.exists():
                        path.mkdir(parents=True)
                    self.ready_data.loc[smask, f"fig_dir_{gas}"] = fig_path
                    y = data[gas]
                    rects = create_rects(y, times[i], m_times[i])
                    create_sparkline(data[[gas]], fig_path, gas, fig, ax, rects)
            except Exception as e:
                logger.error(
                    f"Error when creating graph with matplotlib, "
                    f"most likely not enough memory. Error: {e}"
                )
        # converting list to dict and then to list again removes duplicate items
        daylist = list(dict.fromkeys(daylist))
        for day in daylist:
            data = self.ready_data[self.ready_data.index.date == day]
            if data.empty:
                continue
            sort = None
            create_excel(data, self.excel_path, sort)
        create_excel(self.ready_data, self.excel_path, sort, "all_data")

    # def ifdb_push():
    #     pass


class li7200:
    def __init__(self):
        self.usecols = ["DIAG", "DATE", "TIME", "CO2", "CH4"]
        self.dtypes = {
            "DIAG": "int",
            "TIME": "str",
            "H2O": "float",
            "CO2": "float",
            "CH4": "float",
        }
        self.skiprows = [0, 1, 2, 3, 4, 6]
        self.delimiter = "\t"
        self.date_col = "DATE"
        self.time_col = "TIME"
        self.datetime_col = None
        self.date_fmt = "%Y-%m-%d"
        self.time_fmt = "%H:%M:%S"
        self.diag_col = "DIAG"
        self.gas_cols = ["CO2", "CH4"]

    def read_file(self, f):
        df = pd.read_csv(
            f,
            skiprows=self.skiprows,
            delimiter=self.delimiter,
            usecols=self.usecols,
            dtype=self.dtypes,
        )
        df["datetime"] = pd.to_datetime(
            df[self.date_col] + df[self.time_col],
            format=self.date_fmt + self.time_fmt,
        )
        return df


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
        date = re.search("\d{6}", str(f.name))[0]
        df = pd.read_csv(
            f,
            skiprows=self.skiprows,
            names=self.names,
            dtype=self.dtypes,
        )
        df["snowdepth"] = df["snowdepth"].fillna(0)
        df["date"] = date
        df["datetime"] = df["date"] + df[self.time_col]
        df["datetime"] = pd.to_datetime(df["datetime"], format=self.dt_fmt)
        return df

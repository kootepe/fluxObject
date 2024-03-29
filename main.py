import os
import sys
from pathlib import Path
import pandas as pd
import functools
import configparser
import timeit
from pprint import pprint
from dotenv import dotenv_values


# modules from this repo
from tools.fluxer import (
    pusher,
    snowdepth_parser,
    calculated_data,
    merge_data,
    merge_data2,
    filterer,
    aux_data_reader,
    measurement_reader,
    chamber_cycle,
    file_finder,
    get_start_and_end_time,
    handle_eddypro,
    csv_reader,
    read_manual_measurement_timestamps,
    excel_creator,
    parse_aux_data,
)

from tools.logger import init_logger


def timer(func):
    """Decorator for printing execution time of function."""
    logger = init_logger()

    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        start = timeit.default_timer()
        value = func(*args, **kwargs)
        stop = timeit.default_timer()
        execution_time = stop - start
        logger.info(
            f"{func.__name__} executed in {convert_seconds(execution_time)}."
        )
        return value

    return wrapper_timer


@timer
def eddypro_push(inifile, env_vars):
    """
    Pipeline to handle reading eddypro .zip files, reading the flux
    from inside them and pushing that to influxdb

    args:
    ---
    inifile -- str
        path to the .ini file

    returns:
    ---

    """
    # initiate configparser
    config = configparser.ConfigParser(env_vars, allow_no_value=True)
    config.read(inifile)
    log_level = dict(config.items("defaults")).get("logging_level")
    logger = init_logger(log_level)

    # load parts of the .ini file as dictionaries
    defaults_dict = dict(config.items("defaults"))
    influxdb_dict = dict(config.items("influxDB"))
    measurement_dict = dict(config.items("measurement_data"))

    # get start and end times for data that is going to be
    # calculated
    timestamps_values = get_start_and_end_time(
        influxdb_dict, measurement_dict, defaults_dict
    )

    # generate / search files that contain the data that is going to be
    # calulated
    measurement_files = file_finder(
        measurement_dict,
        defaults_dict,
        timestamps_values.start_timestamp,
        timestamps_values.end_timestamp,
    )

    # read the data from eddypro zip files
    data = handle_eddypro(measurement_files.measurement_files, measurement_dict)

    # if url is defined in the .ini, attempt to push to influxdb
    if influxdb_dict.get("url"):
        pusher(data.data, influxdb_dict)


@timer
def csv_push(inifile, env_vars):
    """
    Reads .csv files and pushes them to influxdb

    args:
    ---
    inifile -- str
        path to the .ini file

    returns:
    ---

    """
    # initiate configparser
    config = configparser.ConfigParser(env_vars, allow_no_value=True)
    config.read(inifile)
    log_level = dict(config.items("defaults")).get("logging_level")
    logger = init_logger(log_level)

    # load parts of the .ini file as dictionaries
    defaults_dict = dict(config.items("defaults"))
    influxdb_dict = dict(config.items("influxDB"))
    measurement_dict = dict(config.items("measurement_data"))

    # get start and end times for data that is going to be
    # calculated
    timestamps_values = get_start_and_end_time(
        influxdb_dict, measurement_dict, defaults_dict
    )

    # generate / search files that contain the data that is going to be
    # calulated
    measurement_files = file_finder(
        measurement_dict,
        defaults_dict,
        timestamps_values.start_timestamp,
        timestamps_values.end_timestamp,
    )

    # read the data from .csv files
    data = csv_reader(measurement_files.measurement_files, measurement_dict)

    # if url is defined in the .ini, attempt to push to influxdb
    if influxdb_dict.get("url"):
        pusher(data.data, influxdb_dict)


@timer
def man_push(inifile, env_vars, test_mode=0):
    """
    Function to handle flux calculation and influx pushing

    args:
    ---
    inifile -- str
        path to the .ini file

    test_mode -- boolean
        defaults to 0, if set to 1 will return the calculated data for
        testing

    returns:
    ---
    ready_data -- class
        if test_mode is set 1, this will be returned
    """
    # initiate configparser
    config = configparser.ConfigParser(env_vars, allow_no_value=True)
    config.read(inifile)
    log_level = dict(config.items("defaults")).get("logging_level")
    logger = init_logger(log_level)

    # load parts of the .ini file as dictionaries
    defaults_dict = dict(config.items("defaults"))
    influxdb_dict = dict(config.items("influxDB"))
    chamber_start_stop_dict = dict(config.items("chamber_start_stop"))
    measuring_chamber_dict = dict(config.items("measuring_chamber"))
    measurement_dict = dict(config.items("measurement_data"))

    manual_measurement_time_data_dict = dict(
        config.items("manual_measurement_time_data")
    )

    # get start and end times for data that is going to be # calculated
    logger.debug("Running get_start_and_end_time")
    timestamps_values = get_start_and_end_time(
        influxdb_dict, measurement_dict, defaults_dict
    )

    # generate / search files that contain the gas measurement that is going
    # to be calulated
    logger.debug("Running file_finder for gas measurements")
    measurement_files = file_finder(
        measurement_dict,
        defaults_dict,
        timestamps_values.start_timestamp,
        timestamps_values.end_timestamp,
    )
    # generate / search files that contain the timestamps for the data
    # that is going to be calulated
    logger.debug("Running file_finder for measurement timestamps")
    measurement_times_files = file_finder(
        manual_measurement_time_data_dict,
        defaults_dict,
        timestamps_values.start_timestamp,
        timestamps_values.end_timestamp,
    )

    # read the gas measurement data
    logger.debug("Running measurement_reader")
    measurement_df = measurement_reader(
        measurement_dict, measurement_files.measurement_files
    )

    # read the manual measurement timestamps
    logger.debug("Running read_manual_measurement_timestamps")
    manual_measurement_time_df = read_manual_measurement_timestamps(
        manual_measurement_time_data_dict,
        measurement_times_files.measurement_files,
        chamber_start_stop_dict,
    )
    # list with three values, start_time, end_time, chamber_num, flux is
    # calculated from the data between start and end times
    logger.debug("Grabbed filter tuple")
    # filter_tuple = manual_measurement_time_df.filter_tuple
    filter_tuple = manual_measurement_time_df.whole_measurement_tuple

    # merge manual_measurement_timestamps to the gas measurement_dict
    # NOTE: is this needed?
    logger.debug("Merging gas measurement to manual measurement timestamps")
    merged_data = merge_data(
        measurement_df.measurement_df,
        manual_measurement_time_df.manual_measurement_df,
    )
    aux_cfgs = parse_aux_data(config)
    merged_data = merge_data2(merged_data.merged_data, aux_cfgs.aux_cfgs)

    logger.debug("Running calculated_data to calculate gas fluxes")
    ready_data = calculated_data(
        merged_data.merged_data,
        measuring_chamber_dict,
        filter_tuple,
        defaults_dict,
    )

    # if url is defined, try and push to db
    if influxdb_dict.get("url"):
        logger.debug("Attempting pushing to influx")
        pusher(ready_data.upload_ready_data, influxdb_dict)

    # if create_excel is 1, create excel summaries
    if defaults_dict.get("create_excel") == "1":
        logger.info("Excel creation enabled, keep and eye on your memory")
        excel = excel_creator(
            merged_data.merged_data,
            ready_data.upload_ready_data,
            filter_tuple,
            defaults_dict.get("excel_directory"),
            defaults_dict.get("excel_sort"),
        )
    else:
        excel = None
        logger.info("Excel creation disabled in .ini, skipping")

    # if test_mode is 1, return ready_data class for testing purposes
    if test_mode == 1:
        return ready_data
    else:
        return measurement_df, manual_measurement_time_df, ready_data, excel


@timer
def ac_push(inifile, env_vars, test_mode=None):
    """
    Function to handle flux calculation and influx pushing

    args:
    ---
    inifile -- str
        path to the .ini file
    test_mode -- boolean
        defaults to 0, if set to 1 will return the calculated data for
        testing

    returns:
    ---
    ready_data -- class
        if test_mode is set 1, this will be returned

    """
    # initiate configparser
    config = configparser.ConfigParser(env_vars, allow_no_value=True)
    config.read(inifile)

    log_level = dict(config.items("defaults")).get("logging_level")
    logger = init_logger(log_level)

    # load parts of the .ini file as dictionaries
    defaults_dict = dict(config.items("defaults"))
    measurement_time_dict = dict(config.items("chamber_start_stop"))
    influxdb_dict = dict(config.items("influxDB"))
    measuring_chamber_dict = dict(config.items("measuring_chamber"))
    measurement_dict = dict(config.items("measurement_data"))

    # get start and end times for data that is going to be
    # calculated
    timestamps_values = get_start_and_end_time(
        influxdb_dict, measurement_dict, defaults_dict
    )

    # generate / search files that contain the gas measurement that is going
    # to be calulated
    measurement_files = file_finder(
        measurement_dict,
        defaults_dict,
        timestamps_values.start_timestamp,
        timestamps_values.end_timestamp,
    )

    # create chamber open and close timestamps from template
    chamber_cycle_df = chamber_cycle(
        measurement_dict,
        defaults_dict,
        measurement_time_dict,
        measurement_files.measurement_files,
    )

    # read the gas measurement data
    measurement_df = measurement_reader(
        measurement_dict, measurement_files.measurement_files
    )

    # list with three values, start_time, end_time, chamber_num, flux is
    # calculated from the data between start and end times
    filter_tuple = chamber_cycle_df.filter_tuple

    merged_data = merge_data(
        measurement_df.measurement_df,
        chamber_cycle_df.chamber_cycle_df,
    )

    aux_cfgs = parse_aux_data(config)
    merged_data = merge_data2(merged_data.merged_data, aux_cfgs.aux_cfgs)

    ready_data = calculated_data(
        merged_data.merged_data,
        measuring_chamber_dict,
        filter_tuple,
        defaults_dict,
    )

    # if url is defined, try and push to db
    if influxdb_dict.get("url"):
        pusher(ready_data.upload_ready_data, influxdb_dict)

    # if create_excel is 1, create excel summaries
    if defaults_dict.get("create_excel") == "1":
        excel = excel_creator(
            merged_data.merged_data,
            ready_data.upload_ready_data,
            filter_tuple,
            defaults_dict.get("excel_directory"),
            defaults_dict.get("excel_sort"),
            defaults_dict,
        )
    else:
        excel = None
        logger.info("Excel creation disabled in .ini, skipping")

    # if test_mode is 1, return ready_data class for testing purposes
    if test_mode == 1:
        return ready_data
    else:
        return measurement_df, ready_data, excel


def list_inis(ini_path):
    """
    List files ending in .ini in given directory
    """
    folder = Path(ini_path)
    files = [file for file in folder.glob("*") if file.is_file()]
    filtered_files = [file for file in files if ".ini" in file.name]

    return filtered_files


@timer
def class_calc(inifile, env_vars):
    config = configparser.ConfigParser(env_vars, allow_no_value=True)
    config.read(inifile)

    defs = dict(config.items("defaults"))
    module = defs.get("module")
    class_name = defs.get("class_name")
    measurement_name = defs.get("measurement_name")

    if module:
        module = importlib.import_module(module)
    if class_name:
        instrument_class = getattr(module, class_name)
    if measurement_name:
        measurement_class = getattr(module, measurement_name)

    log_level = dict(config.items("defaults")).get("logging_level")
    logger = init_logger(log_level)
    # gas_flux_calculator(inifile, env_vars, instrument_class, measurement_class)
    # gas_flux_calculator(inifile, env_vars, instrument_class)
    gas_flux_calculator(inifile, env_vars)


def main(ini_path):
    ini_files = list_inis(ini_path)
    # get environment variables
    env_vars = os.environ
    # python does not like % signs, remove them from keys and values
    filtered_env = {
        key: value
        for key, value in env_vars.items()
        if "%" not in key and "%" not in value
    }

    # Update os.environ with the filtered dictionary
    env_vars.clear()
    env_vars.update(filtered_env)
    logger = init_logger()

    for inifile in ini_files:
        config = configparser.ConfigParser(env_vars, allow_no_value=True)
        config.read(inifile)
        active = config.getboolean("defaults", "active")
        if active:
            use_dotenv = dict(config.items("defaults")).get("use_dotenv")
            if use_dotenv == "1":
                # get environment variables from dotenv
                env_vars = dotenv_values()
                # pass env_vars to parser and reread .ini
                config = configparser.ConfigParser(
                    env_vars, allow_no_value=True
                )
                config.read(inifile)
            mode = dict(config.items("defaults")).get("mode")
            logger.info(f"Running {inifile}.")
            if mode == "ac":
                ac_push(inifile, env_vars)
            if mode == "man":
                man_push(inifile, env_vars)
            if mode == "csv":
                csv_push(inifile, env_vars)
            if mode == "eddypro":
                eddypro_push(inifile, env_vars)
        else:
            logger.info(f"Active set 0, skipped {inifile}")


if __name__ == "__main__":
    # NOTE: Need to use try except blocks in functions to prevent
    # crashes since we are now looping through files in a folder,
    # if one .ini crashes, all the ones after
    ini_path = sys.argv[1]
    # mode = sys.argv[2]
    main(ini_path)

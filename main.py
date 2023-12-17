import os
import sys
from pathlib import Path
import functools
import logging
import configparser
import timeit
from dotenv import dotenv_values


# modules from this repo
from tools.fluxer import (
    pusher,
    snowdepth_parser,
    calculated_data,
    merge_data,
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
)


def timer(func):
    """Decorator for printing execution time of function."""

    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        start = timeit.default_timer()
        value = func(*args, **kwargs)
        stop = timeit.default_timer()
        execution_time = stop - start
        logging.info(
            f"{func.__name__} executed in {str(round(execution_time,3))}s.")
        return value

    return wrapper_timer


@timer
def eddypro_push(inifile):
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
    data = handle_eddypro(
        measurement_files.measurement_files, measurement_dict)

    # if url is defined in the .ini, attempt to push to influxdb
    if influxdb_dict.get("url"):
        pusher(data.data, influxdb_dict)


@timer
def csv_push(inifile):
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
def man_push(inifile, test_mode=0):
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

    # load parts of the .ini file as dictionaries
    defaults_dict = dict(config.items("defaults"))
    influxdb_dict = dict(config.items("influxDB"))
    air_pressure_dict = dict(config.items("air_pressure_data"))
    air_temperature_dict = dict(config.items("air_temperature_data"))
    chamber_start_stop_dict = dict(config.items("chamber_start_stop"))
    measuring_chamber_dict = dict(config.items("measuring_chamber"))
    measurement_dict = dict(config.items("measurement_data"))
    get_temp_and_pressure_from_file = defaults_dict.get(
        "get_temp_and_pressure_from_file"
    )
    manual_measurement_time_data_dict = dict(
        config.items("manual_measurement_time_data")
    )

    # get start and end times for data that is going to be # calculated
    logging.debug("Running get_start_and_end_time")
    timestamps_values = get_start_and_end_time(
        influxdb_dict, measurement_dict, defaults_dict
    )

    # generate / search files that contain the gas measurement that is going
    # to be calulated
    logging.debug("Running file_finder for gas measurements")
    measurement_files = file_finder(
        measurement_dict,
        defaults_dict,
        timestamps_values.start_timestamp,
        timestamps_values.end_timestamp,
    )
    # generate / search files that contain the timestamps for the data
    # that is going to be calulated
    logging.debug("Running file_finder for measurement timestamps")
    measurement_times_files = file_finder(
        manual_measurement_time_data_dict,
        defaults_dict,
        timestamps_values.start_timestamp,
        timestamps_values.end_timestamp,
    )

    # if get_temp_and_pressure_from_file is defined, generate filenames
    # for the air and pressure data
    if get_temp_and_pressure_from_file == "1":
        logging.debug(
            "Running file_finder for air_pressure and air_temperature")
        air_pressure_files = file_finder(
            air_pressure_dict,
            defaults_dict,
            timestamps_values.start_timestamp,
            timestamps_values.end_timestamp,
        )
        air_temperature_files = file_finder(
            air_temperature_dict,
            defaults_dict,
            timestamps_values.start_timestamp,
            timestamps_values.end_timestamp,
        )
    else:
        air_pressure_files = None
        air_temperature_files = None
        logging.debug("Skipped finding air_temp and and air_pressure files")

    # read the gas measurement data
    logging.debug("Running measurement_reader")
    measurement_df = measurement_reader(
        measurement_dict, measurement_files.measurement_files
    )

    # read the manual measurement timestamps
    logging.debug("Running read_manual_measurement_timestamps")
    manual_measurement_time_df = read_manual_measurement_timestamps(
        manual_measurement_time_data_dict,
        measurement_times_files.measurement_files,
        chamber_start_stop_dict,
    )

    # read air_temp and air_pressure if the filenames were generated
    if air_pressure_files is not None and air_temperature_files is not None:
        logging.debug("Reading air_temp and air_pressure files")
        air_temperature_df = aux_data_reader(
            air_temperature_dict, air_temperature_files.measurement_files
        )

        air_pressure_df = aux_data_reader(
            air_pressure_dict, air_pressure_files.measurement_files
        )
    else:
        logging.debug("Skipped reading air_temp and air_pressure files")
        air_temperature_df = None
        air_pressure_df = None

    # list with three values, start_time, end_time, chamber_num, flux is
    # calculated from the data between start and end times
    logging.debug("Grabbed filter tuple")
    filter_tuple = manual_measurement_time_df.filter_tuple

    # filter the measured gas flux
    logging.debug("Filtering gas measurements")
    filtered_measurement = filterer(
        filter_tuple, measurement_df.measurement_df)

    # same list as before but the timestamps with no data or invalid
    # data dropped
    logging.debug("Grabbed filter_tuple cleaned up filter_tuple")
    filter_tuple = filtered_measurement.clean_filter_tuple

    # BUG: air_pressure data is at 10min interval, and it's filtered
    # with measurement_timestamps which are only have 3min length,
    # meaning there's a high chance that these measurements will be
    # dropped
    # if air_pressure_df is not None and air_temperature_df is not None:
    #     air_temperature_df = filterer(
    #         filter_tuple, air_temperature_df.aux_data_df)
    #     air_pressure_df = filterer(filter_tuple, air_pressure_df.aux_data_df)
    # else:
    #     air_temperature_df = None
    #     air_pressure_df = None

    # merge manual_measurement_timestamps to the gas measurement_dict
    # NOTE: is this needed?
    logging.debug("Merging gas measurement to manual measurement timestamps")
    merged_data = merge_data(
        filtered_measurement.filtered_data,
        manual_measurement_time_df.manual_measurement_df,
    )
    # merge air_pressure and air_temp data to measurement_data
    if air_pressure_df is not None and air_temperature_df is not None:
        logging.debug("Merging air_temp and air_pressure to gas measurement")
        merged_data = merge_data(
            merged_data.merged_data, air_temperature_df.aux_data_df
        )
        merged_data = merge_data(
            merged_data.merged_data, air_pressure_df.aux_data_df)
    else:
        logging.debug(
            "No air_temp or air_pressure measurement, "
            "skipped merging to gas measurement")

    logging.debug("Running calculated_data to calculate gas fluxes")
    ready_data = calculated_data(
        merged_data.merged_data,
        measuring_chamber_dict,
        filter_tuple, defaults_dict
    )

    # if url is defined, try and push to db
    if influxdb_dict.get("url"):
        logging.debug("Attempting pushing to influx")
        pusher(ready_data.upload_ready_data, influxdb_dict)

    # if create_excel is 1, create excel summaries
    if defaults_dict.get("create_excel") == "1":
        logging.info("Excel creation enabled, keep and eye on your memory")
        excel_creator(
            merged_data.merged_data,
            ready_data.upload_ready_data,
            filter_tuple,
            defaults_dict.get("excel_directory"),
        )
    else:
        logging.info("Excel creation disabled in .ini, skipping")

    # if test_mode is 1, return ready_data class for testing purposes
    if test_mode == 1:
        return ready_data


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

    # load parts of the .ini file as dictionaries
    defaults_dict = dict(config.items("defaults"))
    measurement_time_dict = dict(config.items("chamber_start_stop"))
    influxdb_dict = dict(config.items("influxDB"))
    air_pressure_dict = dict(config.items("air_pressure_data"))
    air_temperature_dict = dict(config.items("air_temperature_data"))
    measuring_chamber_dict = dict(config.items("measuring_chamber"))
    measurement_dict = dict(config.items("measurement_data"))
    get_temp_and_pressure_from_file = defaults_dict.get(
        "get_temp_and_pressure_from_file"
    )

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

    # if get_temp_and_pressure_from_file is defined, generate filenames
    # for the air and pressure data
    if get_temp_and_pressure_from_file == "1":
        air_pressure_files = file_finder(
            air_pressure_dict,
            defaults_dict,
            timestamps_values.start_timestamp,
            timestamps_values.end_timestamp,
        )
        air_temperature_files = file_finder(
            air_temperature_dict,
            defaults_dict,
            timestamps_values.start_timestamp,
            timestamps_values.end_timestamp,
        )
    else:
        air_pressure_files = None
        air_temperature_files = None

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

    # read air_temp and air_pressure if the filenames were generated
    if air_pressure_files is not None and air_temperature_files is not None:
        air_temperature_df = aux_data_reader(
            air_temperature_dict, air_temperature_files.measurement_files
        )

        air_pressure_df = aux_data_reader(
            air_pressure_dict, air_pressure_files.measurement_files
        )
    else:
        air_temperature_df = None
        air_pressure_df = None

    # list with three values, start_time, end_time, chamber_num, flux is
    # calculated from the data between start and end times
    filter_tuple = chamber_cycle_df.filter_tuple

    # filter the measured gas flux
    filtered_measurement = filterer(
        filter_tuple, measurement_df.measurement_df)

    # same list as before but the timestamps with no data or invalid
    # data dropped
    filter_tuple = filtered_measurement.clean_filter_tuple

    # BUG: air_pressure data is at 10min interval, and it's filtered
    # with measurement_timestamps which are only have 3min length,
    # meaning there's a high chance that these measurements will be
    # dropped
    # if air_pressure_df is not None and air_temperature_df is not None:
    #     air_temperature_df = filterer(
    #         filter_tuple, air_temperature_df.aux_data_df)
    #     air_pressure_df = filterer(filter_tuple, air_pressure_df.aux_data_df)

    # read the snowdepth measurement into a dataframe
    snowdepth_df = snowdepth_parser(
        defaults_dict.get("snowdepth_measurement"),
    )

    # if set_snow_to_zero is 1, there's no snowdepth measurement and
    # snowdepth will be set 0
    set_snow_to_zero = snowdepth_df.set_to_zero
    if set_snow_to_zero is True:
        filtered_measurement.filtered_data["snowdepth"] = 0

    # merge air_pressure and air_temp data to measurement_data
    if air_pressure_df is not None and air_temperature_df is not None:
        data_with_temp = merge_data(
            filtered_measurement.filtered_data,
            air_temperature_df.aux_data_df
        )
        data_with_temp_pressure = merge_data(
            data_with_temp.merged_data, air_pressure_df.aux_data_df
        )
        if set_snow_to_zero is False:
            data_with_temp_pressure = merge_data(
                data_with_temp_pressure.merged_data,
                snowdepth_df.snowdepth_df,
                True
            )
        merged_data = data_with_temp_pressure

    if air_pressure_df is not None and air_temperature_df is not None:
        ready_data = calculated_data(
            merged_data.merged_data,
            measuring_chamber_dict,
            filter_tuple,
            defaults_dict
        )
    else:
        ready_data = calculated_data(
            filtered_measurement.filtered_data,
            measuring_chamber_dict,
            filter_tuple,
            defaults_dict)

    # grapher(
    #     date_filter_list(
    #         measurement_df.measurement_df, chamber_cycle_df.whole_cycle_tuple
    #     ),
    #     chamber_cycle_df.whole_cycle_tuple,
    #     ready_data.slope_times_list,
    #     filter_tuple,
    # )

    # tagged_measurement = measurement_tagger(
    #     date_filter_list(
    #         measurement_df.measurement_df, chamber_cycle_df.whole_cycle_tuple
    #     ),
    #     chamber_cycle_df.whole_cycle_tuple,
    #     ready_data.slope_times_list,
    #     filter_tuple,
    # )

    # if url is defined, try and push to db
    if influxdb_dict.get("url"):
        pusher(ready_data.upload_ready_data, influxdb_dict)

    # if create_excel is 1, create excel summaries
    if defaults_dict.get("create_excel") == "1":
        excel_creator(
            merged_data.merged_data,
            ready_data.upload_ready_data,
            filter_tuple,
            defaults_dict.get("excel_directory"),
        )
    else:
        logging.info("Excel creation disabled in .ini, skipping")

    # if test_mode is 1, return ready_data class for testing purposes
    if test_mode:
        return ready_data


def list_inis(ini_path):
    """
    List files ending in .ini in given directory
    """
    folder = Path(ini_path)
    files = [file for file in folder.glob("*") if file.is_file()]
    filtered_files = [file for file in files if ".ini" in file.name]

    return filtered_files


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

    for inifile in ini_files:
        file_name = Path(inifile).name

        # HACK: this logging is a hack
        filehandler = logging.FileHandler(file_name, "a")
        formatter = logging.Formatter(
            f"%(asctime)s.%(msecs)03d %(levelname)s {file_name}:\t" "%(message)s"
        )
        date_format = "%Y-%m-%d %H:%M:%S"
        filehandler.setFormatter(formatter)
        logger = logging.getLogger()
        for hdlr in logger.handlers[:]:
            if isinstance(hdlr, logging.FileHandler):
                logger.removeHandler(hdlr)
        logger.addHandler(filehandler)
        # TODO: IMPLEMENT GETTING LOGGING LEVEL FROM .INI
        logger.setLevel(logging.INFO)
        logger = logging.getLogger()
        logging.Formatter.default_msec_format = "%s.%03d"
        logging.basicConfig(
            format=f"%(asctime)s %(levelname)s {file_name}:\t" "%(message)s",
            force=True,
        )

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
                    env_vars, allow_no_value=True)
                config.read(inifile)
            mode = dict(config.items("defaults")).get("mode")
            logger.info(f"Running {inifile}.")
            if mode == "ac":
                ac_push(inifile, env_vars)
            if mode == "man":
                man_push(inifile)
            if mode == "csv":
                csv_push(inifile)
            if mode == "eddypro":
                eddypro_push(inifile)
            logger.removeHandler(file_name)
        else:
            logging.info(f"Active set  0, skipped {inifile}")
            logger.removeHandler(file_name)


if __name__ == "__main__":
    # NOTE: Need to use try except blocks in functions to prevent
    # crashes since we are now looping through files in a folder,
    # if one .ini crashes, all the ones after
    ini_path = sys.argv[1]
    # mode = sys.argv[2]
    main(ini_path)

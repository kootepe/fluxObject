import sys
import functools
import logging
import configparser
import timeit


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
    excel_creator
)


def timer(func):
    """Decorator for printing execution time of function."""

    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        start = timeit.default_timer()
        value = func(*args, **kwargs)
        stop = timeit.default_timer()
        execution_time = stop - start
        logging.info(f"{func.__name__} executed in {str(round(execution_time,3))}s.")
        return value

    return wrapper_timer


@timer
def eddypro_push(inifile):
    """
    Pipeline to handle reading eddypro .zip files, reading the flux
    from inside them and pusing that to influxdb

    args:
    ---
    inifile -- str
        path to the .ini file

    returns:
    ---

    """
    config = configparser.ConfigParser()
    config.read(inifile)

    defaults_dict = dict(config.items("defaults"))
    influxdb_dict = dict(config.items("influxDB"))
    measurement_dict = dict(config.items("measurement_data"))

    timestamps_values = get_start_and_end_time(
        influxdb_dict, measurement_dict, defaults_dict.get("season_start")
    )

    measurement_files = file_finder(
        measurement_dict,
        defaults_dict.get("file_timestep"),
        timestamps_values.start_timestamp,
        timestamps_values.end_timestamp,
    )
    data = handle_eddypro(measurement_files.measurement_files, measurement_dict)
    data.data.to_csv("./eddypro_outa.csv")
    # pusher(data.data, influxdb_dict)


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
    config = configparser.ConfigParser()
    config.read(inifile)

    defaults_dict = dict(config.items("defaults"))
    influxdb_dict = dict(config.items("influxDB"))
    measurement_dict = dict(config.items("measurement_data"))

    timestamps_values = get_start_and_end_time(
        influxdb_dict, measurement_dict, defaults_dict.get("season_start")
    )

    measurement_files = file_finder(
        measurement_dict,
        defaults_dict.get("file_timestep"),
        timestamps_values.start_timestamp,
        timestamps_values.end_timestamp,
    )
    data = csv_reader(measurement_files.measurement_files, measurement_dict)
    print(data.data)
    ##pusher(data.data, influxdb_dict)


@timer
def man_push(inifile, test_mode=0):
    """
    Function to handle flux calculation and influx pushing

    args:
    ---
    inifile -- str
        path to the .ini file

    returns:
    ---

    """
    config = configparser.ConfigParser()
    config.read(inifile)

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

    timestamps_values = get_start_and_end_time(
        influxdb_dict, measurement_dict, defaults_dict.get("season_start")
    )

    measurement_files = file_finder(
        measurement_dict,
        defaults_dict.get("file_timestep"),
        timestamps_values.start_timestamp,
        timestamps_values.end_timestamp,
    )
    measurement_times_files = file_finder(
        manual_measurement_time_data_dict,
        defaults_dict.get("file_timestep"),
        timestamps_values.start_timestamp,
        timestamps_values.end_timestamp,
    )

    if get_temp_and_pressure_from_file == "1":
        air_pressure_files = file_finder(
            air_pressure_dict,
            defaults_dict.get("file_timestep"),
            timestamps_values.start_timestamp,
            timestamps_values.end_timestamp,
        )
        air_temperature_files = file_finder(
            air_temperature_dict,
            defaults_dict.get("file_timestep"),
            timestamps_values.start_timestamp,
            timestamps_values.end_timestamp,
        )
    else:
        air_pressure_files = None
        air_temperature_files = None

    # should filter tuple just be generated here?
    measurement_df = measurement_reader(
        measurement_dict, measurement_files.measurement_files
    )

    manual_measurement_time_df = read_manual_measurement_timestamps(
        manual_measurement_time_data_dict,
        measurement_times_files.measurement_files,
        chamber_start_stop_dict,
    )

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
    filter_tuple = manual_measurement_time_df.filter_tuple

    filtered_measurement = filterer(filter_tuple, measurement_df.measurement_df)

    # same list as before but the timestamps with no data or invalid
    # data dropped
    filter_tuple = filtered_measurement.clean_filter_tuple

    if air_pressure_df is not None and air_temperature_df is not None:
        air_temperature_df = filterer(filter_tuple, air_temperature_df.aux_data_df)
        air_pressure_df = filterer(filter_tuple, air_pressure_df.aux_data_df)
    else:
        air_temperature_df = None
        air_pressure_df = None

    if air_pressure_df is not None and air_temperature_df is not None:
        merged_data = merge_data(
            filtered_measurement.filtered_data, air_temperature_df.filtered_data
        )
        merged_data = merge_data(merged_data.merged_data, air_pressure_df.filtered_data)
    else:
        merged_data = merge_data(
            filtered_measurement.filtered_data,
            manual_measurement_time_df.manual_measurement_df,
            True,
        )

    if merged_data is None:
        # filtered_measurement.filtered_data["snowdepth"] = 0
        ready_data = calculated_data(
            filtered_measurement.filtered_data,
            measuring_chamber_dict,
            filter_tuple,
            defaults_dict,
        )

    else:
        ready_data = calculated_data(
            merged_data.merged_data, measuring_chamber_dict, filter_tuple, defaults_dict
        )
    if influxdb_dict.get("influxdb_url") is not None:
        pusher(ready_data.upload_ready_data, influxdb_dict)

    if test_mode == 1:
        return ready_data


@timer
def ac_push(inifile, test_mode=None):
    """
    Function to handle flux calculation and influx pushing

    args:
    ---
    inifile -- str
        path to the .ini file

    returns:
    ---

    """
    config = configparser.ConfigParser()
    config.read(inifile)

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

    timestamps_values = get_start_and_end_time(
        influxdb_dict, measurement_dict, defaults_dict.get("season_start")
    )

    measurement_files = file_finder(
        measurement_dict,
        defaults_dict.get("file_timestep"),
        timestamps_values.start_timestamp,
        timestamps_values.end_timestamp,
    )

    if get_temp_and_pressure_from_file == "1":
        air_pressure_files = file_finder(
            air_pressure_dict,
            defaults_dict.get("file_timestep"),
            timestamps_values.start_timestamp,
            timestamps_values.end_timestamp,
        )
        air_temperature_files = file_finder(
            air_temperature_dict,
            defaults_dict.get("file_timestep"),
            timestamps_values.start_timestamp,
            timestamps_values.end_timestamp,
        )
    else:
        air_pressure_files = None
        air_temperature_files = None

    chamber_cycle_df = chamber_cycle(
        measurement_dict,
        defaults_dict,
        measurement_time_dict,
        measurement_files.measurement_files,
    )

    # should filter tuple just be generated here?
    measurement_df = measurement_reader(
        measurement_dict, measurement_files.measurement_files
    )

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

    filtered_measurement = filterer(filter_tuple, measurement_df.measurement_df)

    # same list as before but the timestamps with no data or invalid
    # data dropped
    filter_tuple = filtered_measurement.clean_filter_tuple

    if air_pressure_df is not None and air_temperature_df is not None:
        air_temperature_df = filterer(filter_tuple, air_temperature_df.aux_data_df)
        air_pressure_df = filterer(filter_tuple, air_pressure_df.aux_data_df)

    snowdepth_df = snowdepth_parser(
        defaults_dict.get("snowdepth_measurement"),
    )
    set_snow_to_zero = snowdepth_df.set_to_zero

    data_with_temp = False
    data_with_temp_pressure = False
    data_with_temp_pressure_snow = False
    if air_pressure_df is not None and air_temperature_df is not None:
        data_with_temp = merge_data(
            filtered_measurement.filtered_data, air_temperature_df.filtered_data
        )
        data_with_temp_pressure = merge_data(
            data_with_temp.merged_data, air_pressure_df.filtered_data
        )
        data_with_temp_pressure_snow = merge_data(
            data_with_temp_pressure.merged_data, snowdepth_df.snowdepth_df, True
        )
    else:
        data_with_snow = merge_data(
            filtered_measurement.filtered_data,
            snowdepth_df.snowdepth_df,
            True,
        )

    if data_with_temp_pressure_snow is True:
        merged_data = data_with_temp_pressure_snow
    else:
        merged_data = data_with_snow


    if set_snow_to_zero is True:
        merged_data.merged_data["snowdepth"] = 0
    ready_data = calculated_data(
        merged_data.merged_data, measuring_chamber_dict, filter_tuple, defaults_dict
    )

    # ready_data.upload_ready_data.to_csv('./AC_data_2023.csv')
    # if there's no URL defined, skip pushing to influxdb
    if influxdb_dict.get("influxdb_url") is not None:
        pusher(ready_data.upload_ready_data, influxdb_dict)

    if test_mode:
        return ready_data


if __name__ == "__main__":
    inifile = sys.argv[1]
    mode = sys.argv[2]
    if mode == "ac":
        ac_push(inifile)
    if mode == "man":
        man_push(inifile)
    if mode == "csv":
        csv_push(inifile)
    if mode == "eddypro":
        eddypro_push(inifile)

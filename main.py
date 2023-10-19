import sys
import functools
import logging
import datetime
import configparser
import timeit


# modules from this repo
from calc.fluxer import pusher, snowdepth_parser, calculated_data, merge_data, filterer, aux_data_reader, measurement_reader, chamber_cycle, file_finder, get_start_and_end_time, handle_eddypro, csv_reader, read_manual_measurement_timestamps


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

    defaults_dict = dict(config.items('defaults'))
    influxdb_dict = dict(config.items('influxDB'))
    measurement_dict = dict(config.items('measurement_data'))

    timestamps_values = get_start_and_end_time(influxdb_dict,
                          measurement_dict,
                          defaults_dict.get('season_start')
                            )

    measurement_files = file_finder(measurement_dict,
                                   int(defaults_dict.get('file_timestep')),
                                   timestamps_values.start_timestamp,
                                   timestamps_values.end_timestamp
                                    )
    data = handle_eddypro(measurement_files.measurement_files, measurement_dict)
    print(data.data)
    #pusher(data.data, influxdb_dict)


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

    defaults_dict = dict(config.items('defaults'))
    influxdb_dict = dict(config.items('influxDB'))
    measurement_dict = dict(config.items('measurement_data'))

    timestamps_values = get_start_and_end_time(influxdb_dict,
                          measurement_dict,
                          defaults_dict.get('season_start')
                            )

    measurement_files = file_finder(measurement_dict,
                                   int(defaults_dict.get('file_timestep')),
                                   timestamps_values.start_timestamp,
                                   timestamps_values.end_timestamp
                                    )
    data = csv_reader(measurement_files.measurement_files, measurement_dict)
    print(data.data)
    ##pusher(data.data, influxdb_dict)

@timer
def man_push(inifile):
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

    defaults_dict = dict(config.items('defaults'))
    measurement_time_dict = dict(config.items('chamber_start_stop'))
    influxdb_dict = dict(config.items('influxDB'))
    air_pressure_dict = dict(config.items('air_pressure_data'))
    air_temperature_dict = dict(config.items('air_temperature_data'))
    measuring_chamber_dict = dict(config.items('measuring_chamber'))
    measurement_dict = dict(config.items('measurement_data'))
    get_temp_and_pressure_from_file = int(defaults_dict.get('get_temp_and_pressure_from_file'))
    manual_measurement_time_data_dict = dict(config.items('manual_measurement_time_data'))

    timestamps_values = get_start_and_end_time(influxdb_dict,
                          measurement_dict,
                          defaults_dict.get('season_start')
                            )

    measurement_files = file_finder(measurement_dict,
                                   int(defaults_dict.get('file_timestep')),
                                   timestamps_values.start_timestamp,
                                   timestamps_values.end_timestamp
                                    )
    measurement_times_files = file_finder(manual_measurement_time_data_dict,
                                          int(defaults_dict.get('file_timestep')),
                                          timestamps_values.start_timestamp,
                                          timestamps_values.end_timestamp)
    print(measurement_files.measurement_files)

    if get_temp_and_pressure_from_file == 1:
        air_pressure_files = file_finder(air_pressure_dict,
                                       int(defaults_dict.get('file_timestep')),
                                       timestamps_values.start_timestamp,
                                       timestamps_values.end_timestamp
                                         )
        air_temperature_files = file_finder(air_temperature_dict,
                                       int(defaults_dict.get('file_timestep')),
                                       timestamps_values.start_timestamp,
                                       timestamps_values.end_timestamp
                                         )

    manual_measurement_df = read_manual_measurement_timestamps(manual_measurement_time_data_dict,
                                                               measurement_times_files.measurement_files,
                                                               measurement_time_dict
                                                               )

    # should filter tuple just be generated here?
    measurement_df = measurement_reader(measurement_dict,
                                        measurement_files.measurement_files)

    if get_temp_and_pressure_from_file == 1:
        air_temperature_df = aux_data_reader(air_temperature_dict,
                                      air_pressure_files.measurement_files)

        air_pressure_df = aux_data_reader(air_pressure_dict,
                                      air_pressure_files.measurement_files)

    # list with three values, start_time, end_time, chamber_num, flux is
    # calculated from the data between start and end times
    filter_tuple = manual_measurement_df.filter_tuple

    filtered_measurement = filterer(filter_tuple,
                                    measurement_df.measurement_df)

    # same list as before but the timestamps with no data or invalid
    # data dropped
    filter_tuple = filtered_measurement.clean_filter_tuple

    if get_temp_and_pressure_from_file == 1:
        air_temperature_df = filterer(filter_tuple,
                                      air_temperature_df.aux_data_df)
        air_pressure_df = filterer(filter_tuple,
                                      air_pressure_df.aux_data_df)

    snowdepth_df = snowdepth_parser(defaults_dict.get('snowdepth_measurement'),)

    if get_temp_and_pressure_from_file == 1:
        merged_data = merge_data(filtered_measurement.filtered_data,
                                 air_temperature_df.filtered_data)
        merged_data = merge_data(merged_data.merged_data,
                                 air_pressure_df.filtered_data)
        merged_data = merge_data(merged_data.merged_data,
                                 snowdepth_df.snowdepth_df)
    else:
        merged_data = merge_data(filtered_measurement.filtered_data,
                                 snowdepth_df.snowdepth_df)

    merged_data.merged_data['snowdepth'] = 0

    ready_data = calculated_data(merged_data.merged_data,
                                      measuring_chamber_dict, filter_tuple,
                                      get_temp_and_pressure_from_file,
                                 float(defaults_dict.get('default_pressure')),
                                 float(defaults_dict.get('default_temperature'))
                                      )
    (ready_data.upload_ready_data)

@timer
def ac_push(inifile):
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

    defaults_dict = dict(config.items('defaults'))
    measurement_time_dict = dict(config.items('chamber_start_stop'))
    influxdb_dict = dict(config.items('influxDB'))
    air_pressure_dict = dict(config.items('air_pressure_data'))
    air_temperature_dict = dict(config.items('air_temperature_data'))
    measuring_chamber_dict = dict(config.items('measuring_chamber'))
    measurement_dict = dict(config.items('measurement_data'))
    get_temp_and_pressure_from_file = int(defaults_dict.get('get_temp_and_pressure_from_file'))

    timestamps_values = get_start_and_end_time(influxdb_dict,
                          measurement_dict,
                          defaults_dict.get('season_start')
                            )

    measurement_files = file_finder(measurement_dict,
                                   int(defaults_dict.get('file_timestep')),
                                   timestamps_values.start_timestamp,
                                   timestamps_values.end_timestamp
                                    )

    if get_temp_and_pressure_from_file == 1:
        air_pressure_files = file_finder(air_pressure_dict,
                                       int(defaults_dict.get('file_timestep')),
                                       timestamps_values.start_timestamp,
                                       timestamps_values.end_timestamp
                                         )
        air_temperature_files = file_finder(air_temperature_dict,
                                       int(defaults_dict.get('file_timestep')),
                                       timestamps_values.start_timestamp,
                                       timestamps_values.end_timestamp
                                         )

    chamber_cycle_df = chamber_cycle(measurement_dict.get('file_timestamp_format'),
                                     defaults_dict.get('chamber_cycle_file'),
                                     int(measurement_time_dict.get('start_of_measurement')),
                                     int(measurement_time_dict.get('end_of_measurement')),
                                     measurement_files.measurement_files
                                     )

    # should filter tuple just be generated here?
    measurement_df = measurement_reader(measurement_dict,
                                        measurement_files.measurement_files)

    if get_temp_and_pressure_from_file == 1:
        air_temperature_df = aux_data_reader(air_temperature_dict,
                                      air_pressure_files.measurement_files)

        air_pressure_df = aux_data_reader(air_pressure_dict,
                                      air_pressure_files.measurement_files)

    # list with three values, start_time, end_time, chamber_num, flux is
    # calculated from the data between start and end times
    filter_tuple = chamber_cycle_df.filter_tuple

    filtered_measurement = filterer(filter_tuple,
                                    measurement_df.measurement_df)

    # same list as before but the timestamps with no data or invalid
    # data dropped
    filter_tuple = filtered_measurement.clean_filter_tuple

    if get_temp_and_pressure_from_file == 1:
        air_temperature_df = filterer(filter_tuple,
                                      air_temperature_df.aux_data_df)
        air_pressure_df = filterer(filter_tuple,
                                      air_pressure_df.aux_data_df)

    snowdepth_df = snowdepth_parser(defaults_dict.get('snowdepth_measurement'),)

    if get_temp_and_pressure_from_file == 1:
        merged_data = merge_data(filtered_measurement.filtered_data,
                                 air_temperature_df.filtered_data)
        merged_data = merge_data(merged_data.merged_data,
                                 air_pressure_df.filtered_data)
        merged_data = merge_data(merged_data.merged_data,
                                 snowdepth_df.snowdepth_df)
    else:
        merged_data = merge_data(filtered_measurement.filtered_data,
                                 snowdepth_df.snowdepth_df)

    merged_data.merged_data['snowdepth'] = 0

    ready_data = calculated_data(merged_data.merged_data,
                                      measuring_chamber_dict, filter_tuple,
                                      get_temp_and_pressure_from_file,
                                 float(defaults_dict.get('default_pressure')),
                                 float(defaults_dict.get('default_temperature'))
                                      )
    ready_data.upload_ready_data.to_csv('./AC_data_2023.csv')
    #pusher(ready_data.upload_ready_data, influxdb_dict)


if __name__=="__main__":
    inifile = sys.argv[1]
    mode = sys.argv[2]
    if mode == 'ac':
        ac_push(inifile)
    if mode == 'man':
        man_push(inifile)
    if mode == 'csv':
        csv_push(inifile)
    if mode == 'eddypro':
        eddypro_push(inifile)

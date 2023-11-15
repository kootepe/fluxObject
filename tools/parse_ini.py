import configparser
import logging

logging = logging.getLogger("__main__")


def parse_ini_to_dicts(ini_file):
    config = configparser.ConfigParser()
    config.read(ini_file)

    defaults_dict = dict(config.items('defaults'))
    measuring_chamber_dict = dict(config.items('measuring_chamber'))
    measurement_time_dict = dict(config.items('chamber_start_stop'))
    measurement_dict = dict(config.items('measurement_data'))
    air_pressure_dict = dict(config.items('air_pressure_data'))
    air_temperature_dict = dict(config.items('air_temperature_data'))
    influxdb_dict = dict(config.items('influxDB'))

    return influxdb_dict, measurement_dict, defaults_dict,
    measuring_chamber_dict, measurement_time_dict, measurement_dict,
    air_pressure_dict, air_temperature_dict


def parse_ini_dict(ini_dict):
    pass

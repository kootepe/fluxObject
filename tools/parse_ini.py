import configparser
import logging
from tools.aux_cfg_parser import (
    parse_file_cfg,
    parse_db_cfg,
    parse_aux_cfg,
)

logger = logging.getLogger("defaultLogger")


class iniHandler:
    def __init__(self, ini_path, env_vars):
        self.ini_path = ini_path
        self.env_vars = env_vars
        self.cfg = configparser.ConfigParser()
        self.load_ini_file()
        self.get_dicts()
        self.get_defs()
        self.get_measurement()
        self.get_dicts()
        self.get_chamber_settings()
        self.mode = self.defaults.get("mode")
        self.use_defaults = self.defaults.get("use_def_t_p")
        self.use_ini_dates = self.defaults.get("use_ini_dates")
        self.aux_cfgs = parse_aux_cfg(self.cfg)

    def load_ini_file(self):
        self.cfg.read(self.ini_path)

    def get(self, section, key):
        return self.cfg.get(section, key)

    def get_dicts(self):
        self.defaults = dict(self.cfg.items("defaults"))
        self.measuring_chamber_dict = dict(self.cfg.items("measuring_chamber"))
        self.chamber_cycle = dict(self.cfg.items("chamber_start_stop"))
        self.measurement_dict = dict(self.cfg.items("measurement_data"))
        self.measurement_time_dict = dict(
            self.cfg.items("manual_measurement_time_data")
        )
        self.influxdb_dict = dict(self.cfg.items("influxDB"))
        self.aux_dicts = self.get_aux_dicts()

    def get_defaults(self):
        return dict(self.cfg.items("defaults"))

    def get_aux_dicts(self):
        cfg_names = [s for s in self.cfg.sections() if "aux_data_" in s]
        aux_cfg_sects = [dict(self.cfg.items(c)) for c in cfg_names]
        return aux_cfg_sects

    def get_defs(self):
        self.ini_name = self.defaults.get("name")
        self.chamber_cycle_file = self.defaults.get("chamber_cycle_file")
        self.use_ini_dates = self.defaults.get("use_ini_dates")
        self.mode = self.defaults.get("mode")
        self.use_defaults = int(self.defaults.get("use_def_t_p"))
        self.def_press = float(self.defaults.get("default_pressure"))
        self.def_temp = float(self.defaults.get("default_temperature"))
        self.excel_path = self.defaults.get("excel_directory")
        mprc = self.defaults.get("measurement_perc")
        if mprc:
            self.meas_perc = float(mprc)
        else:
            self.meas_perc = 20
        self.s_ts = self.defaults.get("start_ts")
        self.e_ts = self.defaults.get("end_ts")

    def get_measurement(self):
        self.data_path = self.measurement_dict.get("path")
        self.data_ext = self.measurement_dict.get("file_extension")
        self.file_ts_fmt = self.measurement_dict.get("file_timestamp_format")
        self.scan_or_gen = int(self.measurement_dict.get("scan_or_generate"))

    def get_chamber_settings(self):
        self.ch_ct = int(self.chamber_cycle.get("start_of_measurement"))
        self.ch_ot = int(self.chamber_cycle.get("end_of_measurement"))
        self.meas_et = int(self.chamber_cycle.get("end_of_cycle"))
        self.chamber_h = float(self.measuring_chamber_dict.get("chamber_height"))


def parse_ini_to_dicts(ini_file):
    config = configparser.ConfigParser()
    config.read(ini_file)

    defaults_dict = dict(config.items("defaults"))
    measuring_chamber_dict = dict(config.items("measuring_chamber"))
    measurement_time_dict = dict(config.items("chamber_start_stop"))
    measurement_dict = dict(config.items("measurement_data"))
    air_pressure_dict = dict(config.items("air_pressure_data"))
    air_temperature_dict = dict(config.items("air_temperature_data"))
    influxdb_dict = dict(config.items("influxDB"))

    return (
        influxdb_dict,
        measurement_dict,
        defaults_dict,
    )
    (
        measuring_chamber_dict,
        measurement_time_dict,
        measurement_dict,
    )
    air_pressure_dict, air_temperature_dict

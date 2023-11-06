# test_with_unittest discover
from tools.fluxer import measurement_reader
from tools.fluxer import aux_data_reader
from tools.fluxer import pusher
from tools.time_funcs import ordinal_timer
import configparser
import numpy as np
import main

inifile = 'tests/test_ini.ini'
def config_read(inifile):
    config = configparser.ConfigParser()
    config.read(inifile)
    aux_dict = dict(config.items('air_pressure_data'))
    influx_dict = dict(config.items('influxDB'))
    meas_dict = dict(config.items('measurement_data'))
    return config, aux_dict, influx_dict, meas_dict
ini, aux_dict, influx_dict, meas_dict = config_read(inifile)


def test_ordinal_timer():
    time = "05:18:36"
    assert ordinal_timer(time) == 0.22125


def test_aux_data_ini_parser():
    self = " self"
    assert aux_data_reader.aux_data_ini_parser(self, aux_dict) == ('./tests/', ',', 4, '%Y%m%d%H%M', [1, 3], ['datetime', 'air_pressure'], {'datetime':'str', 'air_pressure':'float'})

# def test_pusher():
#     influx = pusher(df, influx_dict)
#     assert influx.influx_dict == influx_dict

def test_measurement_reader():
    # config = configparser.ConfigParser()
    # config.read(inifile)
    files = [ '210103.DAT' ]
    # mes_dict = dict(config.items('measurement_data'))
    measurement = measurement_reader(meas_dict, files)
    measurement_cols = measurement.measurement_df.dtypes.to_dict()
    datatypes = { 'ordinal_date': np.dtype('int64'),
                 'ordinal_time': np.dtype('float64'),
                 'ordinal_datetime': np.dtype('float64'),
                 'error_code': np.dtype('int64'),
                 'date': np.dtype('O'),
                 'time': np.dtype('O'),
                 'h2o': np.dtype('float64'),
                 'co2':np.dtype('float64'),
                 'ch4':np.dtype('float64'),
                 }
    assert measurement.measurement_files == ['210103.DAT']
    assert measurement.measurement_df.dtypes.to_dict() == datatypes

def test_main_py():
    main.ac_push(inifile)

    # if __name__ == "__main__":
    #     inifile = 'tests/test_ini.ini'
    #     test_main_py()

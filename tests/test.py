# test_with_unittest discover
from calc.fluxer import ordinal_timer_test
from calc.fluxer import measurement_reader
from calc.fluxer import aux_data_reader
import configparser

inifile = 'tests/test_ini.ini'
def config_read(inifile):
    config = configparser.ConfigParser()
    config.read(inifile)
    aux_dict = dict(config.items('air_pressure_data'))
    return config, aux_dict
ini, aux_dict = config_read(inifile)

def test_ordinal_timer_test():
    time = "05:18:36"
    assert ordinal_timer_test(time) == 0.22125

def test_ordinal_timer():
    time = "05:18:36"
    self = " self"
    assert measurement_reader.ordinal_timer(self, time) == 0.22125

def test_aux_data_ini_parser():
    self = " self"
    assert aux_data_reader.aux_data_ini_parser(self, aux_dict) == ('/data/path/', ',', 4, '%Y%m%d%H%M', [1, 3], ['datetime', 'air_pressure'], {'datetime':'str', 'air_pressure':'float'})


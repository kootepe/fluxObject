import pytest
import pandas as pd
import sys

# test_with_unittest discover
import main

inifile = "tests/test_ini.ini"

cols = pd.read_csv("tests/test_results.csv")
cols["datetime"] = pd.to_datetime(cols["datetime"], format="%Y-%m-%d %H:%M:%S")
cols.set_index("datetime", inplace=True)
cols = cols.columns.to_list()

csv = pd.read_csv("tests/test_results.csv")
csv["datetime"] = pd.to_datetime(csv["datetime"], format="%Y-%m-%d %H:%M:%S")
csv.set_index("datetime", inplace=True)

data_to_test = main.ac_push(inifile, 1)


@pytest.mark.parametrize("test_input", cols)
def test_main_defaults(test_input):
    input = test_input
    correct = csv
    test = data_to_test
    if input == "chamber_close" or input == "chamber_open":
        correct[input] = pd.to_datetime(correct[input], format="%Y-%m-%d %H:%M:%S")
    # assert correct.snowdepth.tolist() == test.upload_ready_data.snowdepth.tolist()
    assert correct[input].tolist() == test.upload_ready_data[input].tolist()

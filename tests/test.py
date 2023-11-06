import pytest
import pandas as pd
import sys

# test_with_unittest discover
import main

# ini file with test data
ac_ini = "tests/test_inis/test_ini_ac.ini"


# csv with correct data
csv = pd.read_csv("tests/test_data/test_results_ac.csv")
csv["datetime"] = pd.to_datetime(csv["datetime"], format="%Y-%m-%d %H:%M:%S")
csv.set_index("datetime", inplace=True)

ac_cols = csv.columns.to_list()

# run main py with test flag to return classes
ac_data_to_test = main.ac_push(ac_ini, 1)


@pytest.mark.parametrize("test_input", ac_cols)
def test_main_ac_defaults(test_input):
    input = test_input
    correct = csv
    test = ac_data_to_test
    if input == "chamber_close" or input == "chamber_open":
        correct[input] = pd.to_datetime(correct[input], format="%Y-%m-%d %H:%M:%S")
    # assert correct.snowdepth.tolist() == test.upload_ready_data.snowdepth.tolist()
    assert correct[input].tolist() == test.upload_ready_data[input].tolist()


man_ini = "tests/test_inis/test_ini_man.ini"

man_csv = pd.read_csv("tests/test_data/test_results_man.csv")
man_csv["datetime"] = pd.to_datetime(man_csv["datetime"], format="%Y-%m-%d %H:%M:%S")
man_csv.set_index("datetime", inplace=True)

man_cols = man_csv.columns.to_list()

# run main py with test flag to return classes
man_data_to_test = main.man_push(man_ini, 1)
# man_data_to_test.upload_ready_data.to_csv("tests/test_data/test_results_man.csv")
# sys.exit()


@pytest.mark.parametrize("test_input", man_cols)
def test_main_manual_defaults(test_input):
    input = test_input
    correct = man_csv
    test = man_data_to_test
    if input == "chamber_close" or input == "chamber_open":
        correct[input] = pd.to_datetime(correct[input], format="%Y-%m-%d %H:%M:%S")
    # assert correct.snowdepth.tolist() == test.upload_ready_data.snowdepth.tolist()
    assert correct[input].tolist() == test.upload_ready_data[input].tolist()
    pass

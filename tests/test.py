import pytest
import pandas as pd
import numpy as np
import sys

# test_with_unittest discover
import main
from tools.merging import merge_aux_by_column
from tools.snow_height import read_snow_measurement

# initialize data for AC test
ac_ini = "tests/test_inis/test_ini_ac.ini"
env_vars = {}


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


# initialize data for AC test
ac_ini_ap = "tests/test_inis/test_ini_ac_airpres.ini"


ac_csv_ap = pd.read_csv("tests/test_data/test_results_ac_ap.csv")
ac_csv_ap["datetime"] = pd.to_datetime(
    ac_csv_ap["datetime"], format="%Y-%m-%d %H:%M:%S"
)
ac_csv_ap.set_index("datetime", inplace=True)

ac_col_ap = ac_csv_ap.columns.to_list()

# run main py with test flag to return classes
ac_data_to_test_ap = main.ac_push(ac_ini_ap, 1)
# ac_data_to_test_ap.upload_ready_data.to_csv("tests/test_data/test_results_ac_ap.csv")
# sys.exit()


@pytest.mark.parametrize("test_input", ac_col_ap)
def test_main_ac_ap(test_input):
    input = test_input
    correct = ac_csv_ap
    test = ac_data_to_test_ap
    if input == "chamber_close" or input == "chamber_open":
        correct[input] = pd.to_datetime(correct[input], format="%Y-%m-%d %H:%M:%S")
    # assert correct.snowdepth.tolist() == test.upload_ready_data.snowdepth.tolist()
    assert correct[input].tolist() == test.upload_ready_data[input].tolist()


# initialize data for manual test
man_ini = "tests/test_inis/test_ini_man.ini"

man_csv = pd.read_csv(
    "tests/test_data/test_results_man.csv", dtype={"start": "str", "notes": "str"}
)
man_csv["datetime"] = pd.to_datetime(man_csv["datetime"], format="%Y-%m-%d %H:%M:%S")
man_csv.set_index("datetime", inplace=True)
man_csv["notes"] = man_csv["notes"].fillna("")
man_csv["validity"] = man_csv["validity"].fillna("")

man_cols = man_csv.columns.to_list()

# run main py with test flag to return classes
man_data_to_test = main.man_push(man_ini, 1)


@pytest.mark.parametrize("test_input", man_cols)
def test_main_manual_defaults(test_input):
    input = test_input
    correct = man_csv
    test = man_data_to_test
    if input in [
        "chamber_close",
        "chamber_open",
        "end_time",
        "close_time",
        "open_time",
        "start_time",
    ]:
        correct[input] = pd.to_datetime(correct[input], format="%Y-%m-%d %H:%M:%S")
    assert correct[input].tolist() == test.upload_ready_data[input].tolist()


# initialize data for manual test
man_ap_ini = "tests/test_inis/test_ini_man_airpres.ini"

man_ap_csv = pd.read_csv(
    "tests/test_data/test_results_man_ap.csv", dtype={"start": "str", "notes": "str"}
)
man_ap_csv["datetime"] = pd.to_datetime(
    man_ap_csv["datetime"], format="%Y-%m-%d %H:%M:%S"
)
man_ap_csv.set_index("datetime", inplace=True)
man_ap_csv["notes"] = man_ap_csv["notes"].fillna("")
man_ap_csv["validity"] = man_ap_csv["validity"].fillna("")

man_ap_cols = man_ap_csv.columns.to_list()

# run main py with test flag to return classes
man_ap_data_to_test = main.man_push(man_ap_ini, 1)
# man_ap_data_to_test.upload_ready_data.to_csv("tests/test_data/test_results_man_ap.csv")
# sys.exit()


@pytest.mark.parametrize("test_input", man_ap_cols)
def test_man_manual_airpres(test_input):
    input = test_input
    correct = man_ap_csv
    test = man_ap_data_to_test
    if input in [
        "chamber_close",
        "chamber_open",
        "end_time",
        "close_time",
        "open_time",
        "start_time",
    ]:
        correct[input] = pd.to_datetime(correct[input], format="%Y-%m-%d %H:%M:%S")
    assert correct[input].tolist() == test.upload_ready_data[input].tolist()


main_snow_test_df = pd.read_csv("tests/snow_test_function.csv")
main_snow_test_df["datetime"] = pd.to_datetime(
    main_snow_test_df["datetime"], format="%Y-%m-%d %H:%M:%S"
)
main_snow_test_df.set_index("datetime", inplace=True)

aux_snow_test_df, _ = read_snow_measurement("tests/snow_test_for_function.xlsx")


def test_ac_snowdepth():
    snow_df = merge_aux_by_column(main_snow_test_df, aux_snow_test_df)

    correct_seq = [5, 5, 1, 0, 3, 2, 5, 4, 7, 6, 7, 6, 7, 6]
    assert correct_seq == snow_df.snowdepth.tolist()

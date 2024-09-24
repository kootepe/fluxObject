import pandas as pd
import numpy as np
import sys
from pathlib import Path
import datetime
from tools.time_funcs import time_to_numeric
import pytest

# test_with_unittest discover
import main

# +from tools.merging import merge_aux_by_column
from tools.snow_height import read_snow_measurement
from tools.validation import check_air_temp_col
from tools.file_tools import (
    read_man_meas_f,
    filter_between_dates,
    find_files,
    mk_date_dict,
    get_files,
)
from tools.gas_funcs import (
    calculate_pearsons_r,
    calculate_slope,
    calculate_gas_flux,
)
from tools.fluxer import fluxCalculator
from tools.fluxer import li7810
from tools.filter import mk_tuple

man_time_files = list(Path("tests/data/manual_times/").glob("*.txt"))
man_time_df = read_man_meas_f(man_time_files, (60, 240, 300))
expected_tuple = [
    mk_tuple(
        (
            pd.Timestamp("2021-10-03 02:05:00"),
            pd.Timestamp("2021-10-03 02:06:00"),
            pd.Timestamp("2021-10-03 02:09:00"),
            pd.Timestamp("2021-10-03 02:10:00"),
            71,
        )
    ),
    mk_tuple(
        (
            pd.Timestamp("2021-10-03 02:20:00"),
            pd.Timestamp("2021-10-03 02:21:00"),
            pd.Timestamp("2021-10-03 02:24:00"),
            pd.Timestamp("2021-10-03 02:25:00"),
            67,
        )
    ),
]


man_ini_path = "tests/inis/test_ini_man.ini"
env_vars = None
ac_ini_path = "tests/inis/test_ini_ac.ini"

man_f = fluxCalculator(man_ini_path, env_vars)
ac_f = fluxCalculator(man_ini_path, env_vars)

test_data_path = "tests/data/measurement_data/"
test_data_files = [
    Path("tests/data/measurement_data/TG10-01143-2021-10-04T000000.data"),
    Path("tests/data/measurement_data/TG10-01143-2021-10-03T000000.data"),
    Path("tests/data/measurement_data/TG10-01143-2021-10-05T000000.data"),
    # Path("tests/data/measurement_data/210105.DAT"),
    # Path("tests/data/measurement_data/210104.DAT"),
    # Path("tests/data/measurement_data/210103.DAT"),
]

dfa = pd.DataFrame(data={"air_temperature": [1, 2, 3, 4]})
dfb = pd.DataFrame(data={"air_pressure": [1, 2, 3, 4]})
dfc = None

device = li7810()
gas_df = device.read_file(
    Path("tests/data/measurement_data/TG10-01143-2021-10-03T000000.data")
)


gas_df["numeric_datetime"] = (
    gas_df[device.sec_col].astype(str) + "." + gas_df[device.nsec_col].astype(str)
).astype(float)
# +# initialize data for manual test
# +man_ini = "tests/test_inis/test_ini_man.ini"

gas_df["numeric_date"] = pd.to_datetime(gas_df["datetime"]).map(
    datetime.datetime.toordinal
)
# +man_csv = pd.read_csv(
# +    "tests/test_data/test_results_man.csv", dtype={"start": "str", "notes": "str"}
gas_df["numeric_time"] = time_to_numeric(gas_df[device.time_col].values)
gas_df["numeric_datetime"] = gas_df["numeric_time"] + gas_df["numeric_date"]
gas_df["air_pressure"] = 1000
gas_df["air_temperature"] = 10

chamber_h = 500
def_temp = 10
def_press = 1000
measurement_name = "CH4"
use_defs = 0

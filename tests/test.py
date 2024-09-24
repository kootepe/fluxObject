import pytest
from tools.time_funcs import time_to_numeric

# test_with_unittest discover
import main
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
from tools.filter import mk_fltr_tuples
from tools.fluxer import li7810


from tests.test_data import (
    test_data_files,
    test_data_path,
    dfa,
    dfb,
    dfc,
    gas_df,
    chamber_h,
    def_temp,
    def_press,
    measurement_name,
    use_defs,
    man_f,
    ac_f,
    man_time_df,
    expected_tuple,
)


@pytest.mark.parametrize("input,expected", [(man_time_df.head(2), expected_tuple)])
def test_mk_fltr_tuple(input, expected):
    assert mk_fltr_tuples(input) == expected


@pytest.mark.parametrize(
    "input1,input2,expected",
    [
        (gas_df["numeric_datetime"], gas_df[measurement_name], -0.00957082),
        (gas_df["datetime"], gas_df[measurement_name], pytest.raises(TypeError)),
    ],
)
def test_slope_calc(input1, input2, expected):
    if not isinstance(expected, float):
        with expected:
            assert calculate_slope(input1, input2) == expected
    else:
        assert calculate_slope(input1, input2) == expected


@pytest.mark.parametrize(
    "input1,input2,expected1,expected2,expected3",
    [
        (
            gas_df["numeric_datetime"],
            gas_df[measurement_name],
            -0.00957082,
            0.15986399,
            -11.708871776588436,
        ),
        (
            gas_df["datetime"],
            gas_df[measurement_name],
            -0.00957082,
            0.15986399,
            pytest.raises(TypeError),
        ),
    ],
)
def test_gas_calc(input1, input2, expected1, expected2, expected3):
    if not isinstance(expected3, float):
        with expected3:
            assert calculate_slope(input1, input2) == expected3
    else:
        slope = calculate_slope(input1, input2)
        pearson = calculate_pearsons_r(input1, input2)
        flux = calculate_gas_flux(
            gas_df,
            "CH4",
            slope,
            chamber_h,
        )
        assert slope == -0.00957082
        assert pearson == 0.15986399
        assert flux == -11.708871776588436


@pytest.mark.parametrize(
    "input,expected",
    [
        (dfa, False),
        (dfb, True),
        ([1, 2], pytest.raises(AttributeError)),
        (dfc, pytest.raises(AttributeError)),
        (dfa, False),
        (dfb, True),
    ],
)
def test_check_air_temp_col(input, expected):
    if not isinstance(expected, bool):
        with expected:
            assert check_air_temp_col(input) == expected
    else:
        assert check_air_temp_col(input) == expected


def test_file_find():
    assert len(find_files(test_data_path)) == 3
    assert find_files(test_data_path) == test_data_files


@pytest.mark.parametrize(
    "input,expected",
    [(man_f.ini_handler.def_temp, 10.0), (ac_f.ini_handler.def_temp, 10.0)],
)
def test_def_temp(input, expected):
    assert input == expected
    assert input == expected


# def test_ac_snowdepth():
#     snow_df = merge_aux_by_column(main_snow_test_df, aux_snow_test_df)
#
#     correct_seq = [5, 5, 1, 0, 3, 2, 5, 4, 7, 6, 7, 6, 7, 6]
# assert correct_seq == snow_df.snowdepth.tolist()
# # initialize data for AC test
# ac_ini = "tests/test_inis/test_ini_ac.ini"
# env_vars = {}
#
#
# csv = pd.read_csv("tests/test_data/test_results_ac.csv")
# csv["datetime"] = pd.to_datetime(csv["datetime"], format="%Y-%m-%d %H:%M:%S")
# csv.set_index("datetime", inplace=True)
#
# ac_cols = csv.columns.to_list()
#
# # run main py with test flag to return classes
# ac_data_to_test = main.class_calc(ac_ini, env_vars)
#
#
# @pytest.mark.parametrize("test_input", ac_cols)
# def test_main_ac_defaults(test_input):
#     input = test_input
#     correct = csv
#     test = ac_data_to_test
#     if input == "chamber_close" or input == "chamber_open":
#         correct[input] = pd.to_datetime(correct[input], format="%Y-%m-%d %H:%M:%S")
#     # assert correct.snowdepth.tolist() == test.upload_ready_data.snowdepth.tolist()
#     assert correct[input].tolist() == test.upload_ready_data[input].tolist()
#
#
# # initialize data for AC test
# ac_ini_ap = "tests/test_inis/test_ini_ac_airpres.ini"
#
#
# ac_csv_ap = pd.read_csv("tests/test_data/test_results_ac_ap.csv")
# ac_csv_ap["datetime"] = pd.to_datetime(
#     ac_csv_ap["datetime"], format="%Y-%m-%d %H:%M:%S"
# )
# ac_csv_ap.set_index("datetime", inplace=True)
#
# ac_col_ap = ac_csv_ap.columns.to_list()
#
# # run main py with test flag to return classes
# ac_data_to_test_ap = main.ac_push(ac_ini_ap, env_vars, 1)
# # ac_data_to_test_ap.upload_ready_data.to_csv("tests/test_data/test_results_ac_ap.csv")
# # sys.exit()
#
#
# @pytest.mark.parametrize("test_input", ac_col_ap)
# def test_main_ac_ap(test_input):
#     input = test_input
#     correct = ac_csv_ap
#     test = ac_data_to_test_ap
#     if input == "chamber_close" or input == "chamber_open":
#         correct[input] = pd.to_datetime(correct[input], format="%Y-%m-%d %H:%M:%S")
#     # assert correct.snowdepth.tolist() == test.upload_ready_data.snowdepth.tolist()
#     assert correct[input].tolist() == test.upload_ready_data[input].tolist()
#
#
# # initialize data for manual test
# man_ini = "tests/test_inis/test_ini_man.ini"
#
# man_csv = pd.read_csv(
#     "tests/test_data/test_results_man.csv", dtype={"start": "str", "notes": "str"}
# )
# man_csv["datetime"] = pd.to_datetime(man_csv["datetime"], format="%Y-%m-%d %H:%M:%S")
# man_csv.set_index("datetime", inplace=True)
# man_csv["notes"] = man_csv["notes"].fillna("")
# man_csv["validity"] = man_csv["validity"].fillna("")
#
# man_cols = man_csv.columns.to_list()
#
# # run main py with test flag to return classes
# man_data_to_test = main.man_push(man_ini, env_vars, 1)
#
#
# @pytest.mark.parametrize("test_input", man_cols)
# def test_main_manual_defaults(test_input):
#     input = test_input
#     correct = man_csv
#     test = man_data_to_test
#     if input in [
#         "chamber_close",
#         "chamber_open",
#         "end_time",
#         "close_time",
#         "open_time",
#         "start_time",
#     ]:
#         correct[input] = pd.to_datetime(correct[input], format="%Y-%m-%d %H:%M:%S")
#     assert correct[input].tolist() == test.upload_ready_data[input].tolist()
#
#
# # initialize data for manual test
# man_ap_ini = "tests/test_inis/test_ini_man_airpres.ini"
#
# man_ap_csv = pd.read_csv(
#     "tests/test_data/test_results_man_ap.csv", dtype={"start": "str", "notes": "str"}
# )
# man_ap_csv["datetime"] = pd.to_datetime(
#     man_ap_csv["datetime"], format="%Y-%m-%d %H:%M:%S"
# )
# man_ap_csv.set_index("datetime", inplace=True)
# man_ap_csv["notes"] = man_ap_csv["notes"].fillna("")
# man_ap_csv["validity"] = man_ap_csv["validity"].fillna("")
#
# man_ap_cols = man_ap_csv.columns.to_list()
#
# # run main py with test flag to return classes
# man_ap_data_to_test = main.man_push(man_ap_ini, env_vars, 1)
# # man_ap_data_to_test.upload_ready_data.to_csv("tests/test_data/test_results_man_ap.csv")
# # sys.exit()
#
#
# @pytest.mark.parametrize("test_input", man_ap_cols)
# def test_man_manual_airpres(test_input):
#     input = test_input
#     correct = man_ap_csv
#     test = man_ap_data_to_test
#     if input in [
#         "chamber_close",
#         "chamber_open",
#         "end_time",
#         "close_time",
#         "open_time",
#         "start_time",
#     ]:
#         correct[input] = pd.to_datetime(correct[input], format="%Y-%m-%d %H:%M:%S")
#     assert correct[input].tolist() == test.upload_ready_data[input].tolist()
#
#
# main_snow_test_df = pd.read_csv("tests/snow_test_function.csv")
# main_snow_test_df["datetime"] = pd.to_datetime(
#     main_snow_test_df["datetime"], format="%Y-%m-%d %H:%M:%S"
# )
# main_snow_test_df.set_index("datetime", inplace=True)
#
# aux_snow_test_df, _ = read_snow_measurement("tests/snow_test_for_function.xlsx")

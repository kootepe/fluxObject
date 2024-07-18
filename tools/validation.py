import pandas as pd
import logging
from tools.filter import date_filter
from tools.time_funcs import get_time_diff

logger = logging.getLogger("defaultLogger")


def set_next_true(df, column):
    true_indices = df[df[column]].index
    for idx in true_indices:
        next_idx = df.index.get_loc(idx) + 1
        if next_idx < len(df):
            df.iloc[next_idx, df.columns.get_loc(column)] = True
    return df


def overlap_test(df):
    """
    Checks overlapping measurement in the measurement times.
    """
    df = df.sort_values(by="start_time")
    df["next_start_time"] = df["start_time"].shift(-1)
    df["overlap"] = df["end_time"] > df["next_start_time"]
    # df = set_next_true(df, "overlap")
    return df


def check_too_many(df, measurement_time):
    return len(df) > measurement_time * 1.1


def validate_checks(list, string):
    """
    Overlap check sets the next measurement to also have overlap so there can be
    double validation strings in the checks column
    """
    return [item for item in list if item not in string]


def check_diag_col(df, device):
    return df[device.diag_col].sum() != 0


def check_air_temp_col(df, col):
    return "air_temperature" not in df.columns


def check_air_press_col(df, col):
    return "air_pressure" not in df.columns


def check_too_few(df, measurement_time):
    return measurement_time * 0.9 > len(df)


def check_valid(dataframe, filter_tuple, device, measurement_time):
    # NOTE: Should this be moved inside one of the existing loops?
    # This loops through everything again, would be better to have it in the
    # same loop where we calculate flux?
    logger.debug("Checking validity")
    dfa = []
    missing_data = False
    for date in filter_tuple:
        df = date_filter(dataframe, (date[2], date[3])).copy()

        has_errors = check_diag_col(df, device)
        no_air_temp = check_air_temp_col(df, "air_temperature")
        no_air_pressure = check_air_press_col(df, "air_pressure")
        is_empty = df.empty
        has_overlap = df.overlap.any()
        too_many = check_too_many(df, measurement_time)
        too_few = check_too_few(df, measurement_time)

        if (
            has_errors
            or no_air_temp
            or no_air_pressure
            or is_empty
            or missing_data
            or has_overlap
            or too_many
            or too_few
        ):
            checks = []
            if is_empty:
                checks.append("no data,")
            if has_errors:
                checks.append("has errors,")
            if no_air_temp:
                checks.append("no air temp,")
            if no_air_pressure:
                checks.append("no air pressure,")
            if has_overlap:
                checks.append("has overlap,")
            if too_many:
                checks.append("too many measurements,")
            if too_few:
                checks.append("too few measurements,")

            checks_str = "".join(checks)
            df.loc[:, "checks"] += checks_str
            df.loc[:, "is_valid"] = False

        dfa.append(df)
    dfas = pd.concat(dfa)
    return dfas

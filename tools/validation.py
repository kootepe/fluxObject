import pandas as pd
import logging
from tools.filter import date_filter
from tools.time_funcs import get_time_diff

logger = logging.getLogger("defaultLogger")


def check_valid(dataframe, filter_tuple, device):
    # NOTE: Should this be moved inside one of the existing loops?
    # This loops through everything again, would be better to have it in the
    # same loop where we calculate flux?
    logger.debug("Checking validity")
    # logger.debug(dataframe)
    # self.calc_tuple = [add_min_to_fltr_tuple(t) for t in filter_tuple]
    # self.calc_tuple = [add_min_to_fltr_tuple(t) for t in self.calc_tuple]
    dfa = []
    for date in filter_tuple:
        df = date_filter(dataframe, date)

        has_errors = df[device.diag_col].sum() != 0

        if "air_temperature" not in df.columns:
            no_air_temp = True
        else:
            no_air_temp = df["air_temperature"].isna().all()

        if "air_pressure" not in df.columns:
            no_air_pressure = True
        else:
            no_air_pressure = df["air_pressure"].isna().all()

        is_empty = df.empty

        time_diff = get_time_diff(date[0], date[1])
        # NOTE: this only works for measurements where there's data once
        # every second
        if time_diff * 0.9 > len(df):
            missing_data = True
        else:
            missing_data = False

        if has_errors or no_air_temp or no_air_pressure or is_empty or missing_data:
            checks = []
            if has_errors:
                checks.append("has errors,")
            if no_air_temp:
                checks.append("no air temp,")
            if no_air_pressure:
                checks.append("no air pressure,")
            if is_empty:
                checks.append("no data,")
            if missing_data:
                checks.append("missing over 10%,")
            checks_str = "".join(checks)
            df.loc[:, "checks"] += checks_str
            df.loc[:, "is_valid"] = False

        dfa.append(df)
    dfa = pd.concat(dfa)
    return dfa

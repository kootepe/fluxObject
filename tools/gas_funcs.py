import numpy as np
import pandas as pd
from tools.filter import date_filter
import logging

logger = logging.getLogger("defaultLogger")


def calculate_gas_flux(
    data, measurement_name, chamber_height, def_temp, def_press
):
    """
    Calculates gas flux

    args:
    ---
    df : pandas.dataframe
        dataframe with slope calculated.
    measurement_name : str
        name of the gas that flux is going to be calculated for

    returns:
    ---
    flux : numpy.array
        one column for the dataframe with the calculated gas
        flux
    """

    is_valid = True
    df = data.copy()
    # from mm to m
    if "snowdepth" not in df.columns:
        df["snowdepth"] = 0

    height = (chamber_height * 0.001) - (df["snowdepth"].mean() / 100)

    # slope of the linear fit of the measurement
    slope = df[f"{measurement_name}_slope"]
    # chamber_height
    h = df["height"] = height
    # value to convert ppb to ppm etc.
    conv = 1
    # molar mass of co2. C mass 12 and O mass 16
    m = df["molar_mass"] = 12 + 16 + 16
    # temperature in K
    if df["air_temperature"].isnull().values.sum() > 0:
        is_valid = False
        t = def_temp
    else:
        try:
            t = df["air_temperature"].mean()
        except Exception:
            logger.debug("NO AIR TEMPERATURE IN FILE, USING DEFAULT")
            t = def_temp
            is_valid = False
    if df["air_pressure"].isnull().values.sum() > 0:
        p = def_press
    else:
        try:
            # HPa to Pa
            p = df["air_pressure"].mean()
        except Exception:
            logger.debug("NO AIR PRESSURE IN FILE, USING DEFAULT")
            p = def_press

    # universal gas constant
    r = 8.314

    if measurement_name == "ch4":
        # molar mass of CH4, C mass is 12 and H mass is 1
        m = df["molar_mass"] = 12 + 4
        # ch4 measurement is in ppb, convert to ppm
        conv = df["conv"] = 1000

    flux = round(
        (
            (slope / conv)
            * (60 / 1000000)
            * (h)
            * ((m * (p * 100)) / (r * (273.15 + t)))
            * 1000
            * 60
        ),
        6,
    )

    return flux


def calculate_pearsons_r(df, date, measurement_name):
    """
    Calculates pearsons R for a measurement

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe with ordinal time column and gas measurement column
    date : tuple
        Tuple with start time and end time that will be used to select rows to
        calculate pearsons R from.
    measurement_name : string
        name of the column slope is going to be calculated for.

    Returns
    -------
    pearsons_r : pd.Series
        Dataframe column with calculated pearsons R
    """
    start = date[0]
    end = date[1]
    filter_tuple = (start, end)
    time_array = date_filter(df["ordinal_datetime"], filter_tuple)
    gas_array = date_filter(df[measurement_name], filter_tuple)

    pearsons_r = round(abs(np.corrcoef(time_array, gas_array).item(1)), 8)
    return pearsons_r


def calculate_slope(df, date, measurement_name):
    start = date[0]
    end = date[1]
    filter_tuple = (start, end)
    time_array = date_filter(df["ordinal_datetime"], filter_tuple)
    gas_array = date_filter(df[measurement_name], filter_tuple)
    if len(time_array) == 0:
        logger.debug(f"TIME ARRAY IS EMPTY AT {date[0]}")
        slope = None
        return slope
    if len(gas_array) == 0:
        logger.debug(f"GAS ARRAY IS EMPTY AT {date[0]}")
        slope = None
        return slope

    slope = round(
        np.polyfit(time_array.astype(float), gas_array.astype(float), 1).item(0)
        / 86400,
        8,
    )
    return slope

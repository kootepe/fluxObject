import numpy as np
import pandas as pd
from tools.filter import date_filter
import logging

logger = logging.getLogger("defaultLogger")


def calculate_gas_flux(data, measurement_name, chamber_height):
    """
    Calculates gas flux

    args:
    ---
    df -- pandas.dataframe
        dataframe with slope calculated.
    measurement_name -- str
        name of the gas that flux is going to be calculated for

    returns:
    ---
    flux -- numpy.array
        one column for the dataframe with the calculated gas
        flux
    """

    df = data.copy()
    # from mm to m
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
    t = df["air_temperature"].mean()
    # HPa to Pa
    p = df["air_pressure"].mean()
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
    """Calculate pearsons_r"""
    start = date[0]
    end = date[1]
    filter_tuple = (start, end)
    time_array = date_filter(df["ordinal_datetime"], filter_tuple)
    gas_array = date_filter(df[measurement_name], filter_tuple)

    pearsons_r = round(abs(np.corrcoef(time_array, gas_array).item(1)), 8)
    return pearsons_r


def calculate_slope(df, date, measurement_name):
    """Calculate slope"""
    start = date[0]
    end = date[1]
    filter_tuple = (start, end)
    time_array = date_filter(df["ordinal_datetime"], filter_tuple)
    gas_array = date_filter(df[measurement_name], filter_tuple)

    slope = round(np.polyfit(time_array, gas_array, 1).item(0) / 86400, 8)
    return slope

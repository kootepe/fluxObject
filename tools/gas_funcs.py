import numpy as np
import logging

logger = logging.getLogger("defaultLogger")

# molar masses
masses = {"CH4": 16, "CO2": 44, "H2O": 18}
# conversions to ppm
convs = {"CH4": 1000, "CO2": 1, "H2O": 1}


def calculate_gas_flux(
    df, measurement_name, slope, chamber_height, def_temp=10, def_press=1000
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
    # this value must in m
    h = chamber_height
    # molar_mass
    m = masses.get(measurement_name)
    # value to convert to ppm
    conv = convs.get(measurement_name)
    # C temperature to K
    t = df["air_temperature"].mean() + 273.15
    # hPa to Pa
    p = df["air_pressure"].mean() * 100
    # universal gas constant
    r = 8.314

    flux = round(
        ((slope / conv) * 60 * h * m * p / 1000000 / r / t) * 1000 * 60,
        8,
    )

    return flux


def calculate_pearsons_r(df, measurement_name):
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
    time_array = df["ordinal_datetime"]
    gas_array = df[measurement_name]

    pearsons_r = round(abs(np.corrcoef(time_array, gas_array).item(1)), 8)
    # logger.debug(pearsons_r)
    return pearsons_r


def calculate_slope(x, y):
    slope = round(
        np.polyfit(x.astype(float) / 86400, y.astype(float) / 86400, 1).item(0),
        8,
    )
    return slope

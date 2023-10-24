import numpy as np

def calculate_gas_flux(df, measurement_name, chamber_height,
                       temperature, pressure):
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

    # from mm to m
    height = (chamber_height * 0.001) - df['snowdepth']

    # slope of the linear fit of the measurement
    slope = df[f'{measurement_name}_slope']
    # chamber height in cm
    h = height * 100
    # value to convert ppb to ppm etc.
    conv = 1
    # molar mass of co2. C mass 12 and O mass 16
    m = 12 + 16 + 16
    # temperature in K
    t = 273.15 + temperature
    # HPa to Pa
    p = pressure * 100
    # universal gas constant
    r = 8.314

    if measurement_name == 'ch4':
        # molar mass of CH4, C mass is 12 and H mass is 1
        m = 12 + 4
        # ch4 measurement is in ppb, convert to ppm
        conv = 1000
    flux = round((slope / conv) * (60 / 1000000) * (h / 100) * ((m * p) / (r * t)) * 1000, 5)

    return flux


def calculate_pearsons_r(time_array, measurement_array):
    pearsons_r = abs(np.corrcoef(time_array, measurement_array).item(1)) 
    return pearsons_r


def calculate_slope(time_array, gas_array):
    slope = np.polyfit(time_array, gas_array, 1).item(0) / 86400
    return slope

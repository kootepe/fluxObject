import os
import re

import sys
import logging
import datetime
import configparser
import timeit
import glob
import timeit
import pandas as pd
import numpy as np
import zipfile as zf
from zipfile import BadZipFile
import influxdb_client as ifdb
from pathlib import Path
#from influxdb_client import InfluxDBClient, Point, WriteOptions
#from influxdb_client.client.write_api import SYNCHRONOUS

#import influxdb_client as ifdb
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS, PointSettings

logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
class pusher:
    """
    Pushes pandas dataframes to InfluxDB

    Attributes
    ---
    influxdb_dict : dictionary
        dictionary with the influxdb login information from .ini
    data : pandas dataframe
        data that will be pushed to influxdb
    tag_columns : list
        List of columns that will be used as tags in influxdb

    Methods
    ---
    influxPush(df)
        Pushes data to influxdb
    read_tag_columns()
        Sets tag columns for pushing to influxdb


    """
    def __init__(self, data, influxdb_dict):
        """
        args:
        ---
            data -- pandas dataframe
            influxdb_dict -- dictionary from the .ini file with necessary info about influxdb connection
        """
        self.influxdb_dict = influxdb_dict
        self.data = data
        logging.info('Pushing data to DB')
        self.tag_columns = self.read_tag_columns()
        self.influxPush(self.data)

    def influxPush(self, df):
        """
        Push data to InfluxDB

        args:
        ---
            df -- pandas dataframe

        returns:
        ---

        """
        #grouped = df.groupby('chamber')
        point_settings = PointSettings()
        with ifdb.InfluxDBClient(url = self.influxdb_dict.get('url'),
                                 token = self.influxdb_dict.get('token'),
                                 org = self.influxdb_dict.get('organization'),
                                 ) as client:
            write_api = client.write_api(write_options = SYNCHRONOUS)
            write_api.write(bucket = self.influxdb_dict.get('bucket'), record = df,
                            data_frame_measurement_name = self.influxdb_dict.get('measurement_name'),
                            data_frame_timestamp_timezone = self.influxdb_dict.get('timezone'),
                            data_frame_tag_columns = self.tag_columns,
                            debug = True)
        logging.info('Pushed data to DB')

    def read_tag_columns(self):
        """
        Reads tag columns from .ini and checks that they are in the dataframe, and returns them as list

        args:
        ---

        returns:
        ---
            tag_columns -- list
            list of tag_columns, as defined in .ini
        """
        tag_columns = self.influxdb_dict.get('tag_columns').split(',')
        measurement_columns = self.data.columns
        # if all items in tag_columns are in measuremeny_columns, return true
        check = any(item in tag_columns for item in measurement_columns)
        if len(tag_columns) == 1 and tag_columns[0] == '':
            logging.warning('No tag columns defined')
            return []
        else:
            if check == True:
                return tag_columns
            else:
                logging.warning("Columns labeled for tagging don't exist in dataframe, exiting")
                sys.exit(0)

class snowdepth_parser:
    """
    Class for parsing the snowdepth measurement for automatic chambers

    Attributes
    ---
        snowdepth_measurement -- xlsx file
            .xlsx file for with snowdepth for each chamber
        snowdepth_df -- pandas dataframe
            dataframe version of the .xlsx file.

    Methods
    ---
    add_snowdepth()
        Reads snowdepth measurement and turns it into a dataframe, if it exists. Or creates a dummy one.
    """
    def __init__(self, snowdepth_measurement):
        self.snowdepth_measurement = snowdepth_measurement
        self.snowdepth_df = self.add_snowdepth()

    def add_snowdepth(self):
        """
        Creates a dataframe from snowdepth measurements. If there's no
        measurement, it will create an empty one.

        args:
        ---
            snowdepth_measurement -- .xlsx file

        returns:
        ---
            snowdepth -- df

        """
        if self.snowdepth_measurement:
            snowdepth = pd.read_excel(self.snowdepth_measurement)
            snowdepth['datetime'] = pd.to_datetime(snowdepth['datetime'])
            snowdepth.set_index('datetime', inplace = True)
        else:
            logging.warning('No snow depth measurements found, snow depth ' \
                            'set at zero for all measurements')
            # if theres no measurements of snow depth, set it as 0
            d = {'datetime': ['01-01-2023', '02-01-2023'], 'snowdepth1': [0,0], 'snowdepth2': [0,0]}
            snowdepth = pd.DataFrame(data=d)
            snowdepth['datetime'] = pd.to_datetime(snowdepth['datetime'], format = '%d-%m-%Y')
            snowdepth.set_index('datetime', inplace = True)
            print(snowdepth)
        return snowdepth

class calculated_data:
    """
    Calculates gas flux and pearsonsR of each flux measurement.

    Attributes
    ---
    get_temp_and_pressure_from_file -- bool
        1 to get temp and pressure from files, 0 to use defaults
    chamber_height -- int
        Height of the measurement chamber, for volume
    chamber_width -- int
        Width of the measurement chamber, for volume
    chamber_length -- int
        Length of the measurement chamber, for volume
    filter_tupe -- tuple
        (start_time, end_time, chamber_num), measurement is calculated
        from values between these timestamps,
    default_pressure -- int
        Default pressure value
    default_temp -- int
        Default temperature value
    calculated_data -- df
        Data with gas flux and pearsonsR calculated
    upload_ready_data -- df
        Data with unnecessary columns removed

    Methods
    ---
    calculate_slope_pearsons_r(df, measurement_name)
        Calculates slope and pearsonsR
    calculate_gas_flux(df, measurement_name)
        Calculates gas flux
    summarize(data)
        Grabs first row from each measurement, since flux and r are the
        same per measurement, also removes unnecesary columns

    """
    def __init__(self, measured_data, measuring_chamber_dict, filter_tuple, get_temp_and_pressure_from_file, default_pressure, default_temperature):
        self.measured_data = measured_data
        self.get_temp_and_pressure_from_file = get_temp_and_pressure_from_file
        self.chamber_height = int(measuring_chamber_dict.get('chamber_height'))
        self.chamber_width = int(measuring_chamber_dict.get('chamber_width'))
        self.chamber_length = int(measuring_chamber_dict.get('chamber_length'))
        self.filter_tuple = filter_tuple
        self.default_pressure = default_pressure
        self.default_temperature = default_temperature
        self.calculated_data = self.calculate_slope_pearsons_r(self.measured_data, 'ch4')
        self.calculated_data = self.calculate_slope_pearsons_r(self.calculated_data, 'co2')
        self.upload_ready_data = self.summarize(self.calculated_data)


    def calculate_slope_pearsons_r(self, df, measurement_name):
        """
        Calculates Pearsons R (correlation) and the slope of
        the CH4 flux.

        args:
        ---
            df -- pandas.dataframe
                dataframe of the gas flux
            measurement_name -- str
                name of the gas that slope, pearsons_r and flux is
                going to be calculated for

        returns:
        ---
            dfs -- pandas.dataframe
                same dataframe with additional slope, pearsons_r and
                flux columns
        """
        tmp = []
        # following loop raises a false positive warning, disable it
        pd.options.mode.chained_assignment = None
        for date in self.filter_tuple:
            #print(f'{date[0] = }')
            #print(f'{date[1] = }')
            start = df.index.searchsorted(date[0])
            end = df.index.searchsorted(date[1])
            dfa = df.iloc[start:end]
            if dfa.empty:
                continue
            #print(f'{df = }')
            ordinal_time = dfa['ordinal_datetime']
            measurement = dfa[measurement_name]
            dfa[f'{measurement_name}_slope'] = np.polyfit(ordinal_time, measurement, 1).item(0) / (24*3600)
            if dfa.ch4.isnull().values.any():
                logging.warning(f'Non-numeric values present from {dfa.index[0]} to {dfa.index[-1]}')
            dfa[f'{measurement_name}_pearsons_r'] = abs(np.corrcoef(ordinal_time, measurement).item(1))
            dfa[f'{measurement_name}_flux'] = self.calculate_gas_flux(dfa, measurement_name)
            tmp.append(dfa)
        dfs = pd.concat(tmp)
        pd.options.mode.chained_assignment = 'warn'
        #pearsons_r = np.corrcoef(ordinal_time, measurement).item(1)
        #return slope, pearsons_r
        return dfs

    def calculate_gas_flux(self, df, measurement_name):
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
        slope = df[f'{measurement_name}_slope']
        chamber_volume = (self.chamber_height * 0.001) * (self.chamber_width * 0.001) * ((self.chamber_height * 0.001) - df['snowdepth'])
        if self.get_temp_and_pressure_from_file == 0:
            pressure = df['air_pressure'] = self.default_pressure
            temperature = df['air_temperature'] = self.default_temperature
        if self.get_temp_and_pressure_from_file == 1:
            pressure = df['air_pressure'].mean()
            temperature = df['air_temperature'].mean()
        flux = round(((slope/1000.0)*chamber_volume*(12.0+4.0)*pressure*100.0 /1000000.0/8.314/(273.15+temperature))*1000.0*60.0, 7)
        return flux

    def summarize(self, data):
        """
        Drops most columns as from here they will be pushed to influxdb

        args:
        ---
            data -- pandas.dataframe

        returns:
        ---
            summary -- pandas.dataframe


        """
        dfList = []
        data = data[['ch4_flux', 'co2_flux', 'ch4_pearsons_r', 'co2_pearsons_r', 'chamber']]
        for date in self.filter_tuple:
            start = data.index.searchsorted(date[0])
            end = data.index.searchsorted(date[1])
            dfa = data.iloc[start:end]
            dfList.append(dfa.iloc[:1])
        summary = pd.concat(dfList)
        return summary

class merge_data:
    """
    Merges "auxiliary" data to the main gas measurement. Auxiliary data
    being air pressure, air temperature and snowdepth most of the time.

    Attributes
    ---
    merged_data -- pandas.dataframe
        The gas measurement dataframe
    aux_data -- pandas.dataframe
        The "auxiliary" dataframe, can be any data that has a
        datetimeindex

    Methods
    ---
    merge_aux_data
        Merges the two dataframes if they are sorted by datetimeindex
    is_dataframe_sorted_by_datetime_index
        Checks if input is a dataframe, if it has a datetimeindex and
        that the index is ascending.
    """
    def __init__(self, measurement_df, aux_df):
        self.merged_data = self.merge_aux_data(measurement_df, aux_df)

    def merge_aux_data(self, measurement_df, aux_df):
        """
        Merges 'auxiliary' data to the dataframe, mainly air temperature,
        air pressure and snowdepth.

        args:
        ---
            measurement_df -- pandas.dataframe
                gas measurement dataframe
            aux_df -- pandas.dataframe
                "aux" data dataframe

        returns:
        ---
            df -- pandas.dataframe
                dataframe with aux_data merged into the main gas measurement dataframe
        """
        if self.is_dataframe_sorted_by_datetime_index(measurement_df) and self.is_dataframe_sorted_by_datetime_index(aux_df):
            df = pd.merge_asof(measurement_df,
                                    aux_df,
                                    left_on = 'datetime',
                                    right_on = 'datetime',
                                    tolerance = pd.Timedelta('60m'),
                                    direction = 'nearest',
                                    suffixes =('','_y'))
            # merge_asof creates some unnecessary columns that are named
            #{colname}_y, drop them
            df.drop(df.filter(regex='_y$').columns, axis=1, inplace=True)
            df.set_index('datetime', inplace=True)
        else:
            logging.info('Dataframes are not properly sorted by datetimeindex')
            sys.exit(0)
        return df

    def is_dataframe_sorted_by_datetime_index(self, df):
        """
        Checks that the dataframe is a dataframe, is sorted by a
        datetimeindex and that the index is ascending

        args:
        ---
            df -- pandas.dataframe

        returns:
        ---
            bool

        """
        if not isinstance(df, pd.DataFrame):
            return False

        if not isinstance(df.index, pd.DatetimeIndex):
            return False

        if df.index.is_monotonic_decreasing:
            return False

        return df.index.is_monotonic_increasing

class filterer:
    """
    Takes in a large dataframe and two timestamps, and removes all data
    that isn't between those two timestamps. Also drops dataframes if
    there's no data between the timestamps at all, and if there's a
    column called error_codes (LICOR data), and there's errors, those
    will be dropped.

    Attributes
    ---
    clean_filter_tuple -- list of tuples
        Tuples which are not empty and have no errors in the data will
        be stored here.
    filter_tuple -- list of tuples
        "Raw" tuples generated from chamber cycle or read from files
        [(YYYY-MM-DD HH:MM:SS, YYYY-MM-DD HH:MM:SS, chamberNum), (YYYY-MM-DD HH:MM:SS, YYYY-MM-DD HH:MM:SS, chamberNum)]
    df -- pandas.dataframe
        Any dataframe

    Methods
    ---
    filter_data_by_dateime
        Removes data that isn't between two timestamps

    """
    def __init__(self, filter_tuple, df):
        self.clean_filter_tuple = []
        self.filter_tuple = filter_tuple
        self.filtered_data = self.filter_data_by_datetime(df)

    def filter_data_by_datetime(self, data_to_filter):
        """
        Takes in the dataframe and two timestamps from each tuple, drops
        data that isn't between the timestamps.

        args:
        ---
            data_to_filter -- pandas.dataframe
                Large dataframe that can be thinned

        returns:
        ---
            clean_df -- pandas.dataframe
                Dataframe with data outside the timestamps removed

        """
        # data with no errors
        clean_data = []
        # timestamps for the data with no errors
        clean_timestamps = []
        # datafrrames which have errors
        dirty_data = []
        # timestamps for the data with errors
        dirty_timestamps = []
        # timestamps with no data
        empty_timestamps = []
        # the loop below raises a false positive warning, disable it
        pd.options.mode.chained_assignment = None
        empty_count = 0
        for date in self.filter_tuple:
            #print(f'{date[0] = }')
            #print(f'{date[1] = }')
            start = data_to_filter.index.searchsorted(date[0])
            end = data_to_filter.index.searchsorted(date[1])
            df = data_to_filter.iloc[start:end]
            # drop measurements with no data
            if df.empty:
                logging.info(f'No data between {date[0]} and {date[1]}')
                empty_timestamps.append(date)
                empty_count += 1
                continue
            # drop all measurements that have any error codes
            if 'error_code' in df.columns:
                if df['error_code'].sum() != 0:
                    logging.info(f'Measuring errors between {date[0]} and {date[1]}, dropping measurement.')
                    logging.info(f'Error codes found {df["error_code"].unique()}')
                    dirty_data.append(df)
                    dirty_timestamps.append(date)
                    continue
            df['chamber'] = date[2]
            #print(f'{df = }')
            clean_data.append(df)
            clean_timestamps.append(date)
        if len(self.filter_tuple) == empty_count:
            logging.info('No data found for any timestamp, is there data in the files?')
            sys.exit(0)
        clean_df = pd.concat(clean_data)
        # save tuples that have data and no error codes
        self.clean_filter_tuple = clean_timestamps
        # dataframes with errors
        if len(dirty_data) != 0:
            self.dirty_df = pd.concat(dirty_data)
        # the timestamps which cover data with errors
        self.dirty_timestamps = dirty_timestamps
        self.empty_timestamps = empty_timestamps
        # loop raises false positive error which was turned off before,
        # turn it on
        pd.options.mode.chained_assignment = 'warn'
        return clean_df


class aux_data_reader:
    """
    Reads "auxiliary" as defined in the .ini, aux data being air
    temperature and air pressure data

    Attributes
    ---
    aux_data_dict -- dictionary
        The part of the .ini file which defines air_pressure and
        air_temperature data
    files -- list
        List of the filenames that will be read into pandas dataframe

    Methods
    ---
    aux_data_ini_parser
        Parsers the .ini dictionary into values
    add_aux_data
        Reads the .csv to pandas.dataframe
    """
    def __init__(self, aux_data_dict, files):
        self.aux_data_dict = aux_data_dict
        self.files = files
        self.aux_data_df = self.add_aux_data(self.aux_data_dict)


    def aux_data_ini_parser(self, ini_dict):
        """
        Parses the .ini dictionary into values.

        args:
        ---
            ini_dict -- dictionary
                The part of the .ini which defines how to read the .csv
                file
        returns:
        ---
            path -- str
                path to the file
            delimiter -- str
                .csv delimiter
            skiprows -- int
                how many rows to skip at beginning of file
            timestamp_format -- str
                timestamp format of the .csv timestamp column
            columns -- list of ints
                list of the column numbers which will be read from the
                .csv
            names -- list of strings
                names for the columns that will be read
            dtypes -- dict
                columns and names paired into a dict
        """
        dict = ini_dict
        path = dict.get('path')
        #file = (dict.get('file'))
        # unnecessary?
        #file_timestamp_format = dict.get('file_timestamp_format')
        #if file_timestamp_format == '':
        #    files = [p for p in Path(path).glob(f'*{file}*') if '~' not in str(p)]

        delimiter = dict.get('delimiter')
        skiprows = int(dict.get('skiprows'))
        timestamp_format = dict.get('timestamp_format')
        columns = [int(dict.get('timestamp_column')), int(dict.get('measurement_column'))]
        names = [dict.get('timestamp_column_name'), dict.get('measurement_column_name')]
        dtypes = {dict.get('timestamp_column_name'): dict.get('timestamp_column_dtype'), dict.get('measurement_column_name'): dict.get('measurement_column_dtype')}
        return path, delimiter, skiprows, timestamp_format, columns, names, dtypes

    def add_aux_data(self, ini_dict):
        """
        Reads the .csv file into a pandas.dataframe

        args:
        ---
            ini_dict -- dictionary
                The part of the .ini that defines the .csv file

        returns:
        ---
            dfs -- pandas.dataframe
                The .csv file read into a pandas.dataframe

        """
        path, delimiter, skiprows, timestamp_format, columns, names, dtypes = self.aux_data_ini_parser(ini_dict)
        tmp = []
        for f in self.files:
            try:
                df = pd.read_csv(Path(path) / f,
                                 skiprows = skiprows,
                                 delimiter = delimiter,
                                 usecols = columns,
                                 names = names,
                                 dtype = dtypes
                                 )
            except FileNotFoundError:
                logging.info(f'File not found at {Path(path) / f}, make sure the .ini is correct')
                sys.exit()
            df['datetime'] = pd.to_datetime(df['datetime'], format = timestamp_format)
            df.set_index('datetime', inplace = True)
            tmp.append(df)
        dfs = pd.concat(tmp)
        return dfs

class measurement_reader:
    """
    Reads the LI-COR gas measurement.

    Attributes
    ---
    measurement_dict -- dictionary
        Defines how to read the gas measurement file
    measurement_files -- list
        list of the measurement file names
    measurement_df -- pandas.dataframe
        The measurement .csv files read into a pandas.dataframe
    Methods
    ---
    read_measurement
        Reads the .ini and then reads the .csv into a pandas dataframe
    ordinal_timer
        Calculates ordinal time from the timestamps, required for slope
        calculations

    """
    def __init__(self, measurement_dict, measurement_files):
        self.measurement_dict = measurement_dict
        self.measurement_files = measurement_files
        self.measurement_df = self.read_measurement(self.measurement_dict)

    def read_measurement(self, dict):
        """
        Reads the measurement file into a pandas.dataframe

        args:
        ---
            dict -- dictionary
                The part of the .ini that defines that measurement .csv

        returns:
        ---
            dfs -- pandas.dataframe
                All of the .csv in self.measurement_files read into one
                big dataframe
        """
        path = dict.get('path')
        skiprows = int(dict.get('skiprows'))
        delimiter = dict.get('delimiter')
        names = dict.get('names').split(',')
        columns = list(map(int, dict.get('columns').split(',')))
        # oulanka has data from old licor software version which has no remark column
        columns_alternative = list(map(int, dict.get('columns_alternative').split(',')))
        dtypes = dict.get('dtypes').split(',')
        dtypes = {names[i]: dtypes[i] for i in range(len(names))}

        tmp = []
        for f in self.measurement_files:
            try:
                df = pd.read_csv(Path(path) / f,
                                 skiprows = skiprows,
                                 delimiter = '\t',
                                 usecols = columns,
                                 names = names,
                                 dtype = dtypes,
                                 )
            # there's old LICOR files which have an extra column,
            # this handles those
            except ValueError:
                df = pd.read_csv(Path(path) / f,
                                 skiprows = skiprows,
                                 delimiter = '\t',
                                 usecols = columns_alternative,
                                 names = names,
                                 dtype = dtypes,
                                 )
            tmp.append(df)
        dfs = pd.concat(tmp)
        dfs['datetime'] = pd.to_datetime(dfs['date'].apply(str)+' '+dfs['time'], format = '%Y-%m-%d %H:%M:%S')
        dfs['ordinal_date'] = pd.to_datetime(dfs['datetime']).map(datetime.datetime.toordinal)
        dfs['ordinal_time'] = dfs.apply(lambda row: self.ordinalTimer(row['time']),axis=1)
        dfs['ordinal_datetime'] = dfs['ordinal_time'] + dfs['ordinal_date']
        dfs.set_index('datetime', inplace=True)
        return dfs

    def ordinalTimer(self, time):
        """
        Helper function to calculate ordinal time from HH:MM:SS

        args:
        ---
            time -- numpy.array
                Array of HH:MM:SS string timestamps

        returns:
        ---
            time -- numpy.array
                Array of float timestamps
        """
        h,m,s = map(int, time.split(':'))
        sec = 60
        secondsInDay = 86400
        time = (sec*(sec*h)+sec*m+s)/secondsInDay
        return time

class chamber_cycle:
    """
    Creates the chamber cycle dataframe which will be used to select
    data that will be calculated

    Attributes
    ---
    file_timestamp_format -- str
        Format of the timestamp in the measurement files
    chamber_cycle_file -- str
        Path to the chamber cycle file
    chamber_measurement_start_sec -- int
        How many seconds past the opening of the chamber to start the
        actual measurement
    chamber_measurement_end_sec -- int
        How many seconds past the opening of the chamber to end the
        actual measurement
    measurement_files -- list
        List of the measurement file paths

    Methods
    ---
    call_extract
        Calls date extraction function from the timestamps class
    create_chamber_cycle
        Reads date from each filename, and creates dataframe with
        starting and ending times for each measurement

    """
    def __init__(self, file_timestamp_format, chamber_cycle_file, chamber_measurement_start_sec, chamber_measurement_end_sec, measurement_files):
        self.file_timestamp_format = file_timestamp_format
        self.chamber_cycle_file = chamber_cycle_file
        self.chamber_measurement_start_sec = chamber_measurement_start_sec
        self.chamber_measurement_end_sec = chamber_measurement_end_sec
        self.measurement_files = measurement_files
        self.chamber_cycle_df = self.create_chamber_cycle()

    def call_extract(self, file):
        """
        calls date extraction function from another class

        args:
        ---
            file -- str
                file name
        returns:
        ---
            datetime.datetime
        """
        return timestamps.extract_date(self, file)


    def create_chamber_cycle(self):
        """
        Reads a csv file with the chamber cycles

        args:
        ---

        returns:
        ---
            dfs -- pandas.dataframe
                dataframe with chamber opening times, measurement
                starting times and measurement ending times.
        """
        tmp = []
        for file in self.measurement_files:
            df = pd.read_csv(self.chamber_cycle_file,
                              names = ['time', 'chamber'])
            date = self.call_extract(os.path.splitext(file)[0])
            df['date'] = date
            df['datetime'] = pd.to_datetime(df['date'].astype(str)+' '+df['time'].astype(str))
            df['open_time'] = df['datetime'] + pd.to_timedelta(self.chamber_measurement_start_sec, unit='S')
            df['close_time'] = df['datetime'] + pd.to_timedelta(self.chamber_measurement_end_sec, unit='S')
            tmp.append(df)
        dfs = pd.concat(tmp)
        dfs.set_index('datetime', inplace=True)
        return dfs


class file_finder:
    """
    Generates lists of filenames and then finds them.

    Attributes
    ---
    path -- str
        path to the file
    file_timestamp_format -- str
        format of the timestamp in the filename, in strftime format
    file_timestep -- int
        How many seconds is between each file
    start_timestamp -- str
        The timestamp where the file name generation will start, either
        from influxdb or season_start in .ini
    end_timestamp -- str
        The timestamp where the file name generation will end, from
        the newest file in the input data
    scan_or_generate -- bool
        To generate filenames or just scan them from the folder
    measurement_files -- list
        List of the final measurement files that will be read

    Methods
    ---
    generate_filenames
        Generates the list of filenames
    match_files
        Checks that all of the generated filenames can be found
    """
    def __init__(self, measurement_dict, airdatatimestep, start_timestamp, end_timestamp):
        self.path = measurement_dict.get('path')
        self.file_timestamp_format = measurement_dict.get('file_timestamp_format')
        self.file_timestep = airdatatimestep
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp
        self.scan_or_generate = int(measurement_dict.get('scan_or_generate'))
        self.measurement_files = self.match_files(self.generate_filenames())


    def generate_filenames(self):
        """
        Generates filenames by adding file_timestep to the timestamp in
        the filename

        args:
        ---

        returns:
        ---
            filenames -- list
                list of the filenames
        """
        start_date = self.start_timestamp
        end_date = self.end_timestamp

        filenames = []
        current_date = start_date

        new_filename = 'init'
        while current_date <= end_date:
            filename = current_date.strftime(self.file_timestamp_format)
            if filename == new_filename:
                return [filename]
            filenames.append(filename)
            current_date += datetime.timedelta(seconds=self.file_timestep)
            new_filename = filename

        filenames = sorted(filenames)
        return filenames

    def match_files(self, patterns):
        """
        Finds the files listed in the list of filenames and returns a
        list of the filenames that were found

        args:
        ---
            patterns -- list
                This is the list of filenames

        returns:
        ---
            files -- list
                List of filepaths
        """
        files = []
        p = Path(self.path)
        if self.scan_or_generate == 1:
            for filestr in patterns:
                filestr = f'*{filestr}*'
                try:
                    file = p.rglob(filestr)
                    file = [x for x in file if x.is_file()]
                    file = os.path.basename(str(file[0]))
                except IndexError:
                    continue
                files.append(file)
        # if scan or generate is 0, file_timestamp_format should generate complete filenames
        if self.scan_or_generate == 0:
            return patterns
        return files


class timestamps:
    """
    Gets timestamps between which the files that will be calculated
    / uploaded are selected

    Attributes
    ---
    influxdb_dict -- dict
        Influxdb part of the .ini
    season_start -- str
        This date will be used as starting date if there's no data in
        the influxdb bucket
    path -- str
        Path where to look for files
    file_extension -- str
        file extension of the file that will be scanned for
    file_timestamp_format -- str
        format of the timestamp in the filename, in strftime
    start_timestamp -- str
        Start date for the file name generation
    end_timestamp -- str
        End date for the file name generation

    Methods
    ---
    strftime_to_regex
        Converts strftime to regex, for file matching
    get_newest
        Gets newest file in a folder
    extract_date
        Extracts the date that is given in file_timestamp_format from
        the filename
    check_last_db_timestamp
        Checks the last timestamp in influxdb
    check_timestamp
        Terminates the script if start_timestamp is older than the
        end_timestamp, it means that there's no new data to push to db
    """
    def __init__(self, influxdb_dict, measurement_dict, season_start):
        self.influxdb_dict = influxdb_dict
        self.season_start = season_start
        self.path = measurement_dict.get('path')
        self.file_extension = measurement_dict.get('file_extension')
        self.file_timestamp_format = measurement_dict.get('file_timestamp_format')
        self.start_timestamp = self.check_last_db_timestamp()
        self.end_timestamp = self.extract_date(self.get_newest())
        self.check_timestamp()

    def strftime_to_regex(self):
        """
        Changes the strftime timestamp to regex format

        args:
        ---

        returns:
        ---
            file_timestamp_format in regex

        """
        conversion_dict = {
            "%a": r"(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)",
            "%A": r"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)",
            "%b": r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)",
            "%B": r"(?:January|February|March|April|May|June|July|August|September|October|November|December)",
            "%d": r"(?P<day>\d{2})",
            "%H": r"(?P<hour>\d{2})",
            "%I": r"(?P<hour>\d{2})",
            "%m": r"(?P<month>\d{2})",
            "%M": r"(?P<minute>\d{2})",
            "%p": r"(?:AM|PM)",
            "%S": r"(?P<second>\d{2})",
            "%Y": r"(?P<year>\d{4})",
            "%y": r"(?P<year>\d{2})",
            "%%": r"%",
        }

        regex_pattern = re.sub(
            r"%[aAbBdHIImMpPSYy%]", lambda m: conversion_dict.get(m.group(), m.group()), self.file_timestamp_format
        )

        return regex_pattern

    def get_newest(self):
        """
        Fetchest name of the newest file in a folder

        args:
        ---

        returns:
        ---
            Name of the newest file in a folder

        """
        files = list(Path(self.path).rglob(f'*{self.file_extension}*'))
        if not files:
            print(f'No files found in {self.path}')
            return None

        # linux only
        #newest_file = str(max([f for f in files], key=lambda item: item.stat().st_ctime))
        # cross platform
        newest_file = str(max(files, key = os.path.getmtime))
        return newest_file

    def extract_date(self, datestring):
        """
        Extracts the date from the filename

        args:
        ---
            datestring -- str
                The format of the timestamp in the filename

        returns:
        ---
            datetime.datetime
                timestamp in datetime.datetime format
        """
        #try:
        #    date = re.search(self.strftime_to_regex(), datestring).group(0)
        #except AttributeError:
        #    print('Files are found in folder but no matching file found, is the format of the timestamp correct?')
        #    return None
        if self.file_timestamp_format == timestamps.strftime_to_regex(self):
            logging.info('No strftime formatting in filename, returning current date')
            print(datetime.date.today())
            return datetime.datetime.today()
        date = re.search(timestamps.strftime_to_regex(self), datestring).group(0)
        # class chamber_cycle calls this method and using an instance
        # variable here might cause issues if the timestamp formats
        # should be different
        return datetime.datetime.strptime(date, self.file_timestamp_format)

    def check_last_db_timestamp(self):
        """
        Extract latest date from influxDB

        args:
        ---

        ---
        returns:
            lastTs -- datetime.datetime
                Either the last timestamp in influxdb or season_start from .ini
        """
        # inflxudb query to get the timestamp of the last input
        query = f'from(bucket: "{self.influxdb_dict.get("bucket")}")' \
          '|> range(start: 0, stop: now())' \
          f'|> filter(fn: (r) => r["_measurement"] == "{self.influxdb_dict.get("measurement_name")}")' \
          '|> keep(columns: ["_time"])' \
          '|> sort(columns: ["_time"], desc: false)' \
          '|> last(column: "_time")'

        client = ifdb.InfluxDBClient(url = self.influxdb_dict.get('url'),
                             token = self.influxdb_dict.get('token'),
                             org = self.influxdb_dict.get('organization'),
                             )
        tables = client.query_api().query(query=query)
        # Get last timestamp from influxDB and if it doesn't exist, use
        # one defined in the .ini
        try:
            # removes timezone info from data, will this have implications in the future?
            lastTs = tables[0].records[0]['_time'].replace(tzinfo=None)
            logging.info("Got last timestamp from influxdb.")
        #lastTs = lastTs.strftime('%Y%m%d')
        except IndexError:
            #if there's no timestamp, use some default one
            logging.warning("Couldn't get timestamp from influxdb, using season_start from .ini")
            lastTs = datetime.datetime.strptime(self.season_start, self.influxdb_dict.get('influxdb_timestamp_format'))
        return lastTs

    def check_timestamp(self):
        """
        Compare the start and end timestamps, if start timestamp is
        older than end timestamp, terminate script as then there's no
        new data

        args:
        ---

        returns:
        ---

        """
        if self.start_timestamp > self.end_timestamp:
            logging.info('No files newer than what is already in the database.')
            logging.info('Exiting.')
            sys.exit(0)
        else:
            pass

class zip_open:
    """
    Opens eddypro .zip files and reads the .csv file inside them

    Attributes
    ---
    zip_files -- list
        List of the .zip_files to read
    measurement_dict -- dict
        measurement file part from the .ini
    path -- str
        file path
    names -- list
        list of the new names for the columns to be read
    columns -- list
        List of columns to be read
    delimiter -- str
        Delimiter of the .csv file
    skiprows -- int
        How many rows to skip from the beginning of file
    data -- pandas.dataframe
        Dataframe of the .csv files that are read from inside the
        eddypro zips

    Methods
    ---
    open_eddypro
        opens zipfiles and reads .csv files inside them

    """
    def __init__(self, zip_files, measurement_dict):
        self.zip_files = zip_files
        self.measurement_dict = measurement_dict
        self.path = self.measurement_dict.get('path')
        self.names = self.measurement_dict.get('names').split(',')
        self.columns = list(map(int, self.measurement_dict.get('columns').split(',')))
        #self.columns = self.measurement_dict.get('columns')
        #columns = list(map(int, dict.get('columns').split(',')))
        self.delimiter = self.measurement_dict.get('delimiter')
        self.skiprows = int(self.measurement_dict.get('skiprows'))

        self.data = self.open_eddypro()

    def open_eddypro(self):
        """
        Opens eddypro zips and reads the .csv files inside them into
        pandas.dataframes

        args:
        ---

        returns:
        ---
            ready_data -- pandas.dataframe
                the .csv files from inside the zip files read into a
                pandas.dataframe
        """
        dfList = []
        for i in self.zip_files:
            try:
                # creates a list of the matched files
                zip_file_list = Path(self.path).rglob(i)
                # cheks that everything that matched is file, not a
                # folder
                zip_file_list = [x for x in zip_file if x.is_file()]
                zip_file = str(zip_file[0])
                archive = zf.ZipFile(zip_file , 'r')
            # skip zips that have errors
            except BadZipFile:
                continue
            # get list of all files and folders inside .zip
            files = archive.namelist()
            # remove all files that don't have 'eddypro_exp_full' in
            # them, this should leave only one file
            file = [x for x in files if 'eddypro_exp_full' in x]
            if len(file) == 1:
                filename = file.pop()
            else:
                continue
            csv = archive.open(filename)
            ec = pd.read_csv(csv,
                             usecols = self.columns,
                             skiprows = self.skiprows,
                             names = self.names,
                             na_values = 'NaN'
                            )
            # this file should only have one row, skip if there's more
            if len(ec) != 1:
                continue
            dfList.append(ec)
        ready_data = pd.concat(dfList)
        ready_data['datetime'] = pd.to_datetime(ready_data['date'].apply(str)+' '+ready_data['time'], format = '%Y-%m-%d %H:%M')
        ready_data = ready_data.set_index(ready_data['datetime'])
        ready_data = ready_data.drop(columns = ['date', 'time', 'datetime'])
        return ready_data

class csv_reader:
    """
    Reads .csv files into pandas.dataframes

    Attributes
    ---
    files -- list
        list of the .csv files to be read
    measurement_dict -- dict
        part of the .ini which defines the .csv file
    data -- pandas.dataframe
        the .csv file read into a pandas.dataframe

    Methods
    ---
    csv_ini_parser
        parses the .ini dictionary
    add_csv_data
        Reads the .csv defined in the .ini into a pandas.dataframe

    """
    def __init__(self, csv_files, measurement_dict):
        self.files = csv_files
        self.measurement_dict = measurement_dict
        self.data = self.add_csv_data(self.measurement_dict)
        #print(self.files)


    def csv_ini_parser(self, ini_dict):
        """
        Parser the .ini to variables

        args:
        ---
            ini_dict -- dictionary
                The part of the .ini which defines the .csv file
        returns:
        ---
            path -- str
                path to the file
            delimiter -- str
                .csv delimiter
            skiprows -- int
                how many rows to skip at beginning of file
            timestamp_format -- str
                timestamp format of the .csv timestamp column
            file_extension -- str
                file extension of the file to be read
            columns -- list of ints
                list of the column numbers which will be read from the .csv
            names -- list of strings
                names for the columns that will be read
            dtypes -- dict
                columns and names paired into a dict
        """
        dict = ini_dict
        path = dict.get('path')
        # unnecessary?
        #file_timestamp_format = dict.get('file_timestamp_format')
        #if file_timestamp_format == '':
        #    files = [p for p in Path(path).glob(f'*{file}*') if '~' not in str(p)]

        delimiter = dict.get('delimiter')
        file_extension = dict.get('file_extension')
        skiprows = int(dict.get('skiprows'))
        timestamp_format = dict.get('timestamp_format')
        columns = list(map(int,dict.get('columns').split(',')))
        names = dict.get('names').split(',')
        dtypes = dict.get('dtypes').split(',')
        dtypes = {names[i]: dtypes[i] for i in range(len(names))}
        return path, delimiter, skiprows, timestamp_format, file_extension, columns, names, dtypes

    def add_csv_data(self, ini_dict):
        """
        Reads the .csv files into pandas.dataframes as defined in the .ini

        args:
        ---
            ini_dict -- dictionary
                The part of the .ini file that defines the .csv file
        returns:
        ---
            dfs -- pandas.dataframe
                All of the .csv files read into a pandas.dataframe

        """
        path, delimiter, skiprows, timestamp_format, file_extension, columns, names, dtypes = self.csv_ini_parser(ini_dict)
        tmp = []
        date_and_time = ['date', 'time']
        for f in self.files:
            try:
                print(names)
                print(columns)
                print(dtypes)
                f = f + file_extension
                df = pd.read_csv(Path(path) / f,
                                 skiprows = skiprows,
                                 delimiter = delimiter,
                                 usecols = columns,
                                 names = names,
                                 dtype = dtypes
                                 )
            except FileNotFoundError:
                logging.info(f'File not found at {Path(path) / f}, make sure the .ini is correct')
                sys.exit()
            # check if date and time are separate columns
            check = any(item in date_and_time for item in names)
            # if date and time are separate, combine them to datetime
            if check == True:
                df['datetime'] = pd.to_datetime(df['date'].apply(str)+' '+df['time'], format = timestamp_format)
            # if they are not separate, there should be a datetime column already
            else:
                df['datetime'] = pd.to_datetime(df['datetime'], format = timestamp_format)
            df.set_index('datetime', inplace = True)
            tmp.append(df)
        dfs = pd.concat(tmp)
        return dfs

def eddypro_push(inifile):
    """
    Pipeline to handle reading eddypro .zip files, reading the flux
    from inside them and pusing that to influxdb

    args:
    ---
        inifile -- str
            path to the .ini file

    returns:
    ---

    """
    config = configparser.ConfigParser()
    config.read(inifile)

    defaults_dict = dict(config.items('defaults'))
    influxdb_dict = dict(config.items('influxDB'))
    measurement_dict = dict(config.items('measurement_data'))

    timestamps_values = timestamps(influxdb_dict,
                          measurement_dict,
                          defaults_dict.get('season_start')
                            )

    measurement_files = file_finder(measurement_dict,
                                   int(defaults_dict.get('airdatatimestep')),
                                   timestamps_values.start_timestamp,
                                   timestamps_values.end_timestamp
                                    )
    data = zip_open(measurement_files.measurement_files, measurement_dict)
    print(data.data)
    pusher(data.data, influxdb_dict)

def csv_push(inifile):
    """
    Reads .csv files and pushes them to influxdb

    args:
    ---
        inifile -- str
            path to the .ini file

    returns:
    ---

    """
    config = configparser.ConfigParser()
    config.read(inifile)

    defaults_dict = dict(config.items('defaults'))
    influxdb_dict = dict(config.items('influxDB'))
    measurement_dict = dict(config.items('measurement_data'))

    timestamps_values = timestamps(influxdb_dict,
                          measurement_dict,
                          defaults_dict.get('season_start')
                            )

    measurement_files = file_finder(measurement_dict,
                                   int(defaults_dict.get('airdatatimestep')),
                                   timestamps_values.start_timestamp,
                                   timestamps_values.end_timestamp
                                    )
    data = csv_reader(measurement_files.measurement_files, measurement_dict)
    print(data.data)
    #pusher(data.data, influxdb_dict)

def push_ac(inifile):
    """
    Function to handle flux calculation and influx pushing pipeline

    args:
    ---
        inifile -- str
            path to the .ini file

    returns:
    ---

    """
    config = configparser.ConfigParser()
    config.read(inifile)

    defaults_dict = dict(config.items('defaults'))
    measurement_time_dict = dict(config.items('chamber_start_stop'))
    influxdb_dict = dict(config.items('influxDB'))
    air_pressure_dict = dict(config.items('air_pressure_data'))
    air_temperature_dict = dict(config.items('air_temperature_data'))
    measuring_chamber_dict = dict(config.items('measuring_chamber'))
    measurement_dict = dict(config.items('measurement_data'))
    get_temp_and_pressure_from_file = int(defaults_dict.get('get_temp_and_pressure_from_file'))

    timestamps_values = timestamps(influxdb_dict,
                          measurement_dict,
                          defaults_dict.get('season_start')
                            )

    measurement_files = file_finder(measurement_dict,
                                   int(defaults_dict.get('airdatatimestep')),
                                   timestamps_values.start_timestamp,
                                   timestamps_values.end_timestamp
                                    )

    air_pressure_files = file_finder(air_pressure_dict,
                                   int(defaults_dict.get('airdatatimestep')),
                                   timestamps_values.start_timestamp,
                                   timestamps_values.end_timestamp
                                     )
    air_temperature_files = file_finder(air_temperature_dict,
                                   int(defaults_dict.get('airdatatimestep')),
                                   timestamps_values.start_timestamp,
                                   timestamps_values.end_timestamp
                                     )

    chamber_cycle_df = chamber_cycle(measurement_dict.get('file_timestamp_format'),
                                     defaults_dict.get('chamber_cycle_file'),
                                     int(measurement_time_dict.get('start_of_measurement')),
                                     int(measurement_time_dict.get('end_of_measurement')),
                                     measurement_files.measurement_files
                                     )

    measurement_df = measurement_reader(measurement_dict,
                                        measurement_files.measurement_files)

    if get_temp_and_pressure_from_file == 1:
        air_temperature_df = aux_data_reader(air_temperature_dict,
                                      air_pressure_files.measurement_files)

        air_pressure_df = aux_data_reader(air_pressure_dict,
                                      air_pressure_files.measurement_files)


    # list with three values, start_time, end_time, chamber_num, flux is
    # calculated from the data between start and end times
    filter_tuple = list(zip(chamber_cycle_df.chamber_cycle_df['open_time'],chamber_cycle_df.chamber_cycle_df['close_time'] + datetime.timedelta(0,1),chamber_cycle_df.chamber_cycle_df['chamber']))

    filtered_measurement = filterer(filter_tuple,
                                    measurement_df.measurement_df)

    # same list as before but the timestamps witno data or invalid data
    # dropped
    filter_tuple = filtered_measurement.clean_filter_tuple

    air_temperature_df = filterer(filter_tuple,
                                  air_temperature_df.aux_data_df)
    air_pressure_df = filterer(filter_tuple,
                                  air_pressure_df.aux_data_df)

    snowdepth_df = snowdepth_parser(defaults_dict.get('snowdepth_measurement'),)

    if get_temp_and_pressure_from_file == 1:
        merged_data = merge_data(filtered_measurement.filtered_data,
                                 air_temperature_df.filtered_data)
        merged_data = merge_data(merged_data.merged_data,
                                 air_pressure_df.filtered_data)
        merged_data = merge_data(merged_data.merged_data,
                                 snowdepth_df.snowdepth_df)
    else:
        merged_data = merge_data(filtered_measurement.filtered_data,
                                 snowdepth_df.snowdepth_df)

    merged_data.merged_data['snowdepth'] = 0

    ready_data = calculated_data(merged_data.merged_data,
                                      measuring_chamber_dict, filter_tuple,
                                      get_temp_and_pressure_from_file,
                                 float(defaults_dict.get('default_pressure')),
                                 float(defaults_dict.get('default_temperature'))
                                      )
    print(ready_data.upload_ready_data.head())
    pusher(ready_data.upload_ready_data, influxdb_dict)


if __name__=="__main__":
    inifile = sys.argv[1]
    mode = sys.argv[2]
    if mode == 'ac':
        start = timeit.default_timer()
        push_ac(inifile)
        stop = timeit.default_timer()
        execution_time = stop - start
        print("Program Executed in "+str(execution_time))
    if mode == 'man':
        sys.exit(0)
    if mode == 'csv':
        start = timeit.default_timer()
        csv_push(inifile)
        stop = timeit.default_timer()
        execution_time = stop - start
        print("Program Executed in "+str(execution_time))
    if mode == 'eddypro':
        start = timeit.default_timer()
        eddypro_push(inifile)
        stop = timeit.default_timer()
        execution_time = stop - start
        print("Program Executed in "+str(execution_time))
    #print(measurement_df.measurement_df)
    #print(air_temperature_df.aux_data_df)
    #print(air_pressure_df.aux_data_df)
    #print(filtered_measurement.filtered_data)
    #print(merged_data.merged_data)
    #print(calculated_data.calculated_data)

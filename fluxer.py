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
            df -- pandas dataframe
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
        Reads tag columns from .ini and checks that they are in the file, and returns them as list
        """
        tag_columns = self.influxdb_dict.get('tag_columns').split(',')
        measurement_columns = self.data.columns
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
        snowdepth_measurement : xlsx file
            .xlsx file for with snowdepth for each chamber
        snowdepth_df : pandas dataframe 
            dataframe version of the .xlsx file.

    Methods
    ---
    add_snowdepth()
        Reads snowdepth measurement if it exists, or creates a dummy one.
    """
    def __init__(self, snowdepth_measurement):
        self.snowdepth_measurement = snowdepth_measurement
        self.snowdepth_df = self.add_snowdepth()

    def add_snowdepth(self):
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
    def __init__(self, measured_data, measuring_chamber_dict, filter_tuple, get_temp_and_pressure_from_file, default_pressure, default_temperature):
        self.get_temp_and_pressure_from_file = get_temp_and_pressure_from_file
        self.chamber_height = int(measuring_chamber_dict.get('chamber_height'))
        self.chamber_width = int(measuring_chamber_dict.get('chamber_width'))
        self.chamber_length = int(measuring_chamber_dict.get('chamber_length'))
        self.filter_tuple = filter_tuple
        self.default_pressure = default_pressure
        self.default_temperature = default_temperature
        self.calculated_data = self.calculate_slope_pearsons_r(measured_data, 'ch4')
        self.calculated_data = self.calculate_slope_pearsons_r(self.calculated_data, 'co2')
        self.upload_ready_data = self.summarize(self.calculated_data)


    def calculate_slope_pearsons_r(self, df, measurement_name):
        """
        Function to calculate Pearsons R (correlation) and the slope of
        the CH4 flux.
        ------
        ------
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
        flux calculation
        ------
        ------
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
        ------
        ------
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
    merges auxiliary data to the measurements.
    """
    def __init__(self, measurement_df, aux_df):
        self.merged_data = self.merge_aux_data(measurement_df, aux_df)

    def merge_aux_data(self, measurement_df, aux_df):
        if self.is_dataframe_sorted_by_datetime_index(measurement_df) and self.is_dataframe_sorted_by_datetime_index(aux_df):
            df = pd.merge_asof(measurement_df,
                                    aux_df,
                                    left_on = 'datetime',
                                    right_on = 'datetime',
                                    tolerance = pd.Timedelta('60m'),
                                    direction = 'nearest',
                                    suffixes =('','_y'))
            df.drop(df.filter(regex='_y$').columns, axis=1, inplace=True)
            df.set_index('datetime', inplace=True)
        else: 
            logging.info('Dataframes are not properly sorted by datetimeindex') 
            sys.exit(0)
        return df

    def is_dataframe_sorted_by_datetime_index(self, df):
        """
        Checks that both dataframes are sorted by a datetime index
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
    Removes empty data, data with errors and records the invalid data
    """
    def __init__(self, filter_tuple, df):
        self.clean_filter_tuple = []
        self.filter_tuple = filter_tuple
        self.filtered_data = self.filter_data_by_datetime(df)

    def filter_data_by_datetime(self, data_to_filter):
        """
        To do:
            proper handling of data with errores, currently they are
            just discarded. Timestamps which have no measuremeny data or
            only have data with errorcodes, should be recorded but
            dismissed.
            What to do with data that is there but the timestamps don't hit the chamber cycles?
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
        # tuples that are clean 
        self.clean_filter_tuple = clean_timestamps
        # dataframes with errors
        if len(dirty_data) != 0:
            self.dirty_df = pd.concat(dirty_data)
        # the timestamps which cover data with errors
        self.dirty_timestamps = dirty_timestamps
        self.empty_timestamps = empty_timestamps
        pd.options.mode.chained_assignment = 'warn'
        return clean_df


class aux_data_reader:
    """
    Reads auxiliary data
    """
    def __init__(self, aux_data_dict, files):
        self.aux_data_dict = aux_data_dict
        self.files = files
        self.aux_data_df = self.add_aux_data(self.aux_data_dict)
        

    def aux_data_ini_parser(self, ini_dict):
        """
        Parses then aux data dictionary from the .ini
        """
        dict = ini_dict
        path = dict.get('path')
        file = (dict.get('file'))
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
        Reads the .csv file as defined in the .ini
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
    def __init__(self, measurement_dict, measurement_files):
        self.measurement_dict = measurement_dict
        self.measurement_files = measurement_files
        self.measurement_df = self.read_measurement(self.measurement_dict)

    def read_measurement(self, dict):
        """
        Reads the measurement files according to the .ini
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
        Helper function to calculate ordinal time from HHMMSS
        ------
        inputs:
            HH:MM:SS timestamp
        ------
        returns:
            float of the timestamp in ordinal time
        """
        h,m,s = map(int, time.split(':'))
        sec = 60
        secondsInDay = 86400
        time = (sec*(sec*h)+sec*m+s)/secondsInDay
        return time

class chamber_cycle:
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
        """
        return timestamps.extract_date(self, file)


    def create_chamber_cycle(self):
        """
        Reads a csv file with the chamber cycles 
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
    Finds and generates filenames
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
    Gets starting and ending timestamps
    """
    def __init__(self, influxdb_dict, measurement_dict, season_start):
        self.influxdb_dict = influxdb_dict
        self.season_start = season_start
        self.path = measurement_dict.get('path')
        self.file_extension = measurement_dict.get('file_extension')
        self.file_timestamp_format = measurement_dict.get('file_timestamp_format')
        self.start_timestamp = self.checkLastDbTimestamp()
        self.end_timestamp = self.extract_date(self.get_newest())
        self.check_timestamp()

    def strftime_to_regex(self):
        """
        convert python strf timestamps to regex
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
        Extract date from file name
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
        # class chamber_cycle calls this method and using an instance variable here might cause issues if the timestamp formats should be different
        return datetime.datetime.strptime(date, self.file_timestamp_format)

    def checkLastDbTimestamp(self):
        """
        Extract latest date from influxDB
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
        # Get last timestamp from influxDB and if it doesn't exist, use a
        # hardcoded one
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
        Compare timestamps and stop script if there's no new data
        """
        if self.start_timestamp > self.end_timestamp:
            logging.info('No files newer than what is already in the database.')
            logging.info('Exiting.')
            sys.exit(0)
        else:
            pass

class zip_open:
    """
    Opens zip files
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
        dfList = []
        for i in self.zip_files:
            try:
                zip_file = Path(self.path).rglob(i)
                zip_file = [x for x in zip_file if x.is_file()]
                zip_file = str(zip_file[0])
                archive = zf.ZipFile(zip_file , 'r')
            except BadZipFile:
                continue
            files = archive.namelist()
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
            if len(ec) != 1:
                continue
            dfList.append(ec)
        ready_data = pd.concat(dfList)
        ready_data['datetime'] = pd.to_datetime(ready_data['date'].apply(str)+' '+ready_data['time'], format = '%Y-%m-%d %H:%M')
        ready_data = ready_data.set_index(ready_data['datetime'])
        ready_data = ready_data.drop(columns = ['date', 'time', 'datetime'])
        return ready_data

class csv_reader:
    def __init__(self, csv_files, measurement_dict):
        self.files = csv_files
        self.measurement_dict = measurement_dict
        self.data = self.add_csv_data(self.measurement_dict)
        #print(self.files)
        

    def csv_ini_parser(self, ini_dict):
        """
        Parses then aux data dictionary from the .ini
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
        Reads the .csv file as defined in the .ini
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
            if check == True:
                df['datetime'] = pd.to_datetime(df['date'].apply(str)+' '+df['time'], format = timestamp_format)
            else:
                df['datetime'] = pd.to_datetime(df['datetime'], format = timestamp_format)
            df.set_index('datetime', inplace = True)
            tmp.append(df)
        dfs = pd.concat(tmp)
        return dfs
    def read_csv(self):
        files = self.files

def eddypro_push(inifile):
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
    #pusher(data.data, influxdb_dict)

def csv_push(inifile):
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
    if measurement_dict.get('file_extension') == '.zip':
        data = zip_open(measurement_files.measurement_files, measurement_dict)
    else:
        data = csv_reader(measurement_files.measurement_files, measurement_dict)

        print(measurement_files.measurement_files)
        logging.warning('Some other function')
    print(data.data)
    #pusher(data.data, influxdb_dict)

def push_ac(inifile):
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
    
    snowdepth_df = snowdepth_parser(defaults_dict.get('snowdepth_measurement'),)

    if get_temp_and_pressure_from_file == 1:
        merged_data = merge_data(filtered_measurement.filtered_data,
                                 air_temperature_df.aux_data_df) 
        merged_data = merge_data(merged_data.merged_data,
                                 air_pressure_df.aux_data_df) 
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


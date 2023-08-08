import os
import re
import sys
import logging
import datetime
import configparser
import timeit
import glob
import pandas as pd
import influxdb_client as ifdb
from pathlib import Path
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS

logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

class measurement_data:
    def __init__(self, path, file_timestamp_format, file_extension, season_start, file_timestep, columns, column_names, column_dtypes, chamber_cycle_file, measurement_start, measurement_end):
        self.path = path
        self.file_timestamp_format = file_timestamp_format
        self.file_extension = file_extension
        self.file_timestep = file_timestep
        self.start_timestamp = checkLastDbTimestamp(bucket, measurementName, url, token, organization, seasonStart, influx_timestamp_format)
        self.end_timestamp = self.extract_date(self.get_newest())
        if self.start_timestamp > self.end_timestamp:
            logging.info('No files newer than what is already in the database.')
            logging.info('Exiting.')
            sys.exit(0)
        self.measurement_files = self.match_files(self.generate_filenames())
        self.columns = columns
        self.column_names = column_names
        self.column_dtypes = column_dtypes
        self.chamber_cycle_file = chamber_cycle_file
        self.measurement_start = measurement_start
        self.measurement_end = measurement_end
        self.chamber_cycle_df = self.create_chamber_cycle()
        self.measurement = self.read_measurement()
        self.starts = self.filter_data()

    def get_newest(self):
        """
        Fetchest name of the newest file in a folder
        """
        files = list(Path(self.path).glob(f'*{self.file_extension}*'))
        #files = glob.glob(os.path.join(self.path, f'*{self.file_extension}*'))
        if not files:
            print(f'No files found in {self.path}')
            return None

        newest_file = str(max([f for f in files], key=lambda item: item.stat().st_ctime))
        #newest_file = max(files, key=os.path.getctime)  
        #newest_file = os.path.basename(newest_file)
        return newest_file

    def extract_date(self, datestring):
        try: 
            date = re.search(self.strftime_to_regex(), datestring).group(0)
        except AttributeError:
            print('Files are found in folder but no matching file found, is the format of the timestamp correct?')
            return None
        return datetime.datetime.strptime(date, self.file_timestamp_format)

    def increment_to_next_day(self):
        if isinstance(self.file_timestamp_format, str):
            timestamp = datetime.datetime.strptime(self.file_timestamp_format, self.file_timestamp_format)
        
        next_day = timestamp + datetime.timedelta(days=1)
        next_day = next_day.replace(hour=0, minute=0, second=0, microsecond=0)
        
        return next_day.strftime(self.file_timestamp_format)

    def strftime_to_regex(self):
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

    #def generate_filenames(start_timestamp, end_timestamp, timestep_seconds, start_timestamp_format, end_timestamp_format, filename_format):
    def generate_filenames(self):
        start_date = self.start_timestamp
        end_date = self.end_timestamp

        filenames = []
        current_date = start_date

        while current_date <= end_date:
            filename = current_date.strftime(self.file_timestamp_format)
            filenames.append(filename)
            current_date += datetime.timedelta(seconds=self.file_timestep)

        filenames = sorted(filenames, reverse = True)
        return filenames

    def match_files(self, patterns):
        files = []
        p = Path(self.path)
        for filestr in patterns:
            filepath = p / filestr
            filepath = str(filepath)
            try:
                file = glob.glob(f'{filepath}*')[0]
                file = os.path.basename(file)
            except IndexError:
                continue
            files.append(file)
        return files[::-1]


    def read_measurement(self):
        tmp = []
        for f in self.measurement_files:
            df = pd.read_csv(Path(self.path) / f,
                             skiprows = 7,
                             delimiter = '\t',
                             usecols = self.columns,
                             names = self.column_names,
                             dtype = self.column_dtypes
                             )
            tmp.append(df)
        dfs = pd.concat(tmp)
        dfs['datetime'] = pd.to_datetime(dfs['date'].apply(str)+' '+dfs['time'])
        dfs['ordinal_date'] = pd.to_datetime(dfs['datetime']).map(datetime.datetime.toordinal)
        dfs['ordinal_time'] = dfs.apply(lambda row: ordinalTimer(row['time']),axis=1)
        dfs['ordinal_datetime'] = dfs['ordinal_time'] + dfs['ordinal_date']
        dfs.set_index('datetime', inplace=True)
        return dfs

    def create_chamber_cycle(self):
        tmp = []
        for file in self.measurement_files:
            df = pd.read_csv(self.chamber_cycle_file,
                              names = ['time', 'chamber'])
            date = self.extract_date(file)
            df['date'] = date
            df['datetime'] = pd.to_datetime(df['date'].astype(str)+' '+df['time'].astype(str)) 
            df['open_time'] = df['datetime'] + pd.to_timedelta(self.measurement_start, unit='S')
            df['close_time'] = df['datetime'] + pd.to_timedelta(self.measurement_end, unit='S')
            tmp.append(df)
        dfs = pd.concat(tmp)
        dfs.set_index('datetime', inplace=True)
        print(dfs)
        return dfs

    def filter_data(self):
        starts = self.chamber_cycle_df['open_time']
        closes = self.chamber_cycle_df['close_time']
        return starts

def ordinalTimer(time):
    """
    Helper function to calculate ordinal time from HHMMSS
    ------
    inputs:
        HH:MM:SS timestamp
    ------
    returns:
        float of the timestamp in ordinal time
    """
    h,m,s = time.split(':')
    h = int(h)
    m = int(m)
    s = int(s)
    sec = 60
    secondsInDay = 86400
    time = sec*(sec*h)+sec*m+s
    time = time / secondsInDay
    time = float(time)
    return time

url = '192.168.1.110:8089'
#token = jjHam24-jnCdK1QlT10IrgWqpK07u0D6vhm7eZZZZHK7A38bPAlgpVWdr4B59VarRa-Kqu04fPGUQeOWL5-kJw==
token = 'I6yqllNcyDYWSnShn3MNWpF4pXu93R2ep06c9lebBFbR6o0MQiOYT8LGEkUE6mnjHO6Bf4qClO4WgErnuo28DQ=='
measurementName	= 'fluxOulaTest21'
organization = 'Zack'
bucket = 'Zack'
timeout = 20000
# timezone of the data, needs to be in specific format
timezone = 'ETC/GMT-0'
seasonStart = '2023-02-07 00:00:00'
#influx_timestamp_format =  '%Y-%m-%d %H:%M:%S'
influx_timestamp_format =  '%Y-%m-%d %H:%M:%S'

def checkLastDbTimestamp(bucket, measurementName, url, token, organization, seasonStart, influx_timestamp_format):
# inflxudb query to get the timestamp of the last input
    query = f'from(bucket: "{bucket}")' \
      '|> range(start: 0, stop: now())' \
      f'|> filter(fn: (r) => r["_measurement"] == "{measurementName}")' \
      '|> keep(columns: ["_time"])' \
      '|> sort(columns: ["_time"], desc: false)' \
      '|> last(column: "_time")' 

    client = ifdb.InfluxDBClient(url = url,
                         token=token,
                         org = organization,
                         )
    tables = client.query_api().query(query=query)
    # Get last timestamp from influxDB and if it doesn't exist, use a
    # hardcoded one
    try:
        lastTs = tables[0].records[0]['_time']
        lastTs = datetime.datetime.strptime(lastTs, influx_timestamp_format)
    #print(type(lastTs))
    #print(lastTs)
    #lastTs = lastTs.strftime('%Y%m%d')
    except IndexError:
        #if there's no timestamp, use some default one
        lastTs = datetime.datetime.strptime(seasonStart, influx_timestamp_format)
        #lastTs = pd.Timestamp(seasonStart).strptime(influx_timestamp_format)
    return lastTs


def convert_timestamp(timestamp, current_format, new_format):
    # Parse the input timestamp using the current format
    dt = datetime.datetime.strptime(timestamp, current_format)

    # Format the datetime object with the new format
    #new_timestamp = dt.strftime(new_format)
    return dt

#data1 = measurement_data('/home/eerokos/zackenberg/tigSpace/python/fluxPipeline/measurementDataAC/', '%y%m%d', '.DAT', '2023-02-07', 86400, [3,5,6,7,8,9])
#data2 = measurement_data('/home/zack/zackenberg/tigspace/', '%y%m%d', '.DAT', '2023-02-07', 86400)


if __name__=="__main__":
    iniFile = sys.argv[1]
    config = configparser.ConfigParser()
    config.read(iniFile)

    defaults_dict = dict(config.items('defaults'))
    measurement_dict = dict(config.items('measurementData'))
    measurement_time_dict = dict(config.items('measuringTimesAC'))
    measurement_dtype_dict = dict(config.items('measurementDataDtypes'))
    print(measurement_dtype_dict)
    print(dict(config.items('defaults')))

    data1 = measurement_data(defaults_dict.get('measurementpath'),
                             defaults_dict.get('actimeformat'),
                             defaults_dict.get('file_extension'),
                             defaults_dict.get('seasonstart'),
                             int(defaults_dict.get('airdatatimestep')),
                             list(map(int,measurement_dict.get('columns').split(','))),
                             list(measurement_dict.get('names').split(',')),
                             measurement_dtype_dict,
                             defaults_dict.get('chamber_cycle_file'),
                             int(measurement_time_dict.get('measstart')),
                             int(measurement_time_dict.get('measend')))
    print(data1.path)
    print(data1.file_timestamp_format)
    print(data1.get_newest())
    print(data1.file_timestamp_format)
    print(data1.strftime_to_regex())
    print(data1.measurement_files)
    print(data1.measurement)
    print(data1.chamber_cycle_df)
    print(data1.starts)

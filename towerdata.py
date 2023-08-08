import os
import re
import datetime
import timeit
import glob
import pandas as pd
import influxdb_client as ifdb
from pathlib import Path
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS

def checkLastDbTimestamp(bucket, measurementName, url, token, organization, seasonStart):
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
        lastTs = pd.Timestamp(lastTs).strftime('%Y-%m-%d %H:%M:%S')
    #print(type(lastTs))
    #print(lastTs)
    #lastTs = lastTs.strftime('%Y%m%d')
    except IndexError:
        #if there's no timestamp, use some default one
        lastTs = pd.Timestamp(seasonStart).strftime('%Y-%m-%d %H:%M:%S')
    return lastTs


def generate_filenames(start_timestamp, end_timestamp, timestep_seconds, start_timestamp_format, end_timestamp_format, filename_format):
    """
    """

    print(start_timestamp)
    print(end_timestamp)
    start_date = datetime.datetime.strptime(start_timestamp, start_timestamp_format)
    end_date = datetime.datetime.strptime(end_timestamp, end_timestamp_format)

    filenames = []
    current_date = start_date

    while current_date <= end_date:
        filename = current_date.strftime(filename_format)
        filenames.append(filename)
        current_date += datetime.timedelta(seconds=timestep_seconds)

    filenames = sorted(filenames, reverse = True)
    return filenames

def get_newest(folder_path, filterstr):
    """
    Fetchest name of the newest file in a folder
    """
    files = glob.glob(os.path.join(folder_path, f'*{filterstr}*'))
    if not files:
        return None

    newest_file = max(files, key=os.path.getctime)  
    newest_file = os.path.basename(newest_file)
    return newest_file

def extract_date(file_name, date_format):
    date = re.search(date_format, file_name).group(0)
    return date


def convert_timestamp(timestamp, current_format, new_format):
    # Parse the input timestamp using the current format
    dt = datetime.datetime.strptime(timestamp, current_format)
    
    # Format the datetime object with the new format
    new_timestamp = dt.strftime(new_format)
    
    return new_timestamp

def match_files(strlist, filepath):
    files = []
    p = Path(filepath)
    for filestr in strlist:
        filepath = p / filestr
        filepath = str(filepath)
        try:
            file = glob.glob(f'{filepath}*')[0]
            file = os.path.basename(file)
        except IndexError:
            continue
        files.append(file)
    return files

def increment_to_next_day(timestamp, input_format_str, output_format_str):
    if isinstance(timestamp, str):
        timestamp = datetime.datetime.strptime(timestamp, input_format_str)
    
    next_day = timestamp + datetime.timedelta(days=1)
    next_day = next_day.replace(hour=0, minute=0, second=0, microsecond=0)
    
    return next_day.strftime(output_format_str)

if __name__=="__main__":
    # the filename pattern
    pattern2 = 'GL_Zaf_BM_%Y%m%d_L*'
    # the time pattern in the filename 

    path = '/media/synicos/DATA/GL-ZaF/AC_BACKUP/'
    fileTsFormat = '%Y%m%d'
    timestep2 = 86400
    ts = '20230707'



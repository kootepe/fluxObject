
def check_last_db_timestamp(influxdb_dict):
    """
    Extract latest date from influxDB

    args:
    ---

    returns:
    ---
    Tables -- object with infludbtimestamps 
    """
    # inflxudb query to get the timestamp of the last input
    query = f'from(bucket: "{influxdb_dict.get("bucket")}")' \
        '|> range(start: 0, stop: now())' \
        f'|> filter(fn: (r) => r["_measurement"] == "{influxdb_dict.get("measurement_name")}")' \
        '|> keep(columns: ["_time"])' \
        '|> sort(columns: ["_time"], desc: false)' \
        '|> last(column: "_time")'

    client = ifdb.InfluxDBClient(url=influxdb_dict.get('url'),
                            token=influxdb_dict.get('token'),
                            org=influxdb_dict.get('organization'),
                            )
    tables = client.query_api().query(query=query)
    try:
        last_ts = tables[0].records[0]['_time'].replace(tzinfo=None)
    except IndexError:
        # if there's no timestamp, return None
        logging.warning("Couldn't get timestamp from influxdb, using season_start from .ini")
        last_ts = None
    return last_ts


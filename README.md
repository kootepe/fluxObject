# Flux calculator
Script for calculating gas fluxes from automatic chamber and manual
measurements measured with LI-COR LI-7810.

# Intro

Script is mainly meant to be automated with a docker container, but it
can just as well be ran manually whenever you want.

Currently there are no file outputs, by default the script pushes data
into influxdb. Excel file outputs will be added and some point, and I
also have some plans for a jupyter notebook output.

## ini files
The ini files will define where to read files, what files to read, what
data to read in the files and the influxdb instances to push the data
to.

**examples for flux calculation** <br>
[Breakdown of the .ini for automatic chamber measurements.](./AC_sample.ini)<br>
[Breakdown of the .ini for manual chamber measurements.](./manual_sample.ini)

Timestamps in the ini need to be in [strftime](https://strftime.org/)
format. eg. 2023-01-01 00:00:00 would be %Y-%m-%d %H:%M:%S. And in the
.ini needs to be written with doubled '%' marks, as %%Y-%%m-%%d
%%H:%%M:%%S.
```
timestamp:
2023-01-01 00:00:00
in ini:
%%Y-%%m-%%d %%H:%%M:%%S
```

## Setting up automated calculations with docker

The docker image is built with the `docker-compose.yml` file. To avoid
having any paths in the codebase here, they are defined with a `.env`
file.

The docker image is built from the `dockerfile`, it builds an image which
has `cron` for timing script runs, `vim` for text the editor and copies this
`github` repo inside the container. In the docker-compose `volumes` part
are defined the folders inside your machine that will be mounted inside
the docker container, so that the data is visible for the container.


`Docker-compose.yml` is used to define how the build the docker image.

The format of the `.env` file is like this, it's used to define the
filepaths that will be mounted inside the container.

```.env
INI_DIR=/path/to/inis
INI_DIR_PATH=/data/inis
AUTOCHAMBER_DIR=/path/to/ac/data
AUTOCHAMBER_DIR_PATH=/data/autochamber_measurement
MANUAL_DIR=/path/to/manual/data
MANUAL_DIR_PATH=/data/manual_measurement
MANUAL_TIMES_DIR=/path/to/manual/times
MANUAL_TIMES_DIR_PATH=/data/manual_measurement_times
AIR_DATA_DIR=/path/to/air/data
AIR_DATA_DIR_PATH=/data/air_data
SNOW_MEASUREMENT=/path/to/snow/measurement
SNOW_MEASUREMENT_PATH=/data/snow_measurement
EXCEL_DIR=/path/to/excels
EXCEL_DIR_PATH=/data/excel_summaries
```

These paths are mapped into the docker container with the
`docker-compose.yml` which looks like this:

```yml
version: "3.4"
services:
  python:
    build: 
      context: ./
      dockerfile: ./dockerfile
    env_file: ./.env
    volumes:
      - ${AIR_DATA_DIR}:${AIR_DATA_DIR_PATH}
      - ${INI_DIR}:${INI_DIR_PATH}
      - ${AUTOCHAMBER_DIR}:${AUTOCHAMBER_DIR_PATH}:ro
      - ${MANUAL_DIR}:${MANUAL_DIR_PATH}:ro
      - ${MANUAL_TIMES_DIR}:${MANUAL_TIMES_DIR_PATH}
      - ${SNOW_MEASUREMENT}:${SNOW_MEASUREMENT_PATH}
      - ${EXCEL_DIR}:${EXCEL_DIR_PATH}
    entrypoint:
      ["/run.sh"]
```

The value pairs defined in the `.env` are mapped in the `volumes` part.
Left side is the directory on the computer hosting the docker container
and right side is where the contents of that folder will appear inside
the docker container. `:ro` means that the contents of the mapped folder
are read only in the container.

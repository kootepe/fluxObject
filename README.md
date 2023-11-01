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

The docker image is built from the dockerfile, it builds an image which
has cron for timing script runs, vim for text the editor and copies this
github repo inside the container. In the docker-compose `volumes` part
are defined the folders inside your machine that will be mounted inside
the docker container, so that the data is visible for the container.


`Docker-compose.yml` is used to define how the build the docker image.

The format of the `.env` file is like this, it's used to define the
filepaths that will be mounted inside the container.

```.env
AUTOCHAMBER_DIR=/path/to/autochamber/data
EDDY_DIR=/path/to/eddycovariance/data
MANUAL_DIR=/path/to/manual/measurement/data
MANUAL_TIMES_DIR=/path/to/manual/measurement/times/
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
    volumes:
      - $(AUTOCHAMBER_DIR):/data/AC_Oulanka:ro
      - $(EDDY_DIR):/data/EC_Oulanka:ro
      - $(MANUAL_TIMES_DIR):/data/manual_times_data:ro
      - $(MANUAL_DIR):/data/manual_data:ro
    entrypoint:
      ["/run.sh"]
```

The value pairs defined in the `.env` are mapped in the `volumes` part.
Left side is the directory on the computer hosting the docker container
and right side is where the contents of that folder will appear inside
the docker container. `:ro` means that the contents of the mapped folder
are read only in the container.

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

This will require some knowledge of setting up docker containers but it
isn't that hard. 

The docker image is built with the `docker-compose.yml` file. To avoid
having any paths in the codebase here, they are defined with a `.env`
file. 

The format of the file looks like this:

```
AUTOCHAMBER_DIR=/path/to/autochamber/data
EDDY_DIR=/path/to/eddycovariance/data
MANUAL_DIR=/path/to/manual/measurement/data
MANUAL_TIMES_DIR=/path/to/manual/measurement/times/
```

These value pairs will correspond to the `volumes:` part in the
`docker-compose.yml`. These file paths will be mounted inside the docker
container, and the python script will read the data from there. 

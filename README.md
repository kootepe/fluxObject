# Flux calculator
Script for calculating gas fluxes from automatic chamber and manual
measurements measured with LI-COR LI-7810.

# Setting up
Ini files will define what files to read, and their paths. the
directories where the data is will be in the .env file, where the
docker-compose file will take them and mount the folders into the
docker container so that the python script can read them. 


## ini files
The ini files will define where to read files, what files to read, what
data to read in the files and the influxdb instances to push the data
to.

** examples for flux calculation ** <br>
[Breakdown of the .ini for automatic chamber measurements.](./AC_sample.ini)<br>
[Breakdown of the .ini for manual chamber measurements.](./manual_sammple.ini)

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

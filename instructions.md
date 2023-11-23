# Instrucions on manual measurements

## Collecting manual measurement timestamps
The manual measurement files have a bit of an odd format since the file is
just a straight up copy of the paper that was used to collect the
timestamps before.

Templates for the manual measurement timestamp files can be found
[here]( https://github.com/kootepe/fluxObject/tree/main/manual_measurement_template ). I found [this](https://play.google.com/store/apps/details?id=com.farmerbb.notepad) android app quite good for collecting the measurements. It's just a very simple text editing app without any ads.


Everything above row 11 will discarded, they were used to mark data
about the weather when this was all on paper, you can fill them if you
like. There's enough time for that at least.

So only data below row 10 is used, the script reads it as a .csv file,
so if you have something in notes column, you shouldn't use commas.

Here in the example there are rows with just the plot number, so in
those there would be no measurement, they should be removed when done
measuring.

And it doesn't matter what plots are measurement in a file, I did the
measurements so that every other day I did odd plots and every other
evens. You can make your own templates if you have your own method.

File should be saved as YYMMDD.txt, eg. 230812.txt, 231224.txt. 

```.csv
Date,
Name,
Sky,
Temp,
Wind,
Precipitation,
ACSnowdepth,
FluxAvg,
AcFluxAvg,
Plot Number,Start Time,Notes,Chamber height
2,1030,,5
4,1036,,6
6,1042,,2
8,1052,something happened here,3
10,1059
12,1107,something happened here too,5
14,
16,
18,
20,
22,
24,
26,
28,
30,
32,
34,
36,
```

An example of a file that would be ready for calculation:

```.csv
Date,230812
Name,EK
Sky,clear
Temp,-2
Wind,slight
Precipitation,light snowfall
ACSnowdepth,
FluxAvg,
AcFluxAvg,
Plot Number,Start Time,Notes,Chamber height
2,1030,,5
4,1036,,6
6,1042,,2
8,1052,something happened here,3
10,1059
12,1107,something happened here too,5
```
## Quick instructions on running in docker

Clone the repo from [here](https://github.com/kootepe/fluxObject).
Extract into whatever directory you prefer.

Create a file called .env inside the fluxObject directory, paste the
template below into it. Now you need to fill the empty value pairs with
the paths on your own computer. Fill `INI_DIR=`, `MANUAL_DIR=`,
`MANUAL_TIMES_DIR=` with the paths where you place data that will be
processed. 

`INI_DIR` is the path to the .ini file which defines how to
the script is ran,<br> `MANUAL_DIR` is the path to the licor gas measurement
data,<br> `MANUAL_TIME_DIR` is the path to the timestamp file which were
instructed above.

```.env
# directory on the host with the ini files
INI_DIR=
# directory on the host with the autochamber measurement data files
AUTOCHAMBER_DIR= .\
# directory on the host with the manual measurement data files
MANUAL_DIR=
# directory on the host with the manual measurement timestamp files
MANUAL_TIMES_DIR=
# directory on the host with the airtemp and airpressure data files
AIR_DATA_DIR= .\
# directory on the host with the snowdepth measurement excel
SNOW_MEASUREMENT= .\
# directory on the host where the excel summaries will be placed
EXCEL_DIR= .\excel_outputs

# no need to change these
# name of the folder the inis will be in the container
INI_DIR_PATH=/data/inis
# name of the folder the inis will be in the container
AUTOCHAMBER_DIR_PATH=/data/autochamber_measurement
# name of the folder the manual measurement data will be inside the
# container
MANUAL_DIR_PATH=/data/manual_measurement
# name of the folder the manual measurement timestamps will be inside the
# container
MANUAL_TIMES_DIR_PATH=/data/manual_measurement_times
# name of the folder the air temp and pressure data files will be inside
# the container
AIR_DATA_DIR_PATH=/data/air_data
# name of the directory the snowdepth measurement excel will be inside
# the container
SNOW_MEASUREMENT_PATH=/data/snow_measurement
# name of the directory the excel summaries will be placed inside the
# container
EXCEL_DIR_PATH=/data/excel_summaries


```

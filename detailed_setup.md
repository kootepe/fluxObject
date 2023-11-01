# Instructions for setting up automated flux calculations with docker
1. Setting up .env
2. Setting up docker-compose.yml
3. Setting up crontab
4. Setting up .ini files


## Rundown

The docker image is built as defined in the `dockerfile`, the base image
is a small debian linux image with a stripped down version of python. On
top of that are installed `cron` for timing scripts, `vim` for text editing,
this github repo is copied into it for the python scripts, and then
the `inifiles` folder from the host machine is copied. `docker-compose.yml`
is used to define how the docker image is launched, it will mount the
data folders defined in the `.env` file into to docker container, so
that the python scripts have access to the data.


In the `docker-compose.yml` `volumes` part
are defined the folders inside your machine that will be mounted inside
the docker container, so that the data is visible for running the
scripts.

The format of the `.env` file is like this, it's used to define the
paths that will be mounted inside the container.

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

## Setting up the .ini files


### Autochamber .ini



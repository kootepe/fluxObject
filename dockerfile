FROM python:3.11-slim-bookworm

RUN apt-get update \
 && apt-get install -y git cron vim

RUN pip3 install install matplotlib

ADD https://api.github.com/repos/kootepe/fluxObject/git/refs/heads/main version.json
RUN git clone -b main https://github.com/kootepe/fluxObject.git

COPY run.sh .

COPY requirements.txt .

RUN pip install -r /fluxObject/requirements.txt

COPY crontab /etc/cron.d/crontab

RUN crontab /etc/cron.d/crontab

RUN touch /cronlog

RUN chmod +x run.sh

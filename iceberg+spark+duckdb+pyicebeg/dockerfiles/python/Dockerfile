FROM python:3.14

RUN apt-get update && apt-get -y upgrade

COPY ./requirements.txt .
RUN pip install -U pip
RUN pip install -r requirements.txt

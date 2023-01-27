# set base image(host OS)

FROM python:3.10.4-alpine3.15 AS builder
# set the working directory in the container

RUN apk update \
  && apk add gcc g++ linux-headers
  
WORKDIR /API/mobile_api/mobile_api_get_family_data/

COPY . /API/mobile_api/mobile_api_get_family_data/

# copy the dependencies file to the working directory
# install dependencies
# RUN python -m pip install --upgrade pip
RUN pip --no-cache-dir install -r requirements.txt
# This workers and threads are decided based on the below documentation on 21-Apr-2022 by Shreyas and Rajesh.
# https://medium.com/building-the-system/gunicorn-3-means-of-concurrency-efbb547674b7
# workers = (2*CPU)+1 and workers*threads = concurrent requests.
# Cloud Run concurrent request is configured as 80 so workers*threads = 72.
CMD exec gunicorn --workers 9 --threads 8 --timeout 0 main:app
# set base image(host OS)

FROM python:3.10.4-alpine3.15 AS builder
# set the working directory in the container

RUN apk update \
  && apk add gcc g++ linux-headers
  
WORKDIR /API/mobile_api/mobile_api_get_medical_history/

COPY . /API/mobile_api/mobile_api_get_medical_history/

# copy the dependencies file to the working directory
# install dependencies
# RUN python -m pip install --upgrade pip
RUN pip --no-cache-dir install -r requirements.txt
CMD exec gunicorn --workers 9 --threads 8 --timeout 0 main:app

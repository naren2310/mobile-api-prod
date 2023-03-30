# from google.cloud import spanner
from twilio.rest import Client
from datetime import datetime, timedelta
# from google.cloud import logging as cloudlogging

import config
import logging
import json
import os
import random
import re
import requests
import hashlib
import pytz

# log_client = cloudlogging.Client()

# log_handler = log_client.get_default_handler()
# cloud_logger = logging.getLogger("cloudLogger")
# cloud_logger.setLevel(logging.INFO)
# cloud_logger.setLevel(logging.DEBUG)
# cloud_logger.addHandler(log_handler)

# instance_id = os.environ.get('instance_id')
# database_id = os.environ.get('database_id')

# client = spanner.Client()
# instance = client.instance(instance_id)
# spnDB = instance.database(database_id)

#postgresql 
import psycopg2

conn = psycopg2.connect(
    host='142.132.206.93',  # hostname of the server
    database='postgres',  # database name
    user='tnphruser',  # username
    password='TNphr@3Z4'  # password
)

cursor = conn.cursor()

parameters = config.getParameters()

current_appversion = 'Prior V_3.1.4'
current_userId = 'NA' 

def setApp_Version(app_version):
    global current_appversion
    current_appversion = app_version
    return current_appversion

def validate_mobile_no(mobile_number):
    """
    Method will validate the supplied mobile number using regex
    to find any irrevelant characters exist.
    Input arg: mobile number (Integer)
    Return: Boolean 
    """
    try:
        is_valid_mobile = False        
        if re.fullmatch(parameters['MOBILE_NUMBER_FORMAT'], mobile_number):
            return True        
        return is_valid_mobile
    except Exception as error:
        print("Error in validating mobile number: %s | %s | %s", str(error), current_userId, current_appversion)
        # cloud_logger.error("Error in validating mobile number: %s | %s | %s", str(error), current_userId, current_appversion)
        return False
    
def reconnectToDB():
    global conn, cursor
    conn = psycopg2.connect(host='142.132.206.93',database='postgres',user='tnphruser',password='TNphr@3Z4')
    cursor = conn.cursor()
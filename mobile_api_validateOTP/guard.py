# from google.cloud import spanner
from datetime import datetime, timedelta
# from google.cloud import logging as cloudlogging

import json
import logging
import os
import config
import random
import jwt
import re

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

def validate_inputs(mobile_number, otp):
    """
    Method will validate inputs received as mobile number and OTP
    using regex to find out any irrevelant characters exist.
    Input args: mobile number and OTP
    Return: Boolean
    """
    try:
        is_valid_mobile = False
        is_valid_otp = False       
        if re.fullmatch(parameters['MOBILE_NUMBER_FORMAT'], mobile_number):
            is_valid_mobile = True
        if re.fullmatch(parameters['OTP_FORMAT'], otp):
            is_valid_otp = True        
        return is_valid_mobile, is_valid_otp
    except Exception as error:
        print("Error in validating mobile number and otp : %s | %s | %s", str(error), current_userId, current_appversion)
        # cloud_logger.error("Error in validating mobile number and otp : %s | %s | %s", str(error), current_userId, current_appversion)
        return False

def reconnectToDB():
    global conn, cursor
    conn = psycopg2.connect(host='142.132.206.93',database='postgres',user='tnphruser',password='TNphr@3Z4')
    cursor = conn.cursor()
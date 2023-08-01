
from twilio.rest import Client
from datetime import datetime, timedelta

import config
import logging
import json
import os
import random
import re
import requests
import hashlib
import pytz

parameters = config.getParameters()

#postgresql 
import psycopg2


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
        print("Error in validating mobile number : %s | %s | %s", str(error), current_userId, current_appversion)
        return False

# Write connection string 
def get_db_connection():
    conn = psycopg2.connect(host='10.236.221.123',database='tnphrprod',user='tnphruser',password='P3@PHRmdHT1@123')
    return conn

def reconnectToDB():
    global conn, cursor
    conn = psycopg2.connect(host='10.236.221.123',database='tnphrprod',user='tnphruser',password='P3@PHRmdHT1@123')
    cursor = conn.cursor()
    
# Read connection string 
def get_db_connection_read():
    conn = psycopg2.connect(host='10.236.220.126',database='tnphrprod',user='tnphruser',password='P3@PHRmdHT1@123')
    return conn 

def reconnectToDBRead():
    global conn, cursor
    conn = psycopg2.connect(host='10.236.220.126',database='tnphrprod',user='tnphruser',password='P3@PHRmdHT1@123')
    cursor = conn.cursor()
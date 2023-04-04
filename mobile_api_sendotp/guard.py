
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

def get_db_connection():
    conn = psycopg2.connect(host='142.132.206.93',database='postgres',user='tnphruser',password='TNphr@3Z4')
    return conn
    
def reconnectToDB():
    global conn, cursor
    conn = psycopg2.connect(host='142.132.206.93',database='postgres',user='tnphruser',password='TNphr@3Z4')
    cursor = conn.cursor()
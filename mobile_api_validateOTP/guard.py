
from datetime import datetime, timedelta

import json
import os
import config
import random
import jwt
import re

#postgresql 
import psycopg2

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
        return False

def get_db_connection():
    conn = psycopg2.connect(host='10.236.221.123',database='tnphrprod',user='tnphruser',password='P3@PHRmdHT1@123')
    return conn

def reconnectToDB():
    global conn, cursor
    conn = psycopg2.connect(host='10.236.221.123',database='tnphrprod',user='tnphruser',password='P3@PHRmdHT1@123')
    cursor = conn.cursor()
    
def get_db_connection_read():
    conn = psycopg2.connect(host='10.236.220.126',database='tnphrprod',user='tnphruser',password='P3@PHRmdHT1@123')
    return conn 

def reconnectToDBRead():
    global conn, cursor
    conn = psycopg2.connect(host='10.236.220.126',database='tnphrprod',user='tnphruser',password='P3@PHRmdHT1@123')
    cursor = conn.cursor()
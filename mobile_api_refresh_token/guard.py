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
from datetime import datetime

import os
import config
import json
import jwt
import re


#postgresql 
import psycopg2

parameters = config.getParameters()

current_appversion = 'Prior V_3.1.4'
current_userId = ''

def setApp_Version(appVersion):
    global current_appversion
    current_appversion = appVersion

def set_current_user(userId):
    global current_userId
    current_userId = userId

def validate_id(*ids):
    """
    This method will validate the Id attributes pattern 
    and formats using regular expression module.
    Input Arg: Ids (userId)
    Return: Boolean (True or False)
    """
    
    try:
        valid_ids = []
        for id in ids:
            if id != None and isinstance(id, str) and id != "" and id != " ":
                #id is composed of alphanumeric and hyphen characters
                #id pattern is compiled with alphanumeric and hyphen character in regex.
                id_pattern = re.compile(parameters['ID_PATTERN'])

                #id format is compiled below using regex.
                #sample id = "54cd29b1-ffad-46bb-8390-25a435b6a264"
                id_format = re.compile(parameters['ID_FORMAT'])
                # Checks whether the whole string matches the re.pattern or not
                if re.fullmatch(id_pattern, id) and id_format.match(id) and \
                        id != None and id != "" and len(id) == parameters['ID_LENGTH']:
                    valid_ids.append(True)
                else:
                    print("ID is not valid %s", str(id))
                    # cloud_logger.critical("ID is not valid %s", str(id))
                    valid_ids.append(False)
        if all(item == True for item in valid_ids) and len(valid_ids) != 0:
            return True
        else:
            print("One or more supplied ID not valid.")          
            return False
    except Exception as error:
        print("Error validating Id attribute format : %s | %s | %s", str(error), current_userId, current_appversion)
        return False

def user_token_validation(userId, mobile):
    """
    This method will validate token data belongs to 
    the registered user.
    Input args: mobile number, user id
    Return: Boolean
    """ 
    spnDB_userId = 0
    try:
        conn = get_db_connection_read()
        cursor = conn.cursor()
        query = "SELECT user_id FROM public.user_master WHERE mobile_number=%s AND user_id=%s"
        value = (mobile,userId)
        cursor.execute(query,value)
        results = cursor.fetchall()
        for row in results:
            spnDB_userId = row[0]       #user ID fetched from spannerDB using the mobile number
        if (spnDB_userId != 0):         #Condition to validate userId exist in spannerDB
            if (spnDB_userId == userId):
                return True
            else:
                print("Token is not valid for this user.")
                return False
        else:
            print("Unregistered User/Token-User mismatch.")          
            return False
    except psycopg2.ProgrammingError as e:
        print("get_facilities_data user_token_validation ProgrammingError",e)  
        conn.rollback()
        return False
    except psycopg2.InterfaceError as e:
        print("get_facilities_data user_token_validation InterfaceError",e)
        reconnectToDBRead()
        return False
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception as e:
            print("get_facilities_data",e)
 
    
def get_db_connection_read():
    conn = psycopg2.connect(host='10.236.220.126',database='tnphrprod',user='tnphruser',password='P3@PHRmdHT1@123')
    return conn 

def reconnectToDBRead():
    global conn, cursor
    conn = psycopg2.connect(host='10.236.220.126',database='tnphrprod',user='tnphruser',password='P3@PHRmdHT1@123')
    cursor = conn.cursor()
from datetime import datetime
from pytz import timezone


import config
import jwt
import json
import os
import re
import copy


parameters = config.getParameters()

#postgresql 
import psycopg2


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
                    print("ID is not valid. %s", str(id))
                    valid_ids.append(False)
        if all(item == True for item in valid_ids) and len(valid_ids) != 0:
            return True
        else:
            print("One or more supplied ID not valid..")         
            return False
    except Exception as error:
        print("Error validating Id attribute format : %s | %s | %s ", str(error), current_userId, current_appversion)
        return False

def user_token_validation(userId, mobile, user_facility_id):
    """
    This method will validate token data belongs to 
    the registered user.
    Input args: mobile number, user id
    Return: Boolean
    """ 
    spnDB_userId = 0
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = "SELECT user_id FROM public.user_master WHERE mobile_number=%s AND user_id=%s AND facility_id=%s"
        values = (mobile, userId, user_facility_id)
        cursor.execute(query,values)
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
            print("Unregistered User/Inputs mismatch.")        
            return False
    except psycopg2.ProgrammingError as e:
        print("member_screening_details user_token_validation ProgrammingError",e)  
        conn.rollback()
        return False
    except psycopg2.InterfaceError as e:
        print("member_screening_details user_token_validation InterfaceError",e)
        reconnectToDB()
        return False
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception as e:
            print("member_screening_details user_token_validation",e)

def validate_inputs(content):
    """
    Method will validate the received facility, member, family, screening ids and update
    register formats using regex to find any irrevelant characters exist.
    Input arg: content (request body)
    Return: Boolean
    """
    try:
        is_valid_inputs = True
        message = "Supplied inputs are valid."        
        if not validate_id(content['FACILITY_ID']):
            message = "Supplied FACILITY ID in request is empty or not valid."
            return False, message
        screening_list = content['screening_list']
        for screening_details in screening_list:
            for key, value in screening_details.items():
                if key == 'facility_id':
                    if not validate_id(value):
                        message = "Supplied facility Id in screening details is empty or not valid."
                        return False, message
                if key == 'family_id':
                    if not validate_id(value):
                        message = "Supplied family Id in screening details is empty or not valid."
                        return False, message
                if key == 'member_id':
                    if not validate_id(value):
                        message = "Supplied member Id in screening details is empty or not valid."
                        return False, message
                if key == 'screening_id':
                    if not validate_id(value):
                        message = "Supplied screening Id in screening details is empty or not valid."
                        return False, message
                if key == 'update_register':
                    for update_reg_key, update_reg_val in value.items():
                        if update_reg_key == 'timestamp':
                            if not re.fullmatch(parameters['TS_FORMAT'], update_reg_val) and isinstance(update_reg_val, str):
                                message = "Supplied timestamp in update register of screening details is empty or not valid."
                                return False, message
                        
        return is_valid_inputs, message
    except Exception as error:
        print("Error in validating inputs : %s | %s | %s ", str(error), current_userId, current_appversion)
        return False

def get_db_connection():
    conn = psycopg2.connect(host='142.132.206.93',database='postgres',user='tnphruser',password='TNphr@3Z4')
    return conn  

def reconnectToDB():
    global conn, cursor
    conn = psycopg2.connect(host='142.132.206.93',database='postgres',user='tnphruser',password='TNphr@3Z4')
    cursor = conn.cursor()
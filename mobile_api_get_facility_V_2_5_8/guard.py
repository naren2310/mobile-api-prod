from datetime import datetime
# from google.cloud import spanner
# from google.cloud.spanner_v1 import param_types
# from google.cloud import logging as cloudlogging

import os
import logging
import config
import json
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
            # cloud_logger.info("One or more supplied ID not valid.")            
            return False
    except Exception as error:
        print("Error validating Id attribute format : %s | %s | %s", str(error), current_userId, current_appversion)
        # cloud_logger.error("Error validating Id attribute format : %s | %s | %s", str(error), current_userId, current_appversion)
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
        query = "SELECT user_id FROM public.user_master WHERE mobile_number=%s AND user_id=%s"
        
        value = (mobile,userId)
        cursor.execute(query,value)
        results = cursor.fetchall()
        # with spnDB.snapshot() as snapshot: 
        #     results = snapshot.execute_sql(
        #         query,
        #         params={
        #             "mobile": mobile,
        #             "user_id": userId
        #         },
        #         param_types={
        #             "mobile": param_types.INT64,
        #             "user_id": param_types.STRING
        #         },                   
        #     )
        for row in results:
            spnDB_userId = row[0]       #user ID fetched from spannerDB using the mobile number
        if (spnDB_userId != 0):         #Condition to validate userId exist in spannerDB
            if (spnDB_userId == userId):
                return True
            else:
                print("Token is not valid for this user.")
                # cloud_logger.info("Token is not valid for this user.")  
                return False
        else:
            print("Unregistered User/Token-User mismatch.")
            # cloud_logger.info("Unregistered User/Token-User mismatch.")            
            return False
    except psycopg2.ProgrammingError as e:
        print("get_facilities_data user_token_validation ProgrammingError",e)  
        conn.rollback()
        return False
    except psycopg2.InterfaceError as e:
        print("get_facilities_data user_token_validation InterfaceError",e)
        reconnectToDB()
        return False
    
def reconnectToDB():
    global conn, cursor
    conn = psycopg2.connect(host='142.132.206.93',database='postgres',user='tnphruser',password='TNphr@3Z4')
    cursor = conn.cursor()
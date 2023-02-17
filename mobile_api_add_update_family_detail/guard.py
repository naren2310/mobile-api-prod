# from google.cloud import spanner
from datetime import datetime
# from google.cloud.spanner_v1 import param_types
# from google.cloud.spanner_v1.param_types import INT64, JSON, TIMESTAMP
# from google.cloud import logging as cloudlogging

import config
import jwt
import uuid
import json
import os
import logging
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
    host='10.236.221.123',  # hostname of the server
    database='tnphrprod',  # database name
    user='tnphruser',  # username
    password='P3@PHRmdHT1@123'  # password
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
                    print("ID is not valid. %s", str(id))
                    # cloud_logger.critical("ID is not valid. %s", str(id))
                    valid_ids.append(False)
        if all(item == True for item in valid_ids) and len(valid_ids) != 0:
            return True
        else:
            print("One or more supplied ID not valid..")
            # cloud_logger.info("One or more supplied ID not valid..")            
            return False
    except Exception as error:
        print("Error validating Id attribute format : %s | %s | %s ", str(error), current_userId, current_appversion)
        # cloud_logger.error("Error validating Id attribute format : %s | %s | %s ", str(error), current_userId, current_appversion)
        return False

def user_token_validation(userId, mobile, user_facility_id):
    """
    This method will validate token data belongs to 
    the registered user.
    Input args: mobile number, user id, user facility Id
    Return: Boolean
    """ 
    spnDB_userId = 0
    try:
        query = "SELECT user_id FROM public.user_master WHERE mobile_number=%s AND user_id=%s AND facility_id=%s"
        # with spnDB.snapshot() as snapshot: 
        #     results = snapshot.execute_sql(
        #         query,
        #         params={
        #             "mobile": mobile,
        #             "user_id": userId,
        #             "facility_id": user_facility_id
        #         },
        #         param_types={
        #             "mobile": param_types.INT64,
        #             "user_id": param_types.STRING,
        #             "facility_id": param_types.STRING
        #         }                   
        #     )
        value = (mobile,userId,user_facility_id)
        cursor.execute(query,value)
        results = cursor.fetchall()
        for row in results:
            spnDB_userId = row[0]       #user ID fetched from spannerDB using the mobile number
        if (spnDB_userId != 0):         #Condition to validate userId exist in spannerDB
            if (spnDB_userId == userId):
                return True
            else:
                print("Token is invalid for this user.")
                # cloud_logger.info("Token is invalid for this user.")  
                return False
        else:
            print("Unregistered User/Token-User mismatch.")
            # cloud_logger.info("Unregistered User/Token-User mismatch.")            
            return False
    except Exception as error:
        print("Error validating user token: : %s | %s | %s ", str(error), current_userId, current_appversion)
        # cloud_logger.error("Error validating user token: : %s | %s | %s ", str(error), current_userId, current_appversion)
        return False

def validate_inputs(content):
    """
    Method will validate the received facility, member, family, street ids and
    update register format using regex to find any irrevelant characters exist.
    Input arg: content (request body)
    Return: Boolean
    """
    try:
        is_valid_inputs = True
        message = "Supplied IDs are valid."        
        family_list = content['family_list']
        for family in family_list:
            #Checks provided family members count and family member details are same
            # Below 3 lines of code blocks for the case to add new member to family. Hence commenting them.
            #if family['family_details']['family_members_count'] != len(family['family_member_details']):
                #message = "Supplied family members count not matching with family member details."
                #return False, message
            for key, value in family['family_details'].items():
                if key == 'facility_id':
                    if not validate_id(value):
                        message = "Supplied facility Id in family details is empty or not valid."
                        return False, message                    
                if key == 'family_id':
                    if not validate_id(value):
                        message = "Supplied family Id in family details is empty or not valid."
                        return False, message
                if key == 'street_id':
                    if not validate_id(value):
                        message = "Supplied street Id in family details is empty or not valid."
                        return False, message
                if key == 'update_register':
                    for update_reg_key, update_reg_val in value.items():
                        if update_reg_key == 'timestamp':
                            if not re.fullmatch(parameters['TS_FORMAT'], update_reg_val) and isinstance(update_reg_val, str):
                                message = "Supplied timestamp in update register of family details is empty or not valid."
                                return False, message
                        if update_reg_key == 'user_id':
                            if update_reg_val != "system":#This condition is add to handle the case when user id = system is provided.
                                if not validate_id(update_reg_val):
                                    message = "Supplied user Id in update register of family details is empty or not valid."
                                    return False, message                                                           
            for member in family['family_member_details']:  
                for key, value in member['member_detail'].items():
                    if key == 'facility_id':
                        if not validate_id(value):
                            message = "Supplied facility Id in family member details is empty or not valid."
                            return False, message                        
                    if key == 'family_id':
                        if not validate_id(value):
                            message = "Supplied family Id in family member details is empty or not valid."
                            return False, message
                    if key == 'street_id':
                        if not validate_id(value):
                            message = "Supplied street Id in family member details is empty or not valid."
                            return False, message                
                    if key == 'member_id':
                        if not validate_id(value):
                            message = "Supplied member Id in family member details is empty or not valid."
                            return False, message
                    if key == 'update_register':
                        for update_reg_key, update_reg_val in value.items():
                            if update_reg_key == 'timestamp':
                                if not re.fullmatch(parameters['TS_FORMAT'], update_reg_val) and isinstance(update_reg_val, str):
                                    message = "Supplied timestamp in update register of family member details is empty or not valid."
                                    return False, message
                            if update_reg_key == 'user_id':
                                if update_reg_val != "system":#This condition is add to handle the case when user id = system is provided.
                                    if not validate_id(update_reg_val):
                                        message = "Supplied user Id in update register of family member details is empty or not valid."
                                        return False, message
                                    
        return is_valid_inputs, message
    except Exception as error:
        print("Error in validating inputs : %s | %s | %s ", str(error), current_userId, current_appversion)
        # cloud_logger.error("Error in validating inputs : %s | %s | %s ", str(error), current_userId, current_appversion)
        return False
from pytz import timezone
# from google.cloud import logging as cloudlogging
# from google.cloud import spanner
# from google.cloud.spanner_v1 import param_types

import logging
import os
import json
import config
import jwt
import re

# instance_id = os.environ.get('instance_id')
# database_id = os.environ.get('database_id')

# client = spanner.Client()
# instance = client.instance(instance_id)
# spnDB = instance.database(database_id)

# log_client = cloudlogging.Client()
# log_handler = log_client.get_default_handler()
# cloud_logger = logging.getLogger("cloudLogger")
# cloud_logger.setLevel(logging.INFO)
# cloud_logger.setLevel(logging.DEBUG)
# cloud_logger.addHandler(log_handler)

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

def validate_id_attribute(familyId, memberId):
    """
    This method will validate the family id and member id type and format
    Input args: ids (familyId and memberId)
    Return: Boolean
    """
    is_valid_id = False
    try:
        if familyId == "" and memberId == "":
            print("Both familyId and memberId are empty")
            # cloud_logger.critical("Both familyId and memberId are empty")
            return is_valid_id
        else:
            return validate_id(familyId, memberId)
    except Exception as error:
        print("Error validating Id attribute format : %s | %s | %s ", str(error), current_userId, current_appversion)
        # cloud_logger.error("Error validating Id attribute format : %s | %s | %s ", str(error), current_userId, current_appversion)
        return is_valid_id

def validate_id(*ids):
    """
    This method will validate the Id attributes pattern 
    and formats using regular expression module.
    Input Args: IDs (familyid and memberid)
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
        print("Error in validating inputs : %s | %s | %s ", str(error), current_userId, current_appversion)
        # cloud_logger.error("Error in validating inputs : %s | %s | %s ", str(error), current_userId, current_appversion)
        return False

def check_id_registered(familyId, memberId):
    """
    This method will check the family Id and member Id are registered.
    Input Args: Family ID, Member ID
    Return: Boolean
    """
    try:
        query = 'SELECT EXISTS(SELECT family_id FROM public.family_member_master'
        param = {}
        types = {}

        if familyId != "" and memberId != "": #When both the IDs are not empty
            query += ' WHERE family_id=%s and member_id=%s)'                    
            param['family_id'] = familyId
            param['member_id'] = memberId
            # types['family_id'] = param_types.STRING
            # types['member_id'] = param_types.STRING

        elif familyId != "" and memberId == "": #When only family id is not empty
            query += ' WHERE family_id=%s)'
            param['family_id'] = familyId
            # types['family_id'] = param_types.STRING

        elif familyId == "" and memberId != "": #when only member id is not empty
            query += ' WHERE member_id=%s)'
            param['member_id'] = memberId
            # types['member_id'] = param_types.STRING

        # with spnDB.snapshot() as snapshot: 
            # result = snapshot.execute_sql(query, params= param, param_types=types)
        value = (familyId,memberId)
        cursor.execute(query,value)
        result = cursor.fetchall()
        for row in result:
            id_exist = row[0]
        return id_exist
        
    except Exception as error:
        print("Error occurred in check id registered : %s| %s | %s ", str(error), current_userId, current_appversion)
        # cloud_logger.error("Error occurred in check id registered : %s| %s | %s ", str(error), current_userId, current_appversion)
        return False
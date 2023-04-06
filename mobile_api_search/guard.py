from pytz import timezone

import logging
import config
import json
import jwt
import os
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
            else:
                print("Supplied ID is empty or not valid %s", str(id))
                # cloud_logger.critical("Supplied ID is empty or not valid %s", str(id))
                valid_ids.append(False)
        if all(item == True for item in valid_ids) and len(valid_ids) != 0:
            return True
        else:
            print("One or more supplied ID not valid.")
            # cloud_logger.info("One or more supplied ID not valid.")            
            return False
    except Exception as error:
        print("Error validating Id attribute format : %s | %s | %s ", str(error), current_userId, current_appversion)
        # cloud_logger.error("Error validating Id attribute format : %s | %s | %s ", str(error), current_userId, current_appversion)
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
        conn = get_db_connection()
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
        print("get_search_details user_token_validation ProgrammingError",e)  
        conn.rollback()
        return False
    except psycopg2.InterfaceError as e:
        print("get_search_details user_token_validation InterfaceError",e)
        reconnectToDB()
        return False
    finally:
        cursor.close()
        conn.close()

def validate_mobile_no(mobile_number):
    """
    Method will validate the supplied mobile number using regex
    to find any irrevelant characters exist.
    Input arg: mobile number (Integer)
    Return: Boolean 
    """
    try:
        is_valid_mobile = False
        if re.fullmatch(parameters['MOBILE_NUMBER_FORMAT'], str(mobile_number)) and isinstance(mobile_number, int):
            return True        
        return is_valid_mobile
    except Exception as error:
        print("Error in validating mobile number : %s | %s | %s ", str(error), current_userId, current_appversion)
        return False

def validate_unique_health_id(unique_health_id):
    """
    Method will validate the supplied unique health Id using regex
    to find any irrevelant characters exist.
    Input arg: unique health id (String)
    Return: Boolean
    """
    try:
        is_valid_UHID = False
        if re.fullmatch(parameters['UNIQUE_HEALTH_ID_FORMAT'], unique_health_id) and isinstance(unique_health_id, str):
            return True        
        return is_valid_UHID
    except Exception as error:
        print("Error in validating unique health Id: %s | %s | %s ", str(error), current_userId, current_appversion)
        return False

def validate_pds_smart_card_id(pds_smart_card_id):
    """
    Method will validate the supplied pds smart card Id using regex
    to find any irrevelant characters exist.
    Input arg: pds smart card id (Integer)
    Return: Boolean
    """
    try:
        is_valid_PDSID = False
        if re.fullmatch(parameters['PDS_SMART_CARD_ID_FORMAT'], str(pds_smart_card_id)):
            return True        
        return is_valid_PDSID
    except Exception as error:
        print("Error in validating pds smart card Id: %s | %s | %s ", str(error), current_userId, current_appversion)
        return False

def validate_member_name_inputs(**kwargs):
    """
    Method will validate the supplied inputs using regex
    to find any irrevelant characters exist.
    Input arg: member name (string), district Id (string), 
                block id (string), village id (string)
    Return: Boolean
    """
    try:
        is_valid_inputs = True
        message = "Supplied Inputs are valid"
        for key, value in kwargs.items():
            if key == 'member_name':
                if not (re.fullmatch(parameters['NAME_FORMAT'], value) or not isinstance(value, str) 
                        or value == " " or value == ""):
                    message = "Member name is not valid. Please contact the Administrator."
                    return False, message
            elif key == 'district_id':
                if not validate_id(value):
                    message = "District id is empty or not valid. Please contact the Administrator."
                    return False, message
            elif key == 'block_id':
                if not validate_id(value):
                    message = "Block id is empty or not valid. Please contact the Administrator."
                    return False, message
            elif key == 'village_id' and value != "" and value != None:
                if not validate_id(value):
                    message = "Village id is empty or not valid. Please contact the Administrator."
                    return False, message
        return is_valid_inputs, message
    except Exception as error:
        print("Error in validating member name inputs: %s | %s | %s ", str(error), current_userId, current_appversion)
        return False

def check_id_registered(districtId, blockId, villageId):
    """
    This method will check the district Id, block Id and village Id are registered.
    Input Args: district_id, block_id and village_id
    Return: Boolean
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = 'SELECT EXISTS(SELECT district_id FROM public.address_village_master'
        param = {}
        # types = {}
        value = {}
        
        if districtId != "" and blockId != "" and villageId != "" and villageId != None: #When all the IDs are not empty
            query += ' WHERE district_id=%s AND block_id=%s AND village_id=%s)'                    
            param['district_id'] = districtId
            param['block_id'] = blockId
            param['village_id'] = villageId
            
            # types['district_id'] = param_types.STRING
            # types['block_id'] = param_types.STRING
            # types['village_id'] = param_types.STRING

        elif districtId != "" and blockId != "" and (villageId == "" or villageId == None): #when district and block Ids are not empty
            query += ' WHERE district_id=%s AND block_id=%s)'
            param['district_id'] = districtId
            param['block_id'] = blockId
            # types['district_id'] = param_types.STRING
            # types['block_id'] = param_types.STRING

        value = (districtId,blockId,villageId)
        cursor.execute(query,value)
        result = cursor.fetchall()
        for row in result:
            id_exist = row[0]
        return id_exist
    except psycopg2.ProgrammingError as e:
        print("get_search_details check_id_registered ProgrammingError",e)  
        conn.rollback()
        return False
    except psycopg2.InterfaceError as e:
        print("get_search_details check_id_registered InterfaceError",e)
        reconnectToDB()
        return False
    finally:
        cursor.close()
        conn.close()

def validate_search_parameter(search_parameter):
    """
    Method to validate the search parameter format.
    Input arg: search parameter
    Return: Boolean
    """
    try:
        is_valid_search_param = False
        if re.fullmatch(parameters['SEARCH_PARAMETER_FORMAT'], search_parameter) and isinstance(search_parameter, str):
            return True        
        return is_valid_search_param
    except Exception as error:
        print("Error in validating search parameter : %s | %s | %s ", str(error), current_userId, current_appversion)
        return False

def get_db_connection():
    conn = psycopg2.connect(host='142.132.206.93',database='postgres',user='tnphruser',password='TNphr@3Z4')
    return conn 
 
def reconnectToDB():
    global conn, cursor
    conn = psycopg2.connect(host='142.132.206.93',database='postgres',user='tnphruser',password='TNphr@3Z4')
    cursor = conn.cursor()
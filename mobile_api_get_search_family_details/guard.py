from pytz import timezone

import os
import json
import config
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
            return is_valid_id
        else:
            return validate_id(familyId, memberId)
    except Exception as error:
        print("Error validating Id attribute format : %s | %s | %s ", str(error), current_userId, current_appversion)
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
                    valid_ids.append(False)            
        if all(item == True for item in valid_ids) and len(valid_ids) != 0:
            return True
        else:
            print("One or more supplied ID not valid.")
            return False
    except Exception as error:
        print("Error in validating inputs : %s | %s | %s ", str(error), current_userId, current_appversion)
        return False

def check_id_registered(familyId, memberId):
    """
    This method will check the family Id and member Id are registered.
    Input Args: Family ID, Member ID
    Return: Boolean
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
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

        value = (familyId,memberId)
        cursor.execute(query,value)
        result = cursor.fetchall()
        for row in result:
            id_exist = row[0]
        return id_exist
        
    except psycopg2.ProgrammingError as e:
        print("get_family_and_member_details check_id_registered ProgrammingError",e)  
        conn.rollback()
        return False
    except psycopg2.InterfaceError as e:
        print("get_family_and_member_details check_id_registered InterfaceError",e)
        reconnectToDB()
        return False
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception as e:
            print("get_family_and_member_details check_id_registered",e)

def get_db_connection():
    conn = psycopg2.connect(host='142.132.206.93',database='postgres',user='tnphruser',password='TNphr@3Z4')
    return conn   
   
def reconnectToDB():
    global conn, cursor
    conn = psycopg2.connect(host='142.132.206.93',database='postgres',user='tnphruser',password='TNphr@3Z4')
    cursor = conn.cursor()
from guard import *
import guard 
from flask import Flask, request

"""
******Method Details******
Description: API to retrieve Matching Member Details based on the Search.
MIME Type: application/json
Input: {"USER_ID":<User_Id>, "SEARCH_PARAMETER":<String>, "SEARCH_VALUE":<Value>, "BLOCK_ID":<id>, "DISTRICT_ID":<id>, "VILLAGE_ID":<id>, "API_KEY":<Token>} 

3 letters for name Min.
District / Block Mandetory & Village optional.
Smart_card_id & Mobile Number and Unique_Health_Id - Will be full text. Name requires all district, block/village mandatory

Output: Data from Member
[{
    "member_id":<id>
    "unique_health_id":<uhid>,
    ...,
    ... ,
    "medical_history_id":<id>,
    
}]
"""
app = Flask(__name__)

# decorator for verifying the JWT
def token_required(request):
    token = None
    if 'x-access-token' in request.headers:
        token = request.headers['x-access-token']
    if not token:
        if (str(request.headers['User-Agent']).count("UptimeChecks")!=0):
            print("Uptime check trigger.")
            return False, json.dumps({"status":"API-ACTIVE", "status_code":"200","message":'Uptime check trigger.'})
        else:
            print("Invalid Token.")
            return False, json.dumps({'status':'FAILURE', "status_code":"401", 'message' : 'Invalid Token.'})

    try:
        token = token.strip() #Remove spaces at the beginning and at the end of the token
        token_format = re.compile(parameters['TOKEN_FORMAT'])
        if not token_format.match(token):
            print("Invalid Token format.")
            return False, json.dumps({'status':'FAILURE',"status_code":"401",'message' : 'Invalid Token format.'})
        else:        
            data = jwt.decode(token, parameters['JWT_SECRET_KEY'], algorithms=["HS256"])
            conn = get_db_connection()
            cursor = conn.cursor()
            query = 'SELECT auth_token from public.user_master where mobile_number ={}'.format(data['mobile_number'])
            cursor.execute(query,data['mobile_number'])    
            result = cursor.fetchall()
            for row in result:
                DBToken = row[0]['token_key']
            if DBToken == token:
                print('Tokens are equal')
                 # decoding the payload to fetch the stored details
                return True, data
            else:
                print('Tokens are not equal')
                raise ValueError("Invalid Token.")

    except jwt.ExpiredSignatureError as e:
        print(str(e))
        print("Token Expired: %s", str(e))
        return False, json.dumps({'status':'FAILURE', "status_code":"401", 'message' : 'Token Expired.'})

    except Exception as e:
        print("Invalid Token.")
        return False, json.dumps({'status':'FAILURE', "status_code":"401", 'message' : 'Invalid Token.'})
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception as e:
            print("mobile_api_search token_required",e)

@app.route('/api/mobile_api_search', methods=['POST'])
def get_search_details():

    token_status, token_data = token_required(request)
    if not token_status:
        return token_data    

    response = None
    try:
        print("********Get Search Parameters Details**********")
        # Check the request data for JSON
        if (request.is_json):
            content=request.get_json()
            if('USER_ID' in content): # This case will execute for App Version >= 3.1.4  / 19 April 2022 = b/229353759
                set_current_user(content['USER_ID'])
                userId = content['USER_ID']
            elif('user_id' in content): # This case will execute for App Version < 3.1.4  / 19 April 2022 = b/229353759
                set_current_user(content['user_id']) 
                userId = content['user_id']
            if('APP_VERSION' in content):
                setApp_Version(content['APP_VERSION'])

            result=[]
            if content["search_value"] is None or content["search_value"]=="":
                response =  json.dumps({
                            "message": "Search details are not valid.", 
                            "status": "FAILURE",
                            "status_code":"401",
                            "data": {}
                            })
                print("Search details are not valid. | %s | %s ", guard.current_userId, guard.current_appversion)
                return response

            elif not validate_id(userId):
                response =  json.dumps({
                            "message": "User ID is not valid.", 
                            "status": "FAILURE",
                            "status_code":"401",
                            "data": {}
                            })
                print("User ID is not valid. | %s | %s ", guard.current_userId, guard.current_appversion)
                return response
            else:
                is_token_valid = user_token_validation(userId, token_data["mobile_number"])
                if not is_token_valid:
                    response =  json.dumps({
                            "message": "Unregistered User/Token-User mismatch.", 
                            "status": "FAILURE",
                            "status_code":"401"
                            })
                    return response
                else:
                    print("Token Validated.")
                    search_parameter = content["search_parameter"]
                    if not validate_search_parameter(search_parameter):
                        print("Supplied search parameter is empty or not valid.")
                        response =  json.dumps({
                                    "message": "Invalid search parameter. Please check it.",
                                    "status": "FAILURE",
                                    "status_code":"401",
                                    "data": {}
                                    })
                        return response
                    offset = content['offset']

                    if search_parameter == "unique_health_id":
                        unique_health_id = content["search_value"]
                        if not validate_unique_health_id(unique_health_id):
                            print("Supplied unique health ID is empty or not valid.")
                            response =  json.dumps({
                                        "message": "Invalid unique health ID. Please contact the Administrator.",
                                        "status": "FAILURE",
                                        "status_code":"401",
                                        "data": {}
                                        })
                            return response                        
                        result = search_by_unique_health_id(unique_health_id)

                    elif search_parameter == "member_name":
                        is_valid_input, message = validate_member_name_inputs(member_name = content["search_value"], district_id= content["district_id"], \
                            block_id = content["block_id"], village_id = content["village_id"])
                        if not is_valid_input:
                            print(message)
                            response =  json.dumps({
                                        "message": message,
                                        "status": "FAILURE",
                                        "status_code":"401",
                                        "data": {}
                                        })
                            return response
                        is_id_exist = check_id_registered(content["district_id"], content["block_id"], content["village_id"])
                        if not is_id_exist:
                            print("Supplied IDs are not registered or provide the respective IDs.")
                            response =  json.dumps({
                                        "message": "Supplied IDs are not registered or provide the respective IDs. Please contact the Administrator.",
                                        "status": "FAILURE",
                                        "status_code":"401",
                                        "data": {}
                                        })
                            return response
                        result = search_by_name(content["search_value"], content["district_id"], content["block_id"], content["village_id"], offset)

                    elif search_parameter == "mobile_number":
                        mobile_number = int(content["search_value"])
                        if not validate_mobile_no(mobile_number):
                            print("Supplied mobile number is empty or not valid.")
                            response =  json.dumps({
                                        "message": "Invalid Mobile Number. Please contact the Administrator.",
                                        "status": "FAILURE",
                                        "status_code":"401",
                                        "data": {}
                                        })
                            return response                        
                        result = search_by_mobile_number(mobile_number)

                    elif search_parameter == "pds_smart_card_id" or search_parameter == "family_smart_card_id":
                        pds_smart_card_id = content["search_value"]
                        if not validate_pds_smart_card_id(pds_smart_card_id):
                            print("Supplied pds smart card ID is empty or not valid.")
                            response =  json.dumps({
                                        "message": "Invalid pds smart card ID. Please contact the Administrator.",
                                        "status": "FAILURE",
                                        "status_code":"401",
                                        "data": {}
                                        })
                            return response                        
                        result = search_by_smart_card_id(pds_smart_card_id)
                    else:
                        print("Search Parameter is Invalid.")
                        response =  json.dumps({
                                                "message": "Search Parameter is Invalid.", 
                                                "status": "FAILURE",
                                                "status_code":"401",
                                                "data": {}
                                                })
                        return response                    
                    if len(result) == 0:
                        response =  json.dumps({
                            "message": "There is no member data available, Please contact administrator.", 
                            "status": "SUCCESS",
                            "status_code":"200",
                            "data": {}
                        })
                        searchQuery = str(content['search_parameter'])+" "+str(content['search_value'])+" RETURN ZERO."
                        print("Search_Parameters : {}".format(str(searchQuery)))
                    elif len(result) > 0 and len(result) < 100:
                        response =  json.dumps({
                            "message": "Success retrieving member.", 
                            "status": "SUCCESS-FINAL",
                            "status_code":"200",
                            "data": {"family_member_list":result}
                        })
                        print("Success retrieving member.")
                    else:
                        response =  json.dumps({
                            "message": "Success retrieving member Data.", 
                            "status": "SUCCESS-CONTINUE",
                            "status_code":"200",
                            "data": {"family_member_list":result}
                        })
                        print("Success retrieving member Data.")
        else :
            response =  json.dumps({
                    "message": "Error!! The Request should be in JSON format.", 
                    "status": "FAILURE",
                    "status_code":"401",
                    "data": {}
                })
            print("The Request should be in JSON format.")

    except Exception as e:
        response =  json.dumps({
                    "message": "Error while retrieving member data, Please Retry.",
                    "status": "FAILURE",
                    "status_code": "401",
                    "data": {}
                })
        print("Error while retrieving member data : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)

    finally:
        return response


def getResultFormatted(results,cursor):
    data_list=[]
    for row in results:
        data={}
        fieldIdx=0

        for column in cursor.description:
            field_name=column.name
            # field_type=field.type_
            field_code=column.type_code
            # Code Mapping: STRING-6, TIMESTAMP-4, INT64-2, JSON-11             
            if(field_code==11 and row[fieldIdx] is not None):
                data[field_name]=json.loads(row[fieldIdx])
            elif(field_code==4 and row[fieldIdx] is not None):
                data[field_name]=row[fieldIdx].astimezone(timezone('Asia/Calcutta')).strftime("%Y-%m-%d %H:%M:%S%z")
            elif(field_code==5 and row[fieldIdx] is not None):
                data[field_name]=row[fieldIdx].strftime("%Y-%m-%d")
            else:
                data[field_name]=row[fieldIdx]
            fieldIdx+=1

        data_list.append(data)
    return data_list

#This function is now not in use. Kept only for future ref. 26 May 2022
def get_details_from(familyIdList, memberIdList):
    try:
        member_list=[]
        conn = get_db_connection()
        cursor = conn.cursor()
        query = "with Query_1 as (SELECT fmm.family_id ,fmm.member_id, fmm.member_name, fmm.gender, fmm.member_local_name, to_char(fmm.birth_date,'YYYY-MM-DD') AS birth_date, fmm.unique_health_id FROM public.family_member_master fmm WHERE family_id in unnest(%s) AND member_id in unnest(%s)), Query_2 as(SELECT family_id,member_id,MIN(screening_id) as screening_id, MIN(TO_JSONB(outcome)) as outcome, MAX(to_char(last_update_date AT TIME ZONE 'Asia/Calcutta', 'YYYY-MM-DD HH24:MI:SS')) AS last_update_date FROM public.health_screening WHERE family_id in unnest(%s) AND member_id in unnest(%s) AND concat(member_id, to_char(''%Y-%m-%d %H:%M:%S%z', TIMESTAMP(JSON_VALUE(update_register,'$[0].timestamp')), 'Asia/Calcutta) ) in (select concat(member_id, to_char('%Y-%m-%d %H:%M:%S%z', max_time, 'Asia/Calcutta') ) FROM (SELECT member_id,max(TIMESTAMP(JSON_VALUE(update_register,'$[0].timestamp'))) as max_time  FROM public.health_screening WHERE family_id  in  unnest(%s) AND member_id in unnest(%s) group by 1)) group by 1,2) SELECT  Q1.member_id, Q1.member_name, Q1.gender, Q1.member_local_name,to_char(Q1.birth_date,'YYYY-MM-DD') AS birth_date, Q1.unique_health_id, Q1.family_id, Q2.last_update_date, PARSE_JSON(Q2.outcome)as outcome from Query_1 Q1 left join Query_2 Q2 on Q1.family_id= Q2.family_id and Q1.member_id = Q2.member_id"
        value = (familyIdList,memberIdList,familyIdList,memberIdList,familyIdList,memberIdList)
        cursor.execute(query,value)
        results = cursor.fetchall()
        member_list = getResultFormatted(results,cursor)
            
    except psycopg2.ProgrammingError as e:
        print("get_search_details get_details_from ProgrammingError",e)  
        conn.rollback()
    except psycopg2.InterfaceError as e:
        print("get_search_details get_details_from InterfaceError",e)
        reconnectToDB()
    
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception as e:
            print("get_search_details get_details_from",e)
    return member_list

def search_by_unique_health_id(unique_health_id):
    try:
        print("Search by Unique Health ID.")
        family_details = []
        family_list =[]
        member_list =[]

        conn = get_db_connection()
        cursor = conn.cursor()
        query = "SELECT fmm.family_id ,fmm.member_id, fmm.member_name, fmm.gender, fmm.member_local_name, to_char(fmm.birth_date,'YYYY-MM-DD') AS birth_date, fmm.unique_health_id,null as last_update_date,null as outcome  FROM public.family_member_master fmm WHERE fmm.unique_health_id=%s"
        value = (unique_health_id,)
        cursor.execute(query,value)
        results = cursor.fetchall()
        family_details = getResultFormatted(results,cursor)
        return family_details

    except psycopg2.ProgrammingError as e:
        print("get_search_details search_by_unique_health_id ProgrammingError",e)  
        conn.rollback()
    except psycopg2.InterfaceError as e:
        print("get_search_details search_by_unique_health_id InterfaceError",e)
        reconnectToDB()
    
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception as e:
            print("get_search_details search_by_unique_health_id",e)
    return family_details

def search_by_mobile_number(mobile_number):
    try:
        print("Search by Mobile Number.")
        family_details = []
        family_list =[]
        member_list =[]
        conn = get_db_connection()
        cursor = conn.cursor()
        query = "SELECT fmm.family_id ,fmm.member_id, fmm.member_name, fmm.gender, fmm.member_local_name, to_char(fmm.birth_date,'YYYY-MM-DD') AS birth_date, fmm.unique_health_id,null as last_update_date,null as outcome FROM public.family_member_master fmm WHERE fmm.mobile_number=%s AND fmm.mobile_number != 0"
        value = (mobile_number,)
        cursor.execute(query,value)
        results = cursor.fetchall()
        family_details = getResultFormatted(results,cursor)
        return family_details

    except psycopg2.ProgrammingError as e:
        print("get_search_details search_by_mobile_number ProgrammingError",e)  
        conn.rollback()
    except psycopg2.InterfaceError as e:
        print("get_search_details search_by_mobile_number InterfaceError",e)
        reconnectToDB()
    
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception as e:
            print("get_search_details search_by_mobile_number",e)
    return family_details

def search_by_smart_card_id(pds_smartcard_id):
    try:
        print("Search by smart card ID.")
        # cloud_logger.info("Search by smart card ID.")
        family_details = []
        family_list =[]
        member_list =[]
        conn = get_db_connection()
        cursor = conn.cursor()
        query = "SELECT fmm.family_id ,fmm.member_id, fmm.member_name, fmm.gender, fmm.member_local_name, to_char(fmm.birth_date,'YYYY-MM-DD') AS birth_date, fmm.unique_health_id,null as last_update_date,null as outcome FROM public.family_member_master fmm WHERE family_id in (SELECT family_id FROM public.family_master fmly WHERE  fmly.pds_smart_card_id not in (333000000000,334000000000,0) AND fmly.pds_smart_card_id=%s)"
        value = (pds_smartcard_id,)
        cursor.execute(query,value)
        results = cursor.fetchall()
        family_details = getResultFormatted(results,cursor)
        return family_details

    except psycopg2.ProgrammingError as e:
        print("get_search_details search_by_smart_card_id ProgrammingError",e)  
        conn.rollback()
    except psycopg2.InterfaceError as e:
        print("get_search_details search_by_smart_card_id InterfaceError",e)
        reconnectToDB()
    
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception as e:
            print("get_search_details search_by_smart_card_id",e)
    return family_details


def search_by_name(member_name, district_id, block_id, village_id, offset):
    try:
        print("Search by name.")
        family_details = []
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        query = "SELECT  fmm.family_id ,fmm.member_id, fmm.member_name, fmm.gender, fmm.member_local_name, to_char(fmm.birth_date,'YYYY-MM-DD') AS birth_date, fmm.unique_health_id,null as last_update_date,null as outcome FROM public.family_member_master fmm WHERE fmm.district_id=%s"
        if block_id is not None and block_id != '':
            query += " AND fmm.block_id=%s"
        if village_id is not None and village_id != '':
            query += " AND fmm.village_id=%s" 
        
        if(member_name):
                appendStr = " AND lower(fmm.member_name) like LOWER(%s)"
                query += appendStr
                member_name = "%"+member_name+"%"
        query += ' order by fmm.member_name limit 3000 offset %s'
        
        value = (district_id,block_id,village_id,member_name,offset)
        cursor.execute(query,value)
        results = cursor.fetchall()
        family_details = getResultFormatted(results,cursor)
        return family_details

    except psycopg2.ProgrammingError as e:
        print("get_search_details search_by_name ProgrammingError",e)  
        conn.rollback()
    except psycopg2.InterfaceError as e:
        print("get_search_details search_by_name InterfaceError",e)
        reconnectToDB()
    
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception as e:
            print("get_search_details search_by_name",e)
    return family_details

@app.route('/api/mobile_api_search/hc', methods=['GET'])
def mobile_api_search_health_check():
    return {"status": "OK", "message": "success mobile_api_search health check"} 

if __name__=="__main__":    
    app.run(host="0.0.0.0", port=8000)

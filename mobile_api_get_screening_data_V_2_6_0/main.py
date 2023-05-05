from guard import *
import guard
from flask import Flask, request

"""
******Method Details******
Description: API to retrieve Family Member's Screening Data
MIME Type: application/json
Input: Last update Timestamp - {"USER_ID":<User_Id>, "LAST_UPDATE":<Timestamp>, "API_KEY":<Token>}
Output: Master data from health_screening
[{
    "screening_id":<id>
    "unique_health_id":<uhid>, 
    "screening_id":<id>,
    "screening_values":{JSON},
    ...
    ...
    "symptoms":{JSON}
}]
"""
app = Flask(__name__)

# decorator for verifying the JWT
def token_required(request):
    token = None
        # jwt is passed in the request header
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
            print("mobile_api_get_screening_data token_required",e)

@app.route('/api/mobile_api_get_screening_data', methods=['POST'])
def get_screening_data():
    
    token_status, token_data = token_required(request)
    if not token_status:
      return token_data    

    response = None

    try:
        print("*******Get Screening Data*********")
        screening=[]
        defaultTime = datetime.strptime('2021-09-01 15:52:50+0530', "%Y-%m-%d %H:%M:%S%z")
        
        ## DB Connection
        conn = get_db_connection()
        cursor = conn.cursor()
        
        if request.is_json and isinstance(request.get_json(), dict):
            content=request.get_json()  
            userId = content["USER_ID"]  
            set_current_user(userId)

            if ("APP_VERSION" in content):
               setApp_Version(content['APP_VERSION'])            
            is_valid_id = validate_id(content["USER_ID"])

            if not is_valid_id:
                response =  json.dumps({
                            "message": "User ID is not Valid.", 
                            "status": "FAILURE",
                            "status_code":"401",
                            "data": []
                            })
                print("Provided User ID is not valid. | %s | %s", guard.current_userId, guard.current_appversion)
                return response
            else:
                is_token_valid = user_token_validation(userId, token_data["mobile_number"])
                if not is_token_valid:
                    response =  json.dumps({
                            "message": "Unregistered User/Token-User mismatch.", 
                            "status": "FAILURE",
                            "status_code":"401"
                            })
                    print("Unregistered User/Token-User mismatch. | %s | %s", guard.current_userId, guard.current_appversion)
                    return response
                else:
                    print("Token Validated.")
                    offset = int(content["OFFSET"])
                    lastUpdate = content["LAST_UPDATE"]
                    is_valid_TS = re.match(parameters['TS_FORMAT'], lastUpdate) #Checks the TimeStamp format
                    lastUpdateTS = defaultTime if(lastUpdate is None or lastUpdate=='' or not is_valid_TS) else datetime.strptime(content["LAST_UPDATE"], "%Y-%m-%d %H:%M:%S%z")  
 
                    query = "with family_details as (SELECT DISTINCT fmm.family_id,fmm.member_id FROM public.family_member_master fmm WHERE fmm.facility_id = (SELECT facility_id FROM public.user_master WHERE user_id =%s )) SELECT scrn.member_id,scrn.screening_id,scrn.screening_values,scrn.lab_test, scrn.drugs,scrn.diseases, scrn.outcome, scrn.symptoms, scrn.update_register,to_char(scrn.last_update_date AT TIME ZONE 'Asia/Calcutta', 'YYYY-MM-DD HH24:MI:SS') AS last_update_date, scrn.advices, scrn.additional_services FROM family_details fd left join public.health_screening scrn on scrn.family_id = fd.family_id AND scrn.member_id = fd.member_id WHERE scrn.last_update_date>%s limit 3000 OFFSET %s"
                    value = (userId,lastUpdateTS,offset)
                    cursor.execute(query,value)
                    results = cursor.fetchall()
                    screening = getResultFormatted(results,cursor)

                    if len(screening) == 0:
                            response =  json.dumps({
                                                "message": "There is no Screening available, Please contact Administrator.", 
                                                "status": "SUCCESS",
                                                "status_code":"200",
                                                "data": []
                                                })
                            print("There is no Screening History available, Please contact Administrator.")     
                    elif len(screening) > 0 and len(screening) < 3000:
                            response =  json.dumps({
                                                "message": "Success retrieving Screening History.", 
                                                "status": "SUCCESS-FINAL",
                                                "status_code":"200",
                                                "data": screening
                                                })
                            print("Success retrieving Screening History.")
                        
                    else:
                            response =  json.dumps({
                                                "message": "Success retrieving Screening Data.", 
                                                "status": "SUCCESS-CONTINUE",
                                                "status_code":"200",
                                                "data": screening
                                                })
                            print("Success retrieving Screening Data.")

        else :
            response =  json.dumps({
                                "message": "Error!! The Request Format should be in JSON.", 
                                "status": "FAILURE",
                                "status_code":"401",
                                "data": []
                                })
            print("The Request Format should be in JSON.")
        
    except psycopg2.ProgrammingError as e:
        print("get_screening_data user_token_validation ProgrammingError",e)  
        conn.rollback()
        response =  json.dumps({
                    "message": "Error while retrieving Screening Data, Please Retry.", 
                    "status": "FAILURE",
                    "status_code": "401",
                    "data": []
                })
    except psycopg2.InterfaceError as e:
        print("get_screening_data user_token_validation InterfaceError",e)
        reconnectToDB()
        response =  json.dumps({
                    "message": "Error while retrieving Screening Data, Please Retry.", 
                    "status": "FAILURE",
                    "status_code": "401",
                    "data": []
                })
        print("Error while retrieving Screening Data, Please Retry :%s | %s | %s", str(e), guard.current_userId, guard.current_appversion)

    finally:
        try:
            cursor.close()
            conn.close()
        except Exception as e:
            print("get_screening_data",e)
    return response

def getResultFormatted(results,cursor):
    data_list=[]
    try:
        for row in results:
            
            data={}
            fieldIdx=0

            for column in cursor.description:
                field_name=column.name
                # field_type=field.type_
                field_code=column.type_code    
                # Code Mapping: STRING-6, TIMESTAMP-4, INT64-2, JSON-11             
                if(field_code==11 and row[fieldIdx] is not None):
                    if field_name == "update_register":
                        update_register = getUpdateRegister(json.loads(row[fieldIdx]))
                        data[field_name]=update_register
                    elif field_name == "outcome":
                        #This change is explicitly made for V1 APP run successfully - 11thFeb2022.
                        outcome = json.loads(row[fieldIdx])
                        if "recommend_outcome_result" not in outcome.keys(): 
                            # This means data is uploaded from App V2. To make that data readable for V1 this will help.
                            outcome["follow_up"]= False
                            outcome["follow_up_date"]= None
                            outcome["recommend_outcome_result"]= "Normal"
                            outcome["referral_place_id"]= None
                            outcome["referral_place_name"]= None
                            outcome["referral_type"]= None
                            outcome["remarks"]= None
                            outcome["screening_outcome"]= None
                            data[field_name]=outcome
                        elif 'recommend_outcome_result' != "":
                            outcome["follow_up"]= False
                            outcome["follow_up_date"]= None
                            outcome["recommend_outcome_result"]= "Normal"
                            outcome["referral_place_id"]= None
                            outcome["referral_place_name"]= None
                            outcome["referral_type"]= None
                            outcome["remarks"]= None
                            outcome["screening_outcome"]= None
                            data[field_name]=outcome
                        else: # It will send the older form of data, which App V1 & V2 both can handle.
                            data[field_name]=json.loads(row[fieldIdx])
                    else:
                        data[field_name]=json.loads(row[fieldIdx])
                elif(field_code==4 and row[fieldIdx] is not None):
                    data[field_name]=row[fieldIdx].astimezone(timezone('Asia/Calcutta')).strftime("%Y-%m-%d %H:%M:%S%z")
                else:
                    data[field_name]=row[fieldIdx]
                fieldIdx+=1

            data_list.append(data)
        print("Data List size is : %s", str(len(data_list)))
        return data_list

    except Exception as e:
        print("Error While Formatting the Result :%s | %s | %s", str(e), guard.current_userId, guard.current_appversion)
    
    finally:
        return data_list

def getUpdateRegister(update_register):
    try:
        if isinstance(update_register, dict):
            update_register_list = [update_register]
            if len(update_register_list) == 1 or len(update_register_list) == 0:
                return update_register
            else:
                sorted_list = sorted(update_register_list, key = lambda item: item['timestamp'])
                update_register = sorted_list[-1]
                return update_register
        elif isinstance(update_register, list):
            if len(update_register) == 1:
                return update_register[0]
            else:
                sorted_list = sorted(update_register, key = lambda item: item['timestamp'])
                update_register = sorted_list[-1]
                return update_register
                
    except Exception as e:
        print("Error While parsing Update Register :%s | %s | %s", str(e), guard.current_userId, guard.current_appversion)

@app.route('/api/mobile_api_get_screening_data/hc', methods=['GET'])
def mobile_api_get_screening_data_health_check():
    return {"status": "OK", "message": "success mobile_api_get_screening_data health check"}

if __name__=="__main__":    
    app.run(host="0.0.0.0", port=8000)
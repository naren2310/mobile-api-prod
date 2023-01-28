from guard import *
import guard
from flask import Flask, request
"""
******Method Details******
Description: API to retrieve Family Member's Medical History Data
MIME Type: application/json
Input: Last update Timestamp - {"USER_ID":<User_Id>, "LAST_UPDATE":<Timestamp>, "API_KEY":<Token>}
Output: Master data from health_history
[{
    "member_id":<id>
    "unique_health_id":<uhid>, 
    "medical_history_id":<id>,
    "past_histroy":{JSON},
    ...
    ...
    "eligible_couple_details":{JSON}
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
            # cloud_logger.info("Uptime check trigger.")
            return False, json.dumps({"status":"API-ACTIVE", "status_code":"200","message":'Uptime check trigger.'})
        else:
            print("Invalid Token.")
            # cloud_logger.critical("Invalid Token.")
            return False, json.dumps({'status':'FAILURE', "status_code":"401", 'message' : 'Invalid Token.'})

    try:
        token = token.strip() #Remove spaces at the beginning and at the end of the token
        token_format = re.compile(parameters['TOKEN_FORMAT'])
        if not token_format.match(token):
            print("Invalid Token format.")
            # cloud_logger.critical("Invalid Token format.")
            return False, json.dumps({'status':'FAILURE',"status_code":"401",'message' : 'Invalid Token format.'})
        else:
            # decoding the payload to fetch the stored details
            data = jwt.decode(token, parameters['JWT_SECRET_KEY'], algorithms=["HS256"])
            return True, data

    except jwt.ExpiredSignatureError as e:
        print("Token Expired: %s", str(e))
        # cloud_logger.critical("Token Expired: %s", str(e))
        return False, json.dumps({'status':'FAILURE', "status_code":"401", 'message' : 'Token Expired.'})

    except Exception as e:
        print("Invalid Token: %s", str(e))
        # cloud_logger.critical("Invalid Token: %s", str(e))
        return False, json.dumps({'status':'FAILURE',"status_code":"401",'message' : 'Invalid Token.'})

@app.route('/mobile_api_get_medical_history', methods=['POST'])
def get_medical_history():
    
    token_status, token_data = token_required(request)
    if not token_status:
        return token_data   

    response = None

    try:
        print("********Get Medical History*********")
        # cloud_logger.info("********Get Medical History*********")
        member_history=[]
        defaultTime = datetime.strptime('2021-09-01 15:52:50+0530', "%Y-%m-%d %H:%M:%S%z")
        # Check the request data for JSON
        if (request.is_json):
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
                # cloud_logger.error("Provided User ID is not valid. | %s | %s", guard.current_userId, guard.current_appversion)
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
                    # cloud_logger.error("Unregistered User/Token-User mismatch. | %s | %s", guard.current_userId, guard.current_appversion)
                    return response
                else:
                    print("Token Validated.")
                    # cloud_logger.info("Token Validated.")
                    offset = int(content["OFFSET"])
                    lastUpdate = content["LAST_UPDATE"]
                    is_valid_TS = re.match(parameters['TS_FORMAT'], lastUpdate) #Checks the TimeStamp format
                    lastUpdateTS = defaultTime if(lastUpdate is None or lastUpdate=='' or not is_valid_TS) else datetime.strptime(lastUpdate, "%Y-%m-%d %H:%M:%S%z")

                    # with spnDB.snapshot() as snapshot:   
                        
                        # query = "SELECT fmm.member_id, fmm.unique_health_id, fmm.resident_status, fmm.resident_status_details, hist.medical_history_id, hist.past_history, hist.family_history, hist.lifestyle_details, hist.vaccinations, hist.disability, hist.disability_details, FORMAT_DATE('%F', hist.enrollment_date) as enrollment_date, hist.welfare_method, hist.congenital_anomaly, hist.eligible_couple_id, hist.eligible_couple_status, hist.eligible_couple_details, hist.update_register, FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S%z', hist.last_update_date, 'Asia/Calcutta') as last_update_date, fms.social_details, fms.economic_details from family_member_master fmm INNER JOIN facility_registry fr USING(block_id) INNER JOIN user_master usr ON usr.facility_id=fr.facility_id INNER JOIN health_history hist ON hist.member_id=fmm.member_id LEFT JOIN family_member_socio_economic_ref fms ON fms.member_id=fmm.member_id WHERE usr.user_id=@userId and hist.last_update_date>@lastUpdate order by fmm.member_id limit 100 OFFSET @offset"
                        # query = "SELECT hist.member_id, fmm.unique_health_id, fmm.resident_status, fmm.resident_status_details, hist.medical_history_id, hist.past_history, hist.family_history, hist.lifestyle_details, hist.vaccinations, hist.disability, hist.disability_details, FORMAT_DATE('%F', hist.enrollment_date) as enrollment_date, hist.welfare_method, hist.congenital_anomaly, hist.eligible_couple_id, hist.eligible_couple_status, hist.eligible_couple_details,  hist.mtm_beneficiary, hist.update_register, FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S%z', hist.last_update_date, 'Asia/Calcutta') as last_update_date, fms.social_details, fms.economic_details from health_history@{FORCE_INDEX=HISTORY_LAST_UPDATE_IDX} hist JOIN@{JOIN_METHOD=HASH_JOIN, HASH_JOIN_BUILD_SIDE=BUILD_LEFT} family_member_master fmm ON fmm.member_id=hist.member_id INNER JOIN user_master usr on JSON_VALUE(usr.assigned_jurisdiction, '$.primary_block')=fmm.block_id LEFT JOIN family_member_socio_economic_ref fms ON fms.member_id=fmm.member_id WHERE fmm.facility_id is not NULL and usr.user_id=@userId and hist.last_update_date>@lastUpdate order by hist.medical_history_id limit 3000 OFFSET @offset"
                        # query = "with Query as (select fmm.family_id,fmm.member_id,fmm.facility_id,fmm.unique_health_id, fmm.resident_status, fmm.resident_status_details ,fms.social_details, fms.economic_details FROM family_member_master@{FORCE_INDEX=MEMBER_FACILITY_ID_IDX} fmm left join family_member_socio_economic_ref fms on fms.member_id = fmm.member_id where facility_id = (select facility_id from user_master where user_id =@userId)) SELECT hist.member_id, Q.unique_health_id, Q.resident_status, Q.resident_status_details,hist.medical_history_id, hist.past_history, hist.family_history, hist.lifestyle_details, hist.vaccinations, hist.disability, hist.disability_details, FORMAT_DATE('%F', hist.enrollment_date) as enrollment_date, hist.welfare_method, hist.congenital_anomaly, hist.eligible_couple_id, hist.eligible_couple_status, hist.eligible_couple_details,  hist.mtm_beneficiary, hist.update_register,FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S%z', hist.last_update_date, 'Asia/Calcutta') as last_update_date, Q.social_details, Q.economic_details from health_history@{FORCE_INDEX=HISTORY_LAST_UPDATE_IDX} hist JOIN@{JOIN_METHOD=HASH_JOIN, HASH_JOIN_BUILD_SIDE=BUILD_RIGHT} Query Q on Q.family_id = hist.family_id and Q.member_id = hist.member_id WHERE hist.last_update_date>@lastUpdate limit 3000 OFFSET @offset"
                        # The above query is commented & updates as below as per suggestion of Darshak, Shankar provided the query. 29 April 22.
                        # query = "with Query as (select fmm.family_id,fmm.member_id,fmm.facility_id,fmm.unique_health_id, fmm.resident_status, fmm.resident_status_details FROM family_member_master@{FORCE_INDEX=MEMBER_FACILITY_ID_IDX} fmm where facility_id = (select facility_id from user_master where user_id =@userId)) SELECT hist.member_id, Q.unique_health_id, Q.resident_status, Q.resident_status_details,hist.medical_history_id, hist.past_history, hist.family_history, hist.lifestyle_details, hist.vaccinations, hist.disability, hist.disability_details, FORMAT_DATE('%F', hist.enrollment_date) as enrollment_date, hist.welfare_method, hist.congenital_anomaly, hist.eligible_couple_id, hist.eligible_couple_status, hist.eligible_couple_details, hist.mtm_beneficiary, hist.update_register,FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S%z', hist.last_update_date, 'Asia/Calcutta') as last_update_date , fms.social_details, fms.economic_details from health_history@{FORCE_INDEX=HISTORY_LAST_UPDATE_IDX} hist left join@{JOIN_METHOD=APPLY_JOIN} Query Q on Q.family_id = hist.family_id and Q.member_id = hist.member_id left join@{JOIN_METHOD=APPLY_JOIN} family_member_socio_economic_ref fms on fms.family_id=Q.family_id and fms.member_id = Q.member_id WHERE hist.last_update_date>@lastUpdate limit 3000 OFFSET @offset"
                        
                    query = "WITH Query AS (SELECT fmm.family_id,fmm.member_id,fmm.facility_id,fmm.unique_health_id, fmm.resident_status, fmm.resident_status_details FROM public.family_member_master fmm WHERE facility_id = (SELECT facility_id FROM public.user_master WHERE user_id =%s)) SELECT hist.member_id, Q.unique_health_id, Q.resident_status, Q.resident_status_details,hist.medical_history_id, hist.past_history, hist.family_history, hist.lifestyle_details, hist.vaccinations, hist.disability, hist.disability_details, to_char(hist.enrollment_date, 'YYYY-MM-DD') AS enrollment_date, hist.welfare_method, hist.congenital_anomaly, hist.eligible_couple_id, hist.eligible_couple_status, hist.eligible_couple_details, hist.mtm_beneficiary, hist.update_register,to_char(hist.last_update_date AT TIME ZONE 'Asia/Calcutta', 'YYYY-MM-DD HH24:MI:SS') AS last_update_date , fms.social_details, fms.economic_details FROM Query Q LEFT JOIN public.health_history hist  ON Q.family_id = hist.family_id AND Q.member_id = hist.member_id LEFT JOIN public.family_member_socio_economic_ref fms ON fms.family_id=Q.family_id AND fms.member_id = Q.member_id WHERE hist.member_id IS NOT NULL AND hist.last_update_date>%s LIMIT 3000 OFFSET %s"

                        # results = snapshot.execute_sql(
                        #     query,
                        #     params={
                        #         "userId": userId,
                        #         "offset": offset,
                        #         "lastUpdate": lastUpdateTS
                        #     },
                        #     param_types={
                        #         "userId": param_types.STRING,
                        #         "offset": param_types.INT64,
                        #         "lastUpdate": param_types.TIMESTAMP
                        #     },                   
                        # )
                    value = (userId,lastUpdateTS,offset)
                    cursor.execute(query,value)
                    results = cursor.fetchall()
                    member_history = getResultFormatted(results)

                    if len(member_history) == 0:
                            response =  json.dumps({
                                "message": "There is no Medical History available, please contact administrator.", 
                                "status": "SUCCESS",
                                "status_code":"200",
                                "data": []
                            }) 
                            print("There is no Medical History available, Please Contact Administrator.")
                            # cloud_logger.info("There is no Medical History available, Please Contact Administrator.")       
                    elif len(member_history) > 0 and len(member_history) < 3000:
                            response =  json.dumps({
                                "message": "Success retrieving Medical History.", 
                                "status": "SUCCESS-FINAL",
                                "status_code":"200",
                                "data": member_history
                            })
                            print("Success retrieving Medical History.")
                            # cloud_logger.info("Success retrieving Medical History.")
                    else:
                            response =  json.dumps({
                                "message": "Success retrieving Medical History Data.", 
                                "status": "SUCCESS-CONTINUE",
                                "status_code":"200",
                                "data": member_history
                            })
                            print("Success retrieving Medical History Data.")
                            # cloud_logger.info("Success retrieving Medical History Data.")
        else :
            response =  json.dumps({
                    "message": "Error!! The Request Format should be in JSON.", 
                    "status": "FAILURE",
                    "status_code":"401",
                    "data": []
                })
            print("The Request Format should be in JSON.")
            # cloud_logger.error("The Request Format should be in JSON.")

    except Exception as e:
        response =  json.dumps({
                    "message": "Error while retrieving Medical History Data.", 
                    "status": "FAILURE",
                    "status_code": "401",
                    "data": []
                })
        print("Error while retrieving Medical History :%s | %s | %s", str(e), guard.current_userId, guard.current_appversion)
        # cloud_logger.error("Error while retrieving Medical History :%s | %s | %s", str(e), guard.current_userId, guard.current_appversion)

    finally:
        return response

def getResultFormatted(results):
    data_list=[]

    try:
        for row in results:
            data={}
            fieldIdx=0

            for field in results.fields:
                field_name=field.name
                field_type=field.type_
                field_code=field_type.code    
                # Code Mapping: STRING-6, TIMESTAMP-4, INT64-2, JSON-11             
                if(field_code==11 and row[fieldIdx] is not None):
                    if field_name == "update_register":
                        update_register = getUpdateRegister(json.loads(row[fieldIdx]))
                        data[field_name]=update_register
                    else:
                        data[field_name]=json.loads(row[fieldIdx])
                elif(field_code==4 and row[fieldIdx] is not None):
                    data[field_name]=row[fieldIdx].astimezone(timezone('Asia/Calcutta')).strftime("%Y-%m-%d %H:%M:%S%z")
                else:
                    data[field_name]=row[fieldIdx]
                fieldIdx+=1

            data_list.append(data)
        return data_list

    except Exception as e:
        print("Error While Formatting the Result :%s | %s | %s", str(e), guard.current_userId, guard.current_appversion)
        # cloud_logger.error("Error While Formatting the Result :%s | %s | %s", str(e), guard.current_userId, guard.current_appversion)
    
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
        # cloud_logger.error("Error While parsing Update Register :%s | %s | %s", str(e), guard.current_userId, guard.current_appversion)

if __name__ == '__main__':
    app.debug=False
    app.run(host='0.0.0.0', port=8000)
from guard import *
import guard
from flask import Flask, request

"""
******Method Details******
Description: API to retrieve all Health Facilities Data
MIME Type: application/json
Input: Last update Timestamp - {"USER_ID":<User_Id>, "LAST_UPDATE":<Timestamp>, "API_KEY":<Token>}
Output: Master data from facility_registry
[
    {
    "facility_id":<id>, "facility_gid":<gid>, "facility_name":<name>
    },
    {  
    "facility_id":<id>, "facility_name":<name>, "facility_gid":<name>, "facility_type":<type>
    }
]
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

@app.route('/api/mobile_api_get_facility', methods=['POST'])
def get_facilities_data():
    
    token_status, token_data = token_required(request)
    if not token_status:
        return token_data
    response = None
    facilities=[]
    try:
        print("*********Get Facilities Data************")
        # cloud_logger.info("*********Get Facilities Data************")
        defaultTime = datetime.strptime('2021-09-01 15:52:50+0530', "%Y-%m-%d %H:%M:%S%z")

        # Check the request data for JSON
        if request.is_json:
            content=request.get_json()
            userId = content["USER_ID"]
            set_current_user(userId)
            if ("APP_VERSION" in content):
                setApp_Version(content['APP_VERSION'])

            is_valid_id = validate_id(userId)

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
                    lastUpdate=content["LAST_UPDATE"]
                    is_valid_TS = re.match(parameters['TS_FORMAT'], lastUpdate) #Checks the TimeStamp format
                    #TODO: If last updated timestamp is sent, check master_change_log to send only the changed data
                    # Logic to be changed with API design change.
                    # with spnDB.snapshot() as snapshot:   
                    query = "SELECT fr.facility_id, fr.institution_gid, fr.facility_name, typ.facility_type_name FROM public.facility_registry fr LEFT JOIN public.facility_type_master typ ON typ.facility_type_id=fr.facility_type_id LEFT JOIN public.facility_category_master fcm ON fr.category_id= fcm.category_id WHERE fcm.reference_id NOT IN (40, 72, 65, 68, 66, 70, 62, 61, 71, 25, 67, 69, 64, 74, 73, 41, 16, 17, 51, 23, 75, 33, 35, 36, 56, 79) AND fr.last_update_date>%s" 
                    # results = snapshot.execute_sql(
                    #                     query,
                    #                     params={"lastUpdate": defaultTime if(lastUpdate is None or lastUpdate=='' or not is_valid_TS) else datetime.strptime(lastUpdate, "%Y-%m-%d %H:%M:%S%z")},
                    #                     param_types={"lastUpdate": param_types.TIMESTAMP},                    
                    #                     )
                    dattime = {"lastUpdate": defaultTime if(lastUpdate is None or lastUpdate=='' or not is_valid_TS) else datetime.strptime(lastUpdate, "%Y-%m-%d %H:%M:%S%z")}
                    value = (dattime['lastUpdate'],)
                    cursor.execute(query,value)
                    results = cursor.fetchall()
                    for row in results:
                            facility = {"facility_id":row[0],"facility_gid":row[1],"facility_name":row[2], "facility_type":row[3]}
                            facilities.append(facility)

                    if(len(facilities)>0):
                        response =  json.dumps({
                            "message": "Success retrieving Facility Data.", 
                            "status": "SUCCESS",
                            "status_code":"200",
                            "data": facilities
                        })
                        print("Success retrieving Facility Data.")
                        # cloud_logger.info("Success retrieving Facility Data.")
                    else:
                        response =  json.dumps({
                            "message": "No Data to Sync.", 
                            "status": "SUCCESS",
                            "status_code":"200",
                            "data": facilities
                        })
                        print("No Data to Sync.")
                        # cloud_logger.info("No Data to Sync.")
        else :
            response =  json.dumps({
                    "message": "Error!! The request format should be in JSON.", 
                    "status": "FAILURE",
                    "status_code":"401",
                    "data": []
                })
            print("The request format should be in JSON.")
            # cloud_logger.error("The request format should be in JSON.")

    except Exception as e:
        response =  json.dumps({
            "message": "Error while retrieving Facility Data.",
            "status": "FAILURE",
            "status_code":"401",
            "data": facilities
        })
        print("Error while retrieving Facility Data : %s | %s | %s", str(e), guard.current_userId, guard.current_appversion)
        # cloud_logger.error("Error while retrieving Facility Data : %s | %s | %s", str(e), guard.current_userId, guard.current_appversion)

    finally:
        return response
    
def retrieve_streets(facilityId):
    try:
        streets=[]
        print("Retrieving Street Data.")
        # cloud_logger.info("Retrieving Street Data.")
        # with spnDB.snapshot() as snapshot:   
        query = "SELECT fr.facility_id, fr.institution_gid, fr.facility_name, typ.facility_type_name FROM public.facility_registry fr INNER JOIN public.facility_type_master typ on typ.facility_type_id=fr.facility_type_id WHERE fr.facility_id=%s" 
            # results = snapshot.execute_sql(
            #         query,
            #         params={"facilityId": facilityId},
            #         param_types={"facilityId": param_types.STRING},                   
            #         )
        value = (facilityId,)
        cursor.execute(query,value)
        results = cursor.fetchall()
        for row in results:
                street = {"facility_id":row[0], "facility_gid":row[1], "facility_name":row[2],"facility_type":row[4]}
                streets.append(street)

    except Exception as e:
        print("Error While retrieving Streets Data : %s | %s | %s", str(e), guard.current_userId, guard.current_appversion)
        # cloud_logger.error("Error While retrieving Streets Data : %s | %s | %s", str(e), guard.current_userId, guard.current_appversion)
        return False
    finally:
        return streets

if __name__=="__main__":    
    app.run(host="0.0.0.0", port=8000)
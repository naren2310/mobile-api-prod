from spannerDB import *
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
            # cloud_logger.info("Uptime check trigger.")
            print("Uptime check trigger.")
            return False, json.dumps({"status":"API-ACTIVE", "status_code":"200","message":'Uptime check trigger.'})
        else:
            # cloud_logger.critical("Invalid Token.")
            print("Invalid Token.")
            return False, json.dumps({'status':'FAILURE', "status_code":"401", 'message' : 'Invalid Token.'})

    try:
        token = token.strip() #Remove spaces at the beginning and at the end of the token
        token_format = re.compile(parameters['TOKEN_FORMAT'])
        if not token_format.match(token):
            # cloud_logger.critical("Invalid Token format.")
            print("Invalid Token format.")
            return False, json.dumps({'status':'FAILURE',"status_code":"401",'message' : 'Invalid Token format.'})
        else:            
            # decoding the payload to fetch the stored details
            data = jwt.decode(token, parameters['JWT_SECRET_KEY'], algorithms=["HS256"])
            return True, data

    except jwt.ExpiredSignatureError as e:
        # cloud_logger.critical("Token Expired: %s", str(e))
        print("Token Expired: %s", str(e))
        return False, json.dumps({'status':'FAILURE', "status_code":"401", 'message' : 'Token Expired.'})

    except Exception as e:
        # cloud_logger.critical("Invalid Token.")
        print("Invalid Token.")
        return False, json.dumps({'status':'FAILURE', "status_code":"401", 'message' : 'Invalid Token.'})
    
@app.route('/api/mobile_api_health_history', methods=['POST'])
def member_health_history():

    token_status, token_data = token_required(request)
    if not token_status:
        return token_data    

    response = None
    try:
        # cloud_logger.info("**********Add Medical History***********")
        print("**********Add Medical History***********")
        # Check the request data for JSON
        if request.is_json:
            content=request.get_json()
            medical_history = content["medical_history"]

            if len(medical_history) != 0:
                # Commenting below line for capturing exact user id from request body. 18 April 2022.
                # user_id = medical_history[0]['update_register']['user_id'] #Extract the user Id from update register
                userId = content["USER_ID"]
                set_current_user(userId)
                if('APP_VERSION' in content):
                    setApp_Version(content['APP_VERSION'])

                is_valid_id = validate_id(userId)
                if not is_valid_id:
                    response =  json.dumps({
                                "message": "User ID is not Valid.", 
                                "status": "FAILURE",
                                "status_code":"401",
                                "data": []
                                })
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
                    # cloud_logger.info("Token Validated.")
                    print("Token Validated.")
                    is_valid_inputs, message = validate_inputs(medical_history)
                    if not is_valid_inputs:
                        response =  json.dumps({
                                    "message": message, 
                                    "status": "FAILURE",
                                    "status_code":"401",
                                    "data": []
                                    })
                        return response
                    else:
                        # cloud_logger.info("Inputs Validated.")
                        print("Inputs Validated.")
                        success, ignores, upserts = UpsertMedicalHistory(medical_history)

                        if(success):
                            response = json.dumps({
                                "message": "Medical History Updated: Upserts=" + str(upserts) + ", Ignores=" + str(ignores),
                                "status": "SUCCESS",
                                "status_code": 200,
                                "data": []
                            })
                            # cloud_logger.info("Medical History and additional parameters for the member is Updated.")
                            print("Medical History and additional parameters for the member is Updated.")
                        else:
                            response = json.dumps({
                                "message": "Error in Medical History Update: Upserts=" + str(upserts) + ", Ignores=" + str(ignores) + ", Fails=" + str(len(medical_history)-(upserts+ignores)),
                                "status": "FAILURE",
                                "status_code": 401,
                                "data": []
                            })
                            # cloud_logger.error("Error while retrieving Medical History.| %s | %s ",guard.current_userId, guard.current_appversion)
                            print("Error while retrieving Medical History.| %s | %s ",guard.current_userId, guard.current_appversion)

            else :
                response = json.dumps({
                    "message": "No Medical History Data given.",
                    "status": "FAILURE",
                    "status_code": 401,
                    "data": []
                })
                # cloud_logger.error("No Medical History Data given.")
                print("No Medical History Data given.")
        else:
            response = json.dumps({
                "message": "Error!! The Request Format should be in JSON.",
                "status": "FAILURE",
                "status_code": 401,
                "data": []
            })
            # cloud_logger.error("The Request Format should be in JSON.")
            print("The Request Format should be in JSON.")

    except Exception as e:
        response =  json.dumps({
                    "message": "Error updating Medical History.",
                    "status": "FAILURE",
                    "status_code": 401,
                    "data": []
            })
        # cloud_logger.error("Error updating Medical History : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        print("Error updating Medical History : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)

    finally:
        return response

if __name__=="__main__":    
    app.run(host="0.0.0.0", port=8000)
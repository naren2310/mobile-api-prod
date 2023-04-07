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
            # decoding the payload to fetch the stored details
            data = jwt.decode(token, parameters['JWT_SECRET_KEY'], algorithms=["HS256"])
            return True, data

    except jwt.ExpiredSignatureError as e:
        print("Token Expired: %s", str(e))
        return False, json.dumps({'status':'FAILURE', "status_code":"401", 'message' : 'Token Expired.'})

    except Exception as e:
        print("Invalid Token.")
        return False, json.dumps({'status':'FAILURE', "status_code":"401", 'message' : 'Invalid Token.'})
    
@app.route('/api/mobile_api_health_history', methods=['POST'])
def member_health_history():

    token_status, token_data = token_required(request)
    if not token_status:
        return token_data    

    response = None
    try:
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
                        print("Inputs Validated.")
                        success, ignores, upserts = UpsertMedicalHistory(medical_history)

                        if(success):
                            response = json.dumps({
                                "message": "Medical History Updated: Upserts=" + str(upserts) + ", Ignores=" + str(ignores),
                                "status": "SUCCESS",
                                "status_code": 200,
                                "data": []
                            })
                            print("Medical History and additional parameters for the member is Updated.")
                        else:
                            response = json.dumps({
                                "message": "Error in Medical History Update: Upserts=" + str(upserts) + ", Ignores=" + str(ignores) + ", Fails=" + str(len(medical_history)-(upserts+ignores)),
                                "status": "FAILURE",
                                "status_code": 401,
                                "data": []
                            })
                            print("Error while retrieving Medical History.| %s | %s ",guard.current_userId, guard.current_appversion)

            else :
                response = json.dumps({
                    "message": "No Medical History Data given.",
                    "status": "FAILURE",
                    "status_code": 401,
                    "data": []
                })
                print("No Medical History Data given.")
        else:
            response = json.dumps({
                "message": "Error!! The Request Format should be in JSON.",
                "status": "FAILURE",
                "status_code": 401,
                "data": []
            })
            print("The Request Format should be in JSON.")

    except Exception as e:
        response =  json.dumps({
                    "message": "Error updating Medical History.",
                    "status": "FAILURE",
                    "status_code": 401,
                    "data": []
            })
        print("Error updating Medical History : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)

    finally:
        return response

@app.route('/api/mobile_api_health_history/hc', methods=['GET'])
def mobile_api_health_history_health_check():
    return {"status": "OK", "message": "success mobile_api_health_history health check"} 

if __name__=="__main__":    
    app.run(host="0.0.0.0", port=8000)
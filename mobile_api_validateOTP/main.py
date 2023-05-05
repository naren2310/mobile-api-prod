from spannerDB import *
from flask import Flask, request

app = Flask(__name__)

@app.route('/api/mobile_api_validateOTP', methods=['POST'])
def validateOTP():
    try:
        print("**********Validate OTP************")
        if (request.is_json):
            request_data = request.get_json()
            mobile = request_data['mobile_number']
            otp = request_data['otp']
            if ("APP_VERSION" in request_data):
                setApp_Version(request_data['APP_VERSION'])
            is_valid_mobile, is_valid_otp = validate_inputs(mobile, otp)
            if not is_valid_mobile and not is_valid_otp:
                print("Supplied mobile number and OTP is empty or not Valid.")
                response =  json.dumps({
                            "message": "Invalid Mobile Number and OTP. Please check.",
                            "status": "FAILURE",
                            "status_code":"401",
                            "data": []
                            })
                return response
            elif not is_valid_mobile:
                print("Supplied mobile number is empty or not Valid.")
                response =  json.dumps({
                            "message": "Invalid Mobile Number. Please check.",
                            "status": "FAILURE",
                            "status_code":"401",
                            "data": []
                            })
                return response
            elif not is_valid_otp:
                print("Supplied OTP is empty or not Valid.")
                response =  json.dumps({
                            "message": "Incorrect OTP. Please check.",
                            "status": "FAILURE",
                            "status_code":"401",
                            "data": []
                            })
                return response
            else:
                auth_token, user_details = fetch_from_spanner(mobile)

                spanner_otp = auth_token["otp_key"]
                spanner_otp_ts = auth_token["otp_ts"]
                spanner_token = auth_token["token_key"]
                spanner_token_ts = auth_token["token_ts"]

                print("OTP from User: {}. OTP from Spanner: {}".format(str(otp), str(spanner_otp)))
                if otp == str(spanner_otp):
                    session_result = session_otp(spanner_otp_ts)
                    print("OTP is Valid and Timediff is: {}".format(str(session_result)))
                    if session_result:
                        # if spanner_token is not None:
                        #     if len(spanner_token) == parameters['TOKEN_DIGITS']: 
                        #         token_expiry = session_token(spanner_token_ts)
                        #         print("Token is Valid and Timediff is: {}".format(str(token_expiry)))
                        #         if token_expiry:
                        #             print("User validated succesfully.")
                        #             response_data = [{"user_details": user_details,"token": spanner_token}]
                        #             return json.dumps({"status":"SUCCESS","status_code":"200","message":"User validated succesfully.", "data":response_data})                                    
                        #         else:
                        #             print("User validated succesfully with new token.")
                        #             response_data = save_new_token(mobile, spanner_otp, spanner_otp_ts, user_details)
                        #             return json.dumps({"status":"SUCCESS","status_code":"200","message":"User validated succesfully.", "data":response_data})                                    
                        #     else:
                        #         print("Invalid Token with incorrect length.")
                        #         return json.dumps({'status': 'FAILURE', "status_code":"401",
                        #             'message':'Invalid Token with incorrect length. Please contact administrator.', "data": []})                                
                        # else:
                        #     print("New User validated succesfully.")
                        response_data = save_new_token(mobile, spanner_otp, spanner_otp_ts, user_details)
                        return json.dumps({"status":"SUCCESS","status_code":"200","message":"New User validated succesfully.", "data":response_data})                            
                    else:
                        print("OTP Session timed out.")
                        return json.dumps({"status": "FAILURE", "status_code":"401",
                                "message":'OTP Session timed out. Please try again.', "data": []})                        
                else:
                    print("OTP is Mismatched.")
                    return json.dumps({'status': 'FAILURE', "status_code":"401",
                        'message':'Error: OTP is not a match. Please Check.', "data": []})                 
                
        else:
            if (str(request.headers['User-Agent']).count("UptimeChecks")!=0):
                print("Uptime check trigger.")
                return json.dumps({"status":"API-ACTIVE", "status_code":"200",
                                    "message":'Uptime check trigger.'})
            else:
                print("Error!! The Request Format Should be in JSON.")
                return json.dumps({"status":"FAILURE", "status_code":"401",
                                    "message":'The Request Format Should be in JSON.'})       
            
    except Exception as e:
        print("Error while validating otp : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        return json.dumps({'status': 'FAILURE', "status_code":"401",
                            'message':'Unable to validate otp sent by user.', "data": []})

def create_access_token(mobile_number):
    # generates the JWT Token
    print("Creating Access Token.")
    token = jwt.encode({
        'mobile_number': mobile_number,
        'exp' : datetime.utcnow() + timedelta(minutes = parameters['TOKEN_EXPIRY_TIME'])
        }, parameters['JWT_SECRET_KEY'], algorithm="HS256")

    return token

def save_new_token(mobile, spanner_otp, spanner_otp_ts, user_details):
    try:
        print("Creating and Saving New Token.")

        access_token = create_access_token(mobile) 
        token_log_date = datetime.now()
        token_json = {"otp_key": spanner_otp,
            "otp_ts":str(spanner_otp_ts),
            "token_key": access_token,
            "token_ts":str(token_log_date)
        }
        read_write_transaction(token_json, mobile)
        response_data = [{"user_details": user_details,"token": access_token}]
        return response_data
        
    except Exception as e:
        print("Error while creating and saving token : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)

@app.route('/api/mobile_api_validateOTP/hc', methods=['GET'])
def mobile_api_validateOTP_health_check():
    return {"status": "OK", "message": "success mobile_api_validateOTP health check"} 
      
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
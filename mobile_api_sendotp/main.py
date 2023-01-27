from spannerDB import *
from flask import Flask, request

app = Flask(__name__)

@app.route('/mobile_api_sendotp', methods=['POST'])
def sendotp():
    try:
        print("*********Request OTP***********")
        # cloud_logger.info("*********Request OTP***********")
        if request.is_json and isinstance(request.get_json(), dict):
            request_data = request.get_json()
            mobile = request_data['mobile_number']
            if ("APP_VERSION" in request_data):
                setApp_Version(request_data['APP_VERSION'])
            is_valid_mobile = validate_mobile_no(mobile)
            if not is_valid_mobile:
                print("Supplied mobile number is empty or not Valid.")
                # cloud_logger.info("Supplied mobile number is empty or not Valid.")
                response =  json.dumps({
                            "message": "Invalid Mobile Number. Please contact the Administrator.",
                            "status": "FAILURE",
                            "status_code":"401"
                            })
                return response
            else:
                print("Mobile number validated successfully.")
                # cloud_logger.info("Mobile number validated successfully.")
                active_number, auth_token = active_mobile_number(mobile)
                spanner_token = auth_token["token_key"] if(auth_token is not None) else None
                spanner_token_ts = auth_token["token_ts"] if(auth_token is not None) else None

                if active_number:
                    #Below if condition for playstore user.
                    if mobile==parameters['PlayStore_User']:
                        flag =True
                        otp = parameters['PlayStore_OTP']
                        otp_log_date = str(datetime.now())
                    else:
                        flag, otp, otp_log_date = send_sms_primary(mobile)
                    if flag:
                        otp_json = {
                            "otp_key": otp,
                            "otp_ts": str(otp_log_date),
                            "token_key": spanner_token,
                            "token_ts": str(spanner_token_ts) if(spanner_token_ts is not None) else None
                        }

                        read_write_transaction(otp_json, mobile)
                        print("OTP sent successfully to your Mobile Number.")
                        # cloud_logger.info("OTP sent successfully to your Mobile Number.")
                        return json.dumps({"status":"SUCCESS", "status_code":"200", "message":"OTP sent successfully to your Mobile Number."})

                    else:
                        print("Unable to send OTP to Mobile Number.")
                        # cloud_logger.error("Unable to send OTP to Mobile Number.")
                        return json.dumps({"status":"FAILURE", "status_code":"401",
                                "message":"Unable to send OTP to this {} Mobile Number. Please contact the Administrator.".format(mobile)})
                
                else:
                    print("User does not exist.")
                    # cloud_logger.error("User does not exist.")
                    return json.dumps({"status":"FAILURE", "status_code":"401",
                                    "message":'User does not exist. Please contact the Administrator.'})
        else:            
            if (str(request.headers['User-Agent']).count("UptimeChecks")!=0):
                print("Uptime check trigger.")
                # cloud_logger.info("Uptime check trigger.")
                return json.dumps({"status":"API-ACTIVE", "status_code":"200",
                                    "message":'Uptime check trigger.'})
            else:
                print("Error!! The Request Format Should be in JSON.")
                # cloud_logger.error("Error!! The Request Format Should be in JSON.")
                return json.dumps({"status":"FAILURE", "status_code":"401",
                                    "message":'The Request Format Should be in JSON.'})
    
    except Exception as e:
        print("Unable to send the OTP to the user due to : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        # cloud_logger.error("Unable to send the OTP to the user due to : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        return json.dumps({"status":"FAILURE", "status_code":"401",
                            "message":'Error in Login. Please contact the Administrator.'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
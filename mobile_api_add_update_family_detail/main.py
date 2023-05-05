from spannerOperations import *
from flask import Flask, request

app = Flask(__name__)


"""
REQUEST BODY 

{
"familyList":
	[
		{
			“family_details”: 
                { 
                    “family_id” : “”,
                    “”:””,
                    "street_id":""		
                }
			"family_members":
            [
                {
                    "member_id”:”member1 Details"
                },
                {
                    "member_id”":"member2 Details"
                },
                {
                    "member_id”":"member3 Details"
                },
            ]
		}
	]
}

RESPONSE :
[
    "family_details":
    [
        {
            "family_id":req_family_id,
            "phr_family_id":phr_id
        },
        {
            "family_id":req_family_id,
            "phr_family_id":phr_id
        }
    ]
    "member_details":[
        {
            "member_id":member_id,
            "unique_health_id":new_unique_health_id
        },
        {
            "member_id":member_id,
            "unique_health_id":new_unique_health_id
        }
    ]
]
"""

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
            print("add_update_family_details token_required",e)

@app.route('/api/mobile_api_add_update_family_detail', methods=['POST'])
def add_update_family_details():

    token_status, token_data = token_required(request)
    if not token_status:
        return token_data 
      
    response = None

    try:
        print("********Add Update Family Details********")
        # Check the request data for JSON
        if request.is_json:
            content=request.get_json()
            userId = content['USER_ID']
            set_current_user(userId)
                
            if('APP_VERSION' in content):
                setApp_Version(content['APP_VERSION'])
            
            family_list = content["family_list"]
            
            if len(family_list) != 0:
               
                # print(current_appversion)
                # print(guard.current_appversion)          
                is_valid_id = validate_id(userId, content["FACILITY_ID"])
                if not is_valid_id:
                    response =  json.dumps({
                                "message": "User ID/Facility ID is not Valid.", 
                                "status": "FAILURE",
                                "status_code":"401",
                                "data": []
                                })
                    return response
                else:
                    is_token_valid = user_token_validation(userId, token_data["mobile_number"], content["FACILITY_ID"])
                    if not is_token_valid:
                        response =  json.dumps({
                                "message": "Unregistered User/Inputs mismatch.", 
                                "status": "FAILURE",
                                "status_code":"401"
                                })
                        return response
                    print("Token Validated.")
                    is_valid_inputs, message = validate_inputs(content)
                    if not is_valid_inputs:
                        response =  json.dumps({
                                "message": message, 
                                "status": "FAILURE",
                                "status_code":"401",
                                "data": []
                                })
                        return response
                    else:
                        print('Inputs validated.')
                        print('Starting Upsert of Family Details.')
                        is_success, family_details, member_details = UpsertFamilyDetails(family_list, userId)

                        if is_success:
                            response = json.dumps({
                                "message": "Family details Added/Updated.",
                                "status": "SUCCESS",
                                "status_code": 200,
                                "data": [{"family_details":family_details},{"member_details":member_details}]
                            })
                            print("Family Details and additional parameters are Added/Updated.")
                        else:
                            response = json.dumps({
                                "message": "Error in Addition/Updation of Family Details.",
                                "status": "FAILURE",
                                "status_code": 401,
                                "data": []
                            })
                            print("Error in Addition/Updation of Family Details.| %s | %s ", guard.current_userId, guard.current_appversion)
            else:
                response = json.dumps({
                "message": "No Family Details were given.",
                "status": "FAILURE",
                "status_code": 401,
                "data": []
                })
                print("No Family Details were given.")
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
                    "message": "Error while updating family data, Please Retry.",
                    "status": "FAILURE",
                    "status_code": 401,
                    "data": []
            })
        print("Error while updating family data : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)

    finally:
        return response
    

@app.route('/api/mobile_api_add_update_family_detail/hc', methods=['GET'])
def add_update_family_details_health_check():
    return {"status": "OK", "message": "success mobile_api_add_update_family_detail health check"}
    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
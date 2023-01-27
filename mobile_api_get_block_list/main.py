from guard import *
import guard

"""
******Method Details******
Description: API to retrieve list of all the blocks.
MIME Type: application/json
Input: {"USER_ID":<User_Id>, "DISTRICT_ID":<Timestamp>, "API_KEY":<Token>}
Output: List of Blocks from address_block_master
[{
    "USER_ID":<id>
    "DISTRICT_ID":<id>, 
    
}]
"""

# decorator for verifying the JWT
def token_required(request):
    token = None
        # jwt is passed in the request header
    if 'x-access-token' in request.headers:
        token = request.headers['x-access-token']
    if not token:
        if (str(request.headers['User-Agent']).count("UptimeChecks")!=0):
            cloud_logger.info("Uptime check trigger.")
            return False, json.dumps({"status":"API-ACTIVE", "status_code":"200","message":'Uptime check trigger.'})
        else:
            cloud_logger.critical("Invalid Token.")
            return False, json.dumps({'status':'FAILURE', "status_code":"401", 'message' : 'Invalid Token.'})
    try:
        token = token.strip() #Remove spaces at the beginning and at the end of the token
        token_format = re.compile(parameters['TOKEN_FORMAT'])
        if not token_format.match(token):
            cloud_logger.critical("Invalid Token format.")
            return False, json.dumps({'status':'FAILURE',"status_code":"401",'message' : 'Invalid Token format.'})
        else:
            # decoding the payload to fetch the stored details
            data = jwt.decode(token, parameters['JWT_SECRET_KEY'], algorithms=["HS256"])
            return True, data

    except jwt.ExpiredSignatureError as e:
        cloud_logger.critical("Token Expired: %s", str(e))
        return False, json.dumps({'status':'FAILURE', "status_code":"401", 'message' : 'Token Expired.'})

    except Exception as e:
        cloud_logger.critical("Invalid Token: %s", str(e))
        return False, json.dumps({'status':'FAILURE',"status_code":"401",'message' : 'Invalid Token.'})
    
def get_block_list(request):
    
    token_status, token_data = token_required(request)
    if not token_status:
        return token_data
    response = None

    try:
        cloud_logger.info("********Get Block List*********")
        block_list=[]
        if (request.is_json):
            content=request.get_json()
            districtId = content["DISTRICT_ID"]
            userId = content['USER_ID']
            set_current_user(userId)
                
            if('APP_VERSION' in content):
                setApp_Version(content['APP_VERSION'])
            #request body contains two ID attributes like user id and district id
            is_valid_id = validate_id_attribute(userId, districtId)
            if not is_valid_id:
                response =  json.dumps({
                            "message": "Supplied IDs are not Valid.", 
                            "status": "FAILURE",
                            "status_code":"401",
                            "data": {}
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
                else:
                    cloud_logger.info("Token Validated.")
                    with spnDB.snapshot() as snapshot:   
                        query = "SELECT DISTINCT  block_name ,block_id from address_block_master WHERE district_id=@district_id"

                        results = snapshot.execute_sql(
                            query,
                            params={
                                "district_id": districtId
                                },
                            param_types={
                                "district_id": param_types.STRING},                   
                                )

                        for row in results:
                            block = {
                                    "block_name":row[0],
                                    "block_id":row[1]
                                    }
                            block_list.append(block)

                        if len(block_list) == 0:
                            response =  json.dumps({
                                "message": "There are no Blocks available, Please contact administrator.", 
                                "status": "SUCCESS",
                                "status_code":"200",
                                "data": {}
                            })
                            cloud_logger.info("There is no Blocks available.")
                        else:
                            response =  json.dumps({
                                "message": "Success retrieving Block data.", 
                                "status": "SUCCESS",
                                "status_code":"200",
                                "data": {"block_list":block_list}
                            })
                            cloud_logger.info("Success retrieving Block data.")
        else :
            response =  json.dumps({
                            "message": "Error!! The Request format should be in JSON.", 
                            "status": "FAILURE",
                            "status_code":"401",
                            "data": {}
                        })
            cloud_logger.error("The Request format should be in JSON.")
    except Exception as e:
        response =  json.dumps({
                            "message": "Error while retrieving Block data, Please Retry.",
                            "status": "FAILURE",
                            "status_code": "401",
                            "data": {}
                        })
        cloud_logger.error("Error while retrieving Block data : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
    finally:
        return response
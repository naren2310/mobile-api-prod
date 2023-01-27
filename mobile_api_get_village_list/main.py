from guard import *
import guard

"""
******Method Details******
Description: API to retrieve list of all the VIllages.
MIME Type: application/json
Input: Last update Timestamp - {"USER_ID":<User_Id>, "BLOCK_ID":<Timestamp>, "API_KEY":<Token>}
Output: Master data from address_village_master
{
    "USER_ID":<id>,
    "BLOCK_ID":<id> 
}
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
    
def get_village_list_from_block(request):
    
    token_status, token_data = token_required(request)
    if not token_status:
        return token_data

    response = None

    try:
        cloud_logger.info("********Get Village List*********")
        village_list=[]
        if (request.is_json):
            content=request.get_json()
            blockId = content["BLOCK_ID"]
            userId = content['USER_ID']
            set_current_user(userId)
                
            if('APP_VERSION' in content):
                setApp_Version(content['APP_VERSION'])
            #request body contains two ID attributes like user id and block id
            is_valid_id = validate_id_attribute(userId, blockId)
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
                    if (blockId is not None and blockId !=''):
                        with spnDB.snapshot() as snapshot:   
                            query = "SELECT DISTINCT village_name ,village_id from address_village_master WHERE block_id=@block_id"

                            results = snapshot.execute_sql(
                                query,
                                params={
                                    "block_id": blockId
                                },
                                param_types={
                                    "block_id": param_types.STRING,
                                },                   
                            )

                            for row in results:

                                village = {
                                    "village_name":row[0],
                                    "village_id":row[1]
                                }
                                village_list.append(village)          

                        if len(village_list) == 0:
                            response =  json.dumps({
                                "message": "There are no Villages available, Please Contact Administrator.", 
                                "status": "SUCCESS",
                                "status_code":"200",
                                "data": {}
                            })
                            cloud_logger.info("No Villages available.")
                        else:
                            response =  json.dumps({
                                "message": "Success retrieving Villages data.", 
                                "status": "SUCCESS",
                                "status_code":"200",
                                "data": {"village_list":village_list}
                            })
                            cloud_logger.info("Success retrieving Villages data.")
                    else:
                        response =  json.dumps({
                                "message": "Block ID should not be Empty.", 
                                "status": "SUCCESS",
                                "status_code":"200",
                                "data": {}
                            })
                        cloud_logger.info("Block ID should not be Empty.")
        else :
            response =  json.dumps({
                    "message": "Error!! The Request Format should be in JSON.", 
                    "status": "FAILURE",
                    "status_code":"401",
                    "data": {}
                })
            cloud_logger.info("The Request Format should be in JSON.")

    except Exception as e:
        response =  json.dumps({
                    "message": "Error while retrieving Villages data, Please Retry.",
                    "status": "FAILURE",
                    "status_code": "401",
                    "data": {}
                })
        cloud_logger.info("Error while retrieving Villages data : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)

    finally:
        return response
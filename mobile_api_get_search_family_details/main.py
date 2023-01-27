
from get_family_data import *

"""
******Method Details******
Description: API to get Family and Member details along with Medical, Screening and Socio Reference details for each member.
MIME Type: application/json
Input: {"FAMILY_ID":<User_Id>, "MEMBER_ID":<Member_Id>, "LAST_UPDATE":<Timestamp>, "API_KEY":<Token>}
Output: Master data from family_master, family_socio_economic_ref, family_member_master, family_member_socio_economic_ref, health_history, health_screening
[{
    "FAMILY_ID": <id>,
    "MEMBER_ID": <id>,
    "LAST_UPDATE": <TIMESTAMP>,
    "OFFSET": <int>,
    "LIMIT": <int>
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

def get_family_and_member_details(request):

    token_status, token_data = token_required(request)
    if not token_status:
      return token_data

    response = {}

    try:
        cloud_logger.info("********** Get Search Family and Member Details **********")
        # Check the request data for JSON
        if (request.is_json):            
            content = request.get_json()

            if('USER_ID' in content):
                set_current_user(content['USER_ID'])

            if('APP_VERSION' in content):
                setApp_Version(content['APP_VERSION'])
            
            familyId = content["FAMILY_ID"]
            memberId = content["MEMBER_ID"]
            cloud_logger.info("Fetching Family and Member Details.")

            is_valid_id = validate_id_attribute(familyId, memberId)

            if not is_valid_id:
                response =  json.dumps({
                            "message": "Supplied IDs are empty or not Valid.",
                            "status": "FAILURE",
                            "status_code":"401",
                            "data": {}
                            })
                return response
            else:
                is_id_exist = check_id_registered(familyId, memberId)
                if not is_id_exist:
                    cloud_logger.info("Unregistered family Id/member Id.")
                    response =  json.dumps({
                            "message": "Unregistered family Id/member Id.",
                            "status": "FAILURE",
                            "status_code":"401",
                            "data": {}
                            })
                    return response
                else:
                    cloud_logger.info("Inputs Validated.")
                    if (familyId is not None and familyId !=''):
                        family_details = get_family_data(familyId)
                        members = get_all_member_data(familyId)
                        member_list = []

                        for member in members:
                            member_details = {}
                            memberId =member.get('member_id')
                            medical_history = get_health_history(familyId, memberId)
                            member_ref_details = get_member_socioEconomic_data(familyId, memberId)
                            screenings = get_health_screening(familyId, memberId)

                            if len(medical_history.keys()) > 0 and len(member_ref_details.keys()) > 0:
                                medical_history["family_socio_economic_id"] = member_ref_details.get('family_socio_economic_id')
                                medical_history["social_details"] = member_ref_details.get('social_details')
                                medical_history["economic_details"] = member_ref_details.get('economic_details')
                                medical_history['resident_status'] = member.get('resident_status')
                                medical_history['resident_status_details'] = member.get('resident_status_details')
                            else:
                                medical_history = None

                            member.pop('resident_status')
                            member.pop('resident_status_details')
                            
                            member_details["member_details"] = member
                            member_details["medical_history"] = medical_history
                            member_details["screenings"] = screenings['data_list']
                            member_list.append(member_details)

                        response["family_details"] = family_details
                        response["member_list"] = member_list
                        response['address_details'] = get_address_data(familyId)

                        response =  json.dumps({
                            "message": "Data retrieved Successfully.", 
                            "status": "SUCCESS",
                            "status_code":"200",
                            "data": {"family_details": response['family_details'], "member_list":response["member_list"], "address_details":response['address_details']}
                        })
                        cloud_logger.info("Data retrieved Successfully.")

                    elif (memberId is not None and memberId !=''):
                        familyId = get_member_familyId(memberId)
                        family_details=get_family_data(familyId)
                        members = get_all_member_data(familyId)
                        member_list = []
                        cloud_logger.debug("Member ID = {}".format(memberId))

                        for member in members:
                            member_details = {}
                            memberId =member.get('member_id')
                            medical_history = get_health_history(familyId, memberId)
                            member_ref_details = get_member_socioEconomic_data(familyId, memberId)
                            screenings = get_health_screening(familyId, memberId)

                            if len(medical_history.keys()) > 0 and len(member_ref_details.keys()) > 0:
                                medical_history["family_socio_economic_id"] = member_ref_details.get('family_socio_economic_id')
                                medical_history["social_details"] = member_ref_details.get('social_details')
                                medical_history["economic_details"] = member_ref_details.get('economic_details')
                                medical_history['resident_status'] = member.get('resident_status')
                                medical_history['resident_status_details'] = member.get('resident_status_details')
                            else:
                                medical_history = None

                            member.pop('resident_status')
                            member.pop('resident_status_details')
                            
                            member_details["member_details"] = member
                            member_details["medical_history"] = medical_history
                            member_details["screenings"] = screenings['data_list']
                            member_list.append(member_details)
                        
                        response["family_details"] = family_details
                        response["member_list"] = member_list
                        response['address_details'] = get_address_data(familyId)

                        response =  json.dumps({
                            "message": "Data retrieved Successfully.", 
                            "status": "SUCCESS",
                            "status_code":"200",
                            "data": {"family_details": response['family_details'], "member_list":response["member_list"], "address_details":response['address_details']}
                        })
                        cloud_logger.info("Data retrieved Successfully.")
                    else:
                        response =  json.dumps({
                            "message": "Invalid Inputs. Atleast FamilyId or MemberId is required.", 
                            "status": "FAILURE",
                            "status_code":"401",
                            "data": {}
                        })
                        cloud_logger.error("Invalid Inputs. Atleast FamilyId or MemberId is required.")
        else :
            response =  json.dumps({
                    "message": "Error!! The Request Format must be in JSON.", 
                    "status": "FAILURE",
                    "status_code":"401",
                    "data": {}
                })
            cloud_logger.error("The Request Format must be in JSON.")   

    except Exception as e:
        response =  json.dumps({
                    "message": "Error while retrieving family and member Data.", 
                    "status": "FAILURE",
                    "status_code": "401",
                    "data": {}
                })
        cloud_logger.error("Error while retrieving family and member Data : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)

    finally :
        return response
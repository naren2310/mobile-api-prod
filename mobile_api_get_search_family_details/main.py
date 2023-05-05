
from get_family_data import *
from flask import Flask, request
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
            print("mobile_api_get_search_family_details token_required",e)

@app.route('/api/mobile_api_get_search_family_details', methods=['POST'])
def get_family_and_member_details():

    token_status, token_data = token_required(request)
    if not token_status:
      return token_data

    response = {}

    try:
        print("********** Get Search Family and Member Details **********")
        # Check the request data for JSON
        if (request.is_json):            
            content = request.get_json()

            if('USER_ID' in content):
                set_current_user(content['USER_ID'])

            if('APP_VERSION' in content):
                setApp_Version(content['APP_VERSION'])
            
            familyId = content["FAMILY_ID"]
            memberId = content["MEMBER_ID"]
            print("Fetching Family and Member Details.")

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
                    print("Unregistered family Id/member Id.")
                    response =  json.dumps({
                            "message": "Unregistered family Id/member Id.",
                            "status": "FAILURE",
                            "status_code":"401",
                            "data": {}
                            })
                    return response
                else:
                    print("Inputs Validated.")
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
                        print("Data retrieved Successfully.")

                    elif (memberId is not None and memberId !=''):
                        familyId = get_member_familyId(memberId)
                        family_details=get_family_data(familyId)
                        members = get_all_member_data(familyId)
                        member_list = []
                        print("Member ID = {}".format(memberId))

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
                        print("Data retrieved Successfully.")
                    else:
                        response =  json.dumps({
                            "message": "Invalid Inputs. Atleast FamilyId or MemberId is required.", 
                            "status": "FAILURE",
                            "status_code":"401",
                            "data": {}
                        })
                        print("Invalid Inputs. Atleast FamilyId or MemberId is required.")
        else :
            response =  json.dumps({
                    "message": "Error!! The Request Format must be in JSON.", 
                    "status": "FAILURE",
                    "status_code":"401",
                    "data": {}
                })
            print("The Request Format must be in JSON.") 

    except psycopg2.ProgrammingError as e:
        print("get_family_and_member_details ProgrammingError",e)  
        conn.rollback()
        response =  json.dumps({
                    "message": "Error while retrieving family and member Data.", 
                    "status": "FAILURE",
                    "status_code": "401",
                    "data": {}
                })
    except psycopg2.InterfaceError as e:
        print("get_family_and_member_details InterfaceError",e)
        reconnectToDB()
        response =  json.dumps({
                    "message": "Error while retrieving family and member Data.", 
                    "status": "FAILURE",
                    "status_code": "401",
                    "data": {}
                })

    finally :
        return response

@app.route('/api/mobile_api_get_search_family_details/hc', methods=['GET'])
def mobile_api_get_search_family_details_health_check():
    return {"status": "OK", "message": "success mobile_api_get_search_family_details health check"}

if __name__=="__main__":    
    app.run(host="0.0.0.0", port=8000)

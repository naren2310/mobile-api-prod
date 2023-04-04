from guard import *
import guard
from flask import Flask, request

"""
******Method Details******
Description: API to retrieve Village, Street and Facility Data
MIME Type: application/json
Input: Last update Timestamp - {"USER_ID":<User_Id>, "LAST_UPDATE":<Timestamp>, "API_KEY":<Token>}
Output: Master data from address_village_master, address_street_master, facility_registry, facility_type_master
[{
    "village_id":<id>, "village_gid":<gid>, "village_name":<name>, 
    streets:[{
        "street_id:<id>, "street_gid":<gid>, "street_name":<name>, 
        "facility":[{
            "facility_id":<id>, "facility_name":<name>, "facility_gid":<name>, "facility_type":<type>
        }]
    }]
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
        print("Invalid Token: %s", str(e))
        return False, json.dumps({'status':'FAILURE',"status_code":"401",'message' : 'Invalid Token.'})

@app.route('/api/mobile_api_get_village_street', methods=['POST'])
def get_village_street():

    token_status, token_data = token_required(request)
    if not token_status:
        return token_data    

    response = None

    try:
        print("*********Get Village Street**********")
        villages_list=[]
        # Check the request data for JSON
        if (request.is_json):
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
                    return response
                else:
                    print("Token Validated.")
                    userId = content["USER_ID"]
                    conn = get_db_connection()
                    with conn.cursor() as cursor:   
                        query = "SELECT facility_id,facility_level,country_id,state_id,region_id,district_id,hud_id,block_id FROM public.facility_registry WHERE facility_id = (SELECT facility_id FROM public.user_master WHERE user_id =%s)"
                        value = (userId,)
                        cursor.execute(query,value)
                        results = cursor.fetchall()
                    for row in results:
                            facility_details = {
                                "country_id": row[2] if row[2] is not None else '',
                                "state_id": row[3] if row[3] is not None else '',
                                "district_id": row[5] if row[5] is not None else '',
                                "hud_id" : row[6] if row[6] is not None else '',
                                "block_id" : row[7] if row[7] is not None else ''
                            }
                            villages_list = retrieve_villages_from(facility_details['country_id'], facility_details['state_id'], facility_details['district_id'], facility_details['hud_id'], facility_details['block_id'])

                    if len(villages_list) == 0:
                        response =  json.dumps({
                                                "message": "No Data available.", 
                                                "status": "SUCCESS",
                                                "status_code":"200",
                                                "data": []
                                            })
                    else:
                        response =  json.dumps({
                                                "message": "Success retrieving Village_street Data.", 
                                                "status": "SUCCESS",
                                                "status_code":"200",
                                                "data": villages_list
                                            })
        else:
            response = json.dumps({
                                    "message": "Error!! The Request Format must be in JSON.",
                                    "status": "FAILURE",
                                    "status_code": 401,
                                    "data": []
                                    })
            print("The Request Format must be in JSON.")
    except psycopg2.ProgrammingError as e:
        print("Village_street ProgrammingError",e)  
        conn.rollback()
    except psycopg2.InterfaceError as e:
        print("Village_street InterfaceError",e)
        reconnectToDB()
        response =  json.dumps({
                    "message": "Error while retrieving Village_street Data.", 
                    "status": "FAILURE",
                    "status_code":"401",
                    "data": []
                    })
        print("Error while retrieving Village_street Data : %s | %s | %s", str(e), guard.current_userId, guard.current_appversion)

    finally:
        cursor.close()
        conn.close()
        return response
    

def retrieve_villages_from(countryId, stateId, districtId, hudId, blockId):
    try:
        print("Retrieving the Villages from block id : %s", str(blockId))
        villages_list=[]
        address_list=[]
        conn = get_db_connection()
        with conn.cursor() as cursor: 
            query = "with village as (SELECT village_id,village_gid,village_name,country_id,state_id,district_id,hud_id,block_id FROM public.address_village_master WHERE village_name not like ('Unallocated%%') AND country_id =%s AND state_id =%s AND district_id =%s AND hud_id =%s AND block_id =%s),street as (SELECT v.village_id,v.village_gid,v.village_name,asm.street_id, asm.street_gid, asm.street_name, asm.facility_id FROM village v left join public.address_street_master asm on asm.village_id = v.village_id AND asm.country_id = v.country_id AND asm.state_id = v.state_id AND asm. district_id= v.district_id AND asm.hud_id = v. hud_id AND asm.block_id = v.block_id AND asm.active=true WHERE street_name not like ('Unallocated%%') AND facility_id is not NULL ) SELECT  S.village_id,S.village_gid,S.village_name,S.street_id, S.street_gid, S.street_name, fr.facility_id, fr.institution_gid, fr.facility_name, typ.facility_type_name FROM street s LEFT join public.facility_registry fr on S.facility_id=fr.facility_id LEFT JOIN public.facility_type_master typ on typ.facility_type_id=fr.facility_type_id order by S.village_name"
            value = (countryId,stateId,districtId,hudId,blockId)
            cursor.execute(query,value)
            address_list = cursor.fetchall()
            villages_list = get_villages_list(address_list)
        
    except psycopg2.ProgrammingError as e:
        print("Village_street retrieve_villages_from ProgrammingError",e)  
        conn.rollback()
    except psycopg2.InterfaceError as e:
        print("Village_street retrieve_villages_from InterfaceError",e)
        reconnectToDB()

    finally:
        cursor.close()
        conn.close()
        return villages_list

def get_villages_list(address_list):
    try:
        villages=[]
        streets_list=[]
        villageId=""
        tempVillageId=""
        for row in address_list:
            tempVillageId=row[0]
            if tempVillageId!=villageId:
                village = {
                    "village_id":row[0],
                    "village_gid":row[1],
                    "village_name":row[2],
                    }
                streets_list=[]
                if not any(tempVillageId in x for x in villages):
                    villages.append(village) 
                    
            facility = {
                "facility_id":row[6], 
                "facility_gid":row[7],
                "facility_name":row[8],
                "facility_type":row[9],
            }
            street = {
                "street_id":row[3], 
                "street_gid":row[4],
                "street_name":row[5],
                "facility": [facility]
            }
            streets_list.append(street)
            village["streets"]=streets_list

            villageId=tempVillageId
            # streets_list=[]
        # # Adding the last record to the response list.
        # village["streets"]=streets_list

    except Exception as e:
        print("Error while fetching Villages Data : %s", str(e))

    finally:
        return villages


# This was the older logic for Village Street. It was shifting one street to other village. The fix was made in the above function on 9 May 2022. Shreyas / Rajesh.

# def get_village_list(address_list):
    
#     try:
#         villages=[]
#         streets_list=[]
#         villageId=""
#         tempVillageId=""
#         for row in address_list:
#             tempVillageId=row[0]
#             print(row)
#             facility = {
#                 "facility_id":row[6], 
#                 "facility_gid":row[7],
#                 "facility_name":row[8],
#                 "facility_type":row[9],
#             }
#             street = {
#                 "street_id":row[3], 
#                 "street_gid":row[4],
#                 "street_name":row[5],
#                 "facility": [facility]
#             }
#             streets_list.append(street)

#             if tempVillageId!=villageId:
#                 if villageId!="":
#                     village["streets"]=streets_list
#                     villages.append(village)
#                     streets_list=[]
#                 village = {
#                     "village_id":row[0],
#                     "village_gid":row[1],
#                     "village_name":row[2],
#                 }
#                 villageId=tempVillageId
#         # Adding the last record to the response list.
#         village["streets"]=streets_list
#         villages.append(village)

#     except Exception as e:
#         cloud_logger.error("Error while fetching Villages Data : %s", str(e))

#     finally:
#         return villages


if __name__=="__main__":    
    app.run(host="0.0.0.0", port=8000)
from guard import *
import guard
from flask import Flask, request
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

@app.route('/api/mobile_api_get_village_list', methods=['POST'])
def get_village_list_from_block():
    
    token_status, token_data = token_required(request)
    if not token_status:
        return token_data

    response = None

    try:
        print("********Get Village List*********")
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
                    print("Token Validated.")
                    if (blockId is not None and blockId !=''):
                        conn = get_db_connection()
                        with conn.cursor() as cursor:
                            query = "SELECT country_id ,state_id,district_id,hud_id,block_id from public.address_village_master WHERE block_id=%s"                       
                            value = (blockId,)
                            cursor.execute(query,value)
                            results = cursor.fetchall()
                        for row in results:
                            facility_details = {
                                "country_id": row[0] if row[0] is not None else '',
                                "state_id": row[1] if row[1] is not None else '',
                                "district_id": row[2] if row[2] is not None else '',
                                "hud_id" : row[3] if row[3] is not None else '',
                                "block_id" : row[4] if row[4] is not None else ''
                            }
                            village_list = retrieve_villages_from(facility_details['country_id'], facility_details['state_id'], facility_details['district_id'], facility_details['hud_id'], facility_details['block_id'])
                            
                        if len(village_list) == 0:
                            response =  json.dumps({
                                "message": "There are no Villages available, Please Contact Administrator.", 
                                "status": "SUCCESS",
                                "status_code":"200",
                                "data": {}
                            })
                            print("No Villages available.")
                        else:
                            response =  json.dumps({
                                "message": "Success retrieving Villages data.", 
                                "status": "SUCCESS",
                                "status_code":"200",
                                "data": {"village_list":village_list}
                            })
                            print("Success retrieving Villages data.")
                    else:
                        response =  json.dumps({
                                "message": "Block ID should not be Empty.", 
                                "status": "SUCCESS",
                                "status_code":"200",
                                "data": {}
                            })
                        print("Block ID should not be Empty.")
        else :
            response =  json.dumps({
                    "message": "Error!! The Request Format should be in JSON.", 
                    "status": "FAILURE",
                    "status_code":"401",
                    "data": {}
                })
            print("The Request Format should be in JSON.")

    except psycopg2.ProgrammingError as e:
        print("get_village_list_from_block get_village_list_from_block ProgrammingError",e)  
        conn.rollback()
        response =  json.dumps({
                    "message": "Error while retrieving Villages data, Please Retry.",
                    "status": "FAILURE",
                    "status_code": "401",
                    "data": {}
                })
    except psycopg2.InterfaceError as e:
        print("get_village_list_from_block get_village_list_from_block InterfaceError",e)
        reconnectToDB()
        response =  json.dumps({
                    "message": "Error while retrieving Villages data, Please Retry.",
                    "status": "FAILURE",
                    "status_code": "401",
                    "data": {}
                })
        print("Error while retrieving Villages data : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)

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
        print("get_village_list_from_block retrieve_villages_from ProgrammingError",e)  
        conn.rollback()
    except psycopg2.InterfaceError as e:
        print("get_village_list_from_block retrieve_villages_from InterfaceError",e)
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

    except Exception as e:
        print("Error while fetching Villages Data : %s", str(e))

    finally:
        return villages
        
  
if __name__=="__main__":    
    app.run(host="0.0.0.0", port=8000)
from guard import *
import guard
from flask import Flask, request

"""
******Method Details******
Description: API to retrieve Families Data
MIME Type: application/json
Input: Last update Timestamp - {"USER_ID":<User_Id>, "LAST_UPDATE":<Timestamp>, "API_KEY":<Token>}
Output: Master data from family_master and family_socio_economic_ref
[{
    "family_id":<id>,
    "phr_family_id":<phr_family_id>, 
    ...
    ...
    "economic_details":{}
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
            # cloud_logger.info("Uptime check trigger.")
            print("Uptime check trigger.")
            return False, json.dumps({"status":"API-ACTIVE", "status_code":"200","message":'Uptime check trigger.'})
        else:
            # cloud_logger.critical("Invalid Token.")
            print("Invalid Token.")
            return False, json.dumps({'status':'FAILURE', "status_code":"401", 'message' : 'Invalid Token.'})

    try:
        token = token.strip() #Remove spaces at the beginning and at the end of the token
        token_format = re.compile(parameters['TOKEN_FORMAT'])
        if not token_format.match(token):
            # cloud_logger.critical("Invalid Token format.")
            print("Invalid Token format.")
            return False, json.dumps({'status':'FAILURE',"status_code":"401",'message' : 'Invalid Token format.'})
        else:            
            # decoding the payload to fetch the stored details
            data = jwt.decode(token, parameters['JWT_SECRET_KEY'], algorithms=["HS256"])
            return True, data

    except jwt.ExpiredSignatureError as e:
        # cloud_logger.critical("Token Expired: %s", str(e))
        print("Token Expired: %s", str(e))
        return False, json.dumps({'status':'FAILURE', "status_code":"401", 'message' : 'Token Expired.'})

    except Exception as e:
        # cloud_logger.critical("Invalid Token: %s", str(e))
        print("Invalid Token: %s", str(e))
        return False, json.dumps({'status':'FAILURE',"status_code":"401",'message' : 'Invalid Token.'})

@app.route('/api/mobile_api_get_family_data', methods=['POST'])
def get_families_data():

    token_status, token_data = token_required(request)
    if not token_status:
        return token_data
    
    response = None
    try:
        # cloud_logger.info("********Get Families Data*********")
        print("********Get Families Data*********")
        defaultTime = datetime.strptime('2021-09-01 15:52:50+0530', "%Y-%m-%d %H:%M:%S%z")
        # Check the request data for JSON
        if request.is_json:
            request_data =request.get_json()
            userId = request_data["USER_ID"]
            set_current_user(userId)
            
            if ("APP_VERSION" in request_data):
               setApp_Version(request_data['APP_VERSION'])
            is_valid_id = validate_id(request_data["USER_ID"])

            if not is_valid_id:
                response =  json.dumps({
                            "message": "User ID is not Valid.", 
                            "status": "FAILURE",
                            "status_code":"401",
                            "data": []
                            })
                print("Provided User ID is not valid. | %s | %s", guard.current_userId, guard.current_appversion)
                # cloud_logger.error("Provided User ID is not valid. | %s | %s", guard.current_userId, guard.current_appversion)
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
                    # cloud_logger.error("Unregistered User/Token-User mismatch. | %s | %s", guard.current_userId, guard.current_appversion)
                    return response
                else:
                    print("Token Validated.")
                    # cloud_logger.info("Token Validated.")
                    families = get_family_data(userId, defaultTime, request_data)
                    if len(families) == 0:
                        response =  json.dumps({
                            "message": "There is no family data available, Please contact administrator.", 
                            "status": "SUCCESS",
                            "status_code":"200",
                            "data": []
                        })
                        print("No family data available.")
                        # cloud_logger.info("No family data available.")   
                    elif len(families) > 0 and len(families) < 3000:
                        response =  json.dumps({
                            "message": "Success retrieving Family Data.", 
                            "status": "SUCCESS-FINAL",
                            "status_code":"200",
                            "data": families
                        })
                        print("Success retrieving Family Data.")
                        # cloud_logger.info("Success retrieving Family Data.")
                    
                    else:
                        response =  json.dumps({
                            "message": "Success retrieving Family Data.", 
                            "status": "SUCCESS-CONTINUE",
                            "status_code":"200",
                            "data": families
                        })
                        print("Success retrieving Family Data.")
                        # cloud_logger.info("Success retrieving Family Data.")
        
        else :
            response =  json.dumps({
                    "message": "Error!! The request format should be in JSON.", 
                    "status": "FAILURE",
                    "status_code":"401",
                    "data": []
                })
            print("The request format should be in JSON.")
            # cloud_logger.error("The request format should be in JSON.")

    except Exception as e:
        response =  json.dumps({
                    "message": "Error while retrieving Family Data, Please Retry.", 
                    "status": "FAILURE",
                    "status_code":"401",
                    "data": []
                })
        print("Error while retrieving Family Data : %s | %s | %s", str(e), guard.current_userId, guard.current_appversion)
        # cloud_logger.error("Error while retrieving Family Data : %s | %s | %s", str(e), guard.current_userId, guard.current_appversion)

    finally:
        return response

def get_address_for(familyId):
    address = {}
    isAddressAvailable = False
    try:
        # cloud_logger.info('Getting address details from Family Id : %s', str(familyId))
        print('Getting address details from Family Id : %s', str(familyId))
        query = "with Street as (SELECT street_id,country_id,state_id,district_id,hud_id,block_id,village_id,rev_village_id,area_id,ward_id,habitation_id,hsc_unit_id FROM public.address_street_master WHERE street_gid = (SELECT street_gid FROM public.address_shop_master WHERE shop_id = (SELECT  shop_id FROM public.family_master WHERE family_id = %s AND street_id is null)))SELECT S.country_id as country_id_new,S.state_id as state_id_new ,S.district_id as district_id_new,S.hud_id as hud_id_new, S.block_id as block_id_new,rev.taluk_id as taluk_id_new ,S.village_id as village_id_new,S.rev_village_id as rev_village_id_new ,S.habitation_id as habitation_id_new,S.area_id as area_id_new,S.ward_id as ward_id_new ,S.street_id as street_id_new ,hhg.hhg_id as hhg_id_new FROM Street S left join public.address_hhg_master HHG on S.street_id=HHG.street_id left join public.address_revenue_village_master REV on hhg.rev_village_id=rev.rev_village_id"
        # with spnDB.snapshot() as snapshot:
        #     result = snapshot.execute_sql(
        #     query,
        #         params={"familyId": familyId},
        #         param_types={"familyId": param_types.STRING},
        #     )
        values = (familyId,)
        cursor.execute(query,values)
        result = cursor.fetchall()
        for row in result:
            address["country_id_new"] = row[0]
            address["state_id_new"]=row[1]
            address["district_id_new"]=row[2]
            address["hud_id_new"]=row[3]
            address["block_id_new"]=row[4]
            address["taluk_id_new"]=row[5]
            address["village_id_new"]=row[6]
            address["rev_village_id_new"]=row[7]
            address["habitation_id_new"]=row[8]
            address["area_id_new"]=row[9]
            address["ward_id_new"]=row[10]
            address["street_id_new"]=row[11]
            address["hhg_id_new"]=row[12]         
            isAddressAvailable = True
            print('Got address value.')
            # cloud_logger.info('Got address value.')
        return isAddressAvailable, address

    except psycopg2.ProgrammingError as e:
        print("get_families_data get_address_for ProgrammingError",e)  
        conn.rollback()
    except psycopg2.InterfaceError as e:
        print("get_families_data get_address_for InterfaceError",e)
        reconnectToDB()
        
def getResultFormatted(results):
    data_list=[]
    try:
        for row in results:
            data={}
            fieldIdx=0
            # cloud_logger.info('family Id: %s', row[0])
            if row[19] == None or row[19] == "": # This will get executed only when data for street id is null. - Shreyas G. 25 Feb 22
                # cloud_logger.info('Street Id is not available for this family Id: %s', row[0])
                print('Street Id is not available for this family Id: %s', row[0])
                isAddressAvailable, address_details = get_address_for(row[0])
                if isAddressAvailable:
                    row[8]=address_details["country_id_new"]
                    row[9]=address_details["state_id_new"]
                    row[10]=address_details["district_id_new"]
                    row[11]=address_details["hud_id_new"]
                    row[12]=address_details["block_id_new"]
                    row[13]=address_details["taluk_id_new"]
                    row[14]=address_details["village_id_new"]
                    row[15]=address_details["rev_village_id_new"]
                    row[16]=address_details["habitation_id_new"]
                    row[17]=address_details["area_id_new"]
                    row[18]=address_details["ward_id_new"]
                    row[19]=address_details["street_id_new"]
                    row[20]=address_details["hhg_id_new"]
                    print("Current Updated Row # %s",str(row))
                    # cloud_logger.info("Current Updated Row # %s",str(row))

            for column in cursor.description:
                field_name=column.name
                # field_type=column.type_
                field_code=column.type_code    
                # Code Mapping: STRING-6, TIMESTAMP-4, INT64-2, JSON-11             
                if(field_code==11 and row[fieldIdx] is not None):
                    if field_name == "update_register":
                        update_register = getUpdateRegister(json.loads(row[fieldIdx]))
                        data[field_name]=update_register
                    else:
                        data[field_name]=json.loads(row[fieldIdx])
                elif(field_code==4 and row[fieldIdx] is not None):
                    data[field_name]=row[fieldIdx].astimezone(timezone('Asia/Calcutta')).strftime("%Y-%m-%d %H:%M:%S%z")
                else:
                    data[field_name]=row[fieldIdx]
                fieldIdx+=1

            data_list.append(data)
        return data_list

    except Exception as e:
        print("Error While Formatting the Result : %s | %s | %s", str(e), guard.current_userId, guard.current_appversion)
        # cloud_logger.error("Error While Formatting the Result : %s | %s | %s", str(e), guard.current_userId, guard.current_appversion)
    
    finally:
        return data_list

def get_family_data(userId, defaultTime, content):

    data = {}
    # cloud_logger.info("Fetching Family Data.")
    print("Fetching Family Data.")
    try:
        lastUpdate = content["LAST_UPDATE"]
        is_valid_TS = re.match(parameters['TS_FORMAT'], lastUpdate) #Checks the TimeStamp format
        lastUpdateTS = defaultTime if(lastUpdate is None or lastUpdate=='' or not is_valid_TS) else datetime.strptime(lastUpdate, "%Y-%m-%d %H:%M:%S%z")
        
        offset = int(content["OFFSET"])
        # query = "SELECT fmly.family_id, fmly.phr_family_id, fmly.family_head, fmly.family_members_count, fmly.pds_smart_card_id, fmly.pds_old_card_id, fmly.family_insurances, fmly.shop_id, fmly.country_id, fmly.state_id, fmly.district_id, fmly.hud_id, fmly.block_id, fmly.taluk_id, fmly.village_id,fmly.rev_village_id, fmly.habitation_id, fmly.area_id, fmly.ward_id, fmly.street_id, fmly.hhg_id, fmly.pincode, fmly.door_no, fmly.apartment_name, fmly.postal_address, fmly.facility_id, fmly.hsc_unit_id, fmly.reside_status, fmly.latitude, fmly.longitude, seref.social_details, seref.economic_details, fmly.update_register, FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S%z', fmly.last_update_date, 'Asia/Calcutta') as last_update_date from family_master@{FORCE_INDEX=FAMILY_BLOCK_ID_IDX} fmly INNER JOIN user_master usr on JSON_VALUE(usr.assigned_jurisdiction, '$.primary_block')=fmly.block_id LEFT JOIN family_socio_economic_ref seref on seref.family_id=fmly.family_id WHERE fmly.facility_id is not NULL and user_id=@userId and fmly.last_update_date>@lastUpdate order by fmly.family_id limit 3000 OFFSET @offset"
        # query = "with Query as (SELECT fmly.family_id, fmly.phr_family_id, fmly.family_head, fmly.family_members_count, fmly.pds_smart_card_id, fmly.pds_old_card_id, fmly.family_insurances, fmly.shop_id, fmly.country_id, fmly.state_id, fmly.district_id, fmly.hud_id, fmly.block_id, fmly.taluk_id, fmly.village_id,fmly.rev_village_id, fmly.habitation_id, fmly.area_id, fmly.ward_id, fmly.street_id, fmly.hhg_id, fmly.pincode, fmly.door_no, fmly.apartment_name, fmly.postal_address, fmly.facility_id, fmly.hsc_unit_id, fmly.reside_status, fmly.latitude, fmly.longitude, fmly.update_register, FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S%z', fmly.last_update_date, 'Asia/Calcutta') as last_update_date from family_master@{FORCE_INDEX=FAMILY_FACILITY_ID_IDX} fmly  where fmly.last_update_date >@lastUpdate and facility_id in (select distinct facility_id from user_master where user_id =@userId)) select Q.*,seref.social_details,seref.economic_details  from Query Q left join family_member_socio_economic_ref seref ON seref.family_id = Q.family_id order by Q.family_id limit 3000 OFFSET @offset;"
        # query = "SELECT fmly.family_id, fmly.phr_family_id, fmly.family_head, fmly.family_members_count, fmly.pds_smart_card_id, fmly.pds_old_card_id, fmly.family_insurances, fmly.shop_id, fmly.country_id, fmly.state_id, fmly.district_id, fmly.hud_id, fmly.block_id, fmly.taluk_id, fmly.village_id,fmly.rev_village_id, fmly.habitation_id, fmly.area_id, fmly.ward_id, fmly.street_id, fmly.hhg_id, fmly.pincode, fmly.door_no, fmly.apartment_name, fmly.postal_address, fmly.facility_id, fmly.hsc_unit_id, fmly.reside_status, fmly.latitude, fmly.longitude, seref.social_details, seref.economic_details, fmly.update_register, FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S%z', fmly.last_update_date, 'Asia/Calcutta') as last_update_date from family_master@{FORCE_INDEX=FAMILY_BLOCK_ID_IDX} fmly LEFT JOIN family_socio_economic_ref seref on seref.family_id=fmly.family_id WHERE fmly.facility_id is not NULL and fmly.block_id = (select distinct JSON_VALUE(usr.assigned_jurisdiction, '$.primary_block') from user_master usr where user_id=@userId) and fmly.last_update_date>@lastUpdate order by fmly.family_id limit 3000 OFFSET @offset"
        # The above query is commented because it was hitting to entire block level families. We are now suppose to get it for hsc level only. - 20 April 2022.
        # query = "SELECT fmly.family_id,fmly.phr_family_id,fmly.family_head,fmly.family_members_count,fmly.pds_smart_card_id,fmly.pds_old_card_id,fmly.family_insurances,fmly.shop_id,fmly.country_id,fmly.state_id, fmly.district_id,fmly.hud_id,fmly.block_id,fmly.taluk_id,fmly.village_id,fmly.rev_village_id,fmly.habitation_id,fmly.area_id,fmly.ward_id,fmly.street_id,fmly.hhg_id,fmly.pincode,fmly.door_no,fmly.apartment_name,fmly.postal_address,fmly.facility_id, fmly.hsc_unit_id,fmly.reside_status,fmly.latitude,fmly.longitude,seref.social_details,seref.economic_details,fmly.update_register,FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S%z', fmly.last_update_date, 'Asia/Calcutta') AS last_update_date FROM family_master@{FORCE_INDEX=FAMILY_FACILITY_ID_IDX} fmly LEFT JOIN family_socio_economic_ref seref ON seref.family_id=fmly.family_id WHERE fmly.facility_id = (SELECT facility_id FROM user_master usr WHERE user_id=@userId) AND fmly.last_update_date>@lastUpdate ORDER BY fmly.phr_family_id LIMIT 3000 OFFSET @offset"
        # The above query is optimised post discussion with Darshak (Spanner SPOC from Google.) - 29 April 2022
        query = "SELECT fmly.family_id,fmly.phr_family_id,fmly.family_head,fmly.family_members_count,fmly.pds_smart_card_id,fmly.pds_old_card_id,fmly.family_insurances,fmly.shop_id,fmly.country_id,fmly.state_id, fmly.district_id,fmly.hud_id,fmly.block_id,fmly.taluk_id,fmly.village_id,fmly.rev_village_id,fmly.habitation_id,fmly.area_id,fmly.ward_id,fmly.street_id,fmly.hhg_id,fmly.pincode,fmly.door_no,fmly.apartment_name,fmly.postal_address,fmly.facility_id, fmly.hsc_unit_id,fmly.reside_status,fmly.latitude,fmly.longitude,seref.social_details,seref.economic_details,fmly.update_register,to_char(fmly.last_update_date AT TIME ZONE 'Asia/Calcutta', 'YYYY-MM-DD HH24:MI:SS') AS last_update_date FROM public.family_master fmly left join public.family_socio_economic_ref seref ON seref.family_id=fmly.family_id WHERE fmly.facility_id = (SELECT facility_id FROM public.user_master usr WHERE user_id=%s) AND fmly.last_update_date>%s ORDER BY fmly.phr_family_id LIMIT 3000 OFFSET %s"
        values = (userId,lastUpdateTS, offset)
        cursor.execute(query, values)
        results = cursor.fetchall()
        # with spnDB.snapshot() as snapshot: 
        #     results = snapshot.execute_sql(
        #         query,
        #         params={
        #             "userId": userId,
        #             "offset": offset,
        #             "lastUpdate": lastUpdateTS
        #         },
        #         param_types={
        #             "userId": param_types.STRING,
        #             "offset": param_types.INT64,
        #             "lastUpdate": param_types.TIMESTAMP
        #         },                   
        #     )
        data = getResultFormatted(results)

    except psycopg2.ProgrammingError as e:
        print("get_families_data get_family_data ProgrammingError",e)  
        conn.rollback()
    except psycopg2.InterfaceError as e:
        print("get_families_data get_family_data InterfaceError",e)
        reconnectToDB()
        
    finally:
        return data
    
def getUpdateRegister(update_register):
    try:
        if isinstance(update_register, dict):
            update_register_list = [update_register]
            if len(update_register_list) == 1 or len(update_register_list) == 0:
                return update_register
            else:
                sorted_list = sorted(update_register_list, key = lambda item: item['timestamp'])
                update_register = sorted_list[-1]
                return update_register
        elif isinstance(update_register, list):
            if len(update_register) == 1:
                return update_register[0]
            else:
                sorted_list = sorted(update_register, key = lambda item: item['timestamp'])
                update_register = sorted_list[-1]
                return update_register
    except Exception as e:
        print("Error parsing Update Register : %s | %s | %s", str(e), guard.current_userId, guard.current_appversion)
        # cloud_logger.error("Error parsing Update Register : %s | %s | %s", str(e), guard.current_userId, guard.current_appversion)

if __name__=="__main__":    
    app.run(host="0.0.0.0", port=8000)
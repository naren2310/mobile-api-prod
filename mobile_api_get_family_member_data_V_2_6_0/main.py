from guard import *
import guard
from flask import Flask, request

"""
******Method Details******
Description: API to retrieve Family Member Data
MIME Type: application/json
Input: Last update Timestamp - {"USER_ID":<User_Id>, "LAST_UPDATE":<Timestamp>, "API_KEY":<Token>}
Output: Master data from family_member_master and family_member_socio_economic_ref
[{
    "family_id":<id>,
    "phr_family_id":<phr_family_id>, 
    "member_id":<id>
    "unique_health_id":<uhid>, 
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
            print("Uptime check trigger.")
            # cloud_logger.info("Uptime check trigger.")
            return False, json.dumps({"status":"API-ACTIVE", "status_code":"200","message":'Uptime check trigger.'})
        else:
            print("Invalid Token.")
            # cloud_logger.critical("Invalid Token.")
            return False, json.dumps({'status':'FAILURE', "status_code":"401", 'message' : 'Invalid Token.'})

    try:
        token = token.strip() #Remove spaces at the beginning and at the end of the token
        token_format = re.compile(parameters['TOKEN_FORMAT'])
        if not token_format.match(token):
            print("Invalid Token format.")
            # cloud_logger.critical("Invalid Token format.")
            return False, json.dumps({'status':'FAILURE',"status_code":"401",'message' : 'Invalid Token format.'})
        else:
            # decoding the payload to fetch the stored details
            data = jwt.decode(token, parameters['JWT_SECRET_KEY'], algorithms=["HS256"])
            return True, data

    except jwt.ExpiredSignatureError as e:
        print("Token Expired: %s", str(e))
        # cloud_logger.critical("Token Expired: %s", str(e))
        return False, json.dumps({'status':'FAILURE', "status_code":"401", 'message' : 'Token Expired.'})

    except Exception as e:
        print("Invalid Token: %s", str(e))
        # cloud_logger.critical("Invalid Token: %s", str(e))
        return False, json.dumps({'status':'FAILURE',"status_code":"401",'message' : 'Invalid Token.'})

@app.route('/api/mobile_api_get_family_member_data', methods=['POST'])
def get_family_member_data():
    
    token_status, token_data = token_required(request)
    if not token_status:
        return token_data   

    response = None

    try:
        print("*********Get Family Member Data************")
        # cloud_logger.info("*********Get Family Member Data************")
        family_members=[]
        defaultTime = datetime.strptime('2021-09-01 15:52:50+0530', "%Y-%m-%d %H:%M:%S%z")
        # Check the request data for JSON.
        if (request.is_json):
            content=request.get_json()
            userId = content["USER_ID"]
            set_current_user(userId)
            if ("APP_VERSION" in content):
               setApp_Version(content['APP_VERSION'])

            lastUpdate = content["LAST_UPDATE"]
            print(lastUpdate)
            is_valid_TS = re.match(parameters['TS_FORMAT'], lastUpdate) #Checks the TimeStamp format
            lastUpdateTS = defaultTime if(lastUpdate is None or lastUpdate=='' or not is_valid_TS) else datetime.strptime(lastUpdate, "%Y-%m-%d %H:%M:%S%z")            
            offset = int(content["OFFSET"])
            is_valid_id = validate_id(content["USER_ID"])

            if not is_valid_id:
                response =  json.dumps({
                            "message": "User ID is not Valid.", 
                            "status": "FAILURE",
                            "status_code":"401",
                            "data": []
                            })
                print("Provided User ID is not valid : %s | %s", guard.current_userId, guard.current_appversion)
                # cloud_logger.error("Provided User ID is not valid : %s | %s", guard.current_userId, guard.current_appversion)
                return response
            else:
                is_token_valid = user_token_validation(userId, token_data["mobile_number"])
                if not is_token_valid:
                    response =  json.dumps({
                            "message": "Unregistered User/Token-User mismatch.", 
                            "status": "FAILURE",
                            "status_code":"401"
                            })
                    print("Unregistered User/Token-User mismatch : %s | %s", guard.current_userId, guard.current_appversion)
                    # cloud_logger.error("Unregistered User/Token-User mismatch : %s | %s", guard.current_userId, guard.current_appversion)
                    return response
                else:
                    print("Token Validated.")
                    # cloud_logger.info("Token Validated.")
                    #print(lastUpdateTS)      
                    family_members = get_family_member_details(lastUpdateTS, userId, offset)     
                    if len(family_members) == 0:
                        response =  json.dumps({
                            "message": "There is no family data available, Please Contact Administrator.", 
                            "status": "SUCCESS",
                            "status_code":"200",
                            "data": []
                        })
                        print("No family data available.")
                        # cloud_logger.info("No family data available.")

                    elif len(family_members) > 0 and len(family_members) < 3000:
                        response =  json.dumps({
                            "message": "Success retrieving Family Data.", 
                            "status": "SUCCESS-FINAL",
                            "status_code":"200",
                            "data": family_members
                        })
                        print("Success retrieving Family Data.")
                        # cloud_logger.info("Success retrieving Family Data.")
                    
                    else:
                        response =  json.dumps({
                            "message": "Success retrieving Family Data.", 
                            "status": "SUCCESS-CONTINUE",
                            "status_code":"200",
                            "data": family_members
                        })
                        print("Success retrieving Family Data.")
                        # cloud_logger.info("Success retrieving Family Data.")
        else :
            response =  json.dumps({
                    "message": "Error!! The Request Format should be in JSON.", 
                    "status": "FAILURE",
                    "status_code":"401",
                    "data": []
                })
            print("The Request Format should be in JSON.")
            # cloud_logger.error("The Request Format should be in JSON.")

    except Exception as e:
        response =  json.dumps({
                    "message": "Error while retrieving Family Member Data.", 
                    "status": "FAILURE",
                    "status_code": "401",
                    "data": []
                }
            )
        print("Error while retrieving Family Member Data :%s | %s | %s", str(e), guard.current_userId, guard.current_appversion)
        # cloud_logger.error("Error while retrieving Family Member Data :%s | %s | %s", str(e), guard.current_userId, guard.current_appversion)

    finally:
        return response

def get_address_for(familyId):
    address = {}
    isAddressAvailable = False
    try:
        print('Getting address details from Family Id : %s', str(familyId))
        # cloud_logger.info('Getting address details from Family Id : %s', str(familyId))
        # with spnDB.snapshot() as snapshot:
        query = "with Street as (SELECT street_id,country_id,state_id,district_id,hud_id,block_id,village_id,rev_village_id,area_id,ward_id,habitation_id,hsc_unit_id FROM public.address_street_master WHERE street_gid = (SELECT street_gid FROM public.address_shop_master WHERE shop_id = (SELECT  shop_id FROM public.family_master WHERE family_id = %s AND street_id is null)))SELECT S.country_id as country_id_new,S.state_id as state_id_new ,S.district_id as district_id_new,S.hud_id as hud_id_new, S.block_id as block_id_new,rev.taluk_id as taluk_id_new ,S.village_id as village_id_new,S.rev_village_id as rev_village_id_new ,S.habitation_id as habitation_id_new,S.area_id as area_id_new,S.ward_id as ward_id_new ,S.street_id as street_id_new ,hhg.hhg_id as hhg_id_new FROM Street S left join public.address_hhg_master HHG on S.street_id=HHG.street_id left join public.address_revenue_village_master REV on hhg.rev_village_id=rev.rev_village_id"
            # result = snapshot.execute_sql(
            # query,
            #     params={"familyId": familyId},
            #     param_types={"familyId": param_types.STRING},
            # )
        value = (familyId,)
        cursor.execute(query,value)
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
        print("get_family_member_data get_address_for ProgrammingError",e)  
        conn.rollback()
    except psycopg2.InterfaceError as e:
        print("get_family_member_data get_address_for InterfaceError",e)
        reconnectToDB()


def getResultFormatted(results):
    data_list=[]
    try:
        for row in results:
            data={}
            fieldIdx=0

            if row[33] == None or row[33] == "": # This will get executed only when data for street id is null. - Shreyas G. 25 Feb 22
                print('Street Id is not available for this family Id: %s', row[19])
                # cloud_logger.info('Street Id is not available for this family Id: %s', row[19])
                isAddressAvailable, address_details = get_address_for(row[0])
                if isAddressAvailable:
                    row[22]=address_details["country_id_new"]
                    row[23]=address_details["state_id_new"]
                    row[24]=address_details["district_id_new"]
                    row[25]=address_details["hud_id_new"]
                    row[26]=address_details["block_id_new"]
                    row[27]=address_details["taluk_id_new"]
                    row[28]=address_details["village_id_new"]
                    row[29]=address_details["rev_village_id_new"]
                    row[30]=address_details["habitation_id_new"]
                    row[31]=address_details["ward_id_new"]
                    row[32]=address_details["area_id_new"]
                    row[33]=address_details["street_id_new"]
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
                elif(field_code==5 and row[fieldIdx] is not None):
                    data[field_name]=row[fieldIdx].strftime("%Y-%m-%d")
                else:
                    data[field_name]=row[fieldIdx]
                fieldIdx+=1

            data_list.append(data)

        return data_list
    
    except Exception as e:
        print("Error While Formatting the Result :%s | %s | %s", str(e), guard.current_userId, guard.current_appversion)
        # cloud_logger.error("Error While Formatting the Result :%s | %s | %s", str(e), guard.current_userId, guard.current_appversion)
    
    finally:
        return data_list


def get_family_member_details(lastUpdateTS, userId, offset):
    try:
        data={}
        print("Fetching Family Member Details.")
        # cloud_logger.info("Fetching Family Member Details.")
        # with spnDB.snapshot() as snapshot:
            # query = "SELECT fmm.family_id, fmm.member_id, fmm.unique_health_id, fmm.phr_family_id, fmm.makkal_number, fmm.ndhm_id, fmm.member_name, fmm.member_local_name, fmm.relationshipWith, fmm.relationship, fmm.birth_date, fmm.gender, fmm.mobile_number, fmm.alt_mobile_number, fmm.email, fmm.alt_email, fmm.aadhaar_number, fmm.voter_id, fmm.insurances, fmm.welfare_beneficiary_ids, fmm.program_ids, fmm.eligible_couple_id, fmm.country_id, fmm.state_id, fmm.district_id, fmm.hud_id, fmm.block_id, fmm.taluk_id, fmm.village_id, fmm.rev_village_id, fmm.habitation_id, fmm.ward_id, fmm.area_id, fmm.street_id, fmm.facility_id, fmm.resident_status, fmm.resident_status_details, fmm.consent_status, fmm.consent_details, seref.social_details, seref.economic_details, fmm.update_register, FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S%z', fmm.last_update_date, 'Asia/Calcutta') as last_update_date from family_member_master@{FORCE_INDEX=MEMBER_BLOCK_ID_WITH_STORING_COLS_IDX} fmm INNER JOIN user_master usr on JSON_VALUE(usr.assigned_jurisdiction, '$.primary_block')=fmm.block_id  LEFT JOIN family_member_socio_economic_ref seref on seref.member_id=fmm.member_id WHERE fmm.facility_id is not NULL and user_id=@userId AND fmm.last_update_date>@lastUpdate order by fmm.member_id limit 3000 OFFSET @offset"
            # query = "with Query as (select fmm.family_id, fmm.member_id, fmm.unique_health_id, fmm.phr_family_id, fmm.makkal_number, fmm.ndhm_id, fmm.member_name, fmm.member_local_name, fmm.relationshipWith, fmm.relationship, fmm.birth_date, fmm.gender, fmm.mobile_number, fmm.alt_mobile_number, fmm.email, fmm.alt_email, fmm.aadhaar_number, fmm.voter_id, fmm.insurances, fmm.welfare_beneficiary_ids, fmm.program_ids, fmm.eligible_couple_id, fmm.country_id, fmm.state_id, fmm.district_id, fmm.hud_id, fmm.block_id, fmm.taluk_id, fmm.village_id, fmm.rev_village_id, fmm.habitation_id, fmm.ward_id,fmm.area_id, fmm.street_id, fmm.facility_id, fmm.resident_status, fmm.resident_status_details, fmm.consent_status, fmm.consent_details, fmm.update_register,  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S%z', fmm.last_update_date , 'Asia/Calcutta') AS last_update_date FROM family_member_master@{FORCE_INDEX=MEMBER_FACILITY_ID_IDX} fmm where fmm.last_update_date>@lastUpdate and facility_id in (select distinct facility_id from user_master where user_id =@userId)) select Q.family_id, Q.member_id, Q.unique_health_id, Q.phr_family_id, Q.makkal_number, Q.ndhm_id, Q.member_name, Q.member_local_name, Q.relationshipWith, Q.relationship, Q.birth_date, Q.gender, Q.mobile_number, Q.alt_mobile_number, Q.email, Q.alt_email, Q.aadhaar_number, Q.voter_id, Q.insurances, Q.welfare_beneficiary_ids, Q.program_ids, Q.eligible_couple_id, Q.country_id, Q.state_id, Q.district_id, Q.hud_id, Q.block_id, Q.taluk_id, Q.village_id, Q.rev_village_id, Q.habitation_id, Q.ward_id,Q.area_id, Q.street_id, Q.facility_id, Q.resident_status, Q.resident_status_details, Q.consent_status, Q.consent_details, Q.update_register, Q.last_update_date, seref.social_details,seref.economic_details from Query Q left join family_member_socio_economic_ref seref ON seref.member_id = Q.member_id limit 3000 OFFSET @offset"
            # query = "with Query as (select fmm.family_id, fmm.member_id, fmm.unique_health_id, fmm.phr_family_id, fmm.makkal_number, fmm.ndhm_id, fmm.member_name, fmm.member_local_name, fmm.relationshipWith, fmm.relationship, fmm.birth_date, fmm.gender, fmm.mobile_number, fmm.alt_mobile_number, fmm.email, fmm.alt_email, fmm.aadhaar_number, fmm.voter_id, fmm.insurances, fmm.welfare_beneficiary_ids, fmm.program_ids, fmm.eligible_couple_id, fmm.country_id, fmm.state_id, fmm.district_id, fmm.hud_id, fmm.block_id, fmm.taluk_id, fmm.village_id, fmm.rev_village_id, fmm.habitation_id, fmm.ward_id,fmm.area_id, fmm.street_id, fmm.facility_id, fmm.resident_status,fmm.resident_status_details, fmm.consent_status, fmm.consent_details, fmm.update_register,  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S%z', fmm.last_update_date , 'Asia/Calcutta') AS last_update_date FROM family_member_master@{FORCE_INDEX=MEMBER_FACILITY_ID_IDX} fmm where fmm.last_update_date>@lastUpdate and facility_id = (select distinct facility_id from user_master where user_id =@userId)) select Q.family_id, Q.member_id, Q.unique_health_id, Q.phr_family_id, Q.makkal_number, Q.ndhm_id, Q.member_name, Q.member_local_name, Q.relationshipWith, Q.relationship, Q.birth_date, Q.gender, Q.mobile_number, Q.alt_mobile_number, Q.email, Q.alt_email, Q.aadhaar_number, Q.voter_id, Q.insurances, Q.welfare_beneficiary_ids, Q.program_ids, Q.eligible_couple_id, Q.country_id, Q.state_id, Q.district_id, Q.hud_id, Q.block_id, Q.taluk_id, Q.village_id, Q.rev_village_id, Q.habitation_id, Q.ward_id,Q.area_id, Q.street_id, Q.facility_id, Q.resident_status, Q.resident_status_details, Q.consent_status, Q.consent_details, Q.update_register, Q.last_update_date, seref.social_details,seref.economic_details  from Query Q left join family_member_socio_economic_ref seref ON seref.member_id = Q.member_id limit 3000 offset @offset"
            # The above query is commented because it was hitting to entire block level families. We are now suppose to get it for hsc level only. - 20 April 2022.
        query = "with Query as (SELECT fmm.family_id, fmm.member_id, fmm.unique_health_id, fmm.phr_family_id, fmm.makkal_number, fmm.ndhm_id, fmm.member_name, fmm.member_local_name, fmm.relationshipWith, fmm.relationship,to_char(fmm.birth_date,'YYYY-MM-DD') AS birth_date, fmm.gender, fmm.mobile_number, fmm.alt_mobile_number, fmm.email, fmm.alt_email, fmm.aadhaar_number, fmm.voter_id, fmm.insurances, fmm.welfare_beneficiary_ids, fmm.program_ids, fmm.eligible_couple_id, fmm.country_id, fmm.state_id, fmm.district_id, fmm.hud_id, fmm.block_id, fmm.taluk_id, fmm.village_id, fmm.rev_village_id, fmm.habitation_id, fmm.ward_id,fmm.area_id, fmm.street_id, fmm.facility_id, fmm.resident_status,fmm.resident_status_details, fmm.consent_status, fmm.consent_details, fmm.update_register, to_char(fmm.last_update_date AT TIME ZONE 'Asia/Calcutta', 'YYYY-MM-DD HH24:MI:SS') AS last_update_date FROM public.family_member_master fmm WHERE fmm.last_update_date>%s AND facility_id = (SELECT distinct facility_id FROM public.user_master WHERE user_id =%s)) SELECT Q.family_id, Q.member_id, Q.unique_health_id, Q.phr_family_id, Q.makkal_number, Q.ndhm_id, Q.member_name, Q.member_local_name, Q.relationshipWith, Q.relationship,Q.birth_date, Q.gender, Q.mobile_number, Q.alt_mobile_number, Q.email, Q.alt_email, Q.aadhaar_number, Q.voter_id, Q.insurances, Q.welfare_beneficiary_ids, Q.program_ids, Q.eligible_couple_id, Q.country_id, Q.state_id, Q.district_id, Q.hud_id, Q.block_id, Q.taluk_id, Q.village_id, Q.rev_village_id, Q.habitation_id, Q.ward_id,Q.area_id, Q.street_id, Q.facility_id, Q.resident_status, Q.resident_status_details, Q.consent_status, Q.consent_details, Q.update_register, Q.last_update_date, seref.social_details,seref.economic_details FROM Query Q left join public.family_member_socio_economic_ref seref ON seref.family_id=Q.family_id AND seref.member_id = Q.member_id limit 3000 offset %s"

        # results = snapshot.execute_sql(
        #             query,
        #             params={
        #                 "userId": userId,
        #                 "offset": offset,
        #                 "lastUpdate": lastUpdateTS
        #             },
        #             param_types={
        #                 "userId": param_types.STRING,
        #                 "offset": param_types.INT64,
        #                 "lastUpdate": param_types.TIMESTAMP
        #             },                   
        #         )
        value = (lastUpdateTS,userId,offset)
        cursor.execute(query,value)
        results = cursor.fetchall()
        data = getResultFormatted(results)

    except psycopg2.ProgrammingError as e:
        print("get_family_member_data get_family_member_details ProgrammingError",e)  
        conn.rollback()
    except psycopg2.InterfaceError as e:
        print("get_family_member_data get_family_member_details InterfaceError",e)
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
        print("Error while parsing Update Register :%s | %s | %s", str(e), guard.current_userId, guard.current_appversion)
        # cloud_logger.error("Error while parsing Update Register :%s | %s | %s", str(e), guard.current_userId, guard.current_appversion)

if __name__=="__main__":    
    app.run(host="0.0.0.0", port=8000)
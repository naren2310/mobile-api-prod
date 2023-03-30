import queue
from utils import *

def get_socioEconomic_data(familyId):
    data = {}
    try:
        print("Getting Socio Economic Data.")
        # cloud_logger.info("Getting Socio Economic Data.")
        # with spnDB.snapshot() as snapshot:
        results=None

        query = "SELECT family_socio_economic_id, social_details, economic_details FROM public.family_socio_economic_ref WHERE family_id=%s"
            # results = snapshot.execute_sql(
            #     query,
            #     params = {"familyId":familyId},
            #     param_types = {"familyId": param_types.STRING}
            # )   
        value = (familyId,)
        cursor.execute(query,value)
        results = cursor.fetchall()
        for row in results:
                data['family_socio_economic_id'] = row[0]
                data["social_details"] = row[1] if (row[1] is None) else row[1]
                data["economic_details"]=row[2] if (row[2] is None) else row[2]
 
    except psycopg2.ProgrammingError as e:
        print("get_family_and_member_details get_socioEconomic_data ProgrammingError",e)  
        conn.rollback()
    except psycopg2.InterfaceError as e:
        print("get_family_and_member_details get_socioEconomic_data InterfaceError",e)
        reconnectToDB()
        
    finally:
        return data

def get_member_socioEconomic_data(familyId, memberId):
    data = {}
    print("Getting Member Socio Economic Data.")
    # cloud_logger.info("Getting Member Socio Economic Data.")
    try:
        # with spnDB.snapshot() as snapshot:
            results=None

            # query = "SELECT family_socio_economic_id, social_details, economic_details from family_member_socio_economic_ref WHERE member_id=@memberId"
            query = "SELECT family_socio_economic_id, social_details, economic_details FROM public.family_member_socio_economic_ref WHERE family_id=%s AND member_id=%s"
            # results = snapshot.execute_sql(
            #     query,
            #     params = {"familyId":familyId, "memberId":memberId},
            #     param_types = {"familyId": param_types.STRING, "memberId": param_types.STRING})
            value = (familyId,memberId)
            cursor.execute(query,value)
            results = cursor.fetchall()
            for row in results:
                data['family_socio_economic_id'] = row[0]
                data["social_details"] = row[1] if (row[1] is None) else row[1]
                data["economic_details"]=row[2] if (row[2] is None) else row[2]

    except psycopg2.ProgrammingError as e:
        print("get_family_and_member_details get_member_socioEconomic_data ProgrammingError",e)  
        conn.rollback()
    except psycopg2.InterfaceError as e:
        print("get_family_and_member_details get_member_socioEconomic_data InterfaceError",e)
        reconnectToDB()
        
    finally:
        return data

def get_family_data(familyId):
    family_data = None
    print("Getting Family Data.")
    # cloud_logger.info("Getting Family Data.")
    try:
        # with spnDB.snapshot() as snapshot:
            results=None

            query = "SELECT family_id,phr_family_id,family_head,family_members_count,pds_smart_card_id,pds_old_card_id,family_insurances,shop_id,country_id,state_id,district_id,hud_id,block_id,taluk_id,village_id,rev_village_id,habitation_id,area_id,ward_id,street_id,pincode,door_no,apartment_name,postal_address,facility_id,latitude,longitude,update_register,to_char(last_update_date AT TIME ZONE 'Asia/Calcutta', 'YYYY-MM-DD HH24:MI:SS') AS last_update_date,hhg_id,hsc_unit_id,reside_status FROM public.family_master WHERE family_id=%s"
            # query = " SELECT * FROM public.family_master WHERE family_id=%s"
            # results = snapshot.execute_sql(
            #     query,
            #     params = {"familyId":familyId},
            #     param_types = {"familyId": param_types.STRING}
            # )
            value = (familyId,)
            cursor.execute(query,value)
            results = cursor.fetchall()
            data = getResultFormatted(results)
            family_data = data[0]
            ref_details = get_socioEconomic_data(familyId)

            if len(ref_details.keys()) > 0:
                family_data["family_socio_economic_id"] = ref_details.get('family_socio_economic_id')
                family_data["social_details"] = ref_details.get('social_details')
                family_data["economic_details"] = ref_details.get('economic_details')
            else:
                family_data["family_socio_economic_id"] = None
                family_data["social_details"] = None
                family_data["economic_details"] = None

    except psycopg2.ProgrammingError as e:
        print("get_family_and_member_details get_family_data ProgrammingError",e)  
        conn.rollback()
    except psycopg2.InterfaceError as e:
        print("get_family_and_member_details get_family_data InterfaceError",e)
        reconnectToDB()
        
    finally:
        return family_data


def get_all_member_data(familyId):
    data_list=None
    print("Getting all Member Data.")
    # cloud_logger.info("Getting all Member Data.")
    try:
        # with spnDB.snapshot() as snapshot:
            results=None

            # query = "SELECT * FROM public.family_member_master WHERE family_id=%s"
            query = "SELECT family_id,member_id,unique_health_id,phr_family_id,makkal_number,ndhm_id,member_name,member_local_name,relationshipWith,relationship,to_char(birth_date,'YYYY-MM-DD') AS birth_date,gender,mobile_number,alt_mobile_number,email,alt_email,aadhaar_number,voter_id,insurances,welfare_beneficiary_ids,program_ids,eligible_couple_id,country_id,state_id,district_id,hud_id,block_id,taluk_id,village_id,rev_village_id,habitation_id,ward_id,area_id,street_id,facility_id,resident_status,resident_status_details,update_register,to_char(last_update_date AT TIME ZONE 'Asia/Calcutta', 'YYYY-MM-DD HH24:MI:SS') AS last_update_date,consent_status,consent_details FROM public.family_member_master WHERE family_id=%s"
            # results = snapshot.execute_sql(
            #     query,
            #     params = {"familyId":familyId},
            #     param_types = {"familyId": param_types.STRING}
            # )
            value = (familyId,)
            cursor.execute(query,value)
            results = cursor.fetchall()
            data_list = getResultFormatted(results)

    except psycopg2.ProgrammingError as e:
        print("get_family_and_member_details get_all_member_data ProgrammingError",e)  
        conn.rollback()
    except psycopg2.InterfaceError as e:
        print("get_family_and_member_details get_all_member_data InterfaceError",e)
        reconnectToDB()
        
    finally:
        return data_list

def get_member_familyId(memberId):
    family_id=None
    print("Get Member Family ID.")
    # cloud_logger.info("Get Member Family ID.")
    try:
        # with spnDB.snapshot() as snapshot:
            results=None

            query = "SELECT family_id FROM public.family_member_master WHERE member_id=%s"
            # results = snapshot.execute_sql(
            #     query,
            #     params = {"memberId":memberId},
            #     param_types = {"memberId": param_types.STRING}
            # )
            value = (memberId,)
            cursor.execute(query,value)
            results = cursor.fetchall()
            for row in results:
                family_id = row[0]         

    except psycopg2.ProgrammingError as e:
        print("get_family_and_member_details get_member_familyId ProgrammingError",e)  
        conn.rollback()
    except psycopg2.InterfaceError as e:
        print("get_family_and_member_details get_member_familyId InterfaceError",e)
        reconnectToDB()
        
    finally:
        return family_id


def get_health_screening(familyId,memberId):
    data_list=None
    data = {}
    print("Getting Health Screeninng.")
    # cloud_logger.info("Getting Health Screeninng.")
    try:
        # with spnDB.snapshot() as snapshot:
            results=None
            print("familyId",familyId)
            print("memberId",memberId)
            query = "SELECT family_id,member_id,screening_id,screening_values,lab_test,drugs,diseases,outcome,symptoms,update_register,to_char(last_update_date AT TIME ZONE 'Asia/Calcutta', 'YYYY-MM-DD HH24:MI:SS') AS last_update_date,advices,additional_services FROM public.health_screening WHERE family_id=%s AND member_id=%s ORDER BY last_update_date DESC LIMIT 3"
            # results = snapshot.execute_sql(
                # query,
                # params = {"familyId":familyId, "memberId":memberId},
                # param_types = {"familyId": param_types.STRING, "memberId": param_types.STRING})
            value = (familyId,memberId)
            cursor.execute(query,value)
            results = cursor.fetchall()
            data_list = getResultFormatted(results) 
            data['data_list'] = data_list

    except psycopg2.ProgrammingError as e:
        print("get_family_and_member_details get_health_screening ProgrammingError",e)  
        conn.rollback()
    except psycopg2.InterfaceError as e:
        print("get_family_and_member_details get_health_screening InterfaceError",e)
        reconnectToDB()
        
    finally:
        return data


def get_health_history(familyId, memberId):
    data_list=None
    print("Getting Health History.")
    # cloud_logger.info("Getting Health History.")
    try:
        # with spnDB.snapshot() as snapshot:
            results=None

            # query = "SELECT * FROM health_history where member_id=@memberId ORDER BY last_update_date DESC LIMIT 3"
            query = "SELECT member_id,family_id,medical_history_id,past_history,family_history,lifestyle_details,vaccinations,disability,  disability_details,enrollment_date,congenital_anomaly,eligible_couple_id,eligible_couple_status,eligible_couple_details,update_register, to_char(last_update_date AT TIME ZONE 'Asia/Calcutta', 'YYYY-MM-DD HH24:MI:SS') AS last_update_date,welfare_method,mtm_beneficiary FROM public.health_history WHERE family_id=%s and member_id=%s"
            # results = snapshot.execute_sql(
                # query,
                # params = {"familyId":familyId,"memberId":memberId},
                # param_types = {"familyId": param_types.STRING, "memberId": param_types.STRING}
            # )
            value = (familyId,memberId)
            cursor.execute(query,value)
            results = cursor.fetchall()
            data_list = getResultFormatted(results)
            if len(data_list) > 0:
                return data_list[0]
            else:
                return dict(data_list)

    except psycopg2.ProgrammingError as e:
        print("get_family_and_member_details get_health_history ProgrammingError",e)  
        conn.rollback()
    except psycopg2.InterfaceError as e:
        print("get_family_and_member_details get_health_history InterfaceError",e)
        reconnectToDB()


def get_address_data(familyId):
    data = None
    print("Getting Address Data.")
    # cloud_logger.info("Getting Address Data.")
    try:
        # with spnDB.snapshot() as snapshot:
            results=None

            # query = "SELECT fmly.facility_id, fr.facility_name, fr.institution_gid, ftm.facility_type_name, fmly.village_id, vill.village_gid, vill.village_name, fmly.street_id, strt.street_gid, strt.street_name from family_master fmly LEFT JOIN facility_registry fr USING(facility_id) LEFT JOIN address_village_master vill on vill.village_id=fmly.village_id LEFT JOIN address_street_master strt on strt.street_id=fmly.street_id LEFT JOIN facility_type_master ftm on fr.facility_type_id=ftm.facility_type_id WHERE family_id=@familyId"
            query = "SELECT fmly.facility_id, fr.facility_name, fr.institution_gid, ftm.facility_type_name, fmly.village_id, vill.village_gid, vill.village_name, fmly.street_id, strt.street_gid, strt.street_name FROM public.family_master fmly LEFT JOIN public.facility_registry fr USING(facility_id) LEFT JOIN public.address_village_master vill on vill.village_id=fmly.village_id LEFT JOIN public.address_street_master strt on strt.street_id=fmly.street_id LEFT JOIN public.facility_type_master ftm on fr.facility_type_id=ftm.facility_type_id WHERE family_id=%s"
            value = (familyId,)
            cursor.execute(query,value)
            results = cursor.fetchall()
            # results = snapshot.execute_sql(
            #     query,
            #     params = {"familyId":familyId},
            #     param_types = {"familyId": param_types.STRING}
            # )
                
            for row in results:
                data={
                    "village_id": row[4],
                    "village_gid": row[5],
                    "village_name": row[6],
                    "streets":[{
                        "street_id": row[7],
                        "street_gid": row[8],
                        "street_name": row[9],
                        "facility":[{
                            "facility_id":row[0],
                            "facility_name":row[1],
                            "facility_gid":row[2],
                            "facility_type":row[3]
                        }]
                    }]
                }
                #cloud_logger.debug("Data for Address: {}".format(str(data)))
                
    except psycopg2.ProgrammingError as e:
        print("get_family_and_member_details get_address_data ProgrammingError",e)  
        conn.rollback()
    except psycopg2.InterfaceError as e:
        print("get_family_and_member_details get_address_data InterfaceError",e)
        reconnectToDB()
        
    finally:
        return data
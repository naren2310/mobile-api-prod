import queue
from utils import *

def get_socioEconomic_data(familyId):
    data = {}
    try:
        cloud_logger.info("Getting Socio Economic Data.")
        with spnDB.snapshot() as snapshot:
            results=None

            query = "SELECT family_socio_economic_id, social_details, economic_details from family_socio_economic_ref WHERE family_id=@familyId"
            results = snapshot.execute_sql(
                query,
                params = {"familyId":familyId},
                param_types = {"familyId": param_types.STRING}
            )   
            for row in results:
                data['family_socio_economic_id'] = row[0]
                data["social_details"] = row[1] if (row[1] is None) else json.loads(row[1])
                data["economic_details"]=row[2] if (row[2] is None) else json.loads(row[2])
 
    except Exception as e:
        cloud_logger.error("Error fetching social details : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        
    finally:
        return data

def get_member_socioEconomic_data(familyId, memberId):
    data = {}
    cloud_logger.info("Getting Member Socio Economic Data.")
    try:
        with spnDB.snapshot() as snapshot:
            results=None

            # query = "SELECT family_socio_economic_id, social_details, economic_details from family_member_socio_economic_ref WHERE member_id=@memberId"
            query = "SELECT family_socio_economic_id, social_details, economic_details from family_member_socio_economic_ref WHERE family_id=@familyId and member_id=@memberId"
            results = snapshot.execute_sql(
                query,
                params = {"familyId":familyId, "memberId":memberId},
                param_types = {"familyId": param_types.STRING, "memberId": param_types.STRING})
            
            for row in results:
                data['family_socio_economic_id'] = row[0]
                data["social_details"] = row[1] if (row[1] is None) else json.loads(row[1])
                data["economic_details"]=row[2] if (row[2] is None) else json.loads(row[2])

    except Exception as e:
        cloud_logger.error("Error while fetching Member Socio Economic Details : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        
    finally:
        return data

def get_family_data(familyId):
    family_data = None
    cloud_logger.info("Getting Family Data.")
    try:
        with spnDB.snapshot() as snapshot:
            results=None

            query = "SELECT * FROM family_master where family_id=@familyId"
            results = snapshot.execute_sql(
                query,
                params = {"familyId":familyId},
                param_types = {"familyId": param_types.STRING}
            )
                
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

    except Exception as e:
        cloud_logger.error("Error while fetching family data : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        
    finally:
        return family_data


def get_all_member_data(familyId):
    data_list=None
    cloud_logger.info("Getting all Member Data.")
    try:
        with spnDB.snapshot() as snapshot:
            results=None

            query = "SELECT * FROM family_member_master where family_id=@familyId"
            results = snapshot.execute_sql(
                query,
                params = {"familyId":familyId},
                param_types = {"familyId": param_types.STRING}
            )
                
            data_list = getResultFormatted(results)

    except Exception as e:
        cloud_logger.error("Error while fetching Member data : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        
    finally:
        return data_list

def get_member_familyId(memberId):
    family_id=None
    cloud_logger.info("Get Member Family ID.")
    try:
        with spnDB.snapshot() as snapshot:
            results=None

            query = "SELECT family_id FROM family_member_master where member_id=@memberId"
            results = snapshot.execute_sql(
                query,
                params = {"memberId":memberId},
                param_types = {"memberId": param_types.STRING}
            )
            for row in results:
                family_id = row[0]         

    except Exception as e:
        cloud_logger.error("Error while fetching Family ID : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        
    finally:
        return family_id


def get_health_screening(familyId,memberId):
    data_list=None
    data = {}
    cloud_logger.info("Getting Health Screeninng.")
    try:
        with spnDB.snapshot() as snapshot:
            results=None

            query = "SELECT * FROM health_screening where family_id=@familyId and member_id=@memberId ORDER BY last_update_date DESC LIMIT 3"
            results = snapshot.execute_sql(
                query,
                params = {"familyId":familyId, "memberId":memberId},
                param_types = {"familyId": param_types.STRING, "memberId": param_types.STRING})
            data_list = getResultFormatted(results) 
            data['data_list'] = data_list

    except Exception as e:
        cloud_logger.error("Error while fetching Screening Data : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        
    finally:
        return data


def get_health_history(familyId, memberId):
    data_list=None
    cloud_logger.info("Getting Health History.")
    try:
        with spnDB.snapshot() as snapshot:
            results=None

            # query = "SELECT * FROM health_history where member_id=@memberId ORDER BY last_update_date DESC LIMIT 3"
            query = "SELECT * FROM health_history where family_id=@familyId and member_id=@memberId"
            results = snapshot.execute_sql(
                query,
                params = {"familyId":familyId,"memberId":memberId},
                param_types = {"familyId": param_types.STRING, "memberId": param_types.STRING}
            )

            data_list = getResultFormatted(results)
            if len(data_list) > 0:
                return data_list[0]
            else:
                return dict(data_list)

    except Exception as e:
        cloud_logger.error("Error While fetching Medical History Data : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)


def get_address_data(familyId):
    data = None
    cloud_logger.info("Getting Address Data.")
    try:
        with spnDB.snapshot() as snapshot:
            results=None

            # query = "SELECT fmly.facility_id, fr.facility_name, fr.institution_gid, ftm.facility_type_name, fmly.village_id, vill.village_gid, vill.village_name, fmly.street_id, strt.street_gid, strt.street_name from family_master fmly LEFT JOIN facility_registry fr USING(facility_id) LEFT JOIN address_village_master vill on vill.village_id=fmly.village_id LEFT JOIN address_street_master strt on strt.street_id=fmly.street_id LEFT JOIN facility_type_master ftm on fr.facility_type_id=ftm.facility_type_id WHERE family_id=@familyId"
            query = "SELECT fmly.facility_id, fr.facility_name, fr.institution_gid, ftm.facility_type_name, fmly.village_id, vill.village_gid, vill.village_name, fmly.street_id, strt.street_gid, strt.street_name from family_master fmly LEFT JOIN facility_registry fr USING(facility_id) LEFT JOIN address_village_master vill on vill.village_id=fmly.village_id LEFT JOIN address_street_master strt on strt.street_id=fmly.street_id LEFT JOIN facility_type_master ftm on fr.facility_type_id=ftm.facility_type_id WHERE family_id=@familyId"
            
            results = snapshot.execute_sql(
                query,
                params = {"familyId":familyId},
                param_types = {"familyId": param_types.STRING}
            )
                
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
                
    except Exception as e:
        cloud_logger.error("Error While fetching Address Details : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        
    finally:
        return data
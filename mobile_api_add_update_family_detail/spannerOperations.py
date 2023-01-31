from guard import *
import guard

def get_hierarchy_field(streetId):

    try:
        print('Getting address details for Family for Street Id : %s', str(streetId))
        # cloud_logger.info('Getting address details for Family for Street Id : %s', str(streetId))
        hierarchy_fields = {
            "facility_id": "",
            "hhg_id" : "",
            "street_id" : "",
            "ward_id" : "",
            "area_id" : "", "habitation_id" : "", "rev_village_id" : "", "village_id" : "","block_id" : "", "hud_id" : "", "taluk_id" : "","district_id" : "",
            "state_id" : "", "country_id" :"", "hsc_unit_id":""
        }
        street_gid = ""
        # with spnDB.snapshot() as snapshot:

        query = "SELECT strt.facility_id, hhg.hhg_id, hhg.street_id, hhg.ward_id, hhg.area_id, hhg.habitation_id, hhg.rev_village_id, hhg.village_id, hhg.block_id, hhg.hud_id, rev.taluk_id, hhg.district_id, hhg.state_id, hhg.country_id, hhg.hsc_unit_id, strt.street_gid FROM public.address_hhg_master hhg LEFT JOIN public.address_street_master strt on hhg.street_id=strt.street_id LEFT JOIN public.address_revenue_village_master as rev on hhg.rev_village_id=rev.rev_village_id WHERE strt.street_id=%s" 

            # result = snapshot.execute_sql(
            # query,
            # params={"streetId": streetId},
            # param_types={"streetId": param_types.STRING},
            # )
        value = (streetId,)
        cursor.execute(query,value)
        result = cursor.fetchall()
        for row in result:
                hierarchy_fields["facility_id"]=row[0]
                hierarchy_fields["hhg_id"]=row[1]
                hierarchy_fields["street_id"]=row[2]
                hierarchy_fields["ward_id"]=row[3]
                hierarchy_fields["area_id"]=row[4]
                hierarchy_fields["habitation_id"]=row[5]
                hierarchy_fields["rev_village_id"]=row[6]
                hierarchy_fields["village_id"]=row[7]
                hierarchy_fields["block_id"]=row[8]
                hierarchy_fields["hud_id"]=row[9]
                hierarchy_fields["taluk_id"]=row[10]
                hierarchy_fields["district_id"]=row[11]
                hierarchy_fields["state_id"]=row[12]
                hierarchy_fields["country_id"]=row[13]
                hierarchy_fields["hsc_unit_id"]=row[14]
                street_gid = row[15]
        print("Address Details for Member : {}".format(str(hierarchy_fields)))
        # cloud_logger.debug("Address Details for Member : {}".format(str(hierarchy_fields)))
        return street_gid, hierarchy_fields

    except Exception as e:
        print("Error while retriving the address Data : %s", str(e))
        # cloud_logger.error("Error while retriving the address Data : %s", str(e))

    finally:
        return street_gid, hierarchy_fields

# New function to return address confirmely. 29 March 2022.
def get_hierarchy_fields(streetId, userId):

    queryToGetAddressFromAddHHGMaster = 'SELECT strt.facility_id, hhg.hhg_id, hhg.street_id, hhg.ward_id, hhg.area_id, hhg.habitation_id, hhg.rev_village_id, hhg.village_id, hhg.block_id, hhg.hud_id, rev.taluk_id, hhg.district_id, hhg.state_id, hhg.country_id, hhg.hsc_unit_id, strt.street_gid FROM public.address_hhg_master hhg JOIN public.address_street_master strt on hhg.street_id=strt.street_id LEFT JOIN public.address_revenue_village_master as rev on hhg.rev_village_id=rev.rev_village_id WHERE hhg.street_id=%s'
    
    queryToGetAddressFromAddStreetMaster = 'SELECT strt.facility_id, hhg.hhg_id, strt.street_id, strt.ward_id, strt.area_id, strt.habitation_id, strt.rev_village_id, strt.village_id, strt.block_id, strt.hud_id, rev.taluk_id, strt.district_id, strt.state_id, strt.country_id, strt.hsc_unit_id, strt.street_gid FROM public.address_street_master strt LEFT JOIN public.address_hhg_master hhg on hhg.street_id=strt.street_id LEFT JOIN public.address_revenue_village_master as rev on hhg.rev_village_id=rev.rev_village_id WHERE strt.street_id=%s'
    
    queryToGetAddressFromFacilityIdOfUser = 'SELECT strt.facility_id, hhg.hhg_id, strt.street_id, strt.ward_id, strt.area_id, strt.habitation_id, strt.rev_village_id, strt.village_id, strt.block_id, strt.hud_id, rev.taluk_id, strt.district_id, strt.state_id, strt.country_id, strt.hsc_unit_id, strt.street_gid FROM public.address_street_master strt LEFT JOIN public.address_hhg_master hhg on hhg.street_id=strt.street_id LEFT JOIN public.address_revenue_village_master as rev on hhg.rev_village_id=rev.rev_village_id WHERE strt.street_id=(SELECT street_id FROM public.address_street_master WHERE block_id = (SELECT fr.block_id FROM public.facility_registry fr WHERE facility_id = (SELECT facility_id FROM public.user_master WHERE user_id =%s))limit 1)'
        
    street_gid, hierarchy_fields = get_address(streetId,userId,queryToGetAddressFromAddHHGMaster)
    if((street_gid is not None and street_gid !='') and (hierarchy_fields['street_id'] is not None and hierarchy_fields['street_id'] != '')):
        return street_gid, hierarchy_fields
    else:
        street_gid, hierarchy_fields = get_address(streetId,userId,queryToGetAddressFromAddStreetMaster)
        if (street_gid is not None and street_gid !='') and (hierarchy_fields['street_id'] is not None and hierarchy_fields['street_id'] != ''):
            return street_gid, hierarchy_fields
        else:
            street_gid, hierarchy_fields = get_address(streetId,userId,queryToGetAddressFromFacilityIdOfUser)
            if (street_gid is not None and street_gid !='') and (hierarchy_fields['street_id'] is not None and hierarchy_fields['street_id'] != ''):
                return street_gid, hierarchy_fields
            else:
                # IMPORTANT COMMENTS:
                # 1. State/District users since this user are not suppose this add update families to the state/district facility.
                # 2. Because of facility id not null query CPU utilization is high to optimize that we are making this change to assign the unallocated facility to family. Then whenever CHW/WHV visit that family then they can reallocate it to required facility.
                # on 12th Apr 2022 as per Shankar's suggestion as part of b/226509031.
                street_gid=parameters['DEFAULT_STREETGID']
                hierarchy_fields = {
                    "facility_id": parameters['UNALLOCATED_FACILITY_ID'],
                    "hhg_id" : "",
                    "street_id" : "",
                    "ward_id" : "",
                    "area_id" : "", "habitation_id" : "", "rev_village_id" : "", "village_id" : "","block_id" : "", "hud_id" : "", "taluk_id" : "","district_id" : "",
                    "state_id" : parameters['STATE_ID'] , "country_id" : parameters['COUNTRY_ID'] , "hsc_unit_id":""
                }
                return street_gid, hierarchy_fields
    
def get_address(streetId, UserId, Query): 
    hierarchy_fields = {
            "facility_id": "",
            "hhg_id" : "",
            "street_id" : "",
            "ward_id" : "",
            "area_id" : "", "habitation_id" : "", "rev_village_id" : "", "village_id" : "","block_id" : "", "hud_id" : "", "taluk_id" : "","district_id" : "",
            "state_id" : "", "country_id" :"", "hsc_unit_id":""
        }
    street_gid = None
    
    try:
        # print('Getting address details of Family for Street Id : %s by query : %s', str(streetId), str(Query))
        # cloud_logger.info('Getting address details of Family for Street Id : %s by query : %s', str(streetId), str(Query))
        # with spnDB.snapshot() as snapshot:
        query = Query

            # result = snapshot.execute_sql(
            # query,
            # params={"streetId": streetId, "UserId": UserId},
            # param_types={"streetId": param_types.STRING, "UserId": param_types.STRING},
            # )
        value = (streetId,)
        cursor.execute(query,value)
        result = cursor.fetchall()
        for row in result:  
                street_id_value = row[2]
                if street_id_value is not None and street_id_value !='':
                    hierarchy_fields["facility_id"]=row[0]
                    hierarchy_fields["hhg_id"]=row[1]
                    hierarchy_fields["street_id"]=row[2]
                    hierarchy_fields["ward_id"]=row[3]
                    hierarchy_fields["area_id"]=row[4]
                    hierarchy_fields["habitation_id"]=row[5]
                    hierarchy_fields["rev_village_id"]=row[6]
                    hierarchy_fields["village_id"]=row[7]
                    hierarchy_fields["block_id"]=row[8]
                    hierarchy_fields["hud_id"]=row[9]
                    hierarchy_fields["taluk_id"]=row[10]
                    hierarchy_fields["district_id"]=row[11]
                    hierarchy_fields["state_id"]=row[12]
                    hierarchy_fields["country_id"]=row[13]
                    hierarchy_fields["hsc_unit_id"]=row[14]
                    street_gid = row[15]
                    # print("Address Details from hierarchy : {}".format(str(hierarchy_fields)))
                    # cloud_logger.debug("Address Details from hierarchy : {}".format(str(hierarchy_fields)))
                else:
                    return street_gid, hierarchy_fields
                    
        return street_gid, hierarchy_fields

    except Exception as e:
        print("Error while retriving the address Data : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        # cloud_logger.error("Error while retriving the address Data : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)

    finally:
        return street_gid, hierarchy_fields

def get_shop(street_gid):

    try:
        print('Getting Shop details from Street GId : %s', str(street_gid))
        # cloud_logger.info('Getting Shop details from Street GId : %s', str(street_gid))
        shop_id = None
        # with spnDB.snapshot() as snapshot:
        #     query = "SELECT shop_id from address_shop_master WHERE street_gid=@street_gid" 
        #     result = snapshot.execute_sql(
        #         query,
        #         params={"street_gid": int(street_gid)},
        #         param_types={"street_gid": param_types.INT64},
        #     )
        value = (street_gid,)
        query = "SELECT shop_id FROM public.address_shop_master WHERE street_gid=%s"
        cursor.execute(query,value)
        result = cursor.fetchall()
        for row in result:
                shop_id = row[0]

        return shop_id

    except Exception as e:
        print("Error while retriving the Shop Data : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        # cloud_logger.error("Error while retriving the Shop Data : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)

    finally:
        return shop_id

def get_phr_family_id():
    maxPHRFamilyId = 0
    try:
        print('Generating Unique health Id.')
        # cloud_logger.info('Generating Unique health Id.')
        query = "SELECT value FROM public.operational_parameters WHERE parameter='MAX_PHR_FAMILY_ID'"

        # with spnDB.snapshot() as snapshot:
        #     results = snapshot.execute_sql(
        #         query,
        #         params={'parameter': 'MAX_PHR_FAMILY_ID'},
        #         param_types={'parameter':param_types.STRING}
        #     )
        cursor.execute(query)
        results = cursor.fetchall()
        for row in results:
                maxPHRFamilyId = row[0]+1


        # with spnDB.batch() as batch:
        #     batch.update(
        #         "operational_parameters",
        #         columns=["parameter", "value"],
        #         values=[
        #             ['MAX_PHR_FAMILY_ID', maxPHRFamilyId],
        #         ]
        #     )
        value = (maxPHRFamilyId,)
        query = "UPDATE public.operational_parameters SET value=%s WHERE parameter='MAX_UHID"
        cursor.execute(query,value)
        conn.commit()

    except Exception as e:
        print("Error Fetching Family Details : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        # cloud_logger.error("Error Fetching Family Details : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        return False   

    finally:
        return maxPHRFamilyId

def get_unique_health_id():
    
    maxUHID = 0
    uhid =""
    try:
        print('Generating Unique health Id.')
        # cloud_logger.info('Generating Unique health Id.')
        query = "SELECT value FROM public.operational_parameters WHERE parameter='MAX_PHR_FAMILY_ID'"

        # with spnDB.snapshot() as snapshot:
        #     results = snapshot.execute_sql(
        #         query,
        #         params={'parameter': 'MAX_UHID'},
        #         param_types={'parameter':param_types.STRING}
        #     )
        cursor.execute(query)
        results = cursor.fetchall()
        for row in results:
                maxUHID = row[0]+1

        # with spnDB.batch() as batch:
        #     batch.update(
        #         "operational_parameters",
        #         columns=["parameter", "value"],
        #         values=[
        #             ['MAX_UHID', maxUHID],
        #         ]
        #     )
        value = (maxUHID,)
        query = "UPDATE public.operational_parameters SET value=%s WHERE parameter='MAX_UHID'"
        cursor.execute(query,value)
        conn.commit()
        uInitial = "P0" if len(str(maxUHID))==8 else "P"
        uhid = uInitial+str(maxUHID)
        # cloud_logger.debug("UHID = {}".format(str(uuid)))

    except Exception as e:
        print("Error while fetching unique health id : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        # cloud_logger.error("Error while fetching unique health id : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        return False   

    finally:
        return uhid

def fetchSocioEconomicId(family_id):
    data = None
    try:
        print("Fetching Socio Economic ID for Family.")
        # cloud_logger.info("Fetching Socio Economic ID for Family.")

        query = "SELECT family_socio_economic_id FROM public.family_socio_economic_ref WHERE family_id=%s"

        # with spnDB.snapshot() as snapshot:
        #     results = snapshot.execute_sql(query,params={"family_id": family_id},
        #     param_types={"family_id": param_types.STRING})
        value = (family_id,)
        cursor.execute(query,value)
        results = cursor.fetchall()
        for row in results:
                data = row[0]

        return data

    except Exception as e:
        print("Error While getting family's Last Update Date for : %s %s | %s | %s ", str(family_id),str(e), guard.current_userId, guard.current_appversion)
        # cloud_logger.error("Error While getting family's Last Update Date for : %s %s | %s | %s ", str(family_id),str(e), guard.current_userId, guard.current_appversion)
        return None   


def fetchLastUpdateFamily(family_id):
    data = None
    try:
        print("Fetching Last Update Timestamp for Family.")
        # cloud_logger.info("Fetching Last Update Timestamp for Family.")

        query = "SELECT last_update_date, update_register FROM public.family_master WHERE family_id=%s"

        # with spnDB.snapshot() as snapshot:
        #     results = snapshot.execute_sql(query,params={"family_id": family_id},
        #     param_types={"family_id": param_types.STRING})
        value = (family_id,)
        cursor.execute(query,value)
        results = cursor.fetchall()
        for row in results:
                data = {
                    'last_update_date': row[0],
                    'update_register': row[1] if row[1] is not None else row[1]
                }
        return data

    except Exception as e:
        print("Error While getting family's Last Update Date for Family Id : %s %s | %s | %s ", str(family_id),str(e), guard.current_userId, guard.current_appversion)
        # cloud_logger.error("Error While getting family's Last Update Date for Family Id : %s %s | %s | %s ", str(family_id),str(e), guard.current_userId, guard.current_appversion)
        return False   

def fetchLastUpdateMember(memberId, familyId):
    data = None
    try:
        print("Fetching Last Update Timestamp for Member.")
        # cloud_logger.info("Fetching Last Update Timestamp for Member.")
        # print("Member Id = {}".format(memberId))
        # cloud_logger.debug("Member Id = {}".format(memberId))
        query = "SELECT last_update_date, update_register FROM public.family_member_master WHERE member_id=%s AND family_id=%s"
        # with spnDB.snapshot() as snapshot:
        #     results = snapshot.execute_sql(
        #         query,
        #         params={
        #             "memberId": memberId,
        #             "familyId": familyId
        #         },
        #         param_types={
        #             "memberId": param_types.STRING,
        #             "familyId": param_types.STRING
        #         }
        #     )
        value = (memberId,familyId)
        cursor.execute(query,value)
        results = cursor.fetchall()
        for row in results:
                data = {
                    'last_update_date': row[0],
                    'update_register':row[1] if row[1] is not None else row[1]
                }

        return data
        
    except Exception as e:
        print("Error While retreving member's Last Update Date for Member Id : %s %s | %s | %s ", str(memberId),str(e),guard.current_userId, guard.current_appversion)
        # cloud_logger.error("Error While retreving member's Last Update Date for Member Id : %s %s | %s | %s ", str(memberId),str(e),guard.current_userId, guard.current_appversion)
        return False   

#Important: Temp Fix the Mobile App to be changed.
def isUpdatedFamily(updateRegister, reqUpdateRegister, userId):
    if updateRegister is None or len(updateRegister)==0:
        return True

    reqUpdateRegTS = datetime.strptime(reqUpdateRegister['timestamp'], "%Y-%m-%d %H:%M:%S%z")
    for register in updateRegister:
        updateRegTS = datetime.strptime(register['timestamp'], "%Y-%m-%d %H:%M:%S%z")
        # print("Time from request: ", reqUpdateRegTS, " TS from Spanner: ", updateRegTS)
        # print("User from request: ", userId, " User from Spanner: ", register['user_id'])
        if updateRegTS==reqUpdateRegTS and register['user_id'] == userId:
            return False
        
    return True


def UpsertFamilyDetails(family_list, userId):
    print("Adding Family Details.")
    # cloud_logger.info("Adding Family Details.")

    # Return Values
    family_details = []
    member_details = []
    is_success = True
    try:    
        #Key Arrays
        familyKeys=[]
        serefKeys=[]
        memberKeys=[]
        #Value Arrays
        familyVals=[]
        serefVals=[]
        memberVals=[]

        cntIdx=0

        for family in family_list:
            #Temp Value Arrays
            fvalues=[]
            sValues=[]
            phr_fid=0
            street_gid=""
            familyDetails = family['family_details']
            family_id = familyDetails['family_id']
            lastUpdateDate = familyDetails['last_update_date']
            updateRegister = familyDetails['update_register']
            members_list = family["family_member_details"]
            family.pop('family_member_details')
            address_details = {}
            print("Family Id = {}".format(family_id))
            # cloud_logger.debug("Family Id = {}".format(family_id))
            spannerData = fetchLastUpdateFamily(family_id)
            requestUpdateDate = datetime.strptime(familyDetails['last_update_date'], "%Y-%m-%d %H:%M:%S")
            # TODO - UNDERSTAND THIS WELL
            if (spannerData is None) or (requestUpdateDate > spannerData['last_update_date']):
                isFamilyUpdated = isUpdatedFamily(spannerData['update_register'], updateRegister, userId) if spannerData is not None else True
                if isFamilyUpdated:
                    for key in familyDetails.keys():
                        val=familyDetails[key]
                        if key=='family_insurances' and val is not None:
                            if(cntIdx==0):
                                familyKeys.append(key)
                            jsonVal = json.dumps(val)
                            fvalues.append(jsonVal)         
                        elif key=='social_details':
                            if val is not None:
                                if(cntIdx==0):
                                    serefKeys.append(key)
                                jsonVal = json.dumps(val) 
                                sValues.append(jsonVal)
                        elif key=='economic_details':
                            if val is not None:
                                if(cntIdx==0):
                                    serefKeys.append(key)
                                jsonVal = json.dumps(val)
                                sValues.append(jsonVal)
                        elif key=='family_socio_economic_id':
                            if cntIdx==0:
                                serefKeys.append(key)

                            socioEconomicId = fetchSocioEconomicId(family_id)
                            if(socioEconomicId is None):
                                sValues.append(str(uuid.uuid4()))
                            else:
                                sValues.append(socioEconomicId)
                        elif key=='phr_family_id':
                            if cntIdx==0:
                                familyKeys.append(key)
                            
                            phr_fid = get_phr_family_id() if(val is None or val=="") else val
                            fvalues.append(phr_fid)               
                        elif key=='last_update_date' and (val is not None or val!=""):
                            if(cntIdx==0):
                                familyKeys.append(key)
                            last_update_date = datetime.strptime(val, "%Y-%m-%d %H:%M:%S") 
                            fvalues.append(last_update_date)
                        elif key=='family_id' and (val is not None or val!=""):
                            if(cntIdx==0):
                                familyKeys.append(key)
                                serefKeys.append(key)
                            fvalues.append(val)
                            sValues.append(val)
                        elif key=='pds_smart_card_id' and (val is not None or str(val)!=""): # This fix is done for Version 2.x.x of app where the datatype for pds_smart_card_id was string. 25 March 2022. b/226691200
                            if(cntIdx==0):
                                familyKeys.append(key)
                            # fvalues.append(int(val))
                            #Commented below lines of code to handle the pds smart card value expect the numeric integers. 30thMarch2022.
                            #Below lines of code to handle the pds smart card value expect the numeric integers. 31thMarch2022.
                            print("pds_smart_card_id: %s",str(val))
                            # cloud_logger.info("pds_smart_card_id: %s",str(val))
                            # The following code has been implemented to handle the pds smart card id key as well as null value for pds smart card id.
                            # on 13-Apr-2022 as part of b/229086459 
                            if (val is not None and str(val)!=""):
                                if re.fullmatch(parameters['PDS_SMART_CARD_FORMAT'], val):
                                    fvalues.append(int(val))
                                else:
                                    fvalues.append(None)
                            else:
                                fvalues.append(None)
                        elif key=='street_id' and (val is not None or val != ""):
                            street_gid, address_details = get_hierarchy_fields(val, userId)
                            if len(address_details.keys()):
                                for key in address_details.keys():
                                    if(cntIdx==0):
                                        familyKeys.append(key)
                                    fvalues.append(address_details.get(key))
                                
                                if(cntIdx==0):
                                    familyKeys.append('shop_id')
                                shop_id = get_shop(street_gid)
                                fvalues.append(shop_id)
                        elif key in ['facility_id', 'hhg_id', 'ward_id', 'area_id', 'habitation_id', 'rev_village_id', 'village_id', 'block_id', 'hud_id', 'taluk_id', 'district_id', 'state_id', 'country_id', 'hsc_unit_id']:
                            continue
                        elif key in ['shop_id']:
                            continue
                        elif key=='update_register' and val is not None:
                            if(cntIdx==0):
                                familyKeys.append(key)
                                serefKeys.append(key)
                            # print("Update_Register value for Family: {}".format(str(val)))
                            # cloud_logger.debug("Update_Register value for Family: {}".format(str(val)))
                            update_register = spannerData['update_register'] if(spannerData is not None) else None
                            if update_register is not None:
                                update_register.append(val)
                            else:
                                update_register=[val]
                            fvalues.append(json.dumps(update_register))
                            sValues.append(getUpdateRegisterForFamilySocioRef(family_id, val))
                        elif val is not None or val !="" :
                            if(cntIdx==0):
                                familyKeys.append(key)
                            fvalues.append(val) 
                    

                    familyVals.append(fvalues)
                    serefVals.append(sValues)
                    cntIdx+=1
                    print('Family values added.')
                    # cloud_logger.info('Family values added.')
                    family_details.append({"family_id": family_id, "phr_family_id":phr_fid})
                if len(members_list) != 0:
                    isMemberUpdates, member_keys, mValues, member_details_current_family = UpsertMemberDetails(members_list, phr_fid, address_details, userId)
                    #print("Keys from Member Function:", member_keys)
                    #print("Values from Member Function:", mValues)
                    #print("member_details",member_details_current_family)
                    print("PHR FID Values from update Member Function: {}".format(str(phr_fid)))
                    # print("ADDRESS DETAILS: {}".format(str(address_details)))
                    # cloud_logger.debug("PHR FID Values from update Member Function: {}".format(str(phr_fid)))
                    # cloud_logger.debug("ADDRESS DETAILS: {}".format(str(address_details)))
                    if isMemberUpdates:
                        if len(memberKeys)==0:
                            memberKeys = member_keys
                        memberVals.extend(mValues)
                        # member_details.append(member_details_current_family) 
                        if len(member_details_current_family)>0: # This code is added because for 2 member it was returning only one member details.
                            for member in member_details_current_family: 
                                member_details.append(member)
                    else:
                        print("Error while upsert of Family Member.| %s | %s ",guard.current_userId, guard.current_appversion)
                        # cloud_logger.error("Error while upsert of Family Member.| %s | %s ",guard.current_userId, guard.current_appversion)
                        return False, family_details, member_details
                else:
                    print('No family members need to be updated.')
                    # cloud_logger.info('No family members need to be updated.')

        # cloud_logger.debug("Family Keys: {}, Columns: {}".format(str(familyKeys), str(familyVals)))
        # cloud_logger.debug("SEREF Keys: {}, Columns: {}".format(str(serefKeys), str(serefVals)))
        # cloud_logger.debug("Member Keys: {}, Columns: {}".format(str(memberKeys), str(memberVals)))

        if len(familyKeys)>0:      
            # def upsertFamily(transaction):
                print("Initiating Family insertion.")
                # cloud_logger.info("Initiating Family insertion.")        
                # transaction.insert_or_update(
                #     "family_master",
                #     columns=familyKeys,
                #     values=familyVals
                # )
        value = (familyVals[0][0],familyVals[0][1],familyVals[0][2],familyVals[0][3],familyVals[0][4],familyVals[0][5],familyVals[0][6],familyVals[0][7],familyVals[0][8],familyVals[0][9],familyVals[0][10],familyVals[0][11],familyVals[0][12],familyVals[0][13],familyVals[0][14],familyVals[0][15],familyVals[0][16],familyVals[0][17],familyVals[0][18],familyVals[0][19])
        query = "INSERT INTO public.family_master (family_id,phr_family_id,facility_id,hhg_id,street_id,ward_id,area_id,habitation_id,rev_village_id,village_id, block_id,hud_id,taluk_id,district_id,state_id,country_id,hsc_unit_id,shop_id,last_update_date,update_register) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (family_id) DO UPDATE SET family_id = EXCLUDED.family_id,phr_family_id = EXCLUDED.phr_family_id,facility_id = EXCLUDED.facility_id,hhg_id = EXCLUDED.hhg_id,street_id = EXCLUDED.street_id,ward_id = EXCLUDED.ward_id,area_id = EXCLUDED.area_id,habitation_id = EXCLUDED.habitation_id,rev_village_id = EXCLUDED.rev_village_id,village_id = EXCLUDED.village_id,block_id = EXCLUDED.block_id,hud_id = EXCLUDED.hud_id,taluk_id = EXCLUDED.taluk_id,district_id = EXCLUDED.district_id,state_id = EXCLUDED.state_id,country_id = EXCLUDED.country_id,hsc_unit_id = EXCLUDED.hsc_unit_id,shop_id = EXCLUDED.shop_id,last_update_date = EXCLUDED.last_update_date,update_register=COALESCE(EXCLUDED.update_register, '[]'::jsonb) ||EXCLUDED.update_register  ::jsonb"
        cursor.execute(query,value)
        conn.commit()
            # spnDB.run_in_transaction(upsertFamily)   

        if len(serefKeys)>0:
            # def UpsertSeref(transaction):
            print("Initiating SEREF insertion.")
                # cloud_logger.info("Initiating SEREF insertion.")        
                # transaction.insert_or_update(
                #     "family_socio_economic_ref",
                #     columns=serefKeys,
                #     values=serefVals
                # )
            value = (serefVals[0][0],serefVals[0][1],serefVals[0][2])
            query = "INSERT INTO public.family_socio_economic_ref (family_id,family_socio_economic_id,update_register) VALUES (%s,%s,%s) ON CONFLICT (family_socio_economic_id) DO UPDATE SET family_id = EXCLUDED.family_id,family_socio_economic_id = EXCLUDED.family_socio_economic_id,update_register=COALESCE(EXCLUDED.update_register, '[]'::jsonb) ||EXCLUDED.update_register  ::jsonb"
            cursor.execute(query,value)
            conn.commit()
            # spnDB.run_in_transaction(UpsertSeref)   

        if len(memberKeys)>0:
            # def UpsertMember(transaction):
                print("Initiating Family Member insertion.")
                # cloud_logger.info("Initiating Family Member insertion.")        
                # transaction.insert_or_update(
                #     "family_member_master",
                #     columns=memberKeys,
                #     values=memberVals
                # )
        value = (memberVals[0][0],memberVals[0][1],memberVals[0][2],memberVals[0][3],memberVals[0][4],memberVals[0][5],memberVals[0][6],memberVals[0][7],memberVals[0][8],memberVals[0][9],memberVals[0][10],memberVals[0][11],memberVals[0][12],memberVals[0][13],memberVals[0][14],memberVals[0][15],memberVals[0][16])
        query = "INSERT INTO public.family_member_master (member_id,family_id,facility_id,street_id,ward_id,area_id,habitation_id,rev_village_id,village_id,block_id,hud_id,taluk_id,district_id,state_id,country_id,last_update_date,update_register) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (member_id) DO UPDATE SET member_id = EXCLUDED.member_id,family_id = EXCLUDED.family_id,facility_id = EXCLUDED.facility_id,street_id = EXCLUDED.street_id,ward_id = EXCLUDED.ward_id,area_id = EXCLUDED.area_id,habitation_id = EXCLUDED.habitation_id,rev_village_id = EXCLUDED.rev_village_id,village_id = EXCLUDED.village_id,block_id = EXCLUDED.block_id,hud_id = EXCLUDED.hud_id,taluk_id = EXCLUDED.taluk_id,district_id = EXCLUDED.district_id,state_id = EXCLUDED.state_id,country_id = EXCLUDED.country_id,last_update_date = EXCLUDED.last_update_date,update_register=COALESCE(EXCLUDED.update_register, '[]'::jsonb) ||EXCLUDED.update_register  ::jsonb"
        cursor.execute(query,value)
        conn.commit()
            # spnDB.run_in_transaction(UpsertMember)   

    except Exception as e:
        # print(str(e))
        print("Error while upsert of Family Details : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        # cloud_logger.error("Error while upsert of Family Details : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        is_success = False

    finally:
        return is_success, family_details, member_details


def UpsertMemberDetails(member_list, phr_fid, address_details, userId):
    
    #cloud_logger.info("Adding Member Details.")

    try:    
        #Key Arrays
        memberKeys=[]
        #Value Arrays
        memberVals=[]
        member_details=[]
        cntIdx=0
        uhid = ""
        for memberDetail in member_list:
            #Temp Value Arrays
            mValues=[]

            member = memberDetail['member_detail']
            memberId = member['member_id']      
            familyId = member['family_id']            
            spannerData = fetchLastUpdateMember(memberId, familyId)
            requestUpdateDate = datetime.strptime(member['last_update_date'], "%Y-%m-%d %H:%M:%S")
            if spannerData is None or requestUpdateDate > spannerData['last_update_date']:
                for key in member.keys():
                    val=member[key]
                    if key in ['insurances', 'welfare_beneficiary_ids', 'program_ids', 'resident_status_details', 'consent_details'] and val is not None:
                        if(cntIdx==0):
                            memberKeys.append(key)
                        jsonVal = json.dumps(val) 
                        mValues.append(jsonVal)
                    elif key=='resident_status' and (val is not None or val !=""):
                        if(cntIdx==0):
                            memberKeys.append(key)
                        mValues.append(val)       
                    elif key=='phr_family_id':
                        if(cntIdx==0):
                            memberKeys.append(key)
                        # cloud_logger.debug("VALUE : {}".format(str(val)))
                        # cloud_logger.debug("PHR : {}".format(str(phr_fid)))
                        # phrfid = val if (val is not None or val !="") else phr_fid
                        phrfid = val if (val is not None) else phr_fid
                        # cloud_logger.debug("PHRFID : {}".format(str(phrfid)))
                        mValues.append(phrfid)          
                    elif key=='unique_health_id':
                        if(cntIdx==0):
                            memberKeys.append(key)
                        uhid = get_unique_health_id() if(val is None or val=="") else val
                        mValues.append(uhid)   
                    elif key=='birth_date' and val is not None:
                        if(cntIdx==0):
                            memberKeys.append(key)
                        dateval = datetime.strptime(val,"%Y-%m-%d").date()
                        mValues.append(dateval)
                    elif key=='street_id' and (val is not None or val != ""):
                        #print("val ***** : ",val)
                        # cloud_logger.debug("address details : {}".format(str(address_details)))
                        has_address = bool(address_details)
                        if not has_address:
                            street_gid, addrDetails = get_hierarchy_fields(val, userId)
                            address_details = addrDetails
                            # cloud_logger.debug("Adding address for New Member: {} ".format(str(addrDetails)))
                        if len(address_details.keys())>0:
                            if (cntIdx==0):
                                address_details.pop("hhg_id")
                                address_details.pop("hsc_unit_id")
                            # cloud_logger.debug('address_details: {}'.format(str(address_details.keys())))
                            for key in address_details.keys():
                                if(cntIdx==0):
                                    memberKeys.append(key)
                                mValues.append(address_details.get(key))
                    elif key in ['facility_id', 'ward_id', 'area_id', 'habitation_id', 'rev_village_id', 'village_id', 'block_id', 'hud_id', 'taluk_id', 'district_id', 'state_id', 'country_id']:
                        continue
                    elif key=='last_update_date' and (val is not None or val != ""):
                        if(cntIdx==0):
                            memberKeys.append(key)
                        last_update_date = datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
                        mValues.append(last_update_date)
                    elif key in ['family_id', 'member_id'] and (val is not None or val != ""):
                        if(cntIdx==0):
                            memberKeys.append(key)
                        mValues.append(val)
                    elif key=='update_register' and val is not None:
                        if(cntIdx==0):
                            memberKeys.append(key)
                        update_register = spannerData['update_register'] if(spannerData is not None) else None
                        if update_register is not None:
                            update_register.append(val)
                        else:
                            update_register=[val]
                        mValues.append(json.dumps(update_register))
                    elif val is not None or val !="":
                        if(cntIdx==0):
                            memberKeys.append(key)
                        mValues.append(val) 
                    
                member_details.append({"member_id": memberId, "unique_health_id":uhid})
                # print('Member Details First ',member_details)
                cntIdx+=1
                memberVals.append(mValues)
        
    except Exception as e:
        # print(str(e))
        print("Error while upsert of Member : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        # cloud_logger.error("Error while upsert of Member : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        return False, memberKeys, memberVals, member_details

    finally:
        # print('member Details from upsert Member', member_details)
        return True, memberKeys, memberVals, member_details

def getUpdateRegisterForFamilySocioRef(familyId, updateRegister):
    print("getUpdateRegisterForFamilySocioRef")
    try:
        update_register = None
        
        # with spnDB.snapshot() as snapshot:
        query = "SELECT update_register FROM public.family_socio_economic_ref WHERE family_id=%s"
            # results = snapshot.execute_sql(
            #             query,
            #             params={"family_id": familyId},
            #             param_types={"memberId": param_types.STRING}
            #             )
        value = (familyId,) 
        cursor.execute(query,value)
        results = cursor.fetchall()
        for row in results:
                if row[0] is None:
                    update_register = []
                else:
                    update_register=row[0]

        if update_register is None:
                update_register = []

        update_register.append(updateRegister)
        # cloud_logger.debug("Update_Register Appended SEREF: {}".format(str(update_register)))
        return json.dumps(update_register)

    except Exception as e:
        print("Error while Updating register for Social Details : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        # cloud_logger.error("Error while Updating register for Social Details : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
from guard import *
import guard

def fetchLastUpdate(memberId, familyId):
    try:
        cloud_logger.info("Fetching Last Update Timestamp.")
        query = "SELECT FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S%z', last_update_date, 'Asia/Calcutta') as last_update_date FROM health_history WHERE family_id=@familyId and member_id=@memberId"

        last_update_date = None    

        with spnDB.snapshot() as snapshot:
            results = snapshot.execute_sql(query,params={"memberId": memberId, "familyId": familyId},
            param_types={"memberId": param_types.STRING, "familyId": param_types.STRING})

            for row in results:
                last_update_date = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S%z")

            if last_update_date:
                return last_update_date
            else:
                return None
        
    except Exception as e:
        cloud_logger.error("Error While Last Update Date : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        return None   

def UpsertMedicalHistory(historyList):
    ignores=0
    upserts=0

    cloud_logger.info("Adding Medical History.")
    try:    
        #Key Arrays
        historyKeys=[]
        serefKeys=[]
        memberKeys=[]
        #Value Arrays
        historyVals=[]
        serefVals=[]
        memberVals=[]

        cntIdx=0
        for history in historyList:
            #Temp Value Arrays
            values=[]
            sValues=[]
            mValues=[]
            memberId = history['member_id']
            familyId = history['family_id']
            is_correct_date = False
            lastUpdateDate = fetchLastUpdate(memberId, familyId)
            requestUpdateDate = datetime.strptime(history['last_update_date'], "%Y-%m-%d %H:%M:%S%z")
            if lastUpdateDate is not None:#not first time check
                is_correct_date = requestUpdateDate > lastUpdateDate
            if lastUpdateDate is None or is_correct_date:
                for key in history.keys():
                    val=history[key]
                    if key in ['past_history', 'family_history', 'welfare_method', 'eligible_couple_details', 'lifestyle_details', 'vaccinations', 'disability_details']:
                        if(cntIdx==0):
                            historyKeys.append(key)
                        jsonVal = json.dumps(val) if(val is not None) else val
                        values.append(jsonVal)
                    elif key in ['mtm_beneficiary']:
                        print("****************** MTM Data received for the beneficiary ******* ")
                        if(cntIdx==0):
                            historyKeys.append(key)
                        mtm_val = mtm_data_verification(memberId, history, familyId)
                        jsonVal = json.dumps(mtm_val) if(mtm_val is not None) else val
                        values.append(jsonVal)                
                    elif key in ['resident_status']:
                        if(cntIdx==0):
                            memberKeys.append(key)
                        mValues.append(val)                
                    elif key in ['resident_status_details']:
                        if(cntIdx==0):
                            memberKeys.append(key)
                        jsonVal = json.dumps(val) if(val is not None) else val
                        mValues.append(jsonVal)                
                    elif key in ['social_details', 'economic_details']:
                        if(cntIdx==0):
                            serefKeys.append(key)
                        jsonVal = json.dumps(val) if(val is not None) else val
                        sValues.append(jsonVal)                
                    elif key in ['enrollment_date']:
                        if(cntIdx==0):
                            historyKeys.append(key)
                        dateval = datetime.strptime(val,"%Y-%m-%d").date() if(val is not None) else val
                        values.append(dateval)
                    elif key in ['last_update_date']:
                        if(cntIdx==0):
                            historyKeys.append(key)
                        last_update_date = datetime.strptime(val, "%Y-%m-%d %H:%M:%S%z") if(val is not None) else val
                        values.append(last_update_date)
                    elif key in ['family_id', 'member_id']:
                        if(cntIdx==0):
                            historyKeys.append(key)
                            memberKeys.append(key)
                            serefKeys.append(key)
                        values.append(val)
                        mValues.append(val)
                        sValues.append(val)
                    elif key in ['update_register']:
                        if(cntIdx==0):
                            historyKeys.append(key)
                            memberKeys.append(key)
                            serefKeys.append(key)
                        values.append(getUpdateRegister(memberId, val, familyId))
                        mValues.append(getUpdateRegisterForMemberMaster(memberId, val, familyId))
                        sValues.append(getUpdateRegisterForSocioMemberRef(memberId, val, familyId))
                    
                    else:
                        if(cntIdx==0):
                            historyKeys.append(key)
                        values.append(val) 
                
                cntIdx+=1
                historyVals.append(values)
                memberVals.append(mValues)
                serefVals.append(sValues)
                
                upserts+=1

                cloud_logger.debug("History = {}, Values = {}".format(str(historyKeys), str(historyVals)))     
                def UpsertHistory(transaction):

                    transaction.insert_or_update(
                        "health_history",
                        columns=historyKeys,
                        values=historyVals
                    )

                spnDB.run_in_transaction(UpsertHistory)   
                cloud_logger.debug("Member = {}, Values = {}".format(str(memberKeys), str(memberVals)))
                def UpsertMember(transaction):
                    transaction.insert_or_update(
                        "family_member_master",
                        columns=memberKeys,
                        values=memberVals
                    )

                spnDB.run_in_transaction(UpsertMember)   
                cloud_logger.debug("columns = {}, Values = {}".format(str(serefKeys), str(serefVals)))
                def UpsertSeref(transaction):
                    transaction.insert_or_update(
                        "family_member_socio_economic_ref",
                        columns=serefKeys,
                        values=serefVals
                    )

                spnDB.run_in_transaction(UpsertSeref)
                
            else:
                ignores+=1

        return True, ignores, upserts

    except Exception as e:
        cloud_logger.error("Error while upsert of Medical History : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        return False, ignores, upserts


def getUpdateRegister(memberId, updateRegister, familyId):
    try:
        cloud_logger.info("Formatting Update Register.")
        update_register = None

        with spnDB.snapshot() as snapshot:
            query = "SELECT update_register FROM health_history where family_id=@familyId and member_id=@memberId"
            results = snapshot.execute_sql(
                        query,
                        params={"familyId": familyId, "memberId": memberId},
                        param_types={"familyId": param_types.STRING, "memberId": param_types.STRING}
                        )
            for row in results:
                if row[0] is None:
                    update_register = []
                else:
                    update_register=json.loads(row[0])

            if update_register is None:
                update_register = []

            update_register.append(updateRegister)
        return json.dumps(update_register)

    except Exception as e:
        cloud_logger.error("Error while Updating Register : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)


def getUpdateRegisterForMemberMaster(memberId, updateRegister, familyId):
    try:
        cloud_logger.info("Formatting Update Register for Member.")
        update_register = None

        with spnDB.snapshot() as snapshot:
            query = "SELECT update_register FROM family_member_master where family_id=@familyId and member_id=@memberId"
            results = snapshot.execute_sql(
                        query,
                        params={"familyId": familyId, "memberId": memberId},
                        param_types={"familyId": param_types.STRING, "memberId": param_types.STRING}
                    )
            for row in results:
                if row[0] is None:
                    update_register = []
                else:
                    update_register=json.loads(row[0])

            if update_register is None:
                update_register = []

            update_register.append(updateRegister)
        return json.dumps(update_register)

    except Exception as e:
        cloud_logger.error("Error while Updating Register for Member : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)


def getUpdateRegisterForSocioMemberRef(memberId, updateRegister, familyId):
    try:
        cloud_logger.info("Formatting Update Register for social details.")
        update_register = None
        userId = updateRegister["user_id"]
        timestamp = updateRegister["timestamp"]

        with spnDB.snapshot() as snapshot:
            query = "SELECT update_register FROM family_member_socio_economic_ref where family_id=@familyId and member_id=@memberId"
            results = snapshot.execute_sql(
                        query,
                        params={"familyId": familyId, "memberId": memberId},
                        param_types={"familyId": param_types.STRING, "memberId": param_types.STRING}
                        )
            for row in results:
                if row[0] is None:
                    update_register = []
                else:
                    update_register=json.loads(row[0])

            if update_register is None:
                update_register = []

            update_register.append(updateRegister)
        return json.dumps(update_register)

    except Exception as e:
        cloud_logger.error("Error while Updating Register for social details : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)


def mtm_data_verification(memberId, history, familyId):
    """
    Method to verify the existing mtm beneficiary and upsert the
    mtm beneficiary with the data received from request.
    Input args: member Id, medical history.
    Return: dict 
    """
    try:
        with spnDB.snapshot() as snapshot:
            query = "SELECT mtm_beneficiary from health_history where family_id=@familyId and member_id=@memberId"
            results = snapshot.execute_sql(
                        query,
                        params={"familyId": familyId, "memberId": memberId},
                        param_types={"familyId": param_types.STRING, "memberId": param_types.STRING}
                        )
            for mtm_values in results:
                cloud_logger.info("MTM_Values:")
                print(mtm_values)
                if mtm_values[0] is not None and mtm_values[0]!={}:
                    mtm_json_db = json.loads(mtm_values[0])
                    mtm_keys_db = mtm_json_db.keys()
                    mtm_json_req = history['mtm_beneficiary']
                    if mtm_json_req is not None: # This is when we have mtm data from request. 25 March 2022.
                        mtm_keys_req = history['mtm_beneficiary'].keys()
                        # Check whether any new disease getting added to member.
                        # Checking the length of mtm json from DB and mtm json of request are same.
                        result = True if (len(mtm_json_db)==len(mtm_json_req)) else False
                        if result: #all keys are matching
                            for key in mtm_keys_db:
                                if isinstance(mtm_json_req[key], dict) and isinstance(mtm_json_db[key], dict):
                                    status_db = mtm_json_db[key]["status"]
                                    status_req = mtm_json_req[key]["status"]
                                    if status_db=="no" and status_req=="yes":
                                        mtm_json_db[key]["status"]=status_req
                                        mtm_json_db[key]["enroll_date"]=mtm_json_req[key]["enroll_date"]
                                        mtm_json_db[key]["exit_date"]=None #Set exit date as None due to status is yes.
                                    elif status_db=="yes" and status_req=="no":
                                        mtm_json_db[key]["status"]=status_req                                
                                        mtm_json_db[key]["exit_date"]=mtm_json_req[key]["exit_date"]
                                else:
                                    if mtm_json_req[key] != mtm_json_db[key]:
                                        mtm_json_db[key]=mtm_json_req[key]
                                        
                            return mtm_json_db
                        else: #keys are missing in existing data in db
                            #find the mismatched keys and append to existing data
                            for key in mtm_keys_req:
                                if key not in mtm_keys_db:
                                    mtm_json_db[key]=history['mtm_beneficiary'][key]
                                    return mtm_json_db
                    else: # This is when we don't have mtm data from request we will return db data.
                        cloud_logger.info('return the db value when request mtm value is null.')
                        return mtm_json_db                
                else:
                    return history['mtm_beneficiary']
            # print ("Health history inserted for the first time.")
            return history['mtm_beneficiary']
            
    except Exception as e:
        cloud_logger.error("Error while verifying the MTM data : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        
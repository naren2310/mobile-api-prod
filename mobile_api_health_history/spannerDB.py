from guard import *
import guard

def fetchLastUpdate(memberId, familyId):
    try:
        print("Fetching Last Update Timestamp.")
        last_update_date = None  
          
        conn = get_db_connection_read()
        cursor = conn.cursor()
        query = "SELECT to_char(last_update_date AT TIME ZONE 'Asia/Calcutta', 'YYYY-MM-DD HH24:MI:SS+05:30') as last_update_date FROM public.health_history WHERE family_id=%s AND member_id=%s"
        values = (familyId, memberId)
        cursor.execute(query,values)
        results = cursor.fetchall()


        for row in results:
            last_update_date = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S%z")

        if last_update_date:
            return last_update_date
        else:
            return None
        
    except psycopg2.ProgrammingError as e:
        print("member_health_history fetchLastUpdate ProgrammingError",e)  
        conn.rollback()
        return None
    except psycopg2.InterfaceError as e:
        print("member_health_history fetchLastUpdate InterfaceError",e)
        reconnectToDBRead()
        return None   
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception as e:
            print("member_health_history fetchLastUpdate",e)

def UpsertMedicalHistory(historyList):
    ignores=0
    upserts=0

    print("Adding Medical History.")
    try:    
        #Key Arrays
        historyKeys=[]
        serefKeys=[]
        memberKeys=[]
        #Value Arrays
        historyVals=[]
        serefVals=[]
        memberVals=[]

        ## DB Connection
        conn = get_db_connection()
        cursor = conn.cursor()
        
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
                    elif key in ['immunization_status','premature_baby','congenital_defects','blood_group','attended_age','ifa_tablet_provided','sanitary_napkin_provided','anemia_yes_or_no','pregnant_yes_or_no','antenatal_postnatal','prolonged_disease','Consanguineous_marriage','rch_id']:
                        if(cntIdx==0):
                            memberKeys.append(key)
                        mValues.append(val) 
                    elif key in ['non_communicable_disease','communicable_disease']:
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
                            memberKeys.append(key)
                            serefKeys.append(key)
                        last_update_date = datetime.strptime(val, "%Y-%m-%d %H:%M:%S%z") if(val is not None) else val
                        values.append(last_update_date)
                        mValues.append(last_update_date)
                        sValues.append(last_update_date)
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

                print('health_history inserting')
                print("History = {}, Values = {}".format(str(historyKeys), str(historyVals))) 
                for historyValss in historyVals:
                    value = tuple(historyValss)
                    query = f"INSERT INTO public.health_history ({','.join(historyKeys)}) VALUES ({','.join(['%s']*len(historyKeys))}) ON CONFLICT (medical_history_id) DO UPDATE SET {','.join([f'{key}=%s' for key in historyKeys])}"
                    cursor.execute(query,(value)*2)
                    conn.commit()
 
                print("family_member_master inserting")
                print("Member = {}, Values = {}".format(str(memberKeys), str(memberVals)))
                for memberValss in memberVals:
                    value = tuple(memberValss)
                    query = f"INSERT INTO public.family_member_master ({','.join(memberKeys)}) VALUES ({','.join(['%s']*len(memberKeys))}) ON CONFLICT (member_id) DO UPDATE SET {','.join([f'{key}=%s' for key in memberKeys])}"
                    cursor.execute(query,(value)*2)
                    conn.commit()

                print("family_member_socio_economic_ref inserting")
                print("serefKeys",serefKeys)
                print("serefVals",serefVals)
                for serefValss in serefVals:
                    value = tuple(serefValss)
                    query = f"INSERT INTO public.family_member_socio_economic_ref ({','.join(serefKeys)}) VALUES ({','.join(['%s']*len(serefKeys))}) ON CONFLICT (member_id) DO UPDATE SET {','.join([f'{key}=%s' for key in serefKeys])}"
                    cursor.execute(query,(value)*2)
                    conn.commit()
                
            else:
                ignores+=1

        return True, ignores, upserts

    except psycopg2.ProgrammingError as e:
        print("member_health_history UpsertMedicalHistory ProgrammingError",e)  
        conn.rollback()
        return False, ignores, upserts
    except psycopg2.InterfaceError as e:
        print("member_health_history UpsertMedicalHistory InterfaceError",e)
        reconnectToDB()
        return False, ignores, upserts
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception as e:
            print("member_health_history UpsertMedicalHistory",e)


def getUpdateRegister(memberId, updateRegister, familyId):
    try:
        print("Formatting Update Register.")
        update_register = None
        conn = get_db_connection_read()
        cursor = conn.cursor()
        query = "SELECT update_register FROM public.health_history WHERE family_id=%s AND member_id=%s"
        values = (familyId, memberId)
        cursor.execute(query,values)
        results = cursor.fetchall()
        for row in results:
            if row[0] is None:
                update_register = []
            else:
                update_register=row[0]

        if update_register is None:
            update_register = []

        update_register.append(updateRegister)
        return json.dumps(update_register)

    except psycopg2.ProgrammingError as e:
        print("member_health_history getUpdateRegister ProgrammingError",e)  
        conn.rollback()
    except psycopg2.InterfaceError as e:
        print("member_health_history getUpdateRegister InterfaceError",e)
        reconnectToDBRead()
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception as e:
            print("member_health_history getUpdateRegister",e)


def getUpdateRegisterForMemberMaster(memberId, updateRegister, familyId):
    try:
        print("Formatting Update Register for Member.")
        update_register = None
        conn = get_db_connection_read()
        cursor = conn.cursor()
        query = "SELECT update_register FROM public.family_member_master WHERE family_id=%s AND member_id=%s"
        values = (familyId, memberId)
        cursor.execute(query,values)
        results = cursor.fetchall()
        for row in results:
            if row[0] is None:
                update_register = []
            else:
                update_register=row[0]

        if update_register is None:
            update_register = []

        update_register.append(updateRegister)
        return json.dumps(update_register)

    except psycopg2.ProgrammingError as e:
        print("member_health_history getUpdateRegisterForMemberMaster ProgrammingError",e)  
        conn.rollback()
    except psycopg2.InterfaceError as e:
        print("member_health_history getUpdateRegisterForMemberMaster InterfaceError",e)
        reconnectToDBRead()
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception as e:
            print("member_health_history getUpdateRegisterForMemberMaster",e)


def getUpdateRegisterForSocioMemberRef(memberId, updateRegister, familyId):
    try:
        print("Formatting Update Register for social details.")
        update_register = None
        userId = updateRegister["user_id"]
        timestamp = updateRegister["timestamp"]
        conn = get_db_connection_read()
        cursor = conn.cursor()
        query = "SELECT update_register FROM public.family_member_socio_economic_ref WHERE family_id=%s AND member_id=%s"
        values = (familyId, memberId)
        cursor.execute(query,values)
        results = cursor.fetchall()
        for row in results:
            if row[0] is None:
                update_register = []
            else:
                update_register=row[0]

        if update_register is None:
            update_register = []

        update_register.append(updateRegister)
        return json.dumps(update_register)

    except psycopg2.ProgrammingError as e:
        print("member_health_history getUpdateRegisterForSocioMemberRef ProgrammingError",e)  
        conn.rollback()
    except psycopg2.InterfaceError as e:
        print("member_health_history getUpdateRegisterForSocioMemberRef InterfaceError",e)
        reconnectToDBRead()
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception as e:
            print("member_health_history getUpdateRegisterForSocioMemberRef",e)


def mtm_data_verification(memberId, history, familyId):
    """
    Method to verify the existing mtm beneficiary and upsert the
    mtm beneficiary with the data received from request.
    Input args: member Id, medical history.
    Return: dict 
    """
    try:
        conn = get_db_connection_read()
        cursor = conn.cursor()
        query = "SELECT mtm_beneficiary FROM public.health_history WHERE family_id=%s AND member_id=%s"
        values = (familyId, memberId)
        cursor.execute(query,values)
        results = cursor.fetchall()
        for mtm_values in results:
                print("MTM_Values:")
                if mtm_values[0] is not None and mtm_values[0]!={}:
                    mtm_json_db = mtm_values[0]
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
                        print('return the db value when request mtm value is null.')
                        return mtm_json_db                
                else:
                    return history['mtm_beneficiary']
            # print ("Health history inserted for the first time.")
        return history['mtm_beneficiary']
            
    except psycopg2.ProgrammingError as e:
        print("member_health_history mtm_data_verification ProgrammingError",e)  
        conn.rollback()
    except psycopg2.InterfaceError as e:
        print("member_health_history mtm_data_verification InterfaceError",e)
        reconnectToDBRead()
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception as e:
            print("member_health_history mtm_data_verification",e)
        
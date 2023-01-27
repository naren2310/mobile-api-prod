from guard import *
import guard

def fetchLastUpdate(familyId, memberId):
    try:
        cloud_logger.info("Fetching Last Update Timestamp for memberId %s", str(memberId))
        # query = "SELECT last_update_date FROM health_screening WHERE member_id=@member_id"
        # This will return the latest value. 15 March 2022.
        # This is done specifically to optimise CPU utilisation. 7 April 2022 (Shankar / Atul).
        query = "SELECT MAX (last_update_date) from health_screening WHERE family_id=@familyId and member_id=@memberId"

        last_update_date = None    

        with spnDB.snapshot() as snapshot:
            results = snapshot.execute_sql(query,params={"familyId": familyId, "memberId": memberId},
            param_types={"familyId": param_types.STRING, "memberId": param_types.STRING})

            for row in results:
                last_update_date = row[0]

        return last_update_date
        
    except Exception as e:
        cloud_logger.error("Error While Last Update Date : %s| %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        return None 

def add_screening_details(screenings):

    cloud_logger.info("Add Screening Details.")
    vArray=[]
    kArray=[]
    add_serv_form={}
    mem_add_serv={}
    
    try:
        for screening in screenings:
            values=[]
            
            familyId = screening['family_id'] # This is added specifically for optimising the query. 7 April 2022. (Shankar/Atul)
            memberId = screening["member_id"]
            cloud_logger.info('Screening details from request: %s', str(screening))
            lastUpdateDate = fetchLastUpdate(familyId,memberId)
            requestUpdateDate = datetime.strptime(screening['last_update_date'], "%Y-%m-%d %H:%M:%S%z")
            # This check is removed to allow all the screenings to be get inserted. 25 March 2022
            # if lastUpdateDate is None or requestUpdateDate > lastUpdateDate:
            for key in screening.keys():
                val=screening[key]
                
                if key in ['lab_test', 'drugs', 'advices', 'diseases', 'symptoms']:
                    jsonVal = json.dumps(val) if(val is not None) else val
                    values.append(jsonVal)
                elif key in ['screening_values']: # This fix is done from the api side to support bmi values coming from app Prior V_3.1.4 - 22 April 2022 b/226699999
                    if val['bmi'] is not None:
                        val['bmi'] = round(val['bmi'],2)
                        jsonVal = json.dumps(val) if(val is not None) else val
                        values.append(jsonVal)
                    else:
                        jsonVal = json.dumps(val) if(val is not None) else val
                        values.append(jsonVal)
                elif key in ['outcome']:
                    new_outcome = {}
                    outcome_key = list(val.keys())                        
                    #This change is explicitly added for accepting data from APP V1 - 11thFeb2022.
                    if not isinstance(val[outcome_key[0]], dict):
                        new_outcome['covid_19']=val
                        outcome_jsonVal = json.dumps(new_outcome) if(new_outcome is not None) else val
                        values.append(outcome_jsonVal)
                    else:
                        outcome_jsonVal = json.dumps(val) if(val is not None) else val
                        values.append(outcome_jsonVal)
                elif key in ['last_update_date']:
                    # This check is here to validate the timestamp is the latest than the one in DB. 25 March 2022. 
                    if lastUpdateDate is None or requestUpdateDate > lastUpdateDate:
                        last_update_date = datetime.strptime(val, "%Y-%m-%d %H:%M:%S%z")
                    else: # This will execute only when request has an older record than the DB.
                        # date = datetime.now(timezone(parameters['TIMEZONE']))
                        # last_update_date = datetime.strptime(date, "%Y-%m-%d %H:%M:%S%z")
                        # Commented the line#65 & 66 as per AJ suggestion 
                        # to get the update register timestamp and write to last update date. 28thMarch2022.
                        last_update_date = datetime.strptime(screening["update_register"]['timestamp'],"%Y-%m-%d %H:%M:%S%z")
                    values.append(last_update_date)
                    
                elif key in ['update_register']:
                    updateRegister = [screening["update_register"]]
                    jsonVal = json.dumps(updateRegister)
                    values.append(jsonVal)
                elif key=="unique_health_id":
                    continue
                #Below code to handle the old APK request with additional services
                #wrt MTM beneficiary implementation.
                elif key=="additional_services":                        
                    for as_key, as_value in val.items():
                        if as_value == "no":
                            add_serv_form[as_key]={
                                "enroll_date": "2021-07-07 00:00:00+0530",
                                "exit_date": (datetime.now().astimezone()).strftime("%Y-%m-%d %H:%M:%S%z"),
                                "status": "no"
                            }
                        if as_value == "yes":
                            add_serv_form[as_key]={
                                "enroll_date": "2021-07-07 00:00:00+0530",
                                "exit_date": None,
                                "status": "yes"
                            }
                        #When multiple members are passed in request then, 
                        # it stores the formatted additional services data with memberId as key.
                        mem_add_serv[memberId]=copy.deepcopy(add_serv_form)
                else:
                    values.append(val) 
            print(values)    
            vArray.append(values)
            kArray=list(screening.keys())

            if "unique_health_id" in kArray:
                kArray.remove("unique_health_id")
            if "additional_services" in kArray:
                kArray.remove("additional_services")
            # else:
            #     cloud_logger.info("Already have an updated data for member id : %s", str(memberId))                

        with spnDB.batch() as batch:
                # We were using an insert by batch.insert() till 11 April 2022, but since the app somehow is sending a duplicate screening id.
                # We are changing the code to support the mobile app's screening id duplication. Hence we are doing upsert here. 11 April 2022.
                batch.insert_or_update('health_screening', columns=kArray, values=vArray)

        #API Versioning to handle the old APK request with additional services.
        #Updating the additional services data to mtm beneficiary on health history table.
        if len(mem_add_serv)!=0:
            query="UPDATE health_history SET mtm_beneficiary=@mtm_beneficiary WHERE member_id=@member_id"
            for mkey, mvalue in mem_add_serv.items():                
                results = spnDB.execute_partitioned_dml(
                            query,
                            params={
                                "mtm_beneficiary": json.dumps(mvalue),
                                "member_id": mkey
                                },
                            param_types={
                                "mtm_beneficiary": param_types.JSON,
                                "member_id": param_types.STRING
                                }
                            )
        return True

    except Exception as e:       
        cloud_logger.error("Error While inserting Screening : %s| %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        # cloud_logger.debug('Error for Screening details : %s', str(screenings))
        return False
from guard import *
import guard

def fetchLastUpdate(familyId, memberId):
    try:
        print("Fetching Last Update Timestamp for memberId %s", str(memberId))
        # This will return the latest value. 15 March 2022.
        # This is done specifically to optimise CPU utilisation. 7 April 2022 (Shankar / Atul).
        conn = get_db_connection_read()
        cursor = conn.cursor()
        query = "SELECT MAX (last_update_date) FROM public.health_screening WHERE family_id=%s AND member_id=%s"
        values = (familyId, memberId)
        cursor.execute(query,values)
        results = cursor.fetchall()
        last_update_date = None    

        for row in results:
            last_update_date = row[0]

        return last_update_date
        
    except psycopg2.ProgrammingError as e:
        print("member_screening_details fetchLastUpdate ProgrammingError",e)  
        conn.rollback()
        return None
    except psycopg2.InterfaceError as e:
        print("member_screening_details fetchLastUpdate InterfaceError",e)
        reconnectToDBRead()
        return None 
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception as e:
            print("member_screening_details fetchLastUpdate",e)

def add_screening_details(screenings):

    print("Add Screening Details.")
    vArray=[]
    kArray=[]
    add_serv_form={}
    mem_add_serv={}
    
    try:
        
        ## DB Connection
        conn = get_db_connection()
        cursor = conn.cursor()
        
        for screening in screenings:
            values=[]
            
            familyId = screening['family_id'] # This is added specifically for optimising the query. 7 April 2022. (Shankar/Atul)
            memberId = screening["member_id"]
            print('Screening details from request: %s', str(screening))
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
            vArray.append(values)
            kArray=list(screening.keys())

            if "unique_health_id" in kArray:
                kArray.remove("unique_health_id")
            if "additional_services" in kArray:
                kArray.remove("additional_services")
            else:
                for vArrays in vArray:
                    value = tuple(vArrays)
                    query = f"INSERT INTO public.health_screening ({','.join(kArray)}) VALUES ({','.join(['%s']*len(kArray))}) ON CONFLICT (screening_id) DO UPDATE SET {','.join([f'{key}=%s' for key in kArray])}"
                    cursor.execute(query,(value)*2)
                    conn.commit()
                    
        #API Versioning to handle the old APK request with additional services.
        #Updating the additional services data to mtm beneficiary on health history table.
        if len(mem_add_serv)!=0:
            query="UPDATE public.health_history SET mtm_beneficiary=%s WHERE member_id=%s"
            for mkey, mvalue in mem_add_serv.items():                
                value = (json.dumps(mvalue),mkey)
                cursor.execute(query,value)
                conn.commit()
        return True

    except psycopg2.ProgrammingError as e:
        print("member_screening_details add_screening_details ProgrammingError",e)  
        conn.rollback()
        return False
    except psycopg2.InterfaceError as e:
        print("member_screening_details add_screening_details InterfaceError",e)
        reconnectToDB()
        return False 
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception as e:
            print("member_screening_details add_screening_details",e)
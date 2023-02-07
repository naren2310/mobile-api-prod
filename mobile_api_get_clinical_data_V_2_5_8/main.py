from guard import *
import guard
from flask import Flask, request

"""
******Method Details******
Description: API to access Clinical Master Data
MIME Type: application/json
Input: Last update Timestamp - {"LAST_UPDATE":<Timestamp>, "API_KEY":<Token>}
Output: Master data from vaccination_master, screening_master, protocol_master, drugs_master, lab_tests_master, diagnosis_master
{"VACCINATION_MASTER":[{}], "SCREENING_MASTER":[{}], "PROTOCOL_MASTER":[{}], "DRUGS_MASTER":[{}], LABTESTS_MASTER:[{}], DIAGNOSIS_MASTER:[{}]}
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
                return False, json.dumps({"status":"API-ACTIVE", "status_code":"200",
                                    "message":'Uptime check trigger.'})
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
        return False, json.dumps({'status':'FAILURE', "status_code":"401", 'message' : 'Invalid Token.'})

@app.route('/api/mobile_api_get_clinical_data', methods=['POST'])
def get_clinical_data():

    token_status, token_data = token_required(request)
    if not token_status:
        return token_data

    try:
        print("********Get Clinical Data*********")
        # cloud_logger.info("********Get Clinical Data*********")
        response = None
        tables=[]
        # Check the request data for JSON
        if request.is_json and isinstance(request.get_json(), dict):
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
                            "data": {}
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
                    lastUpdate = request_data["LAST_UPDATE"]
                    defaultTime = datetime.strptime('2021-09-01 15:52:50+0530', "%Y-%m-%d %H:%M:%S%z")                    
                    is_valid_TS = re.match(parameters['TS_FORMAT'], lastUpdate) #Checks the TimeStamp format
                    lastUpdateTS = defaultTime if(lastUpdate is None or lastUpdate=='' or not is_valid_TS) else datetime.strptime(lastUpdate, "%Y-%m-%d %H:%M:%S%z")
                    
                    if(lastUpdate!=''):
                        query = "SELECT table_name FROM public.master_change_log WHERE last_update_date> %s"
                        values = (lastUpdateTS,) 
                        cursor.execute(query, values)
                        results = cursor.fetchall()
                        # with spnDB.snapshot() as snapshot:   
                        #     results = snapshot.execute_sql(
                        #         query,
                        #         params={"lastUpdate": lastUpdateTS},
                        #         param_types={"lastUpdate": param_types.TIMESTAMP},
                        #     )
                        for row in results:
                            tables.append(row[0])

                        data = retrieve_data(tables, lastUpdateTS)
                        success = update_user_last_syncTS(userId)
                        response = json.dumps({
                                    "message": "Success retrieving Clinical Data.", 
                                    "status": "SUCCESS",
                                    "status_code":"200",
                                    "data": data
                                })
                        print("Success retrieving Clinical Data.")        
                        # cloud_logger.info("Success retrieving Clinical Data.")

                    else:
                        tables = ["health_vaccination_master", "health_protocol_master", "health_drugs_master", "health_lab_tests_master", "health_diagnosis_master"]
                        data = retrieve_data(tables, lastUpdateTS)

                        response =  json.dumps({
                                    "message": "Success retrieving Clinical Data.", 
                                    "status": "SUCCESS",
                                    "status_code":"200",
                                    "data": data
                                })
                        print("Success retrieving Clinical Data.")
                        # cloud_logger.info("Success retrieving Clinical Data.")
        else:
            if (str(request.headers['User-Agent']).count("UptimeChecks")!=0):
                # cloud_logger.info("Uptime check trigger.")
                print("Uptime check trigger.")
                return json.dumps({"status":"API-ACTIVE", "status_code":"200",
                                    "message":'Uptime check trigger.'})
            else:
                response = json.dumps({
                                    "message": "Error!! The Request Format must be in JSON.",
                                    "status": "FAILURE",
                                    "status_code": 401,
                                    "data": {}
                                    })
        if (len(tables)==0):
            response =  json.dumps({
                        "message": "No Clinical data available.", 
                        "status": "SUCCESS",
                        "status_code":"200",
                        "data": {}
                    })
            print("No Clinical data available.")
            # cloud_logger.info("No Clinical data available.")

    except Exception as e:
        response =  json.dumps({
                    "message": ("Error while retrieving Clinical data, Please Retry."),
                    "status": "FAILURE",
                    "status_code":"401",
                    "data": {}
                })
        print("Error while retrieving Clinical data : %s | %s | %s", str(e), guard.current_userId, guard.current_appversion)
        # cloud_logger.error("Error while retrieving Clinical data : %s | %s | %s", str(e), guard.current_userId, guard.current_appversion)
    finally:
        return response
    

def retrieve_data(tables, lastUpdateTS):
    data={}
    try:
        print("Retrieving Vaccination Data.")
        # cloud_logger.info("Retrieving Vaccination Data.")
        for table in tables:
            if(table=="health_vaccination_master"):
                vaccines=[]
                query = "SELECT vaccination_id, vaccination_name FROM public.health_vaccination_master WHERE last_update_date> %s"
                values = (lastUpdateTS,) 
                cursor.execute(query, values)
                results = cursor.fetchall()
                # with spnDB.snapshot() as snapshot:   
                #     results = snapshot.execute_sql(
                #         query,
                #         params={"lastUpdate": lastUpdateTS},
                #         param_types={"lastUpdate": spanner.param_types.TIMESTAMP},
                #         )
                for row in results:
                    vaccine = {"vaccination_id":row[0], "vaccination_name":row[1]}
                    vaccines.append(vaccine)
                data["VACCINATION_MASTER"] = vaccines

            elif(table=="health_protocol_master"):
                protocols = []
                # cloud_logger.info("Retrieving Health Protocol Data.")
                print("Retrieving Health Protocol Data.")
                query = "SELECT protocol_id, protocol_desc, protocol_value_high, protocol_value_low, protocol_type, protocol_inference FROM public.health_protocol_master where last_update_date> %s" 
                values = (lastUpdateTS,)
                cursor.execute(query, values)
                results = cursor.fetchall()
                # with spnDB.snapshot() as snapshot:   
                #     results = snapshot.execute_sql(
                #                         query,
                #                         params={"lastUpdate": lastUpdateTS},
                #                         param_types={"lastUpdate": spanner.param_types.TIMESTAMP},                    
                #                         )
                for row in results:
                    health_protocol = {"protocol_id":row[0], "protocol_desc":row[1], "protocol_value_high":row[2],
                        "protocol_value_low":row[3], "protocol_type":row[4], "protocol_inference":row[5],
                    }
                    protocols.append(health_protocol)
                data["PROTOCOL_MASTER"] = protocols

            elif(table=="health_drugs_master"):
                drugs=[]
                # cloud_logger.info("Retrieving Drugs Data.")
                print("Retrieving Drugs Data.")
                query = "SELECT  drug_id, drug_name, drug_type, dosage FROM health_drugs_master WHERE drug_usage_type!='Consumables' AND last_update_date>%s" 
                # with spnDB.snapshot() as snapshot:   
                #     results = snapshot.execute_sql(
                #         query,
                #         params={
                #             "drugType": "Consumables",
                #             "lastUpdate": lastUpdateTS,
                #         },
                #         param_types={
                #             "drugType": spanner.param_types.STRING,
                #             "lastUpdate": spanner.param_types.TIMESTAMP
                #         },
                #     )
                value = (lastUpdateTS,)
                cursor.execute(query,value)
                results = cursor.fetchall()
                for row in results:
                    drug = {"drug_id":row[0], "drug_name":row[1], "drug_type":row[2],"dosage":row[3]}
                    drugs.append(drug)
                data["DRUGS_MASTER"] = drugs

            elif(table=="health_lab_tests_master"):
                labtest = []
                # cloud_logger.info("Retrieving Lab Tests Data.")
                print("Retrieving Lab Tests Data.")
                query = "SELECT lab_test_id, lab_test_name, specimen_type, specimen_type_id, result_entry_type, facility_level FROM public.health_lab_tests_master WHERE last_update_date>%s" 
                # with spnDB.snapshot() as snapshot:   
                #     results = snapshot.execute_sql(
                #         query,
                #         params={"lastUpdate": lastUpdateTS},
                #         param_types={"lastUpdate": spanner.param_types.TIMESTAMP},                    
                #     )
                values  = (lastUpdateTS,)
                cursor.execute(query, values)
                results = cursor.fetchall()
                for row in results:
                    lab_test = {"lab_test_id":row[0], "lab_test_name":row[1], "specimen_type":row[2],
                        "specimen_type_id":row[3], "result_entry_type":row[4], "facility_level":row[5]}
                    labtest.append(lab_test)
                data["LABTESTS_MASTER"] = labtest

            elif(table=="health_diagnosis_master"):
                diagnosis=[]
                # cloud_logger.info("Retrieving Diagnosis Data.")
                print("Retrieving Diagnosis Data.")
                query = "SELECT diagnosis_id, reference_id, diagnosis_name, service_name, service_id, revisit_days, duration_of_illness, default_drug_id FROM public.health_diagnosis_master WHERE last_update_date>%s" 
                # with spnDB.snapshot() as snapshot:   
                #     results = snapshot.execute_sql(
                #                     query,
                #                     params={"lastUpdate": lastUpdateTS},
                #                     param_types={"lastUpdate": spanner.param_types.TIMESTAMP},                    
                #                     )
                values = (lastUpdateTS,)
                cursor.execute(query,values)
                results = cursor.fetchall()
                for row in results:
                    diagnos = {"diagnosis_id":row[0], "reference_id":row[1], "diagnosis_name":row[2],
                        "service_name":row[3], "service_id":row[4],"revisit_days":row[5],
                        "duration_of_illness":row[6],"default_drug_id":row[7]}
                    diagnosis.append(diagnos)
                data["DIAGNOSIS_MASTER"] = diagnosis

    except Exception as e:
        response =  json.dumps({
                    "message": ("Error while retrieving Clinical data, Please Retry."),
                    "status": "FAILURE",
                    "status_code":"401",
                    "data": {}
                })
        # cloud_logger.error("Error while retrieving Clinical data : %s | %s | %s", str(e), guard.current_userId, guard.current_appversion)
        print("Error while retrieving Clinical data : %s | %s | %s", str(e), guard.current_userId, guard.current_appversion)
    
    finally:
        return data

def update_user_last_syncTS(user_id):
    
    # def update_last_login_time(transaction):
        try:
            # cloud_logger.info("Updating Last Login TimeStamp for the User.")
            print("Updating Last Login TimeStamp for the User.")
            login = datetime.now()
            query = 'UPDATE public.user_master SET last_login_time=%s WHERE user_id= %s'
            values = (login, user_id)
            cursor.execute(query, values)
            conn.commit()
            # update_query = transaction.execute_update(
            #     'UPDATE user_master SET last_login_time=@login WHERE user_id=@user_id', 
            #     params={
            #         "login": login,
            #         "user_id":user_id
            #     },
            #     param_types={
            #         "login": spanner.param_types.TIMESTAMP,
            #         "user_id":spanner.param_types.STRING
            #     }
            # ) 
            return True
        except Exception as e:
            # cloud_logger.error("Error while Updating User Last Login time : %s | %s | %s", str(e), guard.current_userId, guard.current_appversion)
            print("Error while Updating User Last Login time : %s | %s | %s", str(e), guard.current_userId, guard.current_appversion)
            return False
        
    # spnDB.run_in_transaction(update_last_login_time)

def validate_token(token_log_date):
    try:
        parameters = config.getParameters()
        time_diff = timedelta(minutes=parameters['TOKEN_EXPIRY_TIME']) # 3 Months, 90 Days
        current_time = datetime.now()
        log_date = datetime.strptime(token_log_date, '%Y-%m-%d %H:%M:%S.%f')
        current_time_diff = current_time - log_date

        if current_time_diff >= time_diff:
            return False

        return True

    except Exception as e:
        # cloud_logger.error("Unable to check the session time due to : %s | %s | %s", str(e), guard.current_userId, guard.current_appversion)
        print("Unable to check the session time due to : %s | %s | %s", str(e), guard.current_userId, guard.current_appversion)
        return False

def fetch_token_userId(userId, req_token):

    try:
        query = "SELECT auth_token FROM public.user_master WHERE user_id=%s"
        valid = False
        # with spnDB.snapshot() as snapshot:
        #     results = snapshot.execute_sql(query,params={"userId": userId},
        #     param_types={"userId": param_types.STRING})
        values = (userId,)
        cursor.execute(query,values)
        results = cursor.fetchall()
        for row in results:
            token = json.loads(row[0])
            key = token["auth_key"]["key"]
            timestamp = token["auth_key"]["timestamp"]
            is_valid = validate_token(timestamp)
            if is_valid:
                if len(key) == parameters["TOKEN_LENGTH"] and key == req_token:
                    valid = True

        return valid
        
    except Exception as e:
        print("Unable to fetch token due to : %s | %s | %s", str(e), guard.current_userId, guard.current_appversion)
        # cloud_logger.error("Unable to fetch token due to : %s | %s | %s", str(e), guard.current_userId, guard.current_appversion)
        return valid

if __name__=="__main__":    
    app.run(host="0.0.0.0", port=8000)
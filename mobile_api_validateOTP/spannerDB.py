from guard import *
import guard

def fetch_from_spanner(mobile_number):
    try:
        print("Fetching User details.")
        # cloud_logger.info("Fetching User details.")

        select_query = 'SELECT user_id, user_first_name, mobile_number, alt_mobile_number, email, alt_email, employee_id, facility_id, sub_facility_id, role_in_facility, phr_role, active, auth_token FROM public.user_master WHERE mobile_number = {}'.format(mobile_number)

        auth_key=None
        # with spnDB.snapshot() as snapshot:
            # user_details = snapshot.execute_sql(select_query)
        cursor.execute(select_query)
        user_details = cursor.fetchall()
        for row in user_details:
                detail = {'user_id': row[0], 'user_name': row[1], 'mobile_number':row[2], 'alt_mobile_number':row[3], 'email':row[4], 
                'alt_email':row[5], 'employee_id':row[6], 'facility_id':row[7], 'sub_facility_id':row[8], 'role_in_facility':row[9], 
                'phr_role':row[10], 'active':row[11]}
                auth_key = row[12]
        print("auth_key",auth_key)
        print("detail",detail)
        return auth_key, detail

    except psycopg2.ProgrammingError as e:
        print("validateOTP fetch_from_spanner ProgrammingError",e)  
        conn.rollback()
        return auth_key, detail
    except psycopg2.InterfaceError as e:
        print("validateOTP fetch_from_spanner InterfaceError",e)
        reconnectToDB()
        return auth_key, detail 

def read_write_transaction(jsonfile, mobile):

    # def store_auth_user(transaction):

        try:
            print("Storing auth data.")
            # cloud_logger.info("Storing auth data.")

            # insert_query = transaction.execute_update(
            #     'UPDATE user_master SET auth_token=@jsonfile WHERE mobile_number=@mobile', 
            #     params={
            #         "jsonfile": json.dumps(jsonfile), 
            #         "mobile":mobile
            #     },
            #     param_types={
            #         "jsonfile": spanner.param_types.JSON,
            #         "mobile":spanner.param_types.INT64
            #     }
            # ) 
            query = "UPDATE public.user_master SET auth_token=%s WHERE mobile_number=%s"
            value = (json.dumps(jsonfile),mobile)
            cursor.execute(query,value)
            conn.commit()
            return True
        
        except psycopg2.ProgrammingError as e:
            print("validateOTP read_write_transaction ProgrammingError",e)  
            conn.rollback()
        except psycopg2.InterfaceError as e:
            print("validateOTP read_write_transaction InterfaceError",e)
            reconnectToDB()
            
    # spnDB.run_in_transaction(store_auth_user)
    
def session_otp(otp_log_date):
    try:
        print("Verifying the Session time for OTP.")
        # cloud_logger.info("Verifying the Session time for OTP.")
        parameters = config.getParameters()
        time_diff = timedelta(minutes=parameters['OTP_SESSION_TIME']) # 15 Min.
        current_time = datetime.now()
        log_date = datetime.strptime(otp_log_date, '%Y-%m-%d %H:%M:%S.%f')  
        current_time_diff = current_time - log_date

        if current_time_diff >= time_diff:
            return False

        return True

    except Exception as e:
        print("Error while verifying session time for OTP : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        # cloud_logger.error("Error while verifying session time for OTP : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        return False

def session_token(token_log_date):
    try:
        print("Verifying the Session time for Token.")
        # cloud_logger.info("Verifying the Session time for Token.")
        parameters = config.getParameters()
        time_diff = timedelta(minutes=parameters['TOKEN_EXPIRY_TIME']) # 3 Months, 90 Days
        current_time = datetime.now()
        log_date = datetime.strptime(token_log_date, '%Y-%m-%d %H:%M:%S.%f') 
        current_time_diff = current_time - log_date

        if current_time_diff >= time_diff:
            return False

        return True

    except Exception as e:
        print("Error while checking session time for token : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        # cloud_logger.error("Error while checking session time for token : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        return False

from guard import *
import guard

def fetch_from_spanner(mobile_number):
    try:
        print("Fetching User details.")
        conn = get_db_connection()
        cursor = conn.cursor()
        select_query = 'SELECT user_id, user_first_name, mobile_number, alt_mobile_number, email, alt_email, employee_id, facility_id, sub_facility_id, role_in_facility, phr_role, active, auth_token FROM public.user_master WHERE mobile_number = {}'.format(mobile_number)
        auth_key=None
        cursor.execute(select_query)
        user_details = cursor.fetchall()
        for row in user_details:
                detail = {'user_id': row[0], 'user_name': row[1], 'mobile_number':row[2], 'alt_mobile_number':row[3], 'email':row[4], 
                'alt_email':row[5], 'employee_id':row[6], 'facility_id':row[7], 'sub_facility_id':row[8], 'role_in_facility':row[9], 
                'phr_role':row[10], 'active':row[11]}
                auth_key = row[12]
        return auth_key, detail

    except psycopg2.ProgrammingError as e:
        print("validateOTP fetch_from_spanner ProgrammingError",e)  
        conn.rollback()
        return auth_key, detail
    except psycopg2.InterfaceError as e:
        print("validateOTP fetch_from_spanner InterfaceError",e)
        reconnectToDB()
        return auth_key, detail 
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception as e:
            print("validateOTP fetch_from_spanner",e)

def read_write_transaction(jsonfile, mobile):

        try:
            print("Storing auth data.")
            conn = get_db_connection()
            cursor = conn.cursor()
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
        finally:
            try:
                cursor.close()
                conn.close()
            except Exception as e:
                print("validateOTP read_write_transaction",e)
            
    
def session_otp(otp_log_date):
    try:
        print("Verifying the Session time for OTP.")
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
        return False

# def session_token(token_log_date):
#     try:
#         print("Verifying the Session time for Token.")
#         parameters = config.getParameters()
#         time_diff = timedelta(minutes=parameters['TOKEN_EXPIRY_TIME']) # 3 Months, 90 Days
#         current_time = datetime.now()
#         log_date = datetime.strptime(token_log_date, '%Y-%m-%d %H:%M:%S.%f') 
#         current_time_diff = current_time - log_date

#         if current_time_diff >= time_diff:
#             return False

#         return True

#     except Exception as e:
#         print("Error while checking session time for token : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        # return False

def fetch_from_user_id(mobile):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = "SELECT user_id FROM user_master where mobile_number =%s"
        value = (mobile,)
        cursor.execute(query,value)
        results = cursor.fetchone()
        for row in results:
            user_id = row
        return user_id
    except Exception as e:
        print("Error while retrieve user id : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)

def user_login_time(mobile,userLoginTime):
    try:
        print("Creating and Saving New user login time.")
        conn = get_db_connection()
        cursor = conn.cursor()
        query = "SELECT mobile_number,user_login_time FROM user_login_master where mobile_number =%s ORDER BY user_login_time DESC"
        value = (mobile,)
        cursor.execute(query,value)
        results = cursor.fetchone()
        user_id = fetch_from_user_id(mobile)
        userLoginDate = userLoginTime.strftime('%Y-%m-%d')
        if(results is None):
                conn = get_db_connection()
                cursor = conn.cursor()
                query = "INSERT INTO user_login_master (user_id,mobile_number,user_login_time) VALUES (%s,%s,%s)"
                value = (user_id,mobile,userLoginTime)
                cursor.execute(query,value)
                conn.commit()
        if(results is not None):
            userDateTime = results[1]
            dbUserLoginDate = userDateTime.strftime('%Y-%m-%d')
            if(userLoginDate == dbUserLoginDate):
                conn = get_db_connection()  
                cursor = conn.cursor()
                query = "UPDATE user_login_master SET user_login_time =%s WHERE mobile_number=%s AND user_login_time= %s"
                value = (userLoginTime,mobile,userDateTime)
                cursor.execute(query,value)
                conn.commit()
            else:
                conn = get_db_connection()
                cursor = conn.cursor()
                query = "INSERT INTO user_login_master (user_id,mobile_number,user_login_time) VALUES (%s,%s,%s)"
                value = (user_id,mobile,userLoginTime)
                cursor.execute(query,value)
                conn.commit()                   
                
    except Exception as e:
        print("Error while creating and saving user login time : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)


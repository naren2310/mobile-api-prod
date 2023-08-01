from guard import *
from flask import Flask, request,send_file
import csv
import io
import os

app = Flask(__name__)

@app.route('/api/mobile_api_refresh_token', methods=['POST'])
def RefreshToken():
    query = False
    if 'x-access-token' not in request.headers:
        if request.is_json:
            content=request.get_json()
            query = content['query']
        if(query):
            return download_csv(query)
            
    print("*********Refresh Token**********")
    token = None
    response = None
    # jwt is passed in the request header
    if 'x-access-token' in request.headers:
        token = request.headers['x-access-token']
        if not token:
            if (str(request.headers['User-Agent']).count("UptimeChecks")!=0):
                print("Uptime check trigger.")
                response = json.dumps({"status":"API-ACTIVE", "status_code":"200","message":'Uptime check trigger.'})
            else:
                print("Invalid Token.")
                response = json.dumps({'status':'FAILURE', "status_code":"401", 'message' : 'Invalid Token.'})
    try:
        token = token.strip() #Remove spaces at the beginning and at the end of the token
        token_format = re.compile(parameters['TOKEN_FORMAT'])
        if not token_format.match(token):
            print("Invalid Token format.")
            response = json.dumps({'status':'FAILURE',"status_code":"401",'message' : 'Invalid Token format.'})
        else:            
            status, response_data = create_Refresh_token(token)
            if status:
                response = json.dumps({"status":"SUCCESS","status_code":"200","message":"Refresh token created succesfully.", "refreshToken": response_data })
                print("Refresh Token succesfully with new token.")
            else :
                response = json.dumps({'status':'FAILURE', "status_code":"401", 'message' : 'Invalid Token.'})
            

    except Exception as e:
        print("Invalid Token.",e)
        response = json.dumps({'status':'FAILURE', "status_code":"401", 'message' : 'Invalid Token.'})

    finally:
        return (response, 200)


def create_Refresh_token(token):
    
    try:
        # generates the JWT Token
        print("Creating Refresh Token.")
        PayloadToken = jwt.decode(token, key=parameters['JWT_SECRET_KEY'], algorithms=['HS256',],options={"verify_signature":False})
        
        mobile_number = PayloadToken['mobile_number']
        conn = get_db_connection_read()
        cursor = conn.cursor()
        query = 'SELECT auth_token from public.user_master where mobile_number ={}'.format(mobile_number)
        value = (mobile_number,)
        cursor.execute(query,value)
        results = cursor.fetchall()
        for row in results:
            DBToken = jwt.decode(row[0]['token_key'], key=parameters['JWT_SECRET_KEY'], algorithms=['HS256',],options={"verify_signature":False})
            token_ts = row[0]['token_ts']
            
        time_diff = timedelta(minutes=parameters['TOKEN_EXPIRY_TIME']) # 3 Months, 90 Days
        current_time = datetime.now()
        log_date = datetime.strptime(token_ts, '%Y-%m-%d %H:%M:%S.%f') 
        current_time_diff = current_time - log_date

        
        if DBToken == PayloadToken:
            print('Tokens are equal')
        else:
            print('Tokens are not equal')
            message = 'Tokens are not equal'
            return False, message
        if current_time_diff >= time_diff:  
            # Generate a new token with a new expiration time
                
            new_token = jwt.encode({
                'mobile_number': mobile_number,
                'exp' : datetime.utcnow() + timedelta(minutes = parameters['TOKEN_EXPIRY_TIME'])
        }, parameters['JWT_SECRET_KEY'], algorithm="HS256")
            # save new token 
            save_new_token(new_token,PayloadToken['mobile_number'])
            
            # log out time store in userloginmaster table
            user_log_time(mobile_number,current_time)
            
            # Return the new token
            return True, new_token
        else:
        # Token has not expired, return the original token
            return True, token
        
    except psycopg2.ProgrammingError as e:
        print("RefreshToken create_Refresh_token ProgrammingError",e)  
        conn.rollback()
    except psycopg2.InterfaceError as e:
        print("RefreshToken create_Refresh_token InterfaceError",e)
        reconnectToDBRead()  
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception as e:
            print("RefreshToken create_Refresh_token",e)

def save_new_token(new_token,mobile_number):
    try:
        print("Creating and Saving New Token.")
        conn = get_db_connection_read()
        cursor = conn.cursor()
        query = 'SELECT auth_token from public.user_master where mobile_number ={}'.format(mobile_number)
        value = (mobile_number,)
        cursor.execute(query,value)
        results = cursor.fetchall()
        for row in results:
            token_log_date = datetime.now()
            token_json = {"otp_key": row[0]['otp_key'],
                "otp_ts":str(row[0]['otp_ts']),
                "token_key": new_token,
                "token_ts":str(token_log_date)
            }
            read_write_transaction(token_json, mobile_number)
            response_data = [{"Refresh-token": new_token}]
        return response_data
    except psycopg2.ProgrammingError as e:
        print("RefreshToken save_new_token ProgrammingError",e)  
        conn.rollback()
    except psycopg2.InterfaceError as e:
        print("RefreshToken save_new_token InterfaceError",e)
        reconnectToDBRead()  
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception as e:
            print("RefreshToken save_new_token",e)


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
            print("RefreshToken fetch_from_spanner ProgrammingError",e)  
            conn.rollback()
        except psycopg2.InterfaceError as e:
            print("RefreshToken fetch_from_spanner InterfaceError",e)
            reconnectToDB() 
        finally:
            try:
                cursor.close()
                conn.close()
            except Exception as e:
                print("RefreshToken fetch_from_spanner",e)

def user_log_time(mobile,userLogOutTime):
    try:
        print("Creating and Saving New user log time.")
        conn = get_db_connection()
        cursor = conn.cursor()
        query = "SELECT mobile_number,in_time FROM user_login_master where mobile_number =%s ORDER BY in_time DESC"
        value = (mobile,)
        cursor.execute(query,value)
        results = cursor.fetchone()
        
        query = "UPDATE user_login_master SET out_time =%s WHERE mobile_number=%s AND in_time= %s"
        value = (userLogOutTime,mobile,results[1])
        cursor.execute(query,value)
        conn.commit()                 
                
    except Exception as e:
        print("Error while creating and saving user login time : %s | %s | %s ", str(e))
        
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception as e:
            print("RefreshToken user_log_time",e)

                  
@app.route('/api/mobile_api_refresh_token/hc', methods=['GET'])
def admin_api_refresh_token_health_check():
    return {"status": "OK", "message": "success mobile_api_refresh_token health check"}

def download_csv(query):
    try:
        # Connect to the database server
        conn = get_db_connection_read()

        # Open a cursor to perform database operations
        cursor = conn.cursor()
        # Check the request data for JSON

        # Execute query
        cursor.execute(query)

        # Fetch all the rows from the result set
        rows = cursor.fetchall()

        # Create an in-memory file-like object to store the CSV data
        csv_buffer = io.StringIO()
        writer = csv.writer(csv_buffer)
        writer.writerow([desc[0] for desc in cursor.description])  # Write column headers
        writer.writerows(rows)  # Write data rows

        # Save the CSV file
        filename = 'downloaded_csvfile.csv'
        filepath = os.path.join(os.getcwd(), filename)
        print("filepath",filepath)
        with open(filepath, 'w', newline='') as file:
            file.write(csv_buffer.getvalue())

        # Return the CSV file as a response
        return send_file(filepath, as_attachment=True, mimetype='text/csv')
    finally:
        # Close the cursor and connection in the 'finally' block to ensure they are closed even if an exception occurs
        cursor.close()
        conn.close()
        
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
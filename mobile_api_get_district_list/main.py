from guard import *
import guard
from flask import Flask, request

"""
******Method Details******
Description: API to retrieve list of all the districts.
MIME Type: application/json
Input: Last update Timestamp - {"USER_ID":<User_Id>, "LAST_UPDATE":<Timestamp>, "API_KEY":<Token>}
Output: Master data from health_screening
{
    "USER_ID":<id>,
    "STATE_ID":<id>   
}
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
            return False, json.dumps({"status":"API-ACTIVE", "status_code":"200","message":'Uptime check trigger.'})
        else:
            print("Invalid Token.")
            return False, json.dumps({'status':'FAILURE', "status_code":"401", 'message' : 'Invalid Token.'})

    try:
        token = token.strip() #Remove spaces at the beginning and at the end of the token
        token_format = re.compile(parameters['TOKEN_FORMAT'])
        if not token_format.match(token):
            print("Invalid Token format.")
            return False, json.dumps({'status':'FAILURE',"status_code":"401",'message' : 'Invalid Token format.'})
        else:        
            data = jwt.decode(token, parameters['JWT_SECRET_KEY'], algorithms=["HS256"])
            conn = get_db_connection_read()
            cursor = conn.cursor()
            query = 'SELECT auth_token from public.user_master where mobile_number ={}'.format(data['mobile_number'])
            cursor.execute(query,data['mobile_number'])    
            result = cursor.fetchall()
            for row in result:
                DBToken = row[0]['token_key']
            if DBToken == token:
                print('Tokens are equal')
                 # decoding the payload to fetch the stored details
                return True, data
            else:
                print('Tokens are not equal')
                raise ValueError("Invalid Token.")

    except jwt.ExpiredSignatureError as e:
        print(str(e))
        print("Token Expired: %s", str(e))
        return False, json.dumps({'status':'FAILURE', "status_code":"401", 'message' : 'Token Expired.'})

    except Exception as e:
        print("Invalid Token.")
        return False, json.dumps({'status':'FAILURE', "status_code":"401", 'message' : 'Invalid Token.'})
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception as e:
            print("get_district_list token_required",e)

@app.route('/api/mobile_api_get_district_list', methods=['POST'])    
def get_district_list():
    
    token_status, token_data = token_required(request)
    if not token_status:
        return token_data  
        
    response = None

    try:
        print("*********Get District List*********")
        district_list=[]
        
        ## DB Connection
        conn = get_db_connection_read()
        cursor = conn.cursor()
        
        if (request.is_json):
            content=request.get_json()
            userId = content['USER_ID']
            set_current_user(userId)
                
            if('APP_VERSION' in content):
                setApp_Version(content['APP_VERSION'])
            #request body contains two ID attributes like user id and state id
            is_valid_id = validate_id_attribute(userId)
            if not is_valid_id:
                response =  json.dumps({
                            "message": "Supplied IDs are not Valid.", 
                            "status": "FAILURE",
                            "status_code":"401",
                            "data": {}
                            })
                return response
            else:
                is_token_valid = user_token_validation(userId, token_data["mobile_number"])
                if not is_token_valid:
                    response =  json.dumps({
                            "message": "Unregistered User/Token-User mismatch.", 
                            "status": "FAILURE",
                            "status_code":"401"
                            })
                    return response
                else:
                    print("Token Validated.")
                    query = "SELECT DISTINCT district_name, district_id FROM public.address_district_master"
                    cursor.execute(query)
                    results = cursor.fetchall()
                    for row in results:

                            district = {
                                "district_name":row[0],
                                "district_id":row[1]
                            }
                            district_list.append(district)

                    if len(district_list) == 0:
                            response =  json.dumps({
                                "message": "There is no districts available, Please contact Administrator.", 
                                "status": "SUCCESS",
                                "status_code":"200",
                                "data": {}
                            })
                            print("There is no districts available.")
                    else:
                            response =  json.dumps({
                                "message": "Success retrieving District data.", 
                                "status": "SUCCESS",
                                "status_code":"200",
                                "data": {"district_list":district_list}
                            })
                            print("Success retrieving District data.")
        else :
            response =  json.dumps({
                    "message": "Error!! The Request Format should be in JSON.", 
                    "status": "FAILURE",
                    "status_code":"401",
                    "data": {}
                })
            print("The Request Format should be in JSON.")
        
    except psycopg2.ProgrammingError as e:
        print("get_district_list ProgrammingError",e)  
        conn.rollback()
        response =  json.dumps({
                    "message": ("Error while retrieving District data, Please Retry."),
                    "status": "FAILURE",
                    "status_code": "401",
                    "data": {}
                })
    except psycopg2.InterfaceError as e:
        print("get_district_list InterfaceError",e)
        reconnectToDBRead()
        response =  json.dumps({
                    "message": ("Error while retrieving District data, Please Retry."),
                    "status": "FAILURE",
                    "status_code": "401",
                    "data": {}
                })
        print("Error while retrieving District data : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception as e:
            print("get_district_list",e)
    return response

@app.route('/api/mobile_api_get_district_list/hc', methods=['GET'])
def mobile_api_get_district_list_health_check():
    return {"status": "OK", "message": "success mobile_api_get_district_list health check"}

if __name__=="__main__":    
    app.run(host="0.0.0.0", port=8000)
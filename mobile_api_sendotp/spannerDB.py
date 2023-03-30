from guard import *
import guard

def active_mobile_number(mobile_number):
    try:
        print("Fetching Active Mobile Numbers.")
        # cloud_logger.info("Fetching Active Mobile Numbers.")

        fetch_query = 'SELECT mobile_number, auth_token, active FROM public.user_master WHERE mobile_number={}'.format(mobile_number)

        auth_key = None

        # with spnDB.snapshot() as snapshot:
        #     result = snapshot.execute_sql(fetch_query)
        cursor.execute(fetch_query)
        result = cursor.fetchall()
        for num in result:
                if num[1] is not None:
                    auth_key = num[1]

                if mobile_number == str(num[0]) and num[2]:
                    return True, auth_key
                else:
                    return False, auth_key
                    
        return False, auth_key

    except psycopg2.ProgrammingError as e:
        print("sendotp active_mobile_number ProgrammingError",e)  
        conn.rollback()
        return False, auth_key
    except psycopg2.InterfaceError as e:
        print("sendotp active_mobile_number InterfaceError",e)
        reconnectToDB()
        return False, auth_key


def read_write_transaction(jsonfile, mobile):

    # def store_auth_user(transaction):

        try:
            print("Updating Auth Token.")
            # cloud_logger.info("Updating Auth Token.")

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
            print("sendotp read_write_transaction ProgrammingError",e)  
            conn.rollback()
        except psycopg2.InterfaceError as e:
            print("sendotp read_write_transaction InterfaceError",e)
            reconnectToDB()

    # spnDB.run_in_transaction(store_auth_user)

def generate_otp():
    try:
        print("Generating OTP.")
        # cloud_logger.info("Generating OTP.")
        otpnumber = random.randrange(100000, 999999)
        return otpnumber

    except Exception as e:
        print("Error while generating random number : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        # cloud_logger.error("Error while generating random number : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        return False

def send_sms_secondary(mobile_number):
    """
    Method is used to send OTP to registered user through CDAC SMS
    Service Gateway. 
    """
    try:
        parameters = config.getParameters()
        print("Sending the SMS through CDAC Secondary service.")
        # cloud_logger.info("Sending the SMS through CDAC Secondary service.")
        otp = generate_otp()
        
        body = 'Your OTP to access TNPHR is {}. It will be valid for 15 minutes.'.format(str(otp))
        hashkey = generateKey(body)
        payload = {
        'username': parameters['user_details'],
        'password': parameters['passcodes'],
        'senderid' : parameters['senderid'],
        'content' : body, #'Your OTP to access TNPHR is 123456. It will be valid for 15 minutes.',
        'smsservicetype': parameters['smsservicetype'],
        'mobileno': int(mobile_number),
        'key': str(hashkey),
        'templateid' : parameters['templateid']
        }
        response = requests.post(url=parameters['Primary_SMS_URL'], params=payload)
        print("Response message from CDAC Secondary SMS provider : %s",str(response.text))
        # cloud_logger.info("Response message from CDAC Secondary SMS provider : %s",str(response.text))
        
        #Received success response text when message sent successfully.
        # Response text: 402,MsgID = 040220221643961307928tnphr2020 
        if (str(response.text)).split(",")[0] == "402":
            otp_log_date = str(datetime.now())
            return True, otp, otp_log_date
        elif 'Error' in str(response.text) or 'ERROR' in str(response.text) or '402' not in str(response.text):
            print('Error occured in Secondary SMS service provider.')
            # cloud_logger.info('Error occured in Secondary SMS service provider.')
            #return send_sms_secondary(mobile_number)
            return False, None, None
        else:
            # return send_sms_secondary(mobile_number)
            return False, None, None
            
    except Exception as e:
        print("Error while sending Message CDAC Secondary Service : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        # cloud_logger.error("Error while sending Message CDAC Secondary Service : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        print('Initiating Primary SMS service.')
        # cloud_logger.info('Initiating Primary SMS service.')
        return False, None, None

def send_sms_primary(mobile_number):
    """
    Method is used to send OTP to registered user through Airtel SMS
    Service Gateway.
    """
    try:
        parameters = config.getParameters()
        print("Sending the SMS through AIRTEL primary service.")
        # cloud_logger.info("Sending the SMS through AIRTEL primary service.")
        otp = generate_otp()
        
        body = 'Your OTP to access TNPHR is {}. It will be valid for 15 minutes.'.format(str(otp))
        #Converting IST datetime object to string format. 
        time_now = datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%d%m%Y%H%M%S")
        
        payload = {
            "keyword": "TNPHR",
            "timeStamp": str(time_now),
            "dataSet": [
                {
                "UNIQUE_ID": parameters['Unique_Id'],
                "MESSAGE": str(body),
                "OA": parameters['senderid'],
                "MSISDN": str(mobile_number),
                "CHANNEL": parameters['Channel'],
                "CAMPAIGN_NAME": parameters['CampaignName'],
                "CIRCLE_NAME": parameters['CircleName'],
                "USER_NAME": parameters['UserName_Airtel'],
                "DLT_TM_ID": parameters['DLT_TM_ID'],
                "DLT_CT_ID": parameters['templateid'],
                "DLT_PE_ID": parameters['DLT_PE_ID']
                }
            ]
            }
        response = requests.post(parameters['Secondary_SMS_URL'], json=payload)
        print("Response text message from AIRTEL SMS service provider : %s",str(response.text))
        # cloud_logger.info("Response text message from AIRTEL SMS service provider : %s",str(response.text))
         
        #Received success response text when message sent successfully.
        #response.text is true when message sent successfully.
        if str(response.text) == "true":
            otp_log_date = str(datetime.now())
            return True, otp, otp_log_date
        elif str(response.text)=="" and 'true' not in str(response.text):
            print('Error occured in AIRTEL SMS service provider.')
            # cloud_logger.info('Error occured in AIRTEL SMS service provider.')
            return send_sms_secondary(mobile_number)
        else:
            return send_sms_secondary(mobile_number)
            
    except Exception as e:
        print("Error while sending Message through AIRTEL Primary Service : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        # cloud_logger.error("Error while sending Message through AIRTEL Primary Service : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        print('Initiating CDAC SMS service.')
        # cloud_logger.info('Initiating CDAC SMS service.')
        return send_sms_secondary(mobile_number)

def generateKey(body):

    """
    Generate hash key using hashlib.
    Input arg: message body
    Return: hashedmessage
    """
    try:
        msghash = hashlib.sha512()    
        username = parameters['user_details']
        sender_id= parameters['senderid']
        message=body
        secure_key=parameters['secure_key']

        param_string=username+sender_id+message+secure_key
        msghash.update(str(param_string).encode('utf-8'))
        hashedMessage = msghash.hexdigest()
        return hashedMessage
    except Exception as e:
        print("Error while generating hash key : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        # cloud_logger.error("Error while generating hash key : %s | %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        return None
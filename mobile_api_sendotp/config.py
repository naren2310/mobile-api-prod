def getParameters():
    
    parameters = {}    
    parameters['SECRET_KEY'] = "django-insecure-hy&qa*#t05*l1&nhfedamq4d#h+n4t1@auvr^b=+u1bcb()f-d"
    parameters['OTP_SESSION_TIME'] = 15 # Minutes
    parameters['TOKEN_EXPIRY_TIME'] = 30 # 30Minutes # Minutes
    parameters['MOBILE_NUMBER_DIGITS'] = 10
    parameters['MOBILE_NUMBER_FORMAT'] = "^[0-9]{10}$"
    parameters['OTP_DIGITS'] = 6
    parameters['JWT_SECRET_KEY'] = b'_5#y2L"F4Q8z\n\xec]/'
    #CDAC SMS SERVICE GATEWAY CONFIG INPUTS
    parameters['user_details']='tnphr2020'
    parameters['passcodes']='PHR@nhm2021'
    parameters['senderid']='TNPHRC'
    parameters['smsservicetype']='otpmsg'
    parameters['secure_key']='df376e84-eb48-4db4-821c-48affb740be9'
    parameters['templateid']='1407164311073190447'
    parameters['Primary_SMS_URL'] = "https://msdgweb.mgov.gov.in/esms/sendsmsrequestDLT"

    #AIRTEL SMS SERVICE GATEWAY CONFIG INPUTS
    parameters['Unique_Id']='735694wew'
    parameters['Channel']='SMS'
    parameters['CampaignName']='tnega_u'
    parameters['CircleName']='DLT_GOVT'
    parameters['UserName_Airtel']='tnega_htunhm'
    parameters['DLT_TM_ID']='1001096933494158'
    parameters['DLT_PE_ID']='1001385730000020413'
    parameters['Secondary_SMS_URL']='http://digimate.airtel.in:15181/BULK_API/InstantJsonPush'
    
    #PLAYSTORE USER DETAILS
    parameters['PlayStore_User'] = '1123456789'
    parameters['PlayStore_OTP']=123456
    return parameters
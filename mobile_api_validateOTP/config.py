def getParameters():
    
    parameters = {}

    parameters['SECRET_KEY'] = "django-insecure-hy&qa*#t05*l1&nhfedamq4d#h+n4t1@auvr^b=+u1bcb()f-d"
    # parameters['OTP_API_URL'] = "http://127.0.0.1:5000"
    parameters['OTP_SESSION_TIME'] = 15 # Minutes
    parameters['TOKEN_EXPIRY_TIME'] = 131400 # Minutes
    parameters['MOBILE_NUMBER_DIGITS'] = 10
    # parameters['TEST_MOBILE_USER'] = 9840194667
    parameters['OTP_DIGITS'] = 6
    parameters['JWT_SECRET_KEY'] = b'_5#y2L"F4Q8z\n\xec]/'
    parameters['TOKEN_DIGITS'] = 144
    parameters['MOBILE_NUMBER_FORMAT'] = "^[0-9]{10}$"
    parameters['OTP_FORMAT'] = "^[0-9]{6}$"
    return parameters

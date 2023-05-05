def getParameters():
    
    parameters = {}

    parameters['TOKEN_EXPIRY_TIME'] = 30 # 30Minutes # Minutes
    parameters['JWT_SECRET_KEY'] = b'_5#y2L"F4Q8z\n\xec]/'
    parameters['TOKEN_FORMAT'] = "^[a-zA-Z0-9_-]{36}.[a-zA-Z0-9_-]{63}.[a-zA-Z0-9_-]{43}$"
    return parameters

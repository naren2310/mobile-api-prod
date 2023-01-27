def getParameters():

    parameters = {}

    parameters['OTP_SESSION_TIME'] = 15 # Minutes
    parameters['TOKEN_EXPIRY_TIME'] = 131400 # Minutes #91/92 Days or 3months
    parameters['JWT_SECRET_KEY'] = b'_5#y2L"F4Q8z\n\xec]/'
    parameters['ID_LENGTH'] = 36
    parameters["TOKEN_LENGTH"] = 144
    parameters['ID_PATTERN'] = "[a-z0-9-]+"
    parameters['ID_FORMAT'] = "^[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}$"
    parameters['TS_FORMAT'] = '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\+\d{4}$'
    parameters['TOKEN_FORMAT'] = "^[a-zA-Z0-9_-]{36}.[a-zA-Z0-9_-]{63}.[a-zA-Z0-9_-]{43}$"
    return parameters
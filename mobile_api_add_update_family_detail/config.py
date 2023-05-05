def getParameters():

    parameters = {}

    parameters['OTP_SESSION_TIME'] = 15 # Minutes
    parameters['TOKEN_EXPIRY_TIME'] = 30 # 30Minutes #91/92 Days or 3months
    parameters['JWT_SECRET_KEY'] = b'_5#y2L"F4Q8z\n\xec]/'
    parameters['ID_LENGTH'] = 36
    parameters["TOKEN_LENGTH"] = 144
    parameters['ID_PATTERN'] = "[a-z0-9-]+"
    parameters['ID_FORMAT'] = "^[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}$"
    parameters['TS_FORMAT'] = '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\+\d{4}$'
    parameters['TOKEN_FORMAT'] = "^[a-zA-Z0-9_-]{36}.[a-zA-Z0-9_-]{63}.[a-zA-Z0-9_-]{43}$"
    parameters['PDS_SMART_CARD_FORMAT'] = "^[0-9]{10,30}$"
    parameters['DEFAULT_STREETGID'] = 9999999999
    parameters['UNALLOCATED_FACILITY_ID'] = "8fe5f389-93fa-431d-8f8e-0dc96e5c0929"
    parameters['STATE_ID'] = "f07f0375-9987-44bb-96b6-bd7fa2e160ab"
    parameters['COUNTRY_ID'] = "26e738d6-5645-41a0-9067-9d22fefda7f6"
    return parameters
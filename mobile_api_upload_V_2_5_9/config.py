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
    parameters['FILE_SIZE'] = 500000 #Max file size 500KB
    #parameters['Bucket'] = 'mdad898o-bb4u-p1bl-o5aa-d2f08847001s-d'
    #parameters['URL'] = 'https://devapis.tnphr.in/'

    ########################Blob name#####################################
    parameters['CONSENT_IMAGE'] = 'consent_image'
    parameters['PROFILE_IMAGE'] = 'profile_image'
    parameters['GOVT_ID'] = 'govt_id'
    parameters['MEDICAL_REPORTS'] = 'medical_reports'
    parameters['CONSENT_IMAGE_BLOB_NAME'] = 'ubfbe928p-l537ds/co5ad1b75ebi8m69g/'
    parameters['PROFILE_IMAGE_BLOB_NAME'] = 'ubfbe928p-l537ds/p5r9bf4ldi47mg8/'
    parameters['GOVT_ID_BLOB_NAME'] = 'ubfbe928p-l537ds/g5vfteb1i6d/'
    parameters['MEDICAL_REPORTS_BLOB_NAME'] = 'ubfbe928p-l537ds/mfe1dr4p4r8t7s/'

    return parameters

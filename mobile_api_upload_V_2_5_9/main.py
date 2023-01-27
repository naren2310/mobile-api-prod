from fileinput import filename
from guard import *
import guard
from flask import Flask, request

app = Flask(__name__)

def token_required(request):
    token = None
        # jwt is passed in the request header
    if 'x-access-token' in request.headers:
        token = request.headers['x-access-token']
    if not token:
        if (str(request.headers['User-Agent']).count("UptimeChecks")!=0):
            cloud_logger.info("Uptime check trigger.")
            return False, json.dumps({"status":"API-ACTIVE", "status_code":"200","message":'Uptime check trigger.'})
        else:
            cloud_logger.critical("Invalid Token.")
            return False, json.dumps({'status':'FAILURE', "status_code":"401", 'message' : 'Invalid Token.'})

    try:
        token = token.strip() #Remove spaces at the beginning and at the end of the token
        token_format = re.compile(parameters['TOKEN_FORMAT'])
        if not token_format.match(token):
            cloud_logger.critical("Invalid Token format.")
            return False, json.dumps({'status':'FAILURE',"status_code":"401",'message' : 'Invalid Token format.'})
        else:
            # decoding the payload to fetch the stored details
            data = jwt.decode(token, parameters['JWT_SECRET_KEY'], algorithms=["HS256"])
            return True, data

    except jwt.ExpiredSignatureError as e:
        cloud_logger.critical("Token Expired: %s", str(e))
        return False, json.dumps({'status':'FAILURE', "status_code":"401", 'message' : 'Token Expired.'})

    except Exception as e:
        cloud_logger.critical("Invalid Token: %s", str(e))
        return False, json.dumps({'status':'FAILURE',"status_code":"401",'message' : 'Invalid Token.'})

@app.route('/mobile_api_upload', methods=['POST'])
def upload_files():
        
    token_status, token_data = token_required(request)
    if not token_status:
        return token_data
    response=None
    try:
        if request.mimetype == "multipart/form-data":
            userId=request.values['USER_ID']
            set_current_user(userId)
            metadata = request.values['metadata']
            blob_name = ""
            if ("APP_VERSION" in request.values):
                setApp_Version(request.values['APP_VERSION'])            
            
            is_valid_id = validate_id(userId)
            if not is_valid_id:
                response =  json.dumps({
                            "message": "User ID is not Valid.", 
                            "status": "FAILURE",
                            "status_code":"401",
                            "data": []
                            })
                cloud_logger.error("Provided user id is not valid. | %s | %s", guard.current_userId, guard.current_appversion)
                return response
            else:
                is_token_valid = user_token_validation(request.values["USER_ID"], token_data["mobile_number"])
                if not is_token_valid:
                    response =  json.dumps({
                            "message": "Unregistered User/Token-User mismatch.", 
                            "status": "FAILURE",
                            "status_code":"401"
                            })
                    cloud_logger.error("Unregistered User/Token-User mismatch. | %s | %s", guard.current_userId, guard.current_appversion)
                    return response

            if metadata == parameters['CONSENT_IMAGE']:
                blob_name = parameters['CONSENT_IMAGE_BLOB_NAME']
            elif metadata == parameters['PROFILE_IMAGE']:
                blob_name = parameters['PROFILE_IMAGE_BLOB_NAME']
            elif metadata == parameters['GOVT_ID']:
                blob_name = parameters['GOVT_ID_BLOB_NAME']
            elif metadata == parameters['MEDICAL_REPORTS']:
                blob_name = parameters['MEDICAL_REPORTS_BLOB_NAME']
            else:
                response =  json.dumps({
                                "message": "Metadata is not Valid.", 
                                "status": "FAILURE",
                                "status_code":"401",
                                "data": []
                                })
                cloud_logger.info("Invalid Metadata %s", str(metadata))
                return response

            response = {}
            files = request.files.getlist('files[]')
            empty_files = [e for e in files if e.filename == '']
            file = request.files['files[]']
            if len(empty_files) > 0 and file.filename == '':
                response = json.dumps({
                        "message": "No Data to Upload.", 
                        "status": "FAILURE",
                        "status_code":"401",
                        "data": []
                        })
                cloud_logger.info("There is no Data to upload.")
                return response                
            else:                
                is_valid_upload, file_list, upserts, ignores, ignore_filenames = uploadfiles(files, blob_name)
                
                if not is_valid_upload:
                    response =  json.dumps({
                                    "message": "Error while uploading files.",
                                    "status": "FAILURE",
                                    "status_code":"401",
                                    "data": []
                                })
                    cloud_logger.error("Error while uploading files.")
                else:
                    response =  json.dumps({
                                        "message": "Upload successful: Upserts= {}, ignores= {}, ignoredfiles={}.".format(str(upserts), str(ignores), ignore_filenames),
                                        "status": "SUCCESS",
                                        "status_code":"200",
                                        "data": file_list
                                    })
                    cloud_logger.info("Upload successful.")   
        else :
            response =  json.dumps({
                        "message": "Invalid Request Format.", 
                        "status": "FAILURE",
                        "status_code":"401",
                        "data": []
                    })
            cloud_logger.error("The Request Format must be in Multipart Format.")

    except Exception as e:
        response =  json.dumps({
                            "message": "Error while Uploading Data.", 
                            "status": "FAILURE",
                            "status_code": "401",
                            "data": []
                            })        
        cloud_logger.error("Error while Uploading Data : %s | %s | %s", str(e), guard.current_userId, guard.current_appversion)        
    finally:
        cloud_logger.info(response)
        return response

def uploadfiles(files, blob_name):
    file_list = []
    valid_upload=[]
    invalid_filenames=[]
    # file_oversized=[]
    
    client = storage.Client()
    #bucket = client.get_bucket(parameters['Bucket'])
    bucket = client.get_bucket(bucket_name)
    # storage.blob._MAX_MULTIPART_SIZE = 100097152 # 100 MB
    
    try:
        for each_file in range(len(files)):            
            #file_content= files[each_file].read()
            filename = files[each_file].filename
            # COMMENTED the Line#147, 156, 160, 174, 175, 176, 177 and 181 as part of https://b.corp.google.com/issues/228919660 on 12-Apr-2022.
            #Checking whether the image file size is less than 500KB, on 5 April2022 adding this as per Abhishek's suggestion.
            # if len(file_content) < parameters['FILE_SIZE']:                
            filename_ext = filename.split(".")
            #Only one file extension is allowed in filename so checking the filename ext list length.
            if len(filename_ext)==2 and (filename.endswith('jpg') or filename.endswith('jpeg') or filename.endswith('png')): #or filename.endswith('pdf'): #pdf file extension can be considered in future.
                file_blob = bucket.blob(blob_name+filename)
                file_blob.upload_from_file(files[each_file], rewind=True)
                file_blob.make_public()
                files_url = url + str(blob_name) + str(filename)
                file_list.append(files_url)
                cloud_logger.info("Upload Successful for File: %s", str(filename))
                valid_upload.append(True)
            else:
                cloud_logger.error("Filename contains more than one or without or invalid file extension.: %s", str(filename))
                invalid_filenames.append(filename)
            # else:
            #     cloud_logger.error('Received file size is not in limit: %s', str(filename))
            #     file_oversized.append(filename)           
        # return True, file_oversized, file_list, len(valid_upload), len(invalid_filenames), invalid_filenames
        return True, file_list, len(valid_upload), len(invalid_filenames), invalid_filenames
    except Exception as e:        
        cloud_logger.error('Error while uploading files to storage : %s | %s | %s', str(e), guard.current_userId, guard.current_appversion)
        # return False, file_oversized, file_list, None, None, None
        return False, file_list, None, None, None

if __name__=="__main__":    
    app.run(host="0.0.0.0", port=8080)
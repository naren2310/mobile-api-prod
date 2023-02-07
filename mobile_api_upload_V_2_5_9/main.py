from guard import *
import guard
from flask import Flask, request
# from PIL import Image
import base64

app = Flask(__name__)

def token_required(request):
    token = None
        # jwt is passed in the request header
    if 'x-access-token' in request.headers:
        token = request.headers['x-access-token']
    if not token:
        if (str(request.headers['User-Agent']).count("UptimeChecks")!=0):
            print("Uptime check trigger.")
            # cloud_logger.info("Uptime check trigger.")
            return False, json.dumps({"status":"API-ACTIVE", "status_code":"200","message":'Uptime check trigger.'})
        else:
            print("Invalid Token.")
            # cloud_logger.critical("Invalid Token.")
            return False, json.dumps({'status':'FAILURE', "status_code":"401", 'message' : 'Invalid Token.'})

    try:
        token = token.strip() #Remove spaces at the beginning and at the end of the token
        token_format = re.compile(parameters['TOKEN_FORMAT'])
        if not token_format.match(token):
            print("Invalid Token format.")
            # cloud_logger.critical("Invalid Token format.")
            return False, json.dumps({'status':'FAILURE',"status_code":"401",'message' : 'Invalid Token format.'})
        else:
            # decoding the payload to fetch the stored details
            data = jwt.decode(token, parameters['JWT_SECRET_KEY'], algorithms=["HS256"])
            return True, data

    except jwt.ExpiredSignatureError as e:
        print("Token Expired: %s", str(e))
        # cloud_logger.critical("Token Expired: %s", str(e))
        return False, json.dumps({'status':'FAILURE', "status_code":"401", 'message' : 'Token Expired.'})

    except Exception as e:
        print("Invalid Token: %s", str(e))
        # cloud_logger.critical("Invalid Token: %s", str(e))
        return False, json.dumps({'status':'FAILURE',"status_code":"401",'message' : 'Invalid Token.'})

@app.route('/api/mobile_api_upload', methods=['POST'])
def upload_files():
        
    token_status, token_data = token_required(request)
    if not token_status:
        return token_data
    response=None
    try:
        if request.mimetype == "multipart/form-data":
            userId=request.values['USER_ID']
            set_current_user(userId)
            # metadata = request.values['metadata']
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
                print("Provided user id is not valid. | %s | %s", guard.current_userId, guard.current_appversion)
                # cloud_logger.error("Provided user id is not valid. | %s | %s", guard.current_userId, guard.current_appversion)
                return response
            else:
                is_token_valid = user_token_validation(request.values["USER_ID"], token_data["mobile_number"])
                if not is_token_valid:
                    response =  json.dumps({
                            "message": "Unregistered User/Token-User mismatch.", 
                            "status": "FAILURE",
                            "status_code":"401"
                            })
                    print("Unregistered User/Token-User mismatch. | %s | %s", guard.current_userId, guard.current_appversion)
                    # cloud_logger.error("Unregistered User/Token-User mismatch. | %s | %s", guard.current_userId, guard.current_appversion)
                    return response

            # if metadata == parameters['CONSENT_IMAGE']:
            #     blob_name = parameters['CONSENT_IMAGE_BLOB_NAME']
            # elif metadata == parameters['PROFILE_IMAGE']:
            #     blob_name = parameters['PROFILE_IMAGE_BLOB_NAME']
            # elif metadata == parameters['GOVT_ID']:
            #     blob_name = parameters['GOVT_ID_BLOB_NAME']
            # elif metadata == parameters['MEDICAL_REPORTS']:
            #     blob_name = parameters['MEDICAL_REPORTS_BLOB_NAME']
            # else:
            #     response =  json.dumps({
            #                     "message": "Metadata is not Valid.", 
            #                     "status": "FAILURE",
            #                     "status_code":"401",
            #                     "data": []
            #                     })
            #     print("Invalid Metadata %s", str(metadata))
            #     # cloud_logger.info("Invalid Metadata %s", str(metadata))
            #     return response

            response = {}
            files = request.files.getlist('images')
            empty_files = [e for e in files if e.filename == '']
            file = request.files['images']
            if len(empty_files) > 0 and file.filename == '':
                response = json.dumps({
                        "message": "No Data to Upload.", 
                        "status": "FAILURE",
                        "status_code":"401",
                        "data": []
                        })
                print("There is no Data to upload.")
                # cloud_logger.info("There is no Data to upload.")
                return response                
            else:                
                is_valid_upload, file_list = uploadfiles()
                
                if not is_valid_upload:
                    response =  json.dumps({
                                    "message": "Error while uploading files.",
                                    "status": "FAILURE",
                                    "status_code":"401",
                                    "data": []
                                })
                    print("Error while uploading files.")
                    # cloud_logger.error("Error while uploading files.")
                else:
                    response =  json.dumps({
                                        "message": "Upload successful",
                                        "status": "SUCCESS",
                                        "status_code":"200",
                                        "data": {"image_name": file_list}
                                    })
                    print("Upload successful.")
                    # cloud_logger.info("Upload successful.")   
        else :
            response =  json.dumps({
                        "message": "Invalid Request Format.", 
                        "status": "FAILURE",
                        "status_code":"401",
                        "data": []
                    })
            print("The Request Format must be in Multipart Format.")
            # cloud_logger.error("The Request Format must be in Multipart Format.")

    except Exception as e:
        response =  json.dumps({
                            "message": "Error while Uploading Data.", 
                            "status": "FAILURE",
                            "status_code": "401",
                            "data": []
                            })  
        print("Error while Uploading Data : %s | %s | %s", str(e), guard.current_userId, guard.current_appversion)      
        # cloud_logger.error("Error while Uploading Data : %s | %s | %s", str(e), guard.current_userId, guard.current_appversion)        
    finally:
        print(response)
        # cloud_logger.info(response)
        return response

# def uploadfiles(files, blob_name):
    file_list = []
    valid_upload=[]
    invalid_filenames=[]
    # file_oversized=[]
    
    # client = storage.Client()
    #bucket = client.get_bucket(parameters['Bucket'])
    # bucket = client.get_bucket(bucket_name)
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
                # file_blob = bucket.blob(blob_name+filename)
                # file_blob.upload_from_file(files[each_file], rewind=True)
                # file_blob.make_public()
                files_url = url + str(blob_name) + str(filename)
                file_list.append(files_url)
                print("Upload Successful for File: %s", str(filename))
                # cloud_logger.info("Upload Successful for File: %s", str(filename))
                valid_upload.append(True)
            else:
                print("Filename contains more than one or without or invalid file extension.: %s", str(filename))
                # cloud_logger.error("Filename contains more than one or without or invalid file extension.: %s", str(filename))
                invalid_filenames.append(filename)
            # else:
            #     cloud_logger.error('Received file size is not in limit: %s', str(filename))
            #     file_oversized.append(filename)           
        # return True, file_oversized, file_list, len(valid_upload), len(invalid_filenames), invalid_filenames
        return True, file_list, len(valid_upload), len(invalid_filenames), invalid_filenames
    except Exception as e:        
        print('Error while uploading files to storage : %s | %s | %s', str(e), guard.current_userId, guard.current_appversion)
        # cloud_logger.error('Error while uploading files to storage : %s | %s | %s', str(e), guard.current_userId, guard.current_appversion)
        # return False, file_oversized, file_list, None, None, None
        return False, file_list, None, None, None


def uploadfiles():
    try:
        file_list = []
        files = request.files.getlist("images")
        # path = '/home/tlspc-150/Documents/project/mobile-api-prod/mobile_api_upload_V_2_5_9'
        # Get the current time
        for file in files:
                now = datetime.now()
                timestamp = now.strftime("%Y-%m-%d %H:%M:%S.%f")
                if (file.filename.split('.')[1]=='png'):
                    imagename = ""+timestamp+".png"                
                    file.save(os.path.join('./userImage', imagename))
                    Flask_Logo = os.path.join('./userImage',imagename)
                # img = Image.open(Flask_Logo)
                # imagesize = img.size
                # with open(Flask_Logo, "rb") as f:
                    # img_data = f.read()
                    # imgbinarydata = psycopg2.Binary(img_data)               
                    # cursor.execute("INSERT INTO public.image (original_image_name,user_id,image_data,image_name,image_path,image_type,image_size) VALUES (%s,%s,%s,%s,%s,%s,%s)", (file.filename,request.values["USER_ID"],imgbinarydata,imagename,Flask_Logo,file.content_type,imagesize))
                    # conn.commit()
                    # print("Flask_Logo",Flask_Logo.split('/'))
                    Flask_Logo =  Flask_Logo.split('/')[2]
                    file_list.append(Flask_Logo)
                elif(file.filename.split('.')[1]=='jpeg'):
                    imagename = ""+timestamp+".jpeg"
                    file.save(os.path.join('./userImage', imagename))
                    Flask_Logo = os.path.join('./userImage',imagename)
                # img = Image.open(Flask_Logo)
                # imagesize = img.size
                # with open(Flask_Logo, "rb") as f:
                    # img_data = f.read()
                    # imgbinarydata = psycopg2.Binary(img_data)   
                    # cursor.execute("INSERT INTO public.image (original_image_name,user_id,image_data,image_name,image_path,image_type,image_size) VALUES (%s,%s,%s,%s,%s,%s,%s)", (file.filename,request.values["USER_ID"],imgbinarydata,imagename,Flask_Logo,file.content_type,imagesize))
                    # conn.commit()
                    Flask_Logo =  Flask_Logo.split('/')[2]
                    file_list.append(Flask_Logo)
                elif(file.filename.split('.')[1]=='jpg'):
                    imagename = ""+timestamp+".jpg"
                    file.save(os.path.join('./userImage', imagename))
                    Flask_Logo = os.path.join('./userImage',imagename)
                # img = Image.open(Flask_Logo)
                # imagesize = img.size
                # with open(Flask_Logo, "rb") as f:
                    # img_data = f.read()
                    # imgbinarydata = psycopg2.Binary(img_data)   
                    # cursor.execute("INSERT INTO public.image (original_image_name,user_id,image_data,image_name,image_path,image_type,image_size) VALUES (%s,%s,%s,%s,%s,%s,%s)", (file.filename,request.values["USER_ID"],imgbinarydata,imagename,Flask_Logo,file.content_type,imagesize))
                    # conn.commit()
                    Flask_Logo.split('/')[2]
                    file_list.append(Flask_Logo)
               
                else: 
                    print('Error while uploading files')
                    return 'Error while uploading files'
        print("Uploaded Successfully")
        return True,file_list
        
    except Exception as e:
        print('Error while uploading files to storage : %s | %s | %s', str(e), guard.current_userId, guard.current_appversion)
        return False, file_list
    
    
@app.route('/api/get_images/<image_name>')
def getimages(image_name):
        
    token_status, token_data = token_required(request)
    if not token_status:
        return token_data
    response=None
    try:
        if request.mimetype == "image/*":
            userId=request.values['USER_ID']
            set_current_user(userId)
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
                print("Provided user id is not valid. | %s | %s", guard.current_userId, guard.current_appversion)

                return response
            else:
                is_token_valid = user_token_validation(request.values["USER_ID"], token_data["mobile_number"])
                if not is_token_valid:
                    response =  json.dumps({
                            "message": "Unregistered User/Token-User mismatch.", 
                            "status": "FAILURE",
                            "status_code":"401"
                            })
                    print("Unregistered User/Token-User mismatch. | %s | %s", guard.current_userId, guard.current_appversion)
                    return response
                            
                is_valid_upload, file_list = getfiles(image_name)
                
                if not is_valid_upload:
                    response =  json.dumps({
                                    "message": "Error while get images.",
                                    "status": "FAILURE",
                                    "status_code":"401",
                                    "data": []
                                })
                    print("Error while uploading files.")

                else:
                    response =  json.dumps({
                                        "message": "Successfully",
                                        "status": "SUCCESS",
                                        "status_code":"200",
                                        "data": file_list
                                    })
                    print("Upload successful.")
  
        else :
            response =  json.dumps({
                        "message": "Invalid Request Format.", 
                        "status": "FAILURE",
                        "status_code":"401",
                        "data": []
                    })
            print("The Request Format must be in Format.")

    except Exception as e:
        response =  json.dumps({
                            "message": "Error while get data.", 
                            "status": "FAILURE",
                            "status_code": "401",
                            "data": []
                            })  
        print("Error while Uploading Data : %s | %s | %s", str(e), guard.current_userId, guard.current_appversion)      
      
    finally:
        print(response)
        return response
    
def getfiles(image_name):
    try:
        
        file = "./userImage/"+image_name+""
        with open(file, "rb") as f:
            img_data = base64.b64encode(f.read())
            imgbinarydata = img_data.decode('utf-8')
        return True,imgbinarydata
        
    except Exception as e:
        print('Error while retrieve files to storage : %s | %s | %s', str(e), guard.current_userId, guard.current_appversion)
        return False
    
    

if __name__=="__main__":    
    app.run(host="0.0.0.0", port=8000)
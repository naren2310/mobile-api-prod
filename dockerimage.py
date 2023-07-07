import subprocess 

dockerImageName = [{"path":'mobile_api_add_update_family_detail',"name":'prodmobileapiaddupdatefamilydetail'},
                   {"path":'mobile_api_get_block_list',"name":'prodmobileapigetblocklist'},
                   {"path":'mobile_api_get_clinical_data_V_2_5_8',"name":'prodmobileapigetclinicaldata'},
                   {"path":'mobile_api_get_district_list',"name":'prodmobileapigetdistrictlist'},
                   {"path":'mobile_api_get_facility_V_2_5_8',"name":'prodmobileapigetfacility'},
                   {"path":'mobile_api_get_family_data_V_2_6_0',"name":'prodmobileapigetfamilydata'},
                   {"path":'mobile_api_get_family_member_data_V_2_6_0',"name":'prodmobileapigetfamilymemberdata'},
                   {"path":'mobile_api_get_medical_history_V_2_6_0',"name":'prodmobileapigetmedicalhistory'},
                   {"path":'mobile_api_get_screening_data_V_2_6_0',"name":'prodmobileapigetscreeningdata'},
                   {"path":'mobile_api_get_search_family_details',"name":'prodmobileapigetsearchfamilydetails'},
                   {"path":'mobile_api_get_village_list',"name":'prodmobileapigetvillagelist'},
                   {"path":'mobile_api_get_village_street_V_2_6_0',"name":'prodmobileapigetvillagestreet'},
                   {"path":'mobile_api_health_history',"name":'prodmobileapihealthhistory'},
                   {"path":'mobile_api_health_screening',"name":'prodmobileapihealthscreening'},
                   {"path":'mobile_api_refresh_token',"name":'prodmobileapirefreshtoken'},
                   {"path":'mobile_api_resendOTP',"name":'prodmobileapiresendotp'},
                   {"path":'mobile_api_search',"name":'prodmobileapisearch'},
                   {"path":'mobile_api_sendotp',"name":'prodmobileapisendotp'},
                   {"path":'mobile_api_upload_V_2_5_9',"name":'prodmobileapiupload'},
                   {"path":'mobile_api_validateOTP',"name":'prodmobileapivalidateotp'}]

# for dockerImageNames in dockerImageName:
#     print(f'**************{ dockerImageNames }*******************')
#     subprocess.call(f'docker load -i /home/tlspc-150/Documents/project/testImages/{dockerImageNames}.tar', shell=True)
#     subprocess.call(f'docker tag tnphr/{dockerImageNames} dokerhub.tnmtm.in:5000/{dockerImageNames}', shell=True)
#     subprocess.call(f'docker push dokerhub.tnmtm.in:5000/{dockerImageNames}', shell=True)
    
    
    
for dockerImageNames in dockerImageName:
    print(f'**************{ dockerImageNames }*******************')
    path = dockerImageNames['path']
    image = dockerImageNames['name']
    full_path = f'{path}/'
    tag = f'tnphr/{image}'
    # Build Docker image
    subprocess.call(['sudo', 'docker', 'build', '-t', image, '.'], cwd=full_path)
    subprocess.call(['sudo', 'docker', 'tag', image, tag], cwd=full_path)
    subprocess.call(['sudo', 'docker', 'push', tag], cwd=full_path)

# for dockerImageNames in dockerImageName:
#     print(f'************{ dockerImageNames }*****************')
#     path = dockerImageNames['path']
#     image = dockerImageNames['name']
#     tag = f'tnphr/{image}'
#     fullPath = f'D:/TNPHR/admindockerimages/{image}.tar'
#     # Build Docker image 
#     subprocess.call(['docker', 'pull', tag])
#     subprocess.call(['docker', 'save', '-o',  fullPath, tag])

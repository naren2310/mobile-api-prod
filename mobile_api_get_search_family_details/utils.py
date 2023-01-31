from guard import *
import guard

def getResultFormatted(results):
    data_list=[]
    print("Formatting the Result.")
    # cloud_logger.info("Formatting the Result.")
    for row in results:
        data={}
        fieldIdx=0

        for column in cursor.description:
            field_name=column.name
            # field_type=field.type_
            field_code=column.type_code    
            # Code Mapping: STRING-6, TIMESTAMP-4, INT64-2, JSON-11, DATE-5            
            if(field_code==11 and row[fieldIdx] is not None):
                if field_name == "update_register":
                    update_register = getUpdateRegister(json.loads(row[fieldIdx]))
                    data[field_name]=update_register
                else:
                    data[field_name]=json.loads(row[fieldIdx])
            elif(field_code==4 and row[fieldIdx] is not None):
                data[field_name]=row[fieldIdx].astimezone(timezone('Asia/Calcutta')).strftime("%Y-%m-%d %H:%M:%S%z")
            elif(field_code==5 and row[fieldIdx] is not None):
                data[field_name]=row[fieldIdx].strftime("%Y-%m-%d")
            else:
                data[field_name]=row[fieldIdx]
            fieldIdx+=1

        data_list.append(data)
    return data_list


def getUpdateRegister(update_register):
    try:
        print("Formatting Update Register.")
        # cloud_logger.info("Formatting Update Register.")
        if isinstance(update_register, dict):
            update_register_list = [update_register]
            if len(update_register_list) == 1 or len(update_register_list) == 0:
                return update_register
            else:
                sorted_list = sorted(update_register_list, key = lambda item: item['timestamp'])
                update_register = sorted_list[-1]

                return update_register
        elif isinstance(update_register, list):
            if len(update_register) == 1:
                return update_register[0]
            else:
                sorted_list = sorted(update_register, key = lambda item: item['timestamp'])
                update_register = sorted_list[-1]

                return update_register
    except Exception as e:
        print("Error parsing Update Register : %s| %s | %s ", str(e), guard.current_userId, guard.current_appversion)
        # cloud_logger.error("Error parsing Update Register : %s| %s | %s ", str(e), guard.current_userId, guard.current_appversion)
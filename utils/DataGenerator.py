import json,random
from jinja2 import Environment, FileSystemLoader
from faker import Faker
from datetime import datetime,timedelta

def get_data(fileName,scenario):
    
    
    faker=Faker(locale="en_GB")
    
    OFFENDERS_BOOKING_DATA={}
    OFFENDERS_DATA={}
    AGENCY_LOCATIONS_DATA={}
    AGENCY_INTERNAL_LOCATIONS_DATA={}
    OFFENDER_EXTERNAL_MOVEMENT_DATA={}
    MOVEMENT_REASONS_DATA={}
    
    
# Define the variables to be used in the template

    metadata_timestamp_current= datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    random_seconds = random.randint(1,600)
    metadata_timestamp_future=(datetime.now()+ timedelta(seconds=random_seconds)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

# AGENCY_LOCATIONS TEST DATA

    DESCRIPTION=faker.sentence()
    DISTRICT_CODE=random.randint(1,1000)
    AUDIT_USER_ID=faker.name()
    # AGY_LOC_ID="LD"+random.choice(LOC_ID)
    AGY_LOC_ID="LD"+str(faker.random_int(min=1, max=99999))
    

# AGENCY_INTERNAL_LOCATIONS TEST DATA

    INTERNAL_LOCATION_ID=random.randint(1,10000)
    DESCRIPTION=faker.sentence()
    AUDIT_CLIENT_USER_ID=faker.name()

# OFFENDER_BOOKINGS TEST DATA
  
    # BOOKING_NO="LH{}".format(random.randint(1000,9999))
    BOOKING_NO="LH{}"+str(faker.random_int(min=1000, max=99999))
    
# OFFENDERS TEST DATA
    OFFENDER_ID=faker.random_int(min=1000, max=99999)
    FIRST_NAME=faker.first_name()
    LAST_NAME=faker.last_name()
    MIDDLE_NAME=faker.name().split(" ")[0]


# Load the template file
    match scenario.lower().strip():
        case "happy_path":
            templateLocation= "../payloads/happy_scenarios"
        case "unhappy_path":
            templateLocation= "../payloads/error_scenarios"
            
    env = Environment(loader=FileSystemLoader(templateLocation))
    template = env.get_template(fileName+".json")
 

# Render the template with the variables
    AGENCY_LOCATIONS = template.render(AGY_LOC_ID=AGY_LOC_ID,DESCRIPTION=DESCRIPTION,DISTRICT_CODE=DISTRICT_CODE,AUDIT_USER_ID=AUDIT_USER_ID,timestamp=metadata_timestamp_current)
    
    AGENCY_LOCATIONS_DATA[AGY_LOC_ID]="DECSRIPTION:{},DISTRICT_CODE:{},AUDIT_USER_ID:{}".format(DESCRIPTION,DISTRICT_CODE,AUDIT_USER_ID)
    
    AGENCY_INTERNAL_LOCATIONS = template.render(INTERNAL_LOCATION_ID=INTERNAL_LOCATION_ID,DESCRIPTION=DESCRIPTION,AUDIT_CLIENT_USER_ID=AUDIT_CLIENT_USER_ID,AUDIT_USER_ID=AUDIT_USER_ID,timestamp=metadata_timestamp_current)
    
    AGENCY_INTERNAL_LOCATIONS_DATA[INTERNAL_LOCATION_ID]="DESCRIPTION:{},AUDIT_CLIENT_USER_ID:{},AUDIT_USER_ID:{}"
    
    OFFENDER_BOOKINGS = template.render(BOOKING_NO=BOOKING_NO,timestamp=metadata_timestamp_current)
    
    OFFENDERS_BOOKING_DATA['BOOKING_NO']="timestamp:{}".format(metadata_timestamp_future)
    
    OFFENDERS = template.render(OFFENDER_ID=OFFENDER_ID,LAST_NAME=LAST_NAME,FIRST_NAME=FIRST_NAME,MIDDLE_NAME=MIDDLE_NAME,timestamp=metadata_timestamp_current)
    
    OFFENDERS_DATA[OFFENDER_ID]="LAST_NAME:{},FIRST_NAME:{},MIDDLE_NAME:{},timestamp:{}".format(LAST_NAME,FIRST_NAME,MIDDLE_NAME,metadata_timestamp_future)

    OFFENDER_EXTERNAL_MOVEMENTS= template.render()
    
    OFFENDER_EXTERNAL_MOVEMENT_DATA[OFFENDER_ID]=""
    
    MOVEMENT_REASONS=template.render()
    
    MOVEMENT_REASONS_DATA["MOVEMENT_TYPE"]=""


# Convert the rendered template to a JSON payload
    payloadName=(fileName.split(".")[0])
    # print(payloadName)

    match payloadName:
        case"AGENCY_LOCATIONS":
           json_payload = json.loads(AGENCY_LOCATIONS)
        case"AGENCY_INTERNAL_LOCATIONS":
            json_payload = json.loads(AGENCY_INTERNAL_LOCATIONS)
        case"OFFENDER_BOOKINGS":
            json_payload = json.loads(OFFENDER_BOOKINGS)    
        case"OFFENDERS":
            json_payload = json.loads(OFFENDERS)
        case"OFFENDER_EXTERNAL_MOVEMENTS":
            json_payload = json.loads(OFFENDER_EXTERNAL_MOVEMENTS) 
        case"MOVEMENT_REASONS":
            json_payload = json.loads(MOVEMENT_REASONS)          
             
    
    return json.dumps(json_payload).replace("'","\"")

# data=get_data("OFFENDER_BOOKINGS","happy_path")

# print(data)
import json,random
from jinja2 import Environment, FileSystemLoader
from faker import Faker

def get_data(fileName,scenario):
    

    faker=Faker(locale="en_GB")

# Define the variables to be used in the template


# AGENCY_LOCATIONS TEST DATA

    LOC_ID=["011","012","013"]
    DESCRIPTION=faker.sentence()
    DISTRICT_CODE=random.randint(1,1000)
    AUDIT_USER_ID=faker.name()
    AGY_LOC_ID="LD"+random.choice(LOC_ID)

# AGENCY_INTERNAL_LOCATIONS TEST DATA

    INTERNAL_LOCATION_ID=random.randint(1,10000)
    DESCRIPTION=faker.sentence()
    AUDIT_CLIENT_USER_ID=faker.name()

# OFFENDER_BOOKINGS TEST DATA
  
    BOOKING_NO="LH{}".format(random.randint(1000,9999))
    
# OFFENDERS TEST DATA
    
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
    AGENCY_LOCATIONS = template.render(AGY_LOC_ID=AGY_LOC_ID,DESCRIPTION=DESCRIPTION,DISTRICT_CODE=DISTRICT_CODE,AUDIT_USER_ID=AUDIT_USER_ID)
    AGENCY_INTERNAL_LOCATIONS = template.render(INTERNAL_LOCATION_ID=INTERNAL_LOCATION_ID,DESCRIPTION=DESCRIPTION,AUDIT_CLIENT_USER_ID=AUDIT_CLIENT_USER_ID,AUDIT_USER_ID=AUDIT_USER_ID)
    OFFENDER_BOOKINGS = template.render(BOOKING_NO=BOOKING_NO)
    OFFENDERS = template.render(LAST_NAME=LAST_NAME,FIRST_NAME=FIRST_NAME,MIDDLE_NAME=MIDDLE_NAME)

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
    
    return json.dumps(json_payload).replace("'","\"")

# data=get_data("OFFENDER_BOOKINGS","happy_path")

# print(data)
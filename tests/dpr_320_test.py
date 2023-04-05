import sys
import os
import json
import random
import os
sys.path.append(os.pardir)
from utils.Kinesis import Kinesis
from utils.Config import getConfig


# GIVEN: ARRANGE

profileName = getConfig("aws", "profile")
stream = Kinesis(getConfig("kinesis", "stream"))

# STUB TABLE DATA

tableNames = {"OFFENDERS", "OFFENDER_BOOKINGS",
              "AGENCY_INTERNAL_LOCATIONS", "AGENCY_LOCATIONS"}

payloadRepo_happy_scenarios = os.pardir+'/payloads/happy_scenarios'
payloadRepo_error_scenarios = os.pardir+'/payloads/error_scenarios'


def pushValidMessages(tableName):
  for filename in os.listdir(payloadRepo_happy_scenarios):  
    if filename.startswith(tableName):
         print("Source: NOMIS  Table Name: {}".format(tableName))
         with open(os.path.join(payloadRepo_happy_scenarios, filename), 'r') as f:
             payload = json.dumps(json.load(f))
         stream.send_stream(data=payload, profileName=profileName)


def pushInValidMessages(tableName):
  for filename in os.listdir(payloadRepo_error_scenarios):  
    if filename.startswith(tableName):
         print("Source: NOMIS  Table Name: {}".format(tableName))
         with open(os.path.join(payloadRepo_error_scenarios, filename), 'r') as f:
             payload = json.dumps(json.load(f))
         stream.send_stream(data=payload, profileName=profileName)



def offenders():
    pushValidMessages("OFFENDERS")


def offender_bookings():
    pushValidMessages("OFFENDER_BOOKINGS")


def agency_locations():
    pushValidMessages(
        "AGENCY_LOCATIONS")


def agency_internal_locations():
    pushValidMessages("AGENCY_INTERNAL_LOCATIONS")
    

def offenders_error_path():
    pushInValidMessages("OFFENDERS")    

happy_paths = [agency_internal_locations,agency_locations,offenders,offender_bookings]

# unhappy_paths=[agency_internal_locations_errors,agency_locations_errors,offenders_errors,offender_bookings_errors]

unhappy_paths = [offenders_error_path]

scenario= "happy_paths"
# scenario="unhappy_paths"

if(scenario=="happy_paths"):   
 for i in range(100):
    random.shuffle(happy_paths)
    for path in happy_paths:
        path()
else:
 for i in range(10):
    random.shuffle(unhappy_paths)
    for path in unhappy_paths:
        path()

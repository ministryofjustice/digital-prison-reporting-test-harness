import sys
import os
import json
import random
sys.path.append(os.pardir)
from utils.Config import getConfig
from utils.Kinesis import Kinesis
from tabulate import tabulate
from utils.DataGenerator import get_data


# GIVEN: ARRANGE

profileName = getConfig("aws", "profile")
stream = Kinesis(getConfig("kinesis", "stream"))

# STUB TABLE DATA

# tableNames = [ "AGENCY_LOCATIONS"]
# tableNames = [ "AGENCY_LOCATIONS"]
# tableNames = [ "MOVEMENT_REASONS"]
# tableNames = [ "OFFENDERS"]

tableNames = ["AGENCY_LOCATIONS", "AGENCY_INTERNAL_LOCATIONS", "OFFENDERS",
              "OFFENDER_BOOKINGS", "OFFENDER_EXTERNAL_MOVEMENTS", "MOVEMENT_REASONS"]



def getPayloadPath():
    path=["HAPPY_PATH  ","UNHAPPY_PATH"]
    flow_weightage=[1.0,0.0]
    total_weight=sum(flow_weightage)
    i=0;
    random_num= random.uniform(0,total_weight)
    while random_num >0:
        random_num-=flow_weightage[i]
        i+=1
    return path[i-1]    
    
# payloadRepo_happy_scenarios = os.pardir+'/payloads/'+happy_scenarios'
messages=[]
def pushMessages():
    # payloadPath=os.pardir+'/payloads/'+getPayloadPath()+'/'
    
    tableName=random.choice(tableNames)
    scenario=getPayloadPath()
    payload=get_data(tableName,scenario)
    
    result=stream.send_stream(data=payload, profileName=profileName)
    print("Table_Name :: {} {}".format(tableName,result))
    messages.append("{}  ::  {}".format(scenario,tableName))
    
for i in range(1,20):
    pushMessages()

scenario_count={}

for message in messages:
    if message in scenario_count:
        scenario_count[message]+=1
    else:
        scenario_count[message]=1

scenario=[]
headers=["SCENARIO/PATH :: TABLENAME","TOTAL COUNT"]

for k,v in scenario_count.items():
    scenario.append([k,v])
scenario.sort(reverse=False)
print("\n")
print(tabulate(scenario,headers=headers))    
            
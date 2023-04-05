import sys
import os
import json
import random
sys.path.append(os.pardir)
from utils.Config import getConfig
from utils.Kinesis import Kinesis
from tabulate import tabulate


# GIVEN: ARRANGE

profileName = getConfig("aws", "profile")
stream = Kinesis(getConfig("kinesis", "stream"))

# STUB TABLE DATA

tableNames = ["OFFENDERS", "OFFENDER_BOOKINGS",
              "AGENCY_INTERNAL_LOCATIONS", "AGENCY_LOCATIONS"]



def getPayloadPath():
    path=["happy_scenarios","error_scenarios"]
    flow_weightage=[0.8,0.2]
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
    payloadPath=os.pardir+'/payloads/'+getPayloadPath()+'/'
    tableName=random.choice(tableNames)
    # print(payloadPath+tableName)
    for filename in os.listdir(payloadPath):
        if filename.startswith(tableName):
            # print("Source: NOMIS  Scenario {} Table Name: {}".format(payloadPath.split('/')[-2].split('_')[0].capitalize(),tableName))
            messages.append("{}  ::  {}".format(payloadPath.split('/')[-2].split('_')[0].upper(),tableName))
            with open(os.path.join(payloadPath, filename), 'r') as f:
                payload = json.dumps(json.load(f))
            stream.send_stream(data=payload, profileName=profileName)

for i in range(1,100):
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
scenario.sort(reverse=True)
print("\n")
print(tabulate(scenario,headers=headers))    
            
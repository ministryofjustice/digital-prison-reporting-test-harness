import os
import random
import sys

sys.path.append(os.pardir)
from utils.Config import get_config
from utils.Kinesis import Kinesis
from tabulate import tabulate
from utils.DataGenerator import get_data

# GIVEN: ARRANGE

profile_name = get_config("aws", "profile")
stream = Kinesis(get_config("kinesis", "stream"))

# STUB TABLE DATA

# tableNames = [ "AGENCY_LOCATIONS"]
# tableNames = [ "AGENCY_LOCATIONS"]
# tableNames = [ "MOVEMENT_REASONS"]
# tableNames = [ "OFFENDERS"]

table_names = ["AGENCY_LOCATIONS", "AGENCY_INTERNAL_LOCATIONS", "OFFENDERS",
              "OFFENDER_BOOKINGS", "OFFENDER_EXTERNAL_MOVEMENTS", "MOVEMENT_REASONS"]


def get_payload_path():
    path = ["HAPPY_PATH  ", "UNHAPPY_PATH"]
    flow_weightage = [0.5, 0.5]
    total_weight = sum(flow_weightage)
    i = 0
    random_num = random.uniform(0, total_weight)
    while random_num > 0:
        random_num -= flow_weightage[i]
        i += 1
    return path[i - 1]


# payloadRepo_happy_scenarios = os.pardir+'/payloads/'+happy_scenarios'
messages = []


def push_messages():
    # payloadPath=os.pardir+'/payloads/'+getPayloadPath()+'/'

    table_name = random.choice(table_names)
    scenario = get_payload_path()
    payload = get_data(table_name, scenario)

    result = stream.send_stream(data=payload, profile_name=profile_name)
    print("Table_Name :: {} {}".format(table_name, result))
    messages.append("{}  ::  {}".format(scenario, table_name))


for i in range(1, 50):
    push_messages()

scenario_count = {}

for message in messages:
    if message in scenario_count:
        scenario_count[message] += 1
    else:
        scenario_count[message] = 1

scenario = []
headers = ["SCENARIO/PATH :: TABLENAME", "TOTAL COUNT"]

for k, v in scenario_count.items():
    scenario.append([k, v])
scenario.sort(reverse=False)
print("\n")
print(tabulate(scenario, headers=headers))

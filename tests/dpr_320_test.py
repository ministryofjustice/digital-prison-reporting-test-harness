import json
import os
import random
import sys

sys.path.append(os.pardir)
from utils.Kinesis import Kinesis
from utils.Config import get_config

# GIVEN: ARRANGE

profile_name = get_config("aws", "profile")
stream = Kinesis(get_config("kinesis", "stream"))

# STUB TABLE DATA

table_names = {"OFFENDERS", "OFFENDER_BOOKINGS",
              "AGENCY_INTERNAL_LOCATIONS", "AGENCY_LOCATIONS"}

payload_repo_happy_scenarios = os.pardir + '/payloads/happy_scenarios'
payload_repo_error_scenarios = os.pardir + '/payloads/error_scenarios'


def push_valid_messages(table_name):
    for filename in os.listdir(payload_repo_happy_scenarios):
        if filename.startswith(table_name):
            print("Source: NOMIS  Table Name: {}".format(table_name))
            with open(os.path.join(payload_repo_happy_scenarios, filename), 'r') as f:
                payload = json.dumps(json.load(f))
            stream.send_stream(data=payload, profile_name=profile_name)


def push_invalid_messages(table_name):
    for filename in os.listdir(payload_repo_error_scenarios):
        if filename.startswith(table_name):
            print("Source: NOMIS  Table Name: {}".format(table_name))
            with open(os.path.join(payload_repo_error_scenarios, filename), 'r') as f:
                payload = json.dumps(json.load(f))
            stream.send_stream(data=payload, profile_name=profile_name)


def offenders():
    push_valid_messages("OFFENDERS")


def offender_bookings():
    push_valid_messages("OFFENDER_BOOKINGS")


def agency_locations():
    push_valid_messages(
        "AGENCY_LOCATIONS")


def agency_internal_locations():
    push_valid_messages("AGENCY_INTERNAL_LOCATIONS")


def offenders_error_path():
    push_invalid_messages("OFFENDERS")


happy_paths = [agency_internal_locations, agency_locations, offenders, offender_bookings]

# unhappy_paths=[agency_internal_locations_errors,agency_locations_errors,offenders_errors,offender_bookings_errors]

unhappy_paths = [offenders_error_path]

# scenario= "happy_paths"
scenario = "unhappy_paths"

if scenario == "happy_paths":
    for i in range(100):
        random.shuffle(happy_paths)
        for path in happy_paths:
            path()
else:
    for i in range(10):
        random.shuffle(unhappy_paths)
        for path in unhappy_paths:
            path()

import json
import os


def get_payload(table_name, path="happy"):
    payload = ""
    if path.lower() != "happy":
        payload_repo_happy_scenarios = os.pardir + '/payloads/happy_scenarios'
        for filename in os.listdir(payload_repo_happy_scenarios):
            if filename.startswith(table_name):
                with open(os.path.join(payload_repo_happy_scenarios, filename), 'r') as f:
                    payload = json.dumps(json.load(f))
        return payload
    else:
        payload_repo_error_scenarios = os.pardir + '/payloads/error_scenarios'
        for filename in os.listdir(payload_repo_error_scenarios):
            if filename.startswith(table_name):
                with open(os.path.join(payload_repo_error_scenarios, filename), 'r') as f:
                    payload = json.dumps(json.load(f))
        return payload

import json,os

def get_payload(tableName, path="happy"):
        payload=""
        if path.lower() != "happy":
            payloadRepo_happy_scenarios = os.pardir+'/payloads/happy_scenarios'
            for filename in os.listdir(payloadRepo_happy_scenarios):
                if filename.startswith(tableName):
                    with open(os.path.join(payloadRepo_happy_scenarios, filename), 'r') as f:
                        payload = json.dumps(json.load(f))
            return payload        
        else:
            payloadRepo_error_scenarios = os.pardir+'/payloads/error_scenarios'
            for filename in os.listdir(payloadRepo_error_scenarios):
                if filename.startswith(tableName):
                    with open(os.path.join(payloadRepo_error_scenarios,filename), 'r') as f:
                        payload = json.dumps(json.load(f))
            return payload        
import json
import random
from datetime import datetime, timedelta

from faker import Faker
from jinja2 import Environment, FileSystemLoader


def get_data(file_name, scenario):
    faker = Faker(locale="en_GB")

    OFFENDERS_BOOKING_DATA = {}
    OFFENDERS_DATA = {}
    AGENCY_LOCATIONS_DATA = {}
    AGENCY_INTERNAL_LOCATIONS_DATA = {}
    OFFENDER_EXTERNAL_MOVEMENT_DATA = {}
    MOVEMENT_REASONS_DATA = {}

    # Define the variables to be used in the template

    metadata_timestamp_current = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    random_seconds = random.randint(1, 600)
    metadata_timestamp_future = (datetime.now(
    ) + timedelta(seconds=random_seconds)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    # AGENCY_LOCATIONS TEST DATA

    DESCRIPTION = faker.sentence()
    DISTRICT_CODE = random.randint(1, 1000)
    AUDIT_USER_ID = faker.name()
    # AGY_LOC_ID="LD"+random.choice(LOC_ID)
    AGY_LOC_ID = "LD" + str(faker.random_int(min=1, max=99999))

    # AGENCY_INTERNAL_LOCATIONS TEST DATA

    INTERNAL_LOCATION_ID = random.randint(1, 10000)
    DESCRIPTION = faker.sentence()
    AUDIT_CLIENT_USER_ID = faker.name()

    # OFFENDER_BOOKINGS TEST DATA

    # BOOKING_NO="LH{}".format(random.randint(1000,9999))
    BOOKING_NO = "LH{}" + str(faker.random_int(min=1000, max=99999))

    # OFFENDERS TEST DATA
    OFFENDER_ID = faker.random_int(min=1000, max=99999)
    FIRST_NAME = faker.first_name()
    LAST_NAME = faker.last_name()
    MIDDLE_NAME = faker.name().split(" ")[0]

    #  OFFENDER_EXTERNAL_MOVEMENTS

    MOVEMENT_SEQ = faker.random_int(min=1, max=999999)

    # MOVEMENT_REASONS

    MOVEMENT_TYPE_CHOICES = ["ADM",
                             "CRI",
                             "CRT",
                             "INTER",
                             "REL",
                             "TAP",
                             "TRN"]

    MOVEMENT_TYPE = random.choice(MOVEMENT_TYPE_CHOICES)

    MOVEMENT_REASON_CODE_CHOICES = ["26", "CONR", "CRT", "DCYP", "LICR", "MED", "V", "W", "F4", "PDH", "PS", "TRIAL",
                                    "VLBH", "VLCH", "VLNH", "VLWT", "99", "AS", "D1", "DEC", "ER", "REL", "1", "C4",
                                    "C5", "F1", "LTX", "R4", "ADMIN", "PRES", "PROG", "ADMN", "CCOM", "DHMP", "H", "R",
                                    "TRN", "TRNTAP", "U", "CA", "CONF", "PR", "REM", "VLCV", "VLPR", "AV", "D5", "NCS",
                                    "NG", "RE", "UAL_ECL", "13", "17", "3", "C2", "C7", "PAP", "R5", "R7", "RESET",
                                    "SP", "28", "RECAT", "FINE", "P", "COM", "INTAKE", "VLEB", "AR", "D6", "DA", "DD",
                                    "ESCP", "EX", "HD", "NP", "PD", "PF", "11", "12", "18", "21", "6", "ACCVISIT", "C1",
                                    "EL", "F9", "FR", "I1", "IM", "YOTR", "29", "CLIF", "ELR", "I", "K", "PSUS", "19",
                                    "22", "BREACH", "CRI", "NA", "NEWTON", "VLAP", "VLCR", "VLPP", "VLPS", "VLRH", "D3",
                                    "DS", "ETR", "HR", "16",
                                    "20", "FA", "YRDR", "PROD", "25", "D", "IMMIG", "M", "PSYC", "Q", "RHDC", "TRNCRT",
                                    "Y", "CAP", "VLBL", "BL", "DL", "HU", "IF", "MRG", "10", "8", "F2", "F3", "F8",
                                    "FAC", "FD", "SE", "APPEALS", "SEC", "A", "B", "C", "FOREIGN", "HLF", "J", "RDTO",
                                    "RECA", "SENT", "T", "AP", "DC", "SEN", "VL", "VLPC", "VLPD", "CE", "D2", "DE",
                                    "PX", "SC", "WEND", "14", "15", "5", "7", "C6", "DENT", "FE", "FF", "OPA", "R2",
                                    "R9", "RO", "TAP", "UPW", "24", "27", "JAIL", "N", "O", "RMND", "YDET", "Z",
                                    "APPEAR", "AU", "CT", "DIR", "BD", "D4", "HE", "9", "C3", "FB", "HOSP", "PC", "R3",
                                    "YMI", "AVIST", "NOTR", "OTHER", "PROAT", "E", "ETB", "F", "G", "INT", "INTER", "L",
                                    "S", "BAIL", "CR", "DISC", "HC", "HP", "RW", "UAL", "2", "4", "C9", "COMP", "ET",
                                    "F6", "F7", "FC", "IR", "PWP", "R1", "R6", "R8", "OJ", "OVCROW"]

    MOVEMENT_REASON_CODE = random.choice(MOVEMENT_REASON_CODE_CHOICES)

    # Load the template file
    match scenario.lower().strip():
        case "happy_path":
            templateLocation = "../payloads/happy_scenarios"
        case "unhappy_path":
            templateLocation = "../payloads/error_scenarios"

    env = Environment(loader=FileSystemLoader(templateLocation))
    template = env.get_template(file_name + ".json")

    # Render the template with the variables
    AGENCY_LOCATIONS = template.render(AGY_LOC_ID=AGY_LOC_ID, DESCRIPTION=DESCRIPTION,
                                       DISTRICT_CODE=DISTRICT_CODE, AUDIT_USER_ID=AUDIT_USER_ID,
                                       timestamp=metadata_timestamp_current)

    AGENCY_LOCATIONS_DATA[AGY_LOC_ID] = "DECSRIPTION:{},DISTRICT_CODE:{},AUDIT_USER_ID:{}".format(
        DESCRIPTION, DISTRICT_CODE, AUDIT_USER_ID)

    AGENCY_INTERNAL_LOCATIONS = template.render(INTERNAL_LOCATION_ID=INTERNAL_LOCATION_ID, DESCRIPTION=DESCRIPTION,
                                                AUDIT_CLIENT_USER_ID=AUDIT_CLIENT_USER_ID, AUDIT_USER_ID=AUDIT_USER_ID,
                                                timestamp=metadata_timestamp_current)

    AGENCY_INTERNAL_LOCATIONS_DATA[INTERNAL_LOCATION_ID] = "DESCRIPTION:{},AUDIT_CLIENT_USER_ID:{},AUDIT_USER_ID:{}"

    OFFENDER_BOOKINGS = template.render(
        BOOKING_NO=BOOKING_NO, timestamp=metadata_timestamp_current)

    OFFENDERS_BOOKING_DATA['BOOKING_NO'] = "timestamp:{}".format(
        metadata_timestamp_future)

    OFFENDERS = template.render(OFFENDER_ID=OFFENDER_ID, LAST_NAME=LAST_NAME,
                                FIRST_NAME=FIRST_NAME, MIDDLE_NAME=MIDDLE_NAME, timestamp=metadata_timestamp_current)

    OFFENDERS_DATA[OFFENDER_ID] = "LAST_NAME:{},FIRST_NAME:{},MIDDLE_NAME:{},timestamp:{}".format(
        LAST_NAME, FIRST_NAME, MIDDLE_NAME, metadata_timestamp_future)

    OFFENDER_EXTERNAL_MOVEMENTS = template.render(OFFENDER_ID=OFFENDER_ID, MOVEMENT_SEQ=MOVEMENT_SEQ,
                                                  timestamp=metadata_timestamp_current)

    OEM_COMPOSITE_KEY = "{}-{}".format(OFFENDER_ID, MOVEMENT_SEQ)

    OFFENDER_EXTERNAL_MOVEMENT_DATA[OEM_COMPOSITE_KEY] = "timestamp:{}".format(
        metadata_timestamp_future)

    MOVEMENT_REASONS = template.render(MOVEMENT_TYPE=MOVEMENT_TYPE, MOVEMENT_REASON_CODE=MOVEMENT_REASON_CODE,
                                       timestamp=metadata_timestamp_current)

    MR_CPOMPOSITE_KEY = "{}-{}".format(OFFENDER_ID, MOVEMENT_SEQ)

    MOVEMENT_REASONS_DATA[MR_CPOMPOSITE_KEY] = "  MOVEMENT_TYPE:{},MOVEMENT_REASON_CODE:{},timestamp:{}".format(
        MOVEMENT_TYPE, MOVEMENT_REASON_CODE, metadata_timestamp_future)

    # Convert the rendered template to a JSON payload
    payload_name = (file_name.split(".")[0])
    # print(payload_name)

    match payload_name:
        case "AGENCY_LOCATIONS":
            json_payload = json.loads(AGENCY_LOCATIONS)
        case "AGENCY_INTERNAL_LOCATIONS":
            json_payload = json.loads(AGENCY_INTERNAL_LOCATIONS)
        case "OFFENDER_BOOKINGS":
            json_payload = json.loads(OFFENDER_BOOKINGS)
        case "OFFENDERS":
            json_payload = json.loads(OFFENDERS)
        case "OFFENDER_EXTERNAL_MOVEMENTS":
            json_payload = json.loads(OFFENDER_EXTERNAL_MOVEMENTS)
        case "MOVEMENT_REASONS":
            json_payload = json.loads(MOVEMENT_REASONS)

    return json.dumps(json_payload).replace("'", "\"")

# data=get_data("OFFENDER_BOOKINGS","happy_path")

# print(data)

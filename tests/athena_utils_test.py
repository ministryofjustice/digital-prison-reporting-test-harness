import sys
import os
sys.path.append(os.pardir)
from utils.Config import getConfig
from utils.Athena import Athena


profileName = getConfig("aws", "profile")

athena_connect = Athena(profileName, 'dpr-320-test',
                        "dpr-304-test", "athena_test")

query = 'SELECT * FROM "{}"."{}" LIMIT 3'.format(
    athena_connect.database, "agency_internal_locations")

# query= 'SELECT count(*) FROM "dpr-304-db"."agency_internal_locations"'

# query = 'SELECT count(*) FROM "dpr-304-db"."agency_internal_locations"'

results = athena_connect.runQuery(query)

print(results)

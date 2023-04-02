import sys,os
sys.path.append(os.pardir)
from utils.Athena import Athena
from utils.Config import getConfig



profileName = getConfig("aws", "profile")

athena_connect = Athena(profileName,'dpr-304-db', "dpr-304-test", "athena_test")

query = 'SELECT * FROM "{}"."{}" LIMIT 3'.format(
    athena_connect.database, "agency_internal_locations")

# query= 'SELECT count(*) FROM "dpr-304-db"."agency_internal_locations"'

# query = 'SELECT count(*) FROM "dpr-304-db"."agency_internal_locations"'

results = athena_connect.runQuery(query)

[print(i) for i in results] 

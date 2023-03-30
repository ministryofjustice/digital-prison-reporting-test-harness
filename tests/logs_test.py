import sys,os
sys.path.append(os.pardir)
from utils.logs import logs

current_logs=logs("dpr-320-datahub-to-structured-zone","hari_profile")

current_logs.get_logs()
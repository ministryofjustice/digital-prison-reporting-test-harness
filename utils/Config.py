import configparser,os

configFile= r'../test_data.ini'
config= configparser.ConfigParser()
config.read(configFile)

environment = os.environ.get('environment') or "development"

def getConfig(section,key):
 return config[section][key].format(environment)   

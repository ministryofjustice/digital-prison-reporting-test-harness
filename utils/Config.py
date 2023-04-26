import configparser

configFile= r'../test_data.ini'
config= configparser.ConfigParser()
config.read(configFile)

def getConfig(section,key):
 return config[section][key]   

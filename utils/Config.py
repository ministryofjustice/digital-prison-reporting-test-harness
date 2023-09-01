import configparser
import os

configFile = r'../test_data.ini'
config = configparser.ConfigParser()
config.read(configFile)

environment = os.environ.get('environment') or "development"


def get_config(section, key):
    return config[section][key].format(environment)

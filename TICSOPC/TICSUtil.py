from datetime import datetime
import configparser, os


def log_time():
    """ Returns date time with ms. Can be used for logging messages"""
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


def readconfigfile(filename, section, key):
    root = ".\TICSOPC"
    filename = os.path.join(root,filename)
    config = configparser.ConfigParser()
    config.read(filename,  encoding='utf-8')
    return config.get(section, key)

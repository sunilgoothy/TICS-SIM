from datetime import datetime
import configparser, csv, os

def log_time():
    """ Returns date time with ms. Can be used for logging messages"""
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

def readconfigfile(filename,section,key):
    root = ".\SIM"
    filename = os.path.join(root,filename)
    config = configparser.ConfigParser()
    config.read(filename,  encoding='utf-8')
    return config.get(section, key)

def csvToDic(filename):
    #print(filename)
    tag_dict = dict()
    try:
        root = ".\SIM"
        filename = os.path.join(root,filename)
        with open(filename) as tags_file:
            csv_reader = csv.DictReader(tags_file, delimiter=',')
            line_count = 0
            for row in csv_reader:
                _temp = {}
                if line_count == 0:
                    _key = list(row.keys())
                    #print(_key)
                commented_line = row[_key[0]].startswith('#')
                if ( not commented_line ):
                    for record in row:
                        if record != _key[0]:
                            _temp[record] = row[record]

                    tag_dict[row[_key[0]]]=_temp
                line_count += 1
            #print(f'<INFO> csvToDic Processed {line_count} lines from {filename}.')

    except Exception as e:
        print(e)
    return tag_dict

def eventDataTag(eventname):
    filename = 'tics_events_data.csv'
    _list = list()
    try:
        root = ".\SIM"
        filename = os.path.join(root,filename)
        with open(filename) as tags_file:
            csv_reader = csv.DictReader(tags_file, delimiter=',')
            line_count = 0
            for row in csv_reader:
                _key = list(row.keys())
                if eventname in _key:
                    if row[eventname] != '':
                        _list.append(row[eventname])
                line_count += 1
            #print(f'<INFO> eventDataTag Processed {line_count} lines from {filename}.')
    except Exception as e:
        print(e)

    return _list
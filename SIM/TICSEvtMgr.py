import ast, redis, sys, csv, json, time, os
from TICSEvents import *
from TICSUtil import *

class TICSEvtMgr:

    def __init__(self, filename='tics_events.csv'):
        """Initialization of the Event Manager class"""
        self.events = dict()
        try:
            self.evt_type = {0:'do', 1:'pu'}
            self.rclient = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses = True)
            self.Events = TICSEvents()
            self.read_events_config(filename)
        except Exception as e:
            print(log_time(), f'<ERR> Failed to Initialize Event Manager. Exception: {e}')

    def event_monitor(self):
        """Monitor for the events on redis channel event_queue. Dispatch the received events through dispatcher"""
        try:
            evq = self.rclient.pubsub()
            evq.subscribe('event_queue')
            while True:
                try:
                    msg = evq.get_message(ignore_subscribe_messages=True)
                    if msg:
                        msg_data = msg['data']
                        #Convert to dictionary
                        data = ast.literal_eval(msg_data)
                        tag = data['tag']
                        #args = data['data']
                        args = data['value']
                        value = data['value']
                        if tag in self.events.keys():
                            valid_evt = False
                            tag_evt_type = self.events[tag]['type']
                            if tag_evt_type == self.evt_type[value]:
                                valid_evt = True
                            if tag_evt_type == 'pu_do':
                                valid_evt = True
                                
                            if (valid_evt):
                                self.event_dispatcher(tag, args)
                            
                        else:
                            pass
                except Exception as e:
                    print(log_time(), f'<ERR> Exception in Subscribe Channel. Error: {e}')
                
                time.sleep(0.001)

        except Exception as e:
            print(log_time(), f'<ERR> Exception in event_monitor. Error: {e}')

    def event_dispatcher(self, tag, *args):
        """Dispatch the change detect event to appropriate event assigned to it"""
        func_name = self.events[tag]['event_name']
        call = getattr(self.Events, func_name)
        call(*args)

    def read_events_config(self, filename):
        """Read Events Config File and register events"""
        try:
            root = ".\SIM"
            filename = os.path.join(root,filename)
            with open(filename, newline='') as fp:
                csv_reader = csv.DictReader(fp)
                line_count = 0
                event_count = 0
                for row in csv_reader:
                    commented_line = row['tag_name'].startswith('#')
                    if ( not commented_line ):
                        evt_tag = row['tag_name']
                        evt_type = row['type']
                        self.events[evt_tag] = row
                        self.rclient.hset('events',evt_tag,evt_type)
                        event_count += 1
                    line_count+=1

                print(log_time(), f'<INFO> Processed {event_count} events from {filename}.')
        except Exception  as e:
            print(log_time(), f'<ERR> Failed to read events config file. Exception: {e}')

    def register_event(self, signal, event_name):
        """Manually Register events with the monitor and dispatcher"""
        pass


if __name__ == '__main__':
    print("Starting Event Manager...")
    i = 0
    for arg in sys.argv:
        if (arg.lower() == 'debug'):
            debug = 1
        if (arg.lower() == '-c'):
            config_file = sys.argv[i+1]
        i+=1
    
    EventMgr = TICSEvtMgr()
    EventMgr.event_monitor()

    

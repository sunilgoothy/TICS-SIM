from TICSUtil import *
import random, redis, inspect, time, threading


def clearAll(_simConfig, rclient):
    for record in _simConfig:
        rclient.hset('tagwrite', _simConfig[record]['tag_addr'], 0)
        rclient.hset('tagwrite', _simConfig[record]['tag_pieceID'], 0)
    print(log_time(), f'<INFO> Signals cleared')


def reset(arg, _func, _simConfig, _pieceID, rclient):
    if arg:
        #delay
        time.sleep(float(_simConfig[_func]['pulse_dur']))

        # reset current
        rclient.hset('tagwrite', _simConfig[_func]['tag_addr'], 0)
        rclient.hset('tagwrite', _simConfig[_func]['tag_pieceID'], 0)

def setnext(arg, _func, _simConfig, _pieceID, rclient):
    if arg:
        #delay
        time.sleep(float(_simConfig[_func]['event_gap']))

        #set next
        rclient.hset('tagwrite', _simConfig[_func]['tag_addr_nxt'], 1)
        rclient.hset('tagwrite', _simConfig[_func]['tag_pieceID_next'], _pieceID)

class TICSEvents:

    def __init__(self):
        self.init = False
        self._pieceID = 0
        self.evt_type = {0:'DROP', 1:'PICKUP'}
        self._simConfig = csvToDic('tics_sim_config.csv')
        self.rclient = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses = True)
        clearAll(self._simConfig, self.rclient)
        self.init = True
        
    def initialize(self, *args):
        print(log_time(), f'<INFO> initialize')
        self.rclient.hset('tagwrite', self._simConfig['evt_extract_req']['tag_addr'], 1)

    def evt_extract_req(self, *args):
        self._pieceID+=1
        print(log_time(), f'<INFO> piece ID: {self._pieceID}')
        _func = inspect.stack()[0][3]
        print(log_time(), f'<INFO> {_func}')
        for arg in args:
            # print(f'arg: {arg}')
            t1 = threading.Thread(target=reset, args = (arg, _func, self._simConfig, self._pieceID, self.rclient))
            t1.daemon = True
            t1.start()
            t2 = threading.Thread(target=setnext, args = (arg, _func, self._simConfig, self._pieceID, self.rclient))
            t2.daemon = True
            t2.start()

    def evt_slab_discharged(self, *args):
        _func = inspect.stack()[0][3]
        print(log_time(), f'<INFO> {_func}')
        for arg in args:
            # print(f'arg: {arg}')
            t1 = threading.Thread(target=reset, args = (arg, _func, self._simConfig, self._pieceID, self.rclient))
            t1.daemon = True
            t1.start()
            t2 = threading.Thread(target=setnext, args = (arg, _func, self._simConfig, self._pieceID, self.rclient))
            t2.daemon = True
            t2.start()

    def evt_rmet_pu_do(self, *args):
        if self.init:
            _func = inspect.stack()[0][3]

            for arg in args:
                # print(f'arg: {arg}')
                print(log_time(), f'<INFO> {_func} {self.evt_type[arg]}')
                t1 = threading.Thread(target=reset, args = (arg, _func, self._simConfig, self._pieceID, self.rclient))
                t1.daemon = True
                t1.start()
                t2 = threading.Thread(target=setnext, args = (arg, _func, self._simConfig, self._pieceID, self.rclient))
                t2.daemon = True
                t2.start()


    def evt_rm_pu_do(self, *args):
        if self.init:
            _func = inspect.stack()[0][3]

            for arg in args:
                # print(f'arg: {arg}')
                print(log_time(), f'<INFO> {_func} {self.evt_type[arg]}')
                t1 = threading.Thread(target=reset, args = (arg, _func, self._simConfig, self._pieceID, self.rclient))
                t1.daemon = True
                t1.start()
                t2 = threading.Thread(target=setnext, args = (arg, _func, self._simConfig, self._pieceID, self.rclient))
                t2.daemon = True
                t2.start()

    def evt_rmdt_pu_do(self, *args):
        if self.init:
            _func = inspect.stack()[0][3]

            for arg in args:
                # print(f'arg: {arg}')
                print(log_time(), f'<INFO> {_func} {self.evt_type[arg]}')
                t1 = threading.Thread(target=reset, args = (arg, _func, self._simConfig, self._pieceID, self.rclient))
                t1.daemon = True
                t1.start()
                t2 = threading.Thread(target=setnext, args = (arg, _func, self._simConfig, self._pieceID, self.rclient))
                t2.daemon = True
                t2.start()

    def evt_fet_pu_do(self, *args):
        if self.init:
            _func = inspect.stack()[0][3]

            for arg in args:
                # print(f'arg: {arg}')
                print(log_time(), f'<INFO> {_func} {self.evt_type[arg]}')
                t1 = threading.Thread(target=reset, args = (arg, _func, self._simConfig, self._pieceID, self.rclient))
                t1.daemon = True
                t1.start()
                t2 = threading.Thread(target=setnext, args = (arg, _func, self._simConfig, self._pieceID, self.rclient))
                t2.daemon = True
                t2.start()

    def evt_fmd_pu_do(self, *args):
        if self.init:
            _func = inspect.stack()[0][3]

            for arg in args:
                # print(f'arg: {arg}')
                print(log_time(), f'<INFO> {_func} {self.evt_type[arg]}')
                t1 = threading.Thread(target=reset, args = (arg, _func, self._simConfig, self._pieceID, self.rclient))
                t1.daemon = True
                t1.start()
                t2 = threading.Thread(target=setnext, args = (arg, _func, self._simConfig, self._pieceID, self.rclient))
                t2.daemon = True
                t2.start()

    def evt_fdt_pu_do(self, *args):
        if self.init:
            _func = inspect.stack()[0][3]

            for arg in args:
                # print(f'arg: {arg}')
                print(log_time(), f'<INFO> {_func} {self.evt_type[arg]}')
                t1 = threading.Thread(target=reset, args = (arg, _func, self._simConfig, self._pieceID, self.rclient))
                t1.daemon = True
                t1.start()
                t2 = threading.Thread(target=setnext, args = (arg, _func, self._simConfig, self._pieceID, self.rclient))
                t2.daemon = True
                t2.start()

    def evt_ct_pu_do(self, *args):
        if self.init:
            _func = inspect.stack()[0][3]

            for arg in args:
                # print(f'arg: {arg}')
                print(log_time(), f'<INFO> {_func} {self.evt_type[arg]}')
                t1 = threading.Thread(target=reset, args = (arg, _func, self._simConfig, self._pieceID, self.rclient))
                t1.daemon = True
                t1.start()
                t2 = threading.Thread(target=setnext, args = (arg, _func, self._simConfig, self._pieceID, self.rclient))
                t2.daemon = True
                t2.start()

    def evt_dc_pu_do(self, *args):
        if self.init:
            _func = inspect.stack()[0][3]

            for arg in args:
                # print(f'arg: {arg}')
                print(log_time(), f'<INFO> {_func} {self.evt_type[arg]}')
                t1 = threading.Thread(target=reset, args = (arg, _func, self._simConfig, self._pieceID, self.rclient))
                t1.daemon = True
                t1.start()
                t2 = threading.Thread(target=setnext, args = (arg, _func, self._simConfig, self._pieceID, self.rclient))
                t2.daemon = True
                t2.start()

    def evt_coil_complete(self, *args):
        if self.init:
            _func = inspect.stack()[0][3]

            for arg in args:
                # print(f'arg: {arg}')
                print(log_time(), f'<INFO> {_func} {self.evt_type[arg]}')
                t1 = threading.Thread(target=reset, args = (arg, _func, self._simConfig, self._pieceID, self.rclient))
                t1.daemon = True
                t1.start()
                t2 = threading.Thread(target=setnext, args = (arg, _func, self._simConfig, self._pieceID, self.rclient))
                t2.daemon = True
                t2.start()

    def evt_coil_weigh(self, *args):
        if self.init:
            _func = inspect.stack()[0][3]

            for arg in args:
                # print(f'arg: {arg}')
                print(log_time(), f'<INFO> {_func} {self.evt_type[arg]}')
                t1 = threading.Thread(target=reset, args = (arg, _func, self._simConfig, self._pieceID, self.rclient))
                t1.daemon = True
                t1.start()
                t2 = threading.Thread(target=setnext, args = (arg, _func, self._simConfig, self._pieceID, self.rclient))
                t2.daemon = True
                t2.start()
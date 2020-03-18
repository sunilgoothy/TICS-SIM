from TICSUtil import *
from TICSSimu import *
import random, redis, inspect, time, threading
from threading import Thread
from models import PDI, PDO, session

def clearAll(_simConfig, rclient):
    for record in _simConfig:
        rclient.hset('tagwrite', _simConfig[record]['tag_addr'], 0)
        rclient.hset('tagwrite', _simConfig[record]['tag_pieceID'], 0)
    print(log_time(), f'<INFO> Signals cleared')

def getPDI(_pieceID):
    #print(_pieceID)
    _pdiData = session.query(PDI).filter_by(c_SlabID=_pieceID).limit(1).all()
    _pdidata_dict = {}
    if _pdiData != []:
        for record in _pdiData:
            #print('pdi: {}'.format(record))
            _pdidata_dict = record.__dict__
    return _pdidata_dict

def reset(arg, _func, _simConfig, _pieceID, rclient):
    if arg:
        pulse_dur = float(_simConfig[_func]['pulse_dur'])
        #simulate
        _signalList = eventDataTag(_func)
        if len(_signalList)>0:
            for _signal in _signalList:
                _pdi = getPDI(_pieceID)
                sim = TICSSimu(_pieceID, rclient, _func, _pdi, _signal)
                t = Thread(target = sim.simulate, args =(pulse_dur, )) 
                t.start()
        #delay
        time.sleep(pulse_dur)

        # reset current
        rclient.hset('tagwrite', _simConfig[_func]['tag_addr'], 0)
        #rclient.hset('tagwrite', _simConfig[_func]['tag_pieceID'], 0)

def setnext(arg, _func, _simConfig, _pieceID, rclient):
    if arg and _func != 'evt_coil_weigh':
        #delay
        time.sleep(float(_simConfig[_func]['event_gap']))

        #set next
        rclient.hset('tagwrite', _simConfig[_func]['tag_pieceID_next'], _pieceID)
        rclient.hset('tagwrite', _simConfig[_func]['tag_addr_nxt'], 1)

def millPacing(_simConfig, rclient):
    _pieceID = 0
    while True:
        _pieceID+=1
        _func = 'evt_extract_req'
        print(log_time(), f'<INFO> new extract request for pieceID: ', str(_pieceID))
        rclient.hset('tagwrite', _simConfig[_func]['tag_pieceID'], _pieceID)
        rclient.hset('tagwrite', _simConfig[_func]['tag_addr'], 1)
        time.sleep(160)

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
        t = threading.Thread(target=millPacing, args = (self._simConfig, self.rclient))
        t.daemon = True
        t.start()

    def evt_extract_req(self, *args):
        self._pieceID+=1
        print(log_time(), f'<INFO> piece ID: {self._pieceID}')
        _func = inspect.stack()[0][3]
        for arg in args:
            print(log_time(), f'<INFO> {_func} {self.evt_type[arg]}')
            t1 = threading.Thread(target=reset, args = (arg, _func, self._simConfig, self._pieceID, self.rclient))
            t1.daemon = True
            t1.start()
            t2 = threading.Thread(target=setnext, args = (arg, _func, self._simConfig, self._pieceID, self.rclient))
            t2.daemon = True
            t2.start()

    def evt_slab_discharged(self, *args):
        _func = inspect.stack()[0][3]
        for arg in args:
            print(log_time(), f'<INFO> {_func} {self.evt_type[arg]}')
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
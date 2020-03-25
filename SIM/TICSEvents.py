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
                sim = TICSSimu(_pieceID, rclient, _func, _pdi)
                t = Thread(target = sim.simulate, args =(pulse_dur, _signal)) 
                t.start()
        #delay
        time.sleep(pulse_dur)

        # reset current
        rclient.hset('tagwrite', _simConfig[_func]['tag_addr'], 0)
        #rclient.hset('tagwrite', _simConfig[_func]['tag_pieceID'], 0)
def reject(arg, _calling_func, _simConfig, _pieceID, rclient):
    if arg:
        _func = inspect.stack()[0][3]
        print(log_time(), f'<INFO> {_func} for {_pieceID}')
        time.sleep(1)
        _tag_reject = 'ns=2;s=SimChannel.Device.Boolean.L_b_Tag13'
        rclient.hset('tagwrite', _simConfig[_calling_func]['tag_addr'], 0)
        rclient.hset('tagwrite', _tag_reject, 1)
        time.sleep(1)
        rclient.hset('tagwrite', _tag_reject, 0)

def cobble(arg, _calling_func, _simConfig, _pieceID, rclient):
    if arg:
        _func = inspect.stack()[0][3]
        print(log_time(), f'<INFO> {_func} pieceID: {_pieceID}')

        time.sleep(3)
        _tag_cobble = 'ns=2;s=SimChannel.Device.Boolean.L_b_Tag14'
        rclient.hset('tagwrite', _simConfig[_calling_func]['tag_addr'], 0)
        rclient.hset('tagwrite', _tag_cobble, 1)
        time.sleep(1)
        rclient.hset('tagwrite', _tag_cobble, 0)

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
        _slbToslb_time = random.randint(150,210)
        _pieceID+=1
        _func = 'evt_extract_req'
        print(log_time(), f'<INFO> new extract request for pieceID: ', str(_pieceID), \
            ' with slab to slab time:', str(_slbToslb_time))
        rclient.hset('tagwrite', _simConfig[_func]['tag_pieceID'], _pieceID)
        time.sleep(0.2)
        rclient.hset('tagwrite', _simConfig[_func]['tag_addr'], 1)
        time.sleep(_slbToslb_time)

class TICSEvents:

    def __init__(self):
        self.init = False
        self._pieceID = 0
        self.evt_type = {0:'DROP', 1:'PICKUP'}
        self._simConfig = csvToDic('tics_sim_config.csv')
        self._simSpeedConfig = csvToDic('tics_speed_sim.csv')
        self.rclient = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses = True)
        clearAll(self._simConfig, self.rclient)
        self.rclient.hset('tagwrite', 'ns=2;s=SimChannel.Device.Float.L_f_Tag1', float('0.5'))
        self.init = True


    def sim_speed(self, _func, _type):
        _config = self._simSpeedConfig[_func]
        _type = self.evt_type[_type]
        pulse_dur = float(self._simConfig[_func]['pulse_dur'])
        _pdi = getPDI(self._pieceID)
        sim = TICSSimu(self._pieceID, self.rclient, _func, _pdi)
        t = Thread(target = sim.speedSim, args =(pulse_dur, _type, _config)) 
        t.start()

    def initialize(self, *args):
        print(log_time(), f'<INFO> initialize')
        t = threading.Thread(target=millPacing, args = (self._simConfig, self.rclient))
        t.daemon = True
        t.start()

    def evt_extract_req(self, *args):
        self._pieceID+=1
        if self._pieceID > 200:
            self._pieceID = 1
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
                if arg:
                    if random. randint(1,5) == 5:
                        print('piece Rejected')
                        t = threading.Thread(target=reject, args = (arg, _func, self._simConfig, self._pieceID, self.rclient))
                        t.daemon = True
                        t.start()
                    else:
                        print('piece Not Rejected')
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
                if arg:
                    if random. randint(1,20) == 10:
                        t = threading.Thread(target=cobble, args = (arg, _func, self._simConfig, self._pieceID, self.rclient))
                        t.daemon = True
                        t.start()
                    else:
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
                self.sim_speed(_func, arg)
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
                self.sim_speed(_func, arg)
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
                self.sim_speed(_func, arg)
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
                self.sim_speed(_func, arg)
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
                self.sim_speed(_func, arg)
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
                self.sim_speed(_func, arg)
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
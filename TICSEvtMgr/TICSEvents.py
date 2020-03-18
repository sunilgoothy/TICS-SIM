from TICSUtil import *
import random, redis, inspect, threading, time
from datetime import datetime
from models import PDI, PDO, session
from threading import Thread


_dict = dict()
_dict_time = dict()
rclient = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses = True)

def write_pdi(pieceID):
    print('pieceID: {}'.format(pieceID))
    _pdiData = session.query(PDI).filter_by(c_SlabID=pieceID).limit(1).all()

    if _pdiData != []:
        for record in _pdiData:
            print('pdi: {}'.format(record))
            _pdidata_dict = record.__dict__
        _pdiTag = csvToDic('pdi_config.csv')
    else:
        print('pdi not exits for pieceID:'.format(pieceID))
        
        for record in _pdiTag:
            _data = float(_pdidata_dict[record])
            print('writing tag {0} with value {1}'.format( _pdiTag[record],_data))
            rclient.hset('tagwrite', _pdiTag[record], _data)

def get_pdo(pieceID):
    return 0

def getPieceID():
    tag_pieceID = 'ns=2;s=SimChannel.Device.Integer.L_i_Tag1'
    _pieceID = rclient.hget('tagread', tag_pieceID)
    return _pieceID

class TICSEvents():

    def __init__(self):
        self.rclient = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses = True)

    def evt_extract_req(self, *args):
        print(log_time(), f'<INFO> evt_extract_req')
        for arg in args:
            print(f'arg: {arg}')
        # t = threading.Thread(target=write_pdi, args=getPieceID())
        # t.daemon = True
        # t.start()
        write_pdi(getPieceID())

    def evt_slab_discharged(self, *args):
        print(log_time(), f'<INFO> evt_slab_discharged')
        for arg in args:
            print(f'arg: {arg}')       

    def evt_rmet_pu_do(self, *args):
        _func = inspect.stack()[0][3]
        _pieceID = str(getPieceID())
        _dict[_pieceID] = {}
        _dict_time[_pieceID] = {}
        print(log_time(), f'<INFO> {_func}')
        for _tag in eventDataTag(_func):
            for arg in args:
                print(f'arg: {arg}')
                _dict[_pieceID].update({_tag : self.rclient.hget('tagread', _tag)})
                _dict_time[_pieceID].update({_tag : datetime.now()})
            print('{} end'.format(_func))
        print(_dict)
        print(_dict_time)

    def evt_rm_pu_do(self, *args):
        print(log_time(), f'<INFO> evt_rm_pu')
        for arg in args:
            print(f'arg: {arg}')

    def evt_rmdt_pu_do(self, *args):
        print(log_time(), f'<INFO> evt_rmdt_pu')
        for arg in args:
            print(f'arg: {arg}')

    def evt_fet_pu_do(self, *args):
        print(log_time(), f'<INFO> evt_fet_pu')
        for arg in args:
            print(f'arg: {arg}')

    def evt_fmd_pu_do(self, *args):
        print(log_time(), f'<INFO> evt_fmd_pu')
        for arg in args:
            print(f'arg: {arg}')

    def evt_fdt_pu_do(self, *args):
        print(log_time(), f'<INFO> evt_fdt_pu')
        for arg in args:
            print(f'arg: {arg}')

    def evt_ct_pu_do(self, *args):
        print(log_time(), f'<INFO> evt_fdt_do')
        for arg in args:
            print(f'arg: {arg}')

    def evt_dc_pu_do(self, *args):
        print(log_time(), f'<INFO> evt_dc_pu')
        for arg in args:
            print(f'arg: {arg}')

    def evt_coil_complete(self, *args):
        print(log_time(), f'<INFO> evt_coil_complete')
        for arg in args:
            print(f'arg: {arg}')

    def evt_coil_weigh(self, *args):
        print(log_time(), f'<INFO> evt_coil_weigh')
        for arg in args:
            print(f'arg: {arg}')

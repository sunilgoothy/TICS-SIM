from TICSUtil import *
import random, redis, inspect, threading, time, json
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
        print('pdi not exits for pieceID:{0}'.format(pieceID))
        
        for record in _pdiTag:
            _data = float(_pdidata_dict[record])
            print('writing tag {0} with value {1}'.format( _pdiTag[record],_data))
            rclient.hset('tagwrite', _pdiTag[record], _data)

    session.query(PDI).filter_by(c_SlabID=pieceID).update({'c_SlabLoc': 'EXT'})
    session.commit()

def get_pdo(pieceID):
    print(log_time(), f'<INFO> get_pdo {0}'.format(pieceID))
    _pieceData = _dict[pieceID]
    with open('piecedata_'+str(pieceID)+'.json', 'w') as fp:
        json.dump(_pieceData, fp)
    _pdoData = {}
    _pdoTag = csvToDic('pdo_config.csv')
    print(_pdoTag)
    print(_pdoTag['f_RMDelTempAve'])
    _pdoTag['f_RMDelTempAve']
    print(log_time(), f'<INFO> check signal data')
    for _pdoField in _pdoTag:
        try:
            _pdoData[_pdoField] = float(_pieceData[_pdoTag[_pdoField]]['value'])
        except Exception as e:
            print(e)
            _pdoData[_pdoField] = 0
    newPDO = PDO(
        i_PieceIndex=pieceID, \
            gt_PieceProductionTime = datetime.now() , \
            f_RMDelTempAve =  _pdoData['f_RMDelTempAve'] , \
            f_FMDelThkAve =  _pdoData['f_FMDelThkAve'] , \
            f_FMDelWidthAve  =  _pdoData['f_FMDelWidthAve'] , \
            f_FMEntTempAve =  _pdoData['f_FMEntTempAve'] , \
            f_FMDelTempAve =  _pdoData['f_FMDelTempAve'] , \
            f_DCTempAve =  _pdoData['f_DCTempAve']
    )
    session.add(newPDO)
    session.commit()

def update_weight(pieceID):
    print(log_time(), f'<INFO> update_weight')

def getPieceID(_func):
    tag_pieceID = 'ns=2;s=SimChannel.Device.Integer.L_i_Tag1'
    _pieceID = rclient.hget('tagread', tag_pieceID)
    return _pieceID

def update_dict(_pieceID, _tag, rclient):
    global _dict
    time.sleep(1)
    _tagread = {_tag : {'value':rclient.hget('tagread', _tag), 'time':str(datetime.now())}}
    if _pieceID not in _dict:
        _dict[_pieceID] = {}
    _tmpDict = _dict[_pieceID]
    #print(_tmpDict)
    for item in _tagread:
        _tmpDict[item]=_tagread[item]
    #print(_tmpDict)
    _dict[_pieceID] = _tmpDict

class TICSEvents():

    def __init__(self):
        self.rclient = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses = True)
        self.evt_type = {0:'DROP', 1:'PICKUP'}

    def evt_extract_req(self, *args):
        _func = inspect.stack()[0][3]
        _pieceID = getPieceID(_func)
        for arg in args:
            print(log_time(), f'<INFO> {_func} {self.evt_type[arg]}')
        write_pdi(_pieceID)

    def evt_slab_discharged(self, *args):
        _func = inspect.stack()[0][3]
        _pieceID = getPieceID(_func)
        #_dict[_pieceID] = {}
        _dict_time[_pieceID] = {}
        for arg in args:
            print(log_time(), f'<INFO> {_func} {self.evt_type[arg]}')
            if arg:
                if len(eventDataTag(_func))>0:
                    for _tag in eventDataTag(_func):
                        t = threading.Thread(target=update_dict, args = (_pieceID, _tag, self.rclient))
                        t.daemon = True
                        t.start()
     

    def evt_rmet_pu_do(self, *args):
        _func = inspect.stack()[0][3]
        _pieceID = getPieceID(_func)
        #_dict[_pieceID] = {}
        _dict_time[_pieceID] = {}
        for arg in args:
            print(log_time(), f'<INFO> {_func} {self.evt_type[arg]}')
            if arg:
                if len(eventDataTag(_func))>0:
                    for _tag in eventDataTag(_func):
                        t = threading.Thread(target=update_dict, args = (_pieceID, _tag, self.rclient))
                        t.daemon = True
                        t.start()

    def evt_rm_pu_do(self, *args):
        _func = inspect.stack()[0][3]
        _pieceID = getPieceID(_func)
        #_dict[_pieceID] = {}
        _dict_time[_pieceID] = {}
        for arg in args:
            print(log_time(), f'<INFO> {_func} {self.evt_type[arg]}')
            if arg:
                if len(eventDataTag(_func))>0:
                    for _tag in eventDataTag(_func):
                        t = threading.Thread(target=update_dict, args = (_pieceID, _tag, self.rclient))
                        t.daemon = True
                        t.start()

    def evt_rmdt_pu_do(self, *args):
        _func = inspect.stack()[0][3]
        _pieceID = getPieceID(_func)
        #_dict[_pieceID] = {}
        _dict_time[_pieceID] = {}
        for arg in args:
            print(log_time(), f'<INFO> {_func} {self.evt_type[arg]}')
            if arg:
                if len(eventDataTag(_func))>0:
                    for _tag in eventDataTag(_func):
                        t = threading.Thread(target=update_dict, args = (_pieceID, _tag, self.rclient))
                        t.daemon = True
                        t.start()
    
    def evt_fet_pu_do(self, *args):
        _func = inspect.stack()[0][3]
        _pieceID = getPieceID(_func)
        #_dict[_pieceID] = {}
        _dict_time[_pieceID] = {}
        for arg in args:
            print(log_time(), f'<INFO> {_func} {self.evt_type[arg]}')
            if arg:
                if len(eventDataTag(_func))>0:
                    for _tag in eventDataTag(_func):
                        t = threading.Thread(target=update_dict, args = (_pieceID, _tag, self.rclient))
                        t.daemon = True
                        t.start()

    def evt_fmd_pu_do(self, *args):
        _func = inspect.stack()[0][3]
        _pieceID = getPieceID(_func)
        #_dict[_pieceID] = {}
        _dict_time[_pieceID] = {}
        for arg in args:
            print(log_time(), f'<INFO> {_func} {self.evt_type[arg]}')
            if arg:
                if len(eventDataTag(_func))>0:
                    for _tag in eventDataTag(_func):
                        t = threading.Thread(target=update_dict, args = (_pieceID, _tag, self.rclient))
                        t.daemon = True
                        t.start()

    def evt_fdt_pu_do(self, *args):
        _func = inspect.stack()[0][3]
        _pieceID = getPieceID(_func)
        #_dict[_pieceID] = {}
        _dict_time[_pieceID] = {}
        for arg in args:
            print(log_time(), f'<INFO> {_func} {self.evt_type[arg]}')
            if arg:
                if len(eventDataTag(_func))>0:
                    for _tag in eventDataTag(_func):
                        t = threading.Thread(target=update_dict, args = (_pieceID, _tag, self.rclient))
                        t.daemon = True
                        t.start()

    def evt_ct_pu_do(self, *args):
        _func = inspect.stack()[0][3]
        _pieceID = getPieceID(_func)
        #_dict[_pieceID] = {}
        _dict_time[_pieceID] = {}
        for arg in args:
            print(log_time(), f'<INFO> {_func} {self.evt_type[arg]}')
            if arg:
                if len(eventDataTag(_func))>0:
                    for _tag in eventDataTag(_func):
                        t = threading.Thread(target=update_dict, args = (_pieceID, _tag, self.rclient))
                        t.daemon = True
                        t.start()

    def evt_dc_pu_do(self, *args):
        _func = inspect.stack()[0][3]
        _pieceID = getPieceID(_func)
        #_dict[_pieceID] = {}
        _dict_time[_pieceID] = {}
        for arg in args:
            print(log_time(), f'<INFO> {_func} {self.evt_type[arg]}')
            if arg:
                if len(eventDataTag(_func))>0:
                    for _tag in eventDataTag(_func):
                        t = threading.Thread(target=update_dict, args = (_pieceID, _tag, self.rclient))
                        t.daemon = True
                        t.start()

    def evt_coil_complete(self, *args):
        _func = inspect.stack()[0][3]
        _pieceID = getPieceID(_func)
        #_dict[_pieceID] = {}
        _dict_time[_pieceID] = {}
        for arg in args:
            print(log_time(), f'<INFO> {_func} {self.evt_type[arg]}')
            if arg:
                if len(eventDataTag(_func))>0:
                    for _tag in eventDataTag(_func):
                        t = threading.Thread(target=update_dict, args = (_pieceID, _tag, self.rclient))
                        t.daemon = True
                        t.start()
        
        with open('data.json', 'w') as fp:
            json.dump(_dict, fp)
        get_pdo(_pieceID)

    def evt_coil_weigh(self, *args):
        _func = inspect.stack()[0][3]
        _pieceID = getPieceID(_func)
        #_dict[_pieceID] = {}
        _dict_time[_pieceID] = {}
        for arg in args:
            print(log_time(), f'<INFO> {_func} {self.evt_type[arg]}')
            if arg:
                if len(eventDataTag(_func))>0:
                    for _tag in eventDataTag(_func):
                        t = threading.Thread(target=update_dict, args = (_pieceID, _tag, self.rclient))
                        t.daemon = True
                        t.start()
                    #print(_dict)
        update_weight(_pieceID)

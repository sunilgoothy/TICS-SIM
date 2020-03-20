from TICSUtil import *
import random, redis, inspect, threading, time, json, os
from datetime import datetime, timedelta
from models import PDI, PDO, r_Shift_Record, session
from sqlalchemy import func, desc
from threading import Thread


_dict = dict()
_dict_time = dict()
rclient = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses = True)
_pdiTag = csvToDic('pdi_config.csv')
_pdoTag = csvToDic('pdo_config.csv')
_shiftConfig = csvToDic2('shift_config.csv')

def get_pdi(pieceID):
    _pdidata_dict = {}
    _pdiData = session.query(PDI).filter_by(c_SlabID=pieceID).limit(1).all()

    if _pdiData != []:
        for record in _pdiData:
            #print('pdi: {}'.format(record))
            _pdidata_dict = record.__dict__
    else:
        print(f'pdi not exits for pieceID:{str(pieceID)}')
    return _pdidata_dict

def write_pdi(pieceID):
    print('pieceID: {}'.format(pieceID))
    _pdidata_dict = get_pdi(pieceID)
    _pdivalidTag = 'ns=2;s=SimChannel.Device.Boolean.L_b_Tag1'
    global _pdiTag

    if _pdidata_dict != {}:
        for record in _pdiTag:
            _data = float(_pdidata_dict[record])
            #print('writing tag {0} with value {1}'.format( _pdiTag[record],_data))
            rclient.hset('tagwrite', _pdiTag[record], _data)
        rclient.hset('tagwrite', _pdivalidTag, 1)
        print(log_time(), f'<INFO> PDI received by L1 for pieceID {str(pieceID)}')
        session.query(PDI).filter_by(c_SlabID=pieceID).update({'c_SlabLoc': 'EXT'})
        session.commit()
    else:
        rclient.hset('tagwrite', _pdivalidTag, 0)
        print(log_time(), f'<ERR> PDI does not exist for pieceID {str(pieceID)}')

def get_pdo(pieceID):
    print(log_time(), f'<INFO> get_pdo for pieceID: {str(pieceID)}')
    global _pdoTag
    global _shiftConfig

    _pieceData = _dict[pieceID]
    _pdoData = {}
    _pdiData = get_pdi(pieceID)
    print(log_time(), f'<INFO> check signal data')
    for _pdoField in _pdoTag:
        try:
            _pdoData[_pdoField] = float(_pieceData[_pdoTag[_pdoField]]['value'])
        except Exception as e:
            print(e)
            _pdoData[_pdoField] = 0
    try:
        _pdoData['gt_PieceProductionTimeStart'] = datetime.strptime(_pieceData['start_time'], '%Y-%m-%d %H:%M:%S.%f')
    except Exception as e:
        print(e)
        _pdoData['gt_PieceProductionTimeStart'] = datetime.now()
    print(log_time(), f'<INFO> copy pdi data')
    f_PieceWeightCalc = float(_pdiData['f_SlabWt']) * 0.98
    gt_PieceProductionTime = datetime.now()
    
    # get i_ShiftIndex
    i_ShiftIndex = 0
    for record in _shiftConfig:
        if gt_PieceProductionTime.hour >= int(_shiftConfig[record]['shift_start_hr']) and \
             gt_PieceProductionTime.hour < int(_shiftConfig[record]['shift_end_hr']):
            
            i_ShiftIndex = int(record)
        elif gt_PieceProductionTime.hour >= int(_shiftConfig[record]['shift_start_hr']) and \
            int(_shiftConfig[record]['shift_start_hr']) > int(_shiftConfig[record]['shift_end_hr']):

            i_ShiftIndex = int(record)

        elif gt_PieceProductionTime.hour < int(_shiftConfig[record]['shift_end_hr']) and \
            int(_shiftConfig[record]['shift_start_hr']) > int(_shiftConfig[record]['shift_end_hr']):

            i_ShiftIndex = int(record)

    # create i_PieceIndex
    i_PieceIndex = 1
    _maxPieceIndex = session.query(PDO).order_by(desc(PDO.i_PieceIndex)).limit(1).all()
    for record in _maxPieceIndex:
        i_PieceIndex = record.__dict__['i_PieceIndex'] + 1 

    # write all data to pdo table
    newPDO = PDO(
            i_PieceIndex = i_PieceIndex , \
            i_PieceID = pieceID , \
            i_ShiftIndex =i_ShiftIndex , \
            gt_PieceProductionTimeStart = _pdoData['gt_PieceProductionTimeStart'] , \
            gt_PieceProductionTimeEnd =  gt_PieceProductionTime , \
            gt_PieceProductionTime = gt_PieceProductionTime , \
            f_RMDelTempAve =  _pdoData['f_RMDelTempAve'] , \
            f_FMDelThkAve =  _pdoData['f_FMDelThkAve'] , \
            f_FMDelWidthAve  =  _pdoData['f_FMDelWidthAve'] , \
            f_FMEntTempAve =  _pdoData['f_FMEntTempAve'] , \
            f_FMDelTempAve =  _pdoData['f_FMDelTempAve'] , \
            f_DCTempAve =  _pdoData['f_DCTempAve'], \
            f_PieceWeightCalc = f_PieceWeightCalc
    )
    session.add(newPDO)
    session.commit()

def update_weight(pieceID):
    print(log_time(), f'<INFO> update_weight')
    f_PieceWeightMeas = float(_dict[pieceID]['ns=2;s=SimChannel.Device.Float.L_f_Tag12']['value'])
    session.query(PDO).filter_by(i_PieceID=pieceID).update({'f_PieceWeightMeas': f_PieceWeightMeas})
    session.commit()

def getPieceID(_func):
    tag_pieceID = 'ns=2;s=SimChannel.Device.Integer.L_i_Tag1'
    _pieceID = rclient.hget('tagread', tag_pieceID)
    return _pieceID

def update_dict(_pieceID, _tag, rclient, _func):
    global _dict
    if _func=='evt_extract_req':
        _tagread = {'start_time' : str(datetime.now())}
    else:
        time.sleep(0.5)
        _tagread = {_tag : {'value':rclient.hget('tagread', _tag), 'time':str(datetime.now())}}

    if _pieceID not in _dict:
        _dict[_pieceID] = {}
    _tmpDict = _dict[_pieceID]

    for item in _tagread:
        _tmpDict[item]=_tagread[item]

    _dict[_pieceID] = _tmpDict
    if _func == 'evt_coil_weigh':
        update_weight(_pieceID)
        root = ".\data"
        filename = os.path.join(root,'piecedata_'+str(_pieceID)+'.json')
        with open(filename , 'w') as fp:
            json.dump(_dict[_pieceID], fp)

def shift_report():
    global _shiftConfig
    _startTime = []
    try:
        i_ShiftIndex = 0
        _shift_record = session.query(r_Shift_Record).order_by(desc(r_Shift_Record.i_ShiftIndex)).limit(1).all()
        for record in _shift_record:
            i_ShiftIndex = record.__dict__['i_ShiftIndex']
    except:
        print(log_time(), f'<ERR> Failed to load shift data from db')

    for record in _shiftConfig:
        _startTime.append(int(_shiftConfig[record]['shift_start_hr']))
    while True:
        _curTime = datetime.now()
        if _curTime.hour in _startTime and _curTime.minute == 23:
            i_ShiftIndex = i_ShiftIndex + 1
            print(log_time(), f'<INFO> Prepare shift report')
            for record in _shiftConfig:
                if int(_shiftConfig[record]['shift_end_hr']) == _curTime.hour:
                    gt_ShiftEndTime = datetime.strptime(str(datetime.now().date()) + ' ' + \
                        _shiftConfig[record]['end_time'] , '%Y-%m-%d %H:%M:%S')
                    gt_ShiftStartTime = gt_ShiftEndTime - timedelta(hours=8)
                    c_ShiftName = _shiftConfig[record]['shift_name']
                    c_ShiftCrew = _shiftConfig[record]['shift_name']
                    i_ShiftNumber = int(record)
                    
                    # first piece records
                    _shift_data = session.query(PDO).filter(PDO.gt_PieceProductionTime > gt_ShiftStartTime, \
                        PDO.gt_PieceProductionTime < gt_ShiftEndTime).order_by(PDO.gt_PieceProductionTime).limit(1).all()
                    for record in _shift_data:
                        _firstPiece = record.__dict__
                    
                    # last piece records
                    _shift_data = session.query(PDO).filter(PDO.gt_PieceProductionTime > gt_ShiftStartTime, \
                        PDO.gt_PieceProductionTime < gt_ShiftEndTime).order_by(desc(PDO.gt_PieceProductionTime)).limit(1).all()
                    for record in _shift_data:
                        _lastPiece = record.__dict__
                    
                    # shift tonnage
                    _shift_data = session.query(PDO).filter(PDO.gt_PieceProductionTime > gt_ShiftStartTime, \
                        PDO.gt_PieceProductionTime < gt_ShiftEndTime).all()
                    f_TotalPieceAllTons =  0
                    for record in _shift_data:
                        f_TotalPieceAllTons += float(record.f_PieceWeightCalc)
                    f_TotalPieceAllTons = f_TotalPieceAllTons / 1000  # converting to Tons

                    # write all data to db
                    newShiftRecord = r_Shift_Record(
                                    i_ShiftIndex = i_ShiftIndex, \
                                    gt_ShiftStartTime = gt_ShiftStartTime , \
                                    gt_ShiftEndTime = gt_ShiftEndTime , \
                                    c_ShiftName = c_ShiftName , \
                                    c_ShiftCrew = c_ShiftCrew , \
                                    i_ShiftNumber = i_ShiftNumber, \
                                    i_ShiftLengthSeconds = (gt_ShiftEndTime-gt_ShiftStartTime).total_seconds(), \
                                    i_PieceIndexFirst = _firstPiece['i_PieceID'] , \
                                    gt_PieceProductionTimeFirst = _firstPiece['gt_PieceProductionTime'] ,\
                                    i_PieceIndexLast = _lastPiece['i_PieceID'] , \
                                    gt_PieceProductionTimeLast = _lastPiece['gt_PieceProductionTime'] , \
                                    f_TotalPieceAllTons = f_TotalPieceAllTons
                    )
                    session.add(newShiftRecord)
                    session.commit()
        time.sleep(60)
class TICSEvents():

    def __init__(self):
        self.rclient = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses = True)
        self.evt_type = {0:'DROP', 1:'PICKUP'}
        t = threading.Thread(target=shift_report)
        t.daemon = True
        t.start()

    def evt_extract_req(self, *args):
        _func = inspect.stack()[0][3]
        _pieceID = getPieceID(_func)
        for arg in args:
            print(log_time(), f'<INFO> {_func} {self.evt_type[arg]}')
        write_pdi(_pieceID)
        t = threading.Thread(target=update_dict, args = (_pieceID, '', self.rclient, _func))
        t.daemon = True
        t.start()

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
                        t = threading.Thread(target=update_dict, args = (_pieceID, _tag, self.rclient, _func))
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
                        t = threading.Thread(target=update_dict, args = (_pieceID, _tag, self.rclient, _func))
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
                        t = threading.Thread(target=update_dict, args = (_pieceID, _tag, self.rclient, _func))
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
                        t = threading.Thread(target=update_dict, args = (_pieceID, _tag, self.rclient, _func))
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
                        t = threading.Thread(target=update_dict, args = (_pieceID, _tag, self.rclient, _func))
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
                        t = threading.Thread(target=update_dict, args = (_pieceID, _tag, self.rclient, _func))
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
                        t = threading.Thread(target=update_dict, args = (_pieceID, _tag, self.rclient, _func))
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
                        t = threading.Thread(target=update_dict, args = (_pieceID, _tag, self.rclient, _func))
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
                        t = threading.Thread(target=update_dict, args = (_pieceID, _tag, self.rclient, _func))
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
                        t = threading.Thread(target=update_dict, args = (_pieceID, _tag, self.rclient, _func))
                        t.daemon = True
                        t.start()
        
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
                        print("start evt_coil_weigh update")
                        t = threading.Thread(target=update_dict, args = (_pieceID, _tag, self.rclient, _func))
                        t.daemon = True
                        t.start()

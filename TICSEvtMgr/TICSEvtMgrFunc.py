from datetime import datetime, timedelta
import configparser, os, csv, ast, redis, sys, json, time, threading
from models import PDI, PDO, r_Shift_Record, session
from sqlalchemy import func, desc
from threading import Thread
from TICSUtil import *

class TICSEvtMgrFunc:
    def __init__(self):
        self._Datadict = dict()
        #self._dict_time = dict()
        try:
            self.rclient = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses = True)
            self._pdiTag = csvToDic('pdi_config.csv')
            self._pdoTag = csvToDic('pdo_config.csv')
            self._shiftConfig = csvToDic2('shift_config.csv')
        except Exception as e:
            print(log_time(), f'<ERR> Error in TICSEvtMgrFunc initialization, msg: {e}')
    def get_pdi(self, pieceID):
        _pdidata_dict = {}
        _pdiData = session.query(PDI).filter_by(c_SlabID=pieceID).limit(1).all()

        if _pdiData != []:
            for record in _pdiData:
                #print('pdi: {}'.format(record))
                _pdidata_dict = record.__dict__
        else:
            print(log_time(), f'<ERR> pdi not exits for pieceID:{str(pieceID)}')
        return _pdidata_dict

    def write_pdi(self, pieceID):
        #print('pieceID: {}'.format(pieceID))
        _pdidata_dict = self.get_pdi(pieceID)
        _pdivalidTag = 'ns=2;s=SimChannel.Device.Boolean.C_b_Tag1'
        self.rclient.hset('tagwrite', _pdivalidTag, 1)
        if _pdidata_dict != {}:
            for record in self._pdiTag:
                try:
                    _data = float(_pdidata_dict[record])
                    self.rclient.hset('tagwrite', self._pdiTag[record], _data)
                except Exception as e:
                    print(log_time(), f'<ERR> Error in getting PDI data Piecename:{pieceID} for pdifield:{record}, error msg:{e}')
                    self.rclient.hset('tagwrite', _pdivalidTag, 0)
            print(log_time(), f'<INFO> PDI received by L1 for pieceID {str(pieceID)}')
            try:
                session.query(PDI).filter_by(c_SlabID=pieceID).update({'c_SlabLoc': 'EXT'})
                session.commit()
            except Exception as e:
                print(log_time(), f'<ERR> Error in setting Piece location set in PDI, error msg: {e}')

        else:
            self.rclient.hset('tagwrite', _pdivalidTag, 0)
            print(log_time(), f'<ERR> PDI does not exist for pieceID {str(pieceID)}')

    def get_pdo(self, pieceID, _func):
        print(log_time(), f'<INFO> get_pdo for pieceID: {str(pieceID)}')

        # create i_PieceIndex
        i_PieceIndex = 1
        _maxPieceIndex = session.query(PDO).order_by(desc(PDO.i_PieceIndex)).limit(1).all()
        for record in _maxPieceIndex:
            i_PieceIndex = record.__dict__['i_PieceIndex'] + 1 

        # set reject/cobble flag
        if _func == 'evt_reject':
            b_RejectFlag = 1
        else:
            b_RejectFlag = 0

        if _func == 'evt_cobble':
            b_CobbleFlag = 1
        else:
            b_CobbleFlag = 0
        
        # get realtime data
        if pieceID in self._Datadict:
            _pieceData = self._Datadict[pieceID]
        else:
            print(log_time(), f'<ERR> No sensor data collected for PDO') 
            _pieceData = {}
        _pdoData = {}
        _pdiData = self.get_pdi(pieceID)
        print(log_time(), f'<INFO> check signal data')
        for _pdoField in self._pdoTag:
            try:
                _pdoData[_pdoField] = float(_pieceData[self._pdoTag[_pdoField]]['value'])
            except Exception as e:
                print(log_time(), f'<ERR> Error in PDO field: {_pdoField},  msg: {e}')
                _pdoData[_pdoField] = 0
        try:
            _pdoData['gt_PieceProductionTimeStart'] = datetime.strptime(_pieceData['start_time'], '%Y-%m-%d %H:%M:%S.%f')
        except Exception as e:
            print(log_time(), f'<ERR> Error in getting start_time, msg: {e}')
            _pdoData['gt_PieceProductionTimeStart'] = datetime.now()
        
        # copy pdi data
        print(log_time(), f'<INFO> copy pdi data')
        f_PieceWeightCalc = float(_pdiData['f_SlabWt']) * 0.98
        
        # get i_ShiftIndex
        gt_PieceProductionTime = datetime.now()
        
        i_ShiftIndex = 0
        for record in self._shiftConfig:
            if gt_PieceProductionTime.hour >= int(self._shiftConfig[record]['shift_start_hr']) and \
                gt_PieceProductionTime.hour < int(self._shiftConfig[record]['shift_end_hr']):
                #print('cond:1')
                i_ShiftIndex = int(record)
                _shiftstart_time = self._shiftConfig[record]['start_time']
                _shiftend_time = self._shiftConfig[record]['end_time']
            elif gt_PieceProductionTime.hour >= int(self._shiftConfig[record]['shift_start_hr']) and \
                int(self._shiftConfig[record]['shift_start_hr']) > int(self._shiftConfig[record]['shift_end_hr']):
                #print('cond:2')
                i_ShiftIndex = int(record)
                _shiftstart_time = self._shiftConfig[record]['start_time']
                _shiftend_time = self._shiftConfig[record]['end_time']
            elif gt_PieceProductionTime.hour < int(self._shiftConfig[record]['shift_end_hr']) and \
                int(self._shiftConfig[record]['shift_start_hr']) > int(self._shiftConfig[record]['shift_end_hr']):
                #print('cond:3')
                i_ShiftIndex = int(record)
                _shiftstart_time = self._shiftConfig[record]['start_time']
                _shiftend_time = self._shiftConfig[record]['end_time']
        #print(f'{_shiftstart_time} {_shiftend_time}' )
        gt_ShiftStartTime = datetime.strptime(str(gt_PieceProductionTime.date()) + ' ' + \
                        _shiftstart_time , '%Y-%m-%d %H:%M:%S')
        if datetime.strptime(_shiftend_time , '%H:%M:%S') > datetime.strptime(_shiftstart_time , '%H:%M:%S'):
            i_ShiftLengthSeconds = (datetime.strptime(_shiftend_time , '%H:%M:%S')-datetime.strptime(_shiftstart_time , '%H:%M:%S')).total_seconds()
        else:
            i_ShiftLengthSeconds = (datetime.strptime(_shiftstart_time , '%H:%M:%S')-datetime.strptime(_shiftend_time , '%H:%M:%S')).total_seconds()
        # write all data to pdo table
        newPDO = PDO(
                i_PieceIndex = i_PieceIndex , \
                i_PieceID = pieceID , \
                i_ShiftIndex =i_ShiftIndex , \
                gt_PieceProductionTimeStart = _pdoData['gt_PieceProductionTimeStart'] , \
                gt_PieceProductionTimeEnd =  gt_PieceProductionTime , \
                gt_PieceProductionTime = gt_PieceProductionTime , \
                gt_ShiftStartTime = gt_ShiftStartTime ,\
                i_ShiftLengthSeconds = i_ShiftLengthSeconds ,\
                f_RMDelTempAve =  _pdoData['f_RMDelTempAve'] , \
                f_FMDelThkAve =  _pdoData['f_FMDelThkAve'] , \
                f_FMDelWidthAve  =  _pdoData['f_FMDelWidthAve'] , \
                f_FMEntTempAve =  _pdoData['f_FMEntTempAve'] , \
                f_FMDelTempAve =  _pdoData['f_FMDelTempAve'] , \
                f_DCTempAve =  _pdoData['f_DCTempAve'], \
                f_PieceWeightCalc = f_PieceWeightCalc , \
                b_RejectFlag = b_RejectFlag ,\
                b_CobbleFlag = b_CobbleFlag
        )
        session.add(newPDO)
        session.commit()
        with open('data.json' , 'w') as fp:
            json.dump(self._Datadict, fp)

    def update_weight(self, pieceID):
        print(log_time(), f'<INFO> update_weight')
        f_PieceWeightMeas = float(self._Datadict[pieceID]['ns=2;s=SimChannel.Device.Float.L_f_Tag12']['value'])
        session.query(PDO).filter_by(i_PieceID=pieceID).update({'f_PieceWeightMeas': f_PieceWeightMeas})
        session.commit()

    def getPieceID(self, _func):
        tag_pieceID = 'ns=2;s=SimChannel.Device.Integer.L_i_Tag1'
        _pieceID = self.rclient.hget('tagread', tag_pieceID)
        return _pieceID

    def clear_dict(self):
        try:
            while True:
                _dict_count = 0
                for _key in self._Datadict:
                    _temp_dt = datetime.strptime(self._Datadict[_key]['start_time'], '%Y-%m-%d %H:%M:%S.%f')
                    if _dict_count == 0:
                        _oldest_key = _key
                        _oldest_dt = _temp_dt
                    else:
                        if _oldest_dt > _temp_dt:
                            _oldest_key = _key
                            _oldest_dt = _temp_dt
                    _dict_count+=1
                if _dict_count > 2:
                    print(log_time(), f'<INFO> Clearing Dictionary having key: {_dict_count} deleting pieceID: {_oldest_key}')
                    self._Datadict.pop(_oldest_key, None)
                else:
                    break
            #print(log_time(), f'<INFO> Clear piece data dictionary')
        except Exception as e:
            print(log_time(), f'<ERR> Error in clear_dict: {e}')

    def update_dict(self,_pieceID, _tag, _func):
        if _func=='evt_extract_req':
            _tagread = {'start_time' : str(datetime.now())}
        else:
            time.sleep(1)
            _tagread = {_tag : {'value':self.rclient.hget('tagread', _tag), 'time':str(datetime.now())}}

        if _pieceID not in self._Datadict:
            self._Datadict[_pieceID] = {}
        _tmpDict = self._Datadict[_pieceID]

        for item in _tagread:
            _tmpDict[item]=_tagread[item]

        self._Datadict[_pieceID] = _tmpDict
        if _func == 'evt_coil_weigh':
            self.update_weight(_pieceID)
            root = ".\data"
            filename = os.path.join(root,'piecedata_'+str(_pieceID)+'.json')
            with open(filename , 'w') as fp:
                json.dump(self._Datadict[_pieceID], fp)
    
    def get_current_shift(self, _time):
        i_ShiftIndex = 0

        for record in self._shiftConfig:
            if _time.hour >= int(self._shiftConfig[record]['shift_start_hr']) and \
                _time.hour < int(self._shiftConfig[record]['shift_end_hr']):

                c_ShiftName = self._shiftConfig['shift_name']
                i_ShiftIndex = int(record)
            elif _time.hour >= int(self._shiftConfig[record]['shift_start_hr']) and \
                int(self._shiftConfig[record]['shift_start_hr']) > int(self._shiftConfig[record]['shift_end_hr']):

                c_ShiftName = self._shiftConfig['shift_name']
                i_ShiftIndex = int(record)

            elif _time.hour < int(self._shiftConfig[record]['shift_end_hr']) and \
                int(self._shiftConfig[record]['shift_start_hr']) > int(self._shiftConfig[record]['shift_end_hr']):

                c_ShiftName = self._shiftConfig['shift_name']
                i_ShiftIndex = int(record)
        return {'i_ShiftIndex':i_ShiftIndex , 'c_ShiftName':c_ShiftName}
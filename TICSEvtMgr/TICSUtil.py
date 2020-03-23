from datetime import datetime
import configparser, os, csv, ast, redis, sys, json, time, threading
from models import PDI, PDO, r_Shift_Record, session
from sqlalchemy import func, desc
from threading import Thread

def log_time():
    """ Returns date time with ms. Can be used for logging messages"""
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

def readconfigfile(filename,section,key):
    root = ".\TICSEvtMgr"
    filename = os.path.join(root,filename)
    config = configparser.ConfigParser()
    config.read(filename,  encoding='utf-8')
    return config.get(section, key)

def eventDataTag(eventname):
    filename = 'tics_events_data.csv'
    _list = list()
    try:
        root = ".\TICSEvtMgr"
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

def csvToDic(filename):
    #print(filename)
    tag_dict = dict()
    try:
        root = ".\TICSEvtMgr"
        filename = os.path.join(root,filename)
        with open(filename) as tags_file:
            csv_reader = csv.DictReader(tags_file, delimiter=',')
            line_count = 0
            for row in csv_reader:
                if line_count == 0:
                    _key = list(row.keys())
                    #print(_key)
                commented_line = row[_key[0]].startswith('#')
                if ( not commented_line ):
                    tag_dict[row[_key[0]]]=row[_key[1]]
                line_count += 1
            #print(f'<INFO> csvToDic Processed {line_count} lines from {filename}.')

    except Exception as e:
        print(e)
    return tag_dict

def csvToDic2(filename):
    #print(filename)
    tag_dict = dict()
    try:
        root = ".\TICSEvtMgr"
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

class TICSEvtMgrFunc:
    def __init__(self):
        self._Datadict = dict()
        self._dict_time = dict()
        self.rclient = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses = True)
        self._pdiTag = csvToDic('pdi_config.csv')
        self._pdoTag = csvToDic('pdo_config.csv')
        self._shiftConfig = csvToDic2('shift_config.csv')

    def get_pdi(self, pieceID):
        _pdidata_dict = {}
        _pdiData = session.query(PDI).filter_by(c_SlabID=pieceID).limit(1).all()

        if _pdiData != []:
            for record in _pdiData:
                #print('pdi: {}'.format(record))
                _pdidata_dict = record.__dict__
        else:
            print(f'pdi not exits for pieceID:{str(pieceID)}')
        return _pdidata_dict

    def write_pdi(self, pieceID):
        print('pieceID: {}'.format(pieceID))
        _pdidata_dict = self.get_pdi(pieceID)
        _pdivalidTag = 'ns=2;s=SimChannel.Device.Boolean.L_b_Tag1'

        if _pdidata_dict != {}:
            for record in self._pdiTag:
                _data = float(_pdidata_dict[record])
                #print('writing tag {0} with value {1}'.format( self._pdiTag[record],_data))
                self.rclient.hset('tagwrite', self._pdiTag[record], _data)
            self.rclient.hset('tagwrite', _pdivalidTag, 1)
            print(log_time(), f'<INFO> PDI received by L1 for pieceID {str(pieceID)}')
            session.query(PDI).filter_by(c_SlabID=pieceID).update({'c_SlabLoc': 'EXT'})
            session.commit()
        else:
            self.rclient.hset('tagwrite', _pdivalidTag, 0)
            print(log_time(), f'<ERR> PDI does not exist for pieceID {str(pieceID)}')

    def get_pdo(self, pieceID):
        print(log_time(), f'<INFO> get_pdo for pieceID: {str(pieceID)}')

        _pieceData = self._Datadict[pieceID]
        _pdoData = {}
        _pdiData = self.get_pdi(pieceID)
        print(log_time(), f'<INFO> check signal data')
        for _pdoField in self._pdoTag:
            try:
                _pdoData[_pdoField] = float(_pieceData[self._pdoTag[_pdoField]]['value'])
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
        for record in self._shiftConfig:
            if gt_PieceProductionTime.hour >= int(self._shiftConfig[record]['shift_start_hr']) and \
                gt_PieceProductionTime.hour < int(self._shiftConfig[record]['shift_end_hr']):
                
                i_ShiftIndex = int(record)
            elif gt_PieceProductionTime.hour >= int(self._shiftConfig[record]['shift_start_hr']) and \
                int(self._shiftConfig[record]['shift_start_hr']) > int(self._shiftConfig[record]['shift_end_hr']):

                i_ShiftIndex = int(record)

            elif gt_PieceProductionTime.hour < int(self._shiftConfig[record]['shift_end_hr']) and \
                int(self._shiftConfig[record]['shift_start_hr']) > int(self._shiftConfig[record]['shift_end_hr']):

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

    def update_weight(self, pieceID):
        print(log_time(), f'<INFO> update_weight')
        f_PieceWeightMeas = float(self._Datadict[pieceID]['ns=2;s=SimChannel.Device.Float.L_f_Tag12']['value'])
        session.query(PDO).filter_by(i_PieceID=pieceID).update({'f_PieceWeightMeas': f_PieceWeightMeas})
        session.commit()

    def getPieceID(self, _func):
        tag_pieceID = 'ns=2;s=SimChannel.Device.Integer.L_i_Tag1'
        _pieceID = self.rclient.hget('tagread', tag_pieceID)
        return _pieceID

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


class ShiftUpdate:
    def __init__(self):
        self._shiftConfig = csvToDic2('shift_config.csv')

    def shift_report(self):

        _startTime = []
        try:
            i_ShiftIndex = 0
            _shift_record = session.query(r_Shift_Record).order_by(desc(r_Shift_Record.i_ShiftIndex)).limit(1).all()
            for record in _shift_record:
                i_ShiftIndex = record.__dict__['i_ShiftIndex']
        except:
            print(log_time(), f'<ERR> Failed to load shift data from db')

        for record in self._shiftConfig:
            _startTime.append(int(self._shiftConfig[record]['shift_start_hr']))
        while True:
            _curTime = datetime.now()
            if _curTime.hour in _startTime and _curTime.minute == 0:
                i_ShiftIndex = i_ShiftIndex + 1
                print(log_time(), f'<INFO> Prepare shift report')
                for record in self._shiftConfig:
                    if int(self._shiftConfig[record]['shift_end_hr']) == _curTime.hour:
                        gt_ShiftEndTime = datetime.strptime(str(datetime.now().date()) + ' ' + \
                            self._shiftConfig[record]['end_time'] , '%Y-%m-%d %H:%M:%S')
                        gt_ShiftStartTime = gt_ShiftEndTime - timedelta(hours=8)
                        c_ShiftName = self._shiftConfig[record]['shift_name']
                        c_ShiftCrew = self._shiftConfig[record]['shift_name']
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
    if __name__ == '__main__':
        pass
from datetime import datetime, timedelta
import configparser, os, csv, ast, redis, sys, json, time, threading
from models import PDI, PDO, r_Shift_Record, session
from sqlalchemy import func, desc
from threading import Thread
from TICSUtil import *

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
                        
                        # shift tonnage, piece count
                        _shift_data = session.query(PDO).filter(PDO.gt_PieceProductionTime > gt_ShiftStartTime, \
                            PDO.gt_PieceProductionTime < gt_ShiftEndTime).all()
                        f_TotalPieceAllTons =  0
                        i_TotalParentCount = 0
                        i_TotalPieceAllCount = 0
                        for record in _shift_data:
                            f_TotalPieceAllTons += float(record.f_PieceWeightCalc)
                            i_TotalParentCount += 1
                            i_TotalPieceAllCount += 1
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
                                        f_TotalPieceAllTons = f_TotalPieceAllTons , \
                                        i_TotalParentCount = i_TotalParentCount , \
                                        i_TotalPieceAllCount = i_TotalPieceAllCount
                        )
                        session.add(newShiftRecord)
                        session.commit()
            time.sleep(60)
    if __name__ == '__main__':
        pass
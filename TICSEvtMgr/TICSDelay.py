from datetime import datetime
import configparser, os, csv, ast, redis, sys, json, time, threading
from models import PDI, PDO, r_Shift_Record, r_Delay_Record, session
from sqlalchemy import func, desc
from threading import Thread
from TICSUtil import *

class TICSDelay:

    def __init__(self):
        self._last_discharged = datetime.now()
        self._delay_start = datetime.now()
        self._delay_stop = datetime.now()
        self.delay = 0
        self.set_interval = 180
        self.delay_active = False
        self._shiftConfig = csvToDic2('shift_config.csv')
        self._curPiece = 0
        self._prevPiece = 0
        t = threading.Thread(target=self.wait_interval)
        t.daemon = True
        t.start()

    def start_delay(self):
        if self.delay == 0:
            self._delay_start = datetime.now()
            self.delay_active = True
        self.delay += 1

    def stop_delay(self):
        self._delay_stop = datetime.now()
        self.delay_active = False
        print(f'last delay: {str(self.delay)} , started: {str(self._delay_start)}, ended: {str(self._delay_stop)}' )
        
    def reset_delay(self):
        self.delay = 0

    def delay_monitor(self, _pieceID):
        _curTime = datetime.now()
        self._curPiece = _pieceID
        if self.delay > 0:
            self.stop_delay()
            self.add_record(_curTime)
            self.reset_delay()
        self._prevPiece = _pieceID
        self._last_discharged = datetime.now()


    def wait_interval(self):
        count = 0
        while count <= 9999:
            count+=1
            time.sleep(1)
            if (datetime.now() - self._last_discharged).total_seconds() >= self.set_interval:
                self.start_delay()
    
    def add_record(self, _curTime):

        _delay_record = session.query(r_Delay_Record).order_by(desc(r_Delay_Record.i_DelayIndex)).limit(1).all()
        i_DelayIndex = 0
        for record in _delay_record:
            i_DelayIndex = record.__dict__['i_DelayIndex']
        i_DelayIndex+=1
        newDelay = r_Delay_Record(
            i_DelayIndex = i_DelayIndex, \
            gt_DelayStartTime = self._delay_start , \
            gt_DelayEndTime = self._delay_stop ,\
            i_DelayLengthSecondsCalc = self.delay , \
            c_ShiftName = self.get_current_shift(_curTime)['c_ShiftName'] , \
            c_ShiftCrew = self.get_current_shift(_curTime)['c_ShiftName'] , \
            i_ShiftNumber = self.get_current_shift(_curTime)['i_ShiftIndex'] , \
            i_PieceIndexBefore = self._prevPiece , \
            i_PieceIndexAfter = self._curPiece , \
        )
        print('committing')
        session.add(newDelay)
        session.commit()

    def get_current_shift(self, _time):
        i_ShiftIndex = 0
        c_ShiftName = ''
        for record in self._shiftConfig:
            if _time.hour >= int(self._shiftConfig[record]['shift_start_hr']) and \
                _time.hour < int(self._shiftConfig[record]['shift_end_hr']):

                c_ShiftName = self._shiftConfig[record]['shift_name']
                i_ShiftIndex = int(record)
            elif _time.hour >= int(self._shiftConfig[record]['shift_start_hr']) and \
                int(self._shiftConfig[record]['shift_start_hr']) > int(self._shiftConfig[record]['shift_end_hr']):

                c_ShiftName = self._shiftConfig[record]['shift_name']
                i_ShiftIndex = int(record)

            elif _time.hour < int(self._shiftConfig[record]['shift_end_hr']) and \
                int(self._shiftConfig[record]['shift_start_hr']) > int(self._shiftConfig[record]['shift_end_hr']):

                c_ShiftName = self._shiftConfig[record]['shift_name']
                i_ShiftIndex = int(record)
        return {'i_ShiftIndex':i_ShiftIndex , 'c_ShiftName':c_ShiftName}

        


    




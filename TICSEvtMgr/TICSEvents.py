from TICSUtil import *
from TICSDelay import *
from TICSEvtMgrFunc import TICSEvtMgrFunc
from ShiftUpdate import ShiftUpdate
import random, redis, inspect, threading, time, json, os
from datetime import datetime, timedelta
from models import PDI, PDO, r_Shift_Record, session
from sqlalchemy import func, desc
from threading import Thread


class TICSEvents():

    def __init__(self):
        self.rclient = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses = True)
        self.evt_type = {0:'DROP', 1:'PICKUP'}
        self.evt_Func = TICSEvtMgrFunc()
        self.shift_Func = ShiftUpdate()
        self.delay_Func = TICSDelay()
        t = threading.Thread(target=self.shift_Func.shift_report)
        t.daemon = True
        t.start()

    def evt_extract_req(self, *args):
        _func = inspect.stack()[0][3]
        self.evt_Func.clear_dict()
        _pieceID = self.evt_Func.getPieceID(_func)
        for arg in args:
            print(log_time(), f'<INFO> {_func} {self.evt_type[arg]}')
            if arg:
                self.evt_Func.write_pdi(_pieceID)
                t = threading.Thread(target=self.evt_Func.update_dict, args = (_pieceID, '', _func))
                t.daemon = True
                t.start()

    def evt_slab_discharged(self, *args):
        _func = inspect.stack()[0][3]
        _pieceID = self.evt_Func.getPieceID(_func)
        self.delay_Func.delay_monitor(_pieceID)
        for arg in args:
            print(log_time(), f'<INFO> {_func} {self.evt_type[arg]}')
            if arg:
                if len(eventDataTag(_func))>0:
                    for _tag in eventDataTag(_func):
                        t = threading.Thread(target=self.evt_Func.update_dict, args = (_pieceID, _tag, _func))
                        t.daemon = True
                        t.start()
     

    def evt_rmet_pu_do(self, *args):
        _func = inspect.stack()[0][3]
        _pieceID = self.evt_Func.getPieceID(_func)
        for arg in args:
            print(log_time(), f'<INFO> {_func} {self.evt_type[arg]}')
            if arg:
                if len(eventDataTag(_func))>0:
                    for _tag in eventDataTag(_func):
                        t = threading.Thread(target=self.evt_Func.update_dict, args = (_pieceID, _tag, _func))
                        t.daemon = True
                        t.start()

    def evt_rm_pu_do(self, *args):
        _func = inspect.stack()[0][3]
        _pieceID = self.evt_Func.getPieceID(_func)
        for arg in args:
            print(log_time(), f'<INFO> {_func} {self.evt_type[arg]}')
            if arg:
                if len(eventDataTag(_func))>0:
                    for _tag in eventDataTag(_func):
                        t = threading.Thread(target=self.evt_Func.update_dict, args = (_pieceID, _tag, _func))
                        t.daemon = True
                        t.start()

    def evt_rmdt_pu_do(self, *args):
        _func = inspect.stack()[0][3]
        _pieceID = self.evt_Func.getPieceID(_func)
        for arg in args:
            print(log_time(), f'<INFO> {_func} {self.evt_type[arg]}')
            if arg:
                if len(eventDataTag(_func))>0:
                    for _tag in eventDataTag(_func):
                        t = threading.Thread(target=self.evt_Func.update_dict, args = (_pieceID, _tag, _func))
                        t.daemon = True
                        t.start()
    
    def evt_fet_pu_do(self, *args):
        _func = inspect.stack()[0][3]
        _pieceID = self.evt_Func.getPieceID(_func)
        for arg in args:
            print(log_time(), f'<INFO> {_func} {self.evt_type[arg]}')
            if arg:
                if len(eventDataTag(_func))>0:
                    for _tag in eventDataTag(_func):
                        t = threading.Thread(target=self.evt_Func.update_dict, args = (_pieceID, _tag, _func))
                        t.daemon = True
                        t.start()

    def evt_fmd_pu_do(self, *args):
        _func = inspect.stack()[0][3]
        _pieceID = self.evt_Func.getPieceID(_func)
        for arg in args:
            print(log_time(), f'<INFO> {_func} {self.evt_type[arg]}')
            if arg:
                if len(eventDataTag(_func))>0:
                    for _tag in eventDataTag(_func):
                        t = threading.Thread(target=self.evt_Func.update_dict, args = (_pieceID, _tag, _func))
                        t.daemon = True
                        t.start()

    def evt_fdt_pu_do(self, *args):
        _func = inspect.stack()[0][3]
        _pieceID = self.evt_Func.getPieceID(_func)
        for arg in args:
            print(log_time(), f'<INFO> {_func} {self.evt_type[arg]}')
            if arg:
                if len(eventDataTag(_func))>0:
                    for _tag in eventDataTag(_func):
                        t = threading.Thread(target=self.evt_Func.update_dict, args = (_pieceID, _tag, _func))
                        t.daemon = True
                        t.start()

    def evt_ct_pu_do(self, *args):
        _func = inspect.stack()[0][3]
        _pieceID = self.evt_Func.getPieceID(_func)
        for arg in args:
            print(log_time(), f'<INFO> {_func} {self.evt_type[arg]}')
            if arg:
                if len(eventDataTag(_func))>0:
                    for _tag in eventDataTag(_func):
                        t = threading.Thread(target=self.evt_Func.update_dict, args = (_pieceID, _tag, _func))
                        t.daemon = True
                        t.start()

    def evt_dc_pu_do(self, *args):
        _func = inspect.stack()[0][3]
        _pieceID = self.evt_Func.getPieceID(_func)
        for arg in args:
            print(log_time(), f'<INFO> {_func} {self.evt_type[arg]}')
            if arg:
                if len(eventDataTag(_func))>0:
                    for _tag in eventDataTag(_func):
                        t = threading.Thread(target=self.evt_Func.update_dict, args = (_pieceID, _tag, _func))
                        t.daemon = True
                        t.start()

    def evt_coil_complete(self, *args):
        _func = inspect.stack()[0][3]
        _pieceID = self.evt_Func.getPieceID(_func)
        for arg in args:
            print(log_time(), f'<INFO> {_func} {self.evt_type[arg]}')
            if arg:
                if len(eventDataTag(_func))>0:
                    for _tag in eventDataTag(_func):
                        t = threading.Thread(target=self.evt_Func.update_dict, args = (_pieceID, _tag, _func))
                        t.daemon = True
                        t.start()
        
        self.evt_Func.get_pdo(_pieceID, _func)

    def evt_coil_weigh(self, *args):
        _func = inspect.stack()[0][3]
        _pieceID = self.evt_Func.getPieceID(_func)
        for arg in args:
            print(log_time(), f'<INFO> {_func} {self.evt_type[arg]}')
            if arg:
                if len(eventDataTag(_func))>0:
                    for _tag in eventDataTag(_func):
                        #print("start evt_coil_weigh update")
                        t = threading.Thread(target=self.evt_Func.update_dict, args = (_pieceID, _tag, _func))
                        t.daemon = True
                        t.start()

    def evt_reject(self, *args):
        _func = inspect.stack()[0][3]
        _pieceID = self.evt_Func.getPieceID(_func)
        for arg in args:
            print(log_time(), f'<INFO> {_func} {self.evt_type[arg]}')
            if arg:
                self.evt_Func.get_pdo(_pieceID, _func)
    
    def evt_cobble(self, *args):
        _func = inspect.stack()[0][3]
        _pieceID = self.evt_Func.getPieceID(_func)
        for arg in args:
            print(log_time(), f'<INFO> {_func} {self.evt_type[arg]}')
            if arg:
                self.evt_Func.get_pdo(_pieceID, _func)

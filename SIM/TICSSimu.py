from TICSUtil import *
import random, redis, inspect, time, threading

class TICSSimu:

    def __init__(self, _pieceID, rclient, _func, _pdi):
        self._pieceID = _pieceID
        self._simSignalConfig = csvToDic('tics_signal_sim_config.csv')
        self._func = _func
        self._pdi = _pdi
        #self._signal = _signal
        self.rclient = rclient
        for _signal in self._simSignalConfig:
            self.rclient.hset('tagwrite', _signal, 0)

    def simulate(self, _time, _signal):
        #print('simulate event: {0} list: {1}'.format(self._func,self._signal))
        #print(log_time(), f'<INFO> simulate')
        #_pdi_field
        try:
            _pdi_field = self._simSignalConfig[_signal]['update_source']
        except:
            _pdi_field = ''

        #_lo_limit
        try:
            _lo_limit =  float(self._simSignalConfig[_signal]['lo_limit'])
        except:
            _lo_limit = 0

        #_hi_limit
        try:
            _hi_limit = float(self._simSignalConfig[_signal]['hi_limit'])
        except:
            _hi_limit = 0

        #_offset
        try:   
            _offset = float(self._simSignalConfig[_signal]['offset'])
        except:
            _offset = 0

        # set opc signal
        if _pdi_field != '':
            if len(self._pdi)>0:
                _pdiVal = self._pdi[_pdi_field]
                _pdiVal = _pdiVal + _offset
            else:
                _pdiVal=0
            _count = 0
            _limit = int(_time * 10)
            while _count < _limit:
                value = random.uniform(_pdiVal*_lo_limit, _pdiVal*_hi_limit)
                self.rclient.hset('tagwrite', _signal, value)
                time.sleep(0.1)
                _count += 1
            self.rclient.hset('tagwrite', _signal, 0)

    def speedSim(self, _time, _type, _config):
        _speed_signal = 'ns=2;s=SimChannel.Device.Float.L_f_Tag1'
        _count = 0
        value = float(self.rclient.hget('tagread',_speed_signal))

        _maxspeed = float(_config['max_speed'])
        _minspeed = float(_config['min_speed'])
        _limit = int(_time * 10)
        while _count < _limit:
            value = value * float(_config[_type])
            if value > _maxspeed:
                value = _maxspeed
            if value < _minspeed:
                value = _minspeed
            self.rclient.hset('tagwrite', _speed_signal, value)
            time.sleep(0.1)
            _count += 1


    

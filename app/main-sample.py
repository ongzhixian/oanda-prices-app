# main2.py
# Based on https://www.oreilly.com/content/algorithmic-trading-in-less-than-100-lines-of-python-code/
# Reference: https://github.com/oanda/oandapy/blob/master/oandapy/oandapy.py

import configparser  # 1 
import oandapy as opy  # 2

config = configparser.ConfigParser()  # 3
config.read('oanda.cfg')  # 4

oanda = opy.API(environment='practice',
                access_token=config['oanda']['access_token'])  # 5
                
import pandas as pd  # 6

data = oanda.get_history(instrument='EUR_USD',  # our instrument
                         start='2016-12-08',  # start data
                         end='2016-12-10',  # end date
                         granularity='M1')  # minute bars  # 7

df = pd.DataFrame(data['candles']).set_index('time')  # 8

df.index = pd.DatetimeIndex(df.index)  # 9

df.info() # 10

import numpy as np  # 11

df['returns'] = np.log(df['closeAsk'] / df['closeAsk'].shift(1))  # 12

cols = []  # 13

for momentum in [15, 30, 60, 120]:  # 14
    col = 'position_%s' % momentum  # 15
    df[col] = np.sign(df['returns'].rolling(momentum).mean())  # 16
    cols.append(col)  # 17

%matplotlib inline
import seaborn as sns; sns.set()  # 18

strats = ['returns']  # 19

for col in cols:  # 20
    strat = 'strategy_%s' % col.split('_')[1]  # 21
    df[strat] = df[col].shift(1) * df['returns']  # 22
    strats.append(strat)  # 23

df[strats].dropna().cumsum().apply(np.exp).plot()  # 24



class MomentumTrader(opy.Streamer):  # 25
    def __init__(self, momentum, *args, **kwargs):  # 26
        opy.Streamer.__init__(self, *args, **kwargs)  # 27
        self.ticks = 0  # 28
        self.position = 0  # 29
        self.df = pd.DataFrame()  # 30
        self.momentum = momentum  # 31
        self.units = 100000  # 32
    def create_order(self, side, units):  # 33
        order = oanda.create_order(config['oanda']['account_id'], 
            instrument='EUR_USD', units=units, side=side,
            type='market')  # 34
        print('\n', order)  # 35
    def on_success(self, data):  # 36
        self.ticks += 1  # 37
        # print(self.ticks, end=', ')
        # appends the new tick data to the DataFrame object
        self.df = self.df.append(pd.DataFrame(data['tick'],
                                 index=[data['tick']['time']]))  # 38
        # transforms the time information to a DatetimeIndex object
        self.df.index = pd.DatetimeIndex(self.df['time'])  # 39
        # resamples the data set to a new, homogeneous interval
        dfr = self.df.resample('5s').last()  # 40
        # calculates the log returns
        dfr['returns'] = np.log(dfr['ask'] / dfr['ask'].shift(1))  # 41
        # derives the positioning according to the momentum strategy
        dfr['position'] = np.sign(dfr['returns'].rolling( 
                                      self.momentum).mean())  # 42
        if dfr['position'].ix[-1] == 1:  # 43
            # go long
            if self.position == 0:  # 44
                self.create_order('buy', self.units)  # 45
            elif self.position == -1:  # 46
                self.create_order('buy', self.units * 2)  # 47
            self.position = 1  # 48
        elif dfr['position'].ix[-1] == -1:  # 49
            # go short
            if self.position == 0:  # 50
                self.create_order('sell', self.units)  # 51
            elif self.position == 1: # 52
                self.create_order('sell', self.units * 2)  # 53
            self.position = -1  # 54
        if self.ticks == 250:  # 55
            # close out the position
            if self.position == 1:  # 56
                self.create_order('sell', self.units)  # 57
            elif self.position == -1:  # 58
                self.create_order('buy', self.units)  # 59
            self.disconnect()  # 60

mt = MomentumTrader(momentum=12, environment='practice', access_token=config['oanda']['access_token'])
mt.rates(account_id=config['oanda']['account_id'], instruments=['DE30_EUR'], ignore_heartbeat=True)
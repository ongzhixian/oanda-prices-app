import logging
from logger import Logger

import pandas as pd
import numpy as np
import seaborn as sns; sns.set()  # 18

from program_arguments import get_oanda_settings
from oanda_api import OandaApi

def setup_logging():
    logging.getLogger('pika').setLevel(logging.WARNING)
    log = Logger()
    return log

def flatten_ba_candles(data):
    f = []
    for record in data:
        fo = {
            'complete': bool(record['complete']), 
            'volume': int(record['volume']), 
            'time': record['time']
        }
        if 'bid' in record:
            fo['bidOpen'] = float(record['bid']['o'])
            fo['bidHigh'] = float(record['bid']['h'])
            fo['bidLow'] = float(record['bid']['l'])
            fo['bidClose'] = float(record['bid']['c'])
        if 'ask' in record:
            fo['askOpen'] = float(record['ask']['o'])
            fo['askHigh'] = float(record['ask']['h'])
            fo['askLow'] = float(record['ask']['l'])
            fo['askClose'] = float(record['ask']['c'])

        f.append(fo)
    return f

if __name__ == "__main__":
    log = setup_logging()
    oanda_settings = get_oanda_settings('C:\\users\\zhixian\\.oanda-config.json')
    oanda_api = OandaApi(oanda_settings, 'C:\\Users\\zhixian\\Dropbox\\myThinkBook\\oanda\\main2')

    data = oanda_api.get_historical_candles('EUR_USD', 'M1')
    # .get_history(instrument='EUR_USD',  # our instrument
    #                      start='2016-12-08',  # start data
    #                      end='2016-12-10',  # end date
    #                      granularity='M1')  # minute bars  # 7
    
    ba_candles = flatten_ba_candles(data['candles'])

    df = pd.DataFrame(ba_candles).set_index('time')  # 8

    df.index = pd.DatetimeIndex(df.index)  # 9

    df.info() # 10

    df['returns'] = np.log(df['askClose'] / df['askClose'].shift(1))  # 12

    cols = []  # 13

    for momentum in [15, 30, 60, 120]:  # 14
        col = 'position_%s' % momentum  # 15
        df[col] = np.sign(df['returns'].rolling(momentum).mean())  # 16
        cols.append(col)  # 17

    strats = ['returns']  # 19

    for col in cols:  # 20
        strat = 'strategy_%s' % col.split('_')[1]  # 21
        df[strat] = df[col].shift(1) * df['returns']  # 22
        strats.append(strat)  # 23

    myplot = df[strats].dropna().cumsum().apply(np.exp).plot()  # 24

    import matplotlib.pyplot as plt
    plt.savefig('save_as_a_png.png')
    # plt.savefig('saving-a-high-resolution-seaborn-plot.png', dpi=300)

    log.info("Program complete", source="program", event="complete")
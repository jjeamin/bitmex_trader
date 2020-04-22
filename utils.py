import pandas as pd
from datetime import timedelta
from indicator import ESCGO


def get_minute_data(client, symbol, minutes, cur_time):
    # get data
    time_starter = cur_time - timedelta(minutes=minutes * 100)

    data = client.Trade.Trade_getBucketed(symbol=symbol,
                                          binSize=f"{minutes}m",
                                          count=minutes * 100,
                                          startTime=time_starter).result()

    # make dataframe
    df = pd.DataFrame(columns=['time', 'open', 'high', 'low', 'close', 'volume', 'Adj Close'])

    for i, d in enumerate(data[0]):
        df.loc[i] = pd.Series({'time': d['timestamp'] + timedelta(hours=9),
                               'open': d['open'],
                               'high': d['high'],
                               'low': d['low'],
                               'close': d['close'],
                               'volume': d['volume'],
                               'Adj Close': d['close']})

    # ESCGO
    v3, t = ESCGO(df)

    df['ESCGO'] = v3

    return df


def get_action(df):
    if df['ESCGO'][-1] > 0.8:
        return 1

    elif df['ESCGO'][-1] < -0.8:
        return -1

    else:
        return 0

import datetime
import pandas as pd
import yfinance as yf
from os import listdir
from os.path import isfile, join
import logging
import requests

logging.basicConfig(filename='add_financial.log', level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s : %(message)s')

bags_dir = "cleaned_filings"

def getFinancialOneDay(row):
    date = row['acceptance_date'].date()
    time = row['acceptance_date'].time()


    print(row['index1'], row['ticker'], row['acceptance_date'])

    if not (16 <= time.hour <= 19):
        logging.info('outside hours')
        print('outside hours')
        return row

    try:
        hist = yf.Ticker(row['ticker']).history(start=date, end=date + pd.Timedelta(days=3))
        # print(hist)
    except Exception as e:
        logging.info(e)
        print(e)
        return row

    if not (date + pd.Timedelta(days=1)) in hist.index:
        logging.info(str(len(hist.index)) + ' day avail')
        print(str(len(hist.index)) + ' day avail')
        return row

    next_day_open_price = hist.loc[(date + pd.Timedelta(days=1))]['Open']
    next_day_close_price = hist.loc[(date + pd.Timedelta(days=1))]['Close']
    prc_change = (next_day_close_price - next_day_open_price) / next_day_open_price
    row['prc_change'] = prc_change
    logging.info(prc_change)

    if (date + pd.Timedelta(days=2)) in hist.index:
        next_day_open_price = hist.loc[(date + pd.Timedelta(days=1))]['Open']
        t2_open_price = hist.loc[(date + pd.Timedelta(days=2))]['Open']
        prc_change = (t2_open_price - next_day_open_price) / next_day_open_price
        row['prc_change_t2'] = prc_change

    return row

onlyfiles = [f for f in listdir(bags_dir) if isfile(join(bags_dir, f)) and f[0] != '.']
onlyfiles = [f.split('_') + [f] for f in onlyfiles]
df = pd.DataFrame(onlyfiles, columns=['cik', 'type', 'filed_date', 'acceptance_date', 'ticker', 'filename'])
df['ticker'] = df['ticker'].apply(lambda s: s.split('.')[0])
df['acceptance_date'] = pd.to_datetime(df['acceptance_date'])
df['acceptance_date'] = df['acceptance_date'].dt.tz_localize('America/New_York')
df = df.sort_values('acceptance_date').reset_index(drop=True)
df['index1'] = df.index
print(df)

df = df.apply(lambda row: getFinancialOneDay(row), axis=1)

print(df)
pd.to_pickle(df, 'financials.pkl')

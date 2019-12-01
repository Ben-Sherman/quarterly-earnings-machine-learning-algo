from filing_cleaner import FilingCleaner
from diff_cleaned_filings import FilingDiffer
from gcp_automl_predictor import AutoMLPredictor
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import alpaca_trade_api as tradeapi
import yfinance as yf
import sys

base_url = 'https://www.sec.gov'

def getTodayFilingCikList(form_type='10-Q'):
    url = 'https://www.sec.gov/cgi-bin/current?q1=0&q2=1&q3='

    response = requests.request('GET', url)

    soup = BeautifulSoup(response.content, "lxml")

    table = soup.find('pre')

    regex = re.compile('(?<=CIK=)[0-9]+')

    cik_list = regex.findall(str(table))

    return [int(i) for i in cik_list]

def getFilingLinksForCik(cik, form_type='10-Q'):
    url = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type={form_type}'.format(cik=cik, form_type=form_type)

    response = requests.request('GET', url)

    soup = BeautifulSoup(response.content, "lxml")

    links = soup.find_all('a', attrs={'id': 'documentsbutton'})

    return [i['href'] for i in links]

def getFiling(document_url, form_type='10-Q'):
    index_response = requests.request('GET', base_url + document_url)

    soup = BeautifulSoup(index_response.content, "lxml")
    html_table = soup.find('table', {'class': 'tableFile', 'summary': 'Document Format Files'})
    for row in html_table.find_all('tr'):
        ele = row.find_all('td')
        if len(ele) >= 4 and ele[3].text == form_type:
            html_filing_element = ele[2].find('a', href=True)
            break

    if ele[3].text != form_type:
        print('different form type')
        return None, None

    html_filing_dir = html_filing_element['href']
    acceptance_date = soup.find('div', attrs={'class': 'infoHead'}, text='Accepted').findNext('div', {'class': 'info'}).text

    if 'ix?' in html_filing_dir:
        html_filing_dir = '/' + '/'.join(html_filing_dir.split('/')[2:])

    html_filing_response = requests.request('GET', base_url + html_filing_dir)

    return pd.to_datetime(acceptance_date), html_filing_response.text

# def submitOrdersForTicker(api, symbol):
#     symbol_bars = api.get_barset(symbol, 'day', 1).df.iloc[0]
#     symbol_price = symbol_bars[symbol]['close']
#
#     shares_to_buy = int(1000 / symbol_price)
#
#     short_order = api.submit_order(symbol, shares_to_buy, 'sell', 'market', 'opg')
#     print(short_order)
#     close_order = api.submit_order(symbol, shares_to_buy, 'buy', 'market', 'cls')
#     print(close_order)

def submitShort(api, symbol, shares):
    short_order = api.submit_order(symbol, shares, 'sell', 'market', 'day')
    print(short_order)

def canShort(api, symbol):
    return api.get_asset(symbol).easy_to_borrow

def main():

    alpaca_api_key_id = sys.argv[1]
    alpaca_api_secret_key = sys.argv[2]
    model_name = sys.argv[3]

    api = tradeapi.REST(
        alpaca_api_key_id,
        alpaca_api_secret_key,
        'https://paper-api.alpaca.markets', api_version='v2'
    )
    print(api.get_account())

    fc = FilingCleaner()

    fd = FilingDiffer()

    predictor = AutoMLPredictor(model_name)

    todays_filings_list = getTodayFilingCikList()
    print(todays_filings_list)

    for cik in todays_filings_list:
        try:
            if not fc.hasTickerFromCik(cik):
                continue

            company_filings = getFilingLinksForCik(cik)

            if len(company_filings) < 2:
                continue

            current_date, current_filing = getFiling(company_filings[0])

            if current_filing == None:
                continue

            if not (16 <= current_date.hour <= 19):
                continue

            # if not current_date.date() == pd.datetime.today():
            #     continue

            last_date, last_filing = getFiling(company_filings[1])

            if last_filing == None:
                continue

            days_since_last_filing = (current_date - last_date).days

            if not (9 * 7 < days_since_last_filing < 17 * 7):
                continue

            current_cleaned = fc.prep_text(current_filing)
            last_cleaned = fc.prep_text(last_filing)
            diff = fd.create_diff(current_cleaned, last_cleaned)

            if len(diff) > 60000:
                continue

            predicted_value = predictor.get_prediction(diff)
            ticker = fc.getTickerFromCik(cik)

            # print('cik: ', cik)
            # print('ticker: ', ticker)
            # print('current_date.hour: ', current_date.hour)
            # print('days_since_last_filing: ', days_since_last_filing)
            # print(diff[:100])
            # print('predicted_value: ', predicted_value)

            if predicted_value == 0:

                try:
                    price = yf.Ticker(ticker).info['regularMarketPreviousClose']
                except Exception as e:
                    print(e)
                    continue

                quantity_to_buy = int(1000 / price)
                print(ticker, quantity_to_buy)
                if api.get_asset(ticker).tradable:
                    submitShort(api, ticker, quantity_to_buy)
            print('length: ', len(diff))
        except Exception as e:
            print(e)
            continue


if __name__ == "__main__":
    main()
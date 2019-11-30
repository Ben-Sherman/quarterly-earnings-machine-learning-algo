import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
import sys

logging.basicConfig(filename='log.out', format='%(asctime)s [%(levelname)s] %(name)s : %(message)s')

base_url = 'https://www.sec.gov/Archives/'

base_domain = 'https://www.sec.gov'

filings_folder = 'filings/'

df = pd.read_csv(sys.argv[1], delimiter='|', header=None, names=['cik', 'name', 'type', 'date', 'text_url', 'index_url'])

target_filings = df[df['type'] == '10-Q'].reset_index()

for i, filing in target_filings.iterrows():
    try:
        print(i, len(target_filings.index), i/len(target_filings.index))

        index_response = requests.request('GET', base_url + filing['index_url'])
        print(base_url + filing['index_url'])

        soup = BeautifulSoup(index_response.content, "lxml")
        html_table = soup.find('table', {'class': 'tableFile', 'summary': 'Document Format Files'})
        html_filing_element = None
        for row in html_table.find_all('tr'):
            ele = row.find_all('td')
            if len(ele) >= 4 and ele[3].text == filing['type']:
                html_filing_element = ele[2].find('a', href=True)
                break

        html_filing_dir = html_filing_element['href']
        html_filing_name = html_filing_element.text
        acceptance_date = soup.find('div', attrs={'class': 'infoHead'}, text='Accepted').findNext('div', {'class': 'info'}).text


        if 'ix?' in html_filing_dir:
            html_filing_dir = '/' + '/'.join(html_filing_dir.split('/')[2:])

        html_filing_response = requests.request('GET', base_domain + html_filing_dir)

        file_name = '_'.join([str(filing['cik']),
                              filing['type'].replace('/', ''),
                              filing['date'],
                              acceptance_date]) + '.htm'


        print(base_domain + html_filing_dir)
        print(file_name)

        file = open(filings_folder + file_name, "w")
        file.write(html_filing_response.text)
        file.close()
    except Exception as e:
        logging.warning(e)
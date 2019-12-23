import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
import sys
import os

logging.basicConfig(filename='log.out', format='%(asctime)s [%(levelname)s] %(name)s : %(message)s')

base_url = 'https://www.sec.gov/Archives/'

base_domain = 'https://www.sec.gov'

filings_folder = 'filings/'

# keep track of processed records
# this will help resume work if an interruption takes place
# this log depends on master.tsv (assumes master.tsv does not change)
# delete the log and start over if you rebuild master.tsv
indexlog = 'dRawIndex.log'
if os.path.isfile(indexlog):
    ilog = open(indexlog, 'r')
    lastl = ''
    for line in ilog:
        lastl = line
    ilog.close()
    try:
        i_last = int(lastl)
    except ValueError:
        print('Bad index log', indexlog, 'please investigate. Last line was:')
        print(lastl)
        sys.exit()
else:
    i_last = -1

print('parsing master TSV file...')
df = pd.read_csv(sys.argv[1], delimiter='|', header=None, names=['cik', 'name', 'type', 'date', 'text_url', 'index_url'])

target_filings = df[df['type'] == '10-Q'].reset_index()
print('...done')

if i_last > -1:
    print('Will skip the first', i_last + 1, 'filings. May take a few moments.')

for i, filing in target_filings.iterrows():
    if i <= i_last:
        continue
    try:
        print(i, len(target_filings.index), (i + 1)/len(target_filings.index))

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

        # slower to open/close every time, but safer
        ilog = open(indexlog, 'a')
        ilog.write(str(i) + '\n')
        ilog.close()
    except Exception as e:
        logging.warning(e)

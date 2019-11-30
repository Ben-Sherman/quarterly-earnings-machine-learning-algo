from functools import partial

from bs4 import BeautifulSoup
import os
import pandas as pd
import html
import multiprocessing as mp
import re

class FilingCleaner:

    def __init__(self):

        self.cleaned_filings_dir = "cleaned_filings"
        self.filings_dir = "filings"

        self.stop_words = set(line.strip().lower() for line in open('stopwords/StopWords_DatesandNumbers_limited.txt'))

        self.cik_df = pd.read_table('ticker.txt', header=None, names=['ticker', 'cik'])

    def additionalStopwords(self):
        return set(line.strip().lower() for line in open('stopwords/StopWords_DatesandNumbers.txt')) | \
               set(line.strip().lower() for line in open('stopwords/StopWords_Names.txt')) | \
               set(line.strip().lower() for line in open('stopwords/StopWords_Geographic.txt'))

    def removeInnerLinks(self, soup):
        [i.extract() for i in soup.find_all('a', href=True) if len(i['href']) > 0 and i['href'][0] == '#']
        return soup

    def getTickerFromCik(self, cik):
        return self.cik_df.loc[self.cik_df['cik'] == cik, 'ticker'].values[0].upper()

    def hasTickerFromCik(self, cik):
        i = self.cik_df.loc[self.cik_df['cik'] == cik, 'ticker']
        return not i.empty and type(i.values[0]) is str

    def remove_xbrli(self, soup):
        [x.extract() for x in soup.find_all(re.compile("^xbrli:"))]
        return soup

    def removeNumericalTables(self, soup):

        def GetDigitPercentage(tablestring):
            if len(tablestring) > 0.0:
                numbers = sum([char.isdigit() for char in tablestring])
                length = len(tablestring)
                return numbers / length
            else:
                return 1

        def containsBgColor(table):
            for row in table.find_all('tr'):
                colored = 'background-color' in str(row) or 'bgcolor' in str(row)
                if colored:
                    return True
            return False

        [x.extract() for x in soup.find_all('table') if containsBgColor(x)]

        [x.extract() for x in soup.find_all('table') if GetDigitPercentage(x.get_text()) > 0.15]

        return soup

    def prep_text(self, text):
        soup = BeautifulSoup(html.unescape(re.sub(r'\s+', ' ', text)), "lxml")

        soup = self.remove_xbrli(soup)

        soup = self.removeInnerLinks(soup)

        soup = self.removeNumericalTables(soup)

        [x.unwrap() for x in soup.find_all(['span', 'font', 'b', 'i', 'u', 'strong'])]

        soup.smooth()

        text = soup.get_text(separator="\n", strip=True)

        pattern = re.compile(r'\b(' + r'|'.join(self.stop_words) + r')\b\s*', re.IGNORECASE)
        text = pattern.sub('', text)

        pattern = re.compile('\s[^a-zA-Z\s]+?(?=(\.*\s))')
        text = pattern.sub('', text)

        text = '\n'.join(
            filter(lambda line: len(line) > 0 and (sum(i.isalpha() for i in line) / len(line) > .5), text.splitlines()))

        return text


    def prep_file(self, path):
        with open(path) as f:
            text = self.prep_text(f.read())
        return text

    def saveBag(self, c, cik, file):
        if self.hasTickerFromCik(cik):
            bag_file_name = '_'.join([os.path.splitext(file)[0], self.getTickerFromCik(cik)]) + '.txt'
            print(bag_file_name)
            with open(self.cleaned_filings_dir + '/' + bag_file_name, 'w') as file:
                file.write(c)


def main():

    pool = mp.Pool(mp.cpu_count())

    fc = FilingCleaner()

    for root, dirs, files in os.walk(fc.filings_dir):
        for file in files:
            if file[0] != '.':
                print(file)
                fullpath = os.path.join(fc.filings_dir, file)
                cik = int(file.split('_')[0])
                # fc.saveBag(fc.prep_file(fullpath), cik, file)
                partial_callback_function = partial(fc.saveBag, cik=cik, file=file)
                pool.apply_async(fc.prep_file, args=[fullpath], callback=partial_callback_function)

    pool.close()
    pool.join()

if __name__ == "__main__":
    main()

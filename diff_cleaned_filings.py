import logging
import os
import nltk.data
import multiprocessing as mp
from functools import partial


import pandas as pd
from fuzzywuzzy import process, fuzz

class FilingDiffer:

    def __init__(self):

        logging.basicConfig(filename='whole_file_diffs.log', format='%(asctime)s [%(levelname)s] %(name)s : %(message)s')

        self.sent_detector = nltk.data.load('tokenizers/punkt/english.pickle')

        self.cleaned_filings_dir = "cleaned_filings"
        self.whole_file_diffs_dir = "whole_file_diffs"

        self.df = pd.read_pickle('financials.pkl')
        self.df = self.df.set_index('acceptance_date')

    def create_diff(self, current_report, last_report):

        new_sentences = []

        current_report_file_lines = current_report.splitlines()
        last_report_file_lines = last_report.splitlines()

        current_report_file_lines_dedup = [line for line in current_report_file_lines if line not in last_report_file_lines]
        last_report_file_lines_dedup = [line for line in last_report_file_lines if line not in current_report_file_lines]

        current_report_file_sentences = list(self.sent_detector.tokenize(' '.join(current_report_file_lines_dedup).strip()))
        last_report_file_sentences = list(self.sent_detector.tokenize(' '.join(last_report_file_lines_dedup).strip()))


        for sentence in current_report_file_sentences:
            match = process.extractOne(sentence, last_report_file_sentences, score_cutoff=85, scorer=fuzz.QRatio)
            if match == None:
                new_sentences.append(sentence)

        return '\n'.join(new_sentences)

    def create_diff_from_files(self, current_report_filename, last_report_filename):
        last_report_file_dir = os.path.join(self.cleaned_filings_dir, last_report_filename)
        current_report_file_dir = os.path.join(self.cleaned_filings_dir, current_report_filename)

        with open(last_report_file_dir) as last_report_file, open(current_report_file_dir) as current_report_file:
            diff = self.create_diff(current_report_file.read(), last_report_file.read())

        return diff


    def save_diff(self, diff, current_report_filename, last_report_date):
        write_filename = os.path.join(self.whole_file_diffs_dir,
                                      current_report_filename.split('.')[0] + '_' + str(last_report_date) + '.' +
                                      current_report_filename.split('.')[1])

        with open(write_filename, 'w') as file:
            file.write(diff)
        print(write_filename)


def main():

    filingDiffer = FilingDiffer()

    pool = mp.Pool(mp.cpu_count())


    for i, row in filingDiffer.df.iterrows():
        four_months_ago = row.name - pd.Timedelta(weeks=17)
        two_months_ago = row.name - pd.Timedelta(weeks=9)
        previous_df = filingDiffer.df[four_months_ago:two_months_ago]
        if row['cik'] not in previous_df['cik'].values:
            logging.warning(str(row['cik']) + 'no prev')
            continue

        last_report_df = previous_df[previous_df['cik'] == row['cik']]
        if len(last_report_df.index) > 1:
            logging.warning(row['cik'] + ' ' + str(len(last_report_df.index)))

        last_report_filename = last_report_df.iloc[0]['filename']
        last_report_date = last_report_df.iloc[0].name
        current_report_filename = row['filename']

        print('found last', last_report_filename, 'from', current_report_filename)

        #save_diff(create_diff(current_report_filename, last_report_filename), current_report_filename, last_report_date)
        partial_callback_function = partial(filingDiffer.save_diff, current_report_filename=current_report_filename, last_report_date=last_report_date)
        pool.apply_async(filingDiffer.create_diff_from_files, args=(current_report_filename, last_report_filename), callback=partial_callback_function)

    pool.close()
    pool.join()

if __name__ == "__main__":
    main()

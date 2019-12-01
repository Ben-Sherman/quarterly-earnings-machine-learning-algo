import os
import pandas as pd

diff_files_dir = "whole_file_diffs"

financials = pd.read_pickle('financials.pkl')

financials = financials.dropna(subset=['prc_change_t2'])

financials['filename'] = financials['filename'].str.split('.').str.slice(stop=1).str.join('')

financials = financials[['prc_change_t2', 'filename']].set_index('filename')

df = pd.DataFrame(columns=['text', 'filename'])

i = 0
for root, dirs, files in os.walk(diff_files_dir):
    for file in files:
        if file[0] != '.':
            print(i)
            i += 1
            text = open(os.path.join(diff_files_dir, file)).read().replace('\n', ' ').replace('"', '')
            df = df.append({'text': text, 'filename': file}, ignore_index=True)

df['filename'] = df['filename'].str.split('/').str.slice(start=-1).str.join('').str.split('_').str.slice(stop=-1).str.join('_')
df = df.set_index('filename')

df = df.join(financials)

df['prc_change_t2'] = pd.qcut(df.prc_change, q=5, labels=range(5))

print(df)

df.to_csv('training_data.csv')
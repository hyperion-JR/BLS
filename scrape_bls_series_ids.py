
import re
import json
import sqlite3

import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

from states import states

# https://www.bls.gov/sae/additional-resources/list-of-published-state-and-metropolitan-area-series/alabama.htm

conn = sqlite3.connect('bls_data.db')
c = conn.cursor()

def scrape_series_ids(state):
    data = {}
    url = 'https://www.bls.gov/sae/additional-resources/list-of-published-state-and-metropolitan-area-series/{}.htm'.format(state)
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'lxml')
    body = soup.find_all('tbody')
    for b in body:
        rows = b.find_all('tr')
        for r in rows:
            series_id = r.find(attrs={'class':'sub0'}).get_text()
            area = r.find_all('td')[0].get_text().rstrip()
            industry = r.find_all('td')[1].get_text().rstrip()
            datatype = r.find_all('td')[2].get_text().rstrip()
            adjustment_method = r.find_all('td')[3].get_text().rstrip()
            data[series_id] = pd.Series([series_id,area,industry,datatype,adjustment_method], 
                index=['series_id','area','industry','datatype','adjusment_method'])
    return pd.DataFrame(data).T

def aggregate_ids():
    dfs = []
    for i in states:
        print(i)
        df = scrape_series_ids(i.lower())
        dfs.append(df)
    pd.concat(dfs).to_sql('series_names', conn, if_exists='replace')
    conn.commit()
    print('Done loading data')

if __name__ == '__main__':
    aggregate_ids()

import sqlite3

import pandas as pd
import numpy as np

conn = sqlite3.connect('bls_data.db')
c = conn.cursor()

def list_of_msas():
    msas = []
    c.execute("SELECT distinct(area) FROM series_names")
    for i in c:
        msa = i[0].rstrip()
        msas.append(msa)
    return msas

def query_msa_data(msa):
    data = {}
    c.execute("""SELECT e.series_id, e.year, e.period, e.periodName, e.value, s.area
                 FROM employee_counts e 
                 LEFT JOIN series_names s ON e.series_id = s.series_id 
                 WHERE s.industry = 'Total Nonfarm'
                 AND s.area = '{}'""".format(msa))
    for i in c:
        series_id = i[0]
        year = i[1]
        period = i[2]
        month = i[3]
        month_year = period+'-'+year
        value = i[4]
        msa = i[5]
        data[month_year] = {'period' : period, 
                            'year' : year, 
                            'value' : value }
    return data

def yoy(msa, data):
    for k, v in data.items():
        if int(v['year']) > 1997:
            prev_period = v['period']+'-'+str(int(v['year'])-1)
            prev_value = float(data[prev_period]['value'])
            value = float(v['value'])
            YOY = round((value-prev_value)/prev_value, 6)
            print(m, k, v['value'], data[prev_period]['value'], YOY)

msas = list_of_msas()
for m in msas[1:]:
    try:
        yoy(m, query_msa_data(m))
    except Exception as e:
        print(e)



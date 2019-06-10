
import csv
import json
import sqlite3

import requests
import prettytable
import pandas as pd
import numpy as np

from config import api_key, api_key2, api_key3

conn = sqlite3.connect('bls_data.db')
c = conn.cursor()

def total_non_farm():
    series_names = pd.read_excel('BLS_Series_Names.xlsx')
    all_employee_counts = series_names['datatype'] == 'avg_weekly_earnings'
    series_names_counts = series_names[all_employee_counts]
    total_nonfarm = series_names_counts['industry'] == 'Total Nonfarm'
    return series_total_nonfarm.series_id.unique().tolist()

def employee_count_list():
    data = []
    c.execute("""SELECT series_id
             FROM series_names 
             WHERE industry = 'Total Nonfarm' 
             AND datatype = 'All Employees, In Thousands' 
             AND adjustment_method = 'Not Seasonally Adjusted' 
             AND area != 'Statewide'""")
    for i in c:
        data.append(i[0])
    return data

def chunks(l, n):
    n = max(1, n)
    return (l[i:i+n] for i in range(0, len(l), n))

def query_employee_counts(series,startyear,endyear):
    data_dict = {}
    counter = 0
    headers = {'Content-type': 'application/json'}
    data = json.dumps({"seriesid": series,"startyear":startyear, "endyear":endyear, 'registrationkey':api_key3['key']})
    p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)
    json_data = json.loads(p.text)
    s = json_data['Results']['series']
    for i in s:
        series_id = i['seriesID']
        series_data = i['data']
        for j in series_data:
            year = j['year']
            period = j['period']
            periodName = j['periodName']
            value = j['value']
            data_dict[counter] = pd.Series([series_id,year,period,periodName,value], 
                index=['series_id','year','period','periodName','value'])
            counter+=1
    return pd.DataFrame(data_dict).T

def get_employee_counts(startyear,endyear):
    em_cnt_list = employee_count_list()
    ec_list = []
    for i in em_cnt_list:
        ec_list.append(i.rstrip())
    dfs = []
    new_list = chunks(ec_list, 50)
    for n in new_list:
        dfs.append(query_employee_counts(n,startyear,endyear))
    pd.concat(dfs).to_sql('employee_counts', conn, if_exists='replace')
    conn.commit()
    print('Done loading data.')

def query_income_data(series,startyear,endyear):
    data_dict = {}
    counter = 0
    headers = {'Content-type': 'application/json'}
    data = json.dumps({"seriesid": [series],"startyear":startyear, "endyear":endyear, 'registrationkey':api_key['key']})
    p = requests.post('https://api.bls.gov/publicAPI/v1/timeseries/data/', data=data, headers=headers)
    json_data = json.loads(p.text)
    d = json_data['Results']['series'][0]['data']
    for i in d:
        data_dict_id = str(counter)
        year = i['year']
        period = i['period']
        periodName = i['periodName']
        value = i['value']
        data_dict[counter] = pd.Series([series,year,period,periodName,value], 
            index=['series','year','period','periodName','value'])
        counter+=1
    return pd.DataFrame(data_dict).T

def get_income_data(startyear,endyear):
    dfs = []
    counter = 0
    for row in total_non_farm():
        series_id = row.rstrip()
        dfs.append(query_income_data(series_id,startyear,endyear))
        counter+=1
    return pd.concat(dfs)

def export_to_excel(data, file_name):
    writer = pd.ExcelWriter("%s.xlsx" % (file_name,))
    data.to_excel(writer,"%s" % (file_name,))
    writer.save()
    print('Done')

# export_to_excel(get_income_data('weekly_earnings.csv', 2008, 2019), 'Avg Weekly Earnings')
# export_to_excel(get_employee_counts(2008,2019), 'Employee Counts')
get_employee_counts(1975,2019)


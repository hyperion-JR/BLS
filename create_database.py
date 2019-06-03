
import sqlite3

conn = sqlite3.connect('bls_data.db')
c = conn.cursor()

def create_series_names_table():
    c.executescript('''DROP TABLE IF EXISTS series_names;''')
    c.execute('''CREATE TABLE series_names (id, series_id, area, industry, datatype, adjustment_method)''')
    conn.commit()
    print('Done creating series_names table.')

def create_employee_counts_table():
    c.executescript('''DROP TABLE IF EXISTS employee_counts;''')
    c.execute('''CREATE TABLE employee_counts (id, series_id, year, period, periodName, value)''')
    conn.commit()
    print('Done creating employee_counts table.')

def create_earnings_table():
    c.executescript('''DROP TABLE IF EXISTS earnings;''')
    c.execute('''CREATE TABLE earnings (id, series_id, year, period, periodName, value)''')
    conn.commit()
    print('Done creating earnings table.')

if __name__ == '__main__':
    create_series_names_table()
    create_employee_counts_table()
    create_earnings_table()
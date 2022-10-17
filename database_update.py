import pyodbc
import etl
import pandas as pd
server = 'tcp:blockgems.database.windows.net'
database = 'BTC_MINING_DATA'
username = 'blockgems'
password = 'AXE16dry'

def insert_row(timestamp, hashrate_in_phs, daily_reward, hoster, pool, miner):
    cnxn = pyodbc.connect(
        'DRIVER={ODBC Driver 18 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    cursor = cnxn.cursor()
    query = "INSERT INTO POOLDATA(timestamp, hashrate_in_phs, daily_reward,hoster, pool, miner) VALUES ('" + timestamp + "'," + hashrate_in_phs + "," + daily_reward + ",'" + hoster + "','" + pool + "','" + miner + "')"
    cursor.execute(query)
    cnxn.commit()

def select_row(timestamp, hoster, pool,miner):
    cnxn = pyodbc.connect(
        'DRIVER={ODBC Driver 18 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    cursor = cnxn.cursor()
    cursor.execute("SELECT * FROM POOLDATA WHERE timestamp='" + timestamp + "' AND hoster='" + hoster + "' AND pool='"+ pool + "' AND miner='" + miner + "'")
    row = cursor.fetchall()
    return row

def yesterday(frmt='%Y-%m-%d', string=True):
    from datetime import datetime, timedelta
    yesterday = datetime.now() - timedelta(1)
    if string:
        return yesterday.strftime(frmt)
    return yesterday

def import_data_from_csv():
    etl.get_file_from_azure("total_raw.csv")
    df = pd.read_csv('total_raw.csv', index_col=False)

    for index, row in df.iterrows():
        insert_row(str(row['timestamp']), str(row['hashrate_in_phs']), str(row['daily_reward']), str(row['hoster']), str(row['pool']), str(row['miner']))


def update(id,timestamp, hashrate_in_phs, daily_reward, hoster, pool, miner):
    cnxn = pyodbc.connect(
        'DRIVER={ODBC Driver 18 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    cursor = cnxn.cursor()
    query = "UPDATE POOLDATA SET timestamp = '" + timestamp + "', hashrate_in_phs = '" + hashrate_in_phs + "', daily_reward = ' " + daily_reward + " ', hoster = ' " + hoster + " ', pool = ' " + pool + " ', miner = ' " + miner + " ' WHERE id = " + id
    cursor.execute(query)
    cnxn.commit()

def process_subaccount(df):
    row = select_row(df['timestamp'].iloc[0], df['hoster'].iloc[0], df['pool'].iloc[0], df['miner'].iloc[0])
    if not row:
        insert_row(df['timestamp'].iloc[0], str(df['hashrate_in_phs'].iloc[0]), str(df['daily_reward'].iloc[0]),
                   df['hoster'].iloc[0], df['pool'].iloc[0], df['miner'].iloc[0])
    else:
        id = str(row[0].id)
        update(id, df['timestamp'].iloc[0], str(df['hashrate_in_phs'].iloc[0]), str(df['daily_reward'].iloc[0]),
               df['hoster'].iloc[0], df['pool'].iloc[0], df['miner'].iloc[0])

def daily_update():
    from datetime import date
    today_date = date.today().strftime('%Y-%m-%d')
    yesterday_date = yesterday(today_date, False)
    yesterday_date = yesterday_date.strftime('%Y-%m-%d')

    df_luxor = etl.get_earnings_luxor_para()
    df_luxor = df_luxor.loc[df_luxor['timestamp'] == yesterday_date]
    process_subaccount(df_luxor)

    df_penguintests19j = etl.get_foundry_penguintests19j()
    df_penguintests19j = df_penguintests19j.loc[df_penguintests19j['timestamp'] == yesterday_date]
    process_subaccount(df_penguintests19j)

    df_eunorths19j = etl.get_foundry_eunorths19j()
    df_eunorths19j = df_eunorths19j.loc[df_eunorths19j['timestamp'] == yesterday_date]
    process_subaccount(df_eunorths19j)

    df_eunorths19xp = etl.get_foundry_eunorths19xp()
    df_eunorths19xp = df_eunorths19xp.loc[df_eunorths19xp['timestamp'] == yesterday_date]
    process_subaccount(df_eunorths19xp)

    df_eueasts19j = etl.get_foundry_eueasts19j()
    df_eueasts19j = df_eueasts19j.loc[df_eueasts19j['timestamp'] == yesterday_date]
    process_subaccount(df_eueasts19j)

    df_eueasts19xp = etl.get_foundry_eueasts19xp()
    df_eueasts19xp = df_eueasts19xp.loc[df_eueasts19xp['timestamp'] == yesterday_date]
    process_subaccount(df_eueasts19xp)

    df_eusouths19j = etl.get_foundry_eusouths19j()
    df_eusouths19j = df_eusouths19j.loc[df_eusouths19j['timestamp'] == yesterday_date]
    process_subaccount(df_eusouths19j)

if __name__ == '__main__':
    daily_update()



































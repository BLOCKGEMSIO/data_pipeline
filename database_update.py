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
    query = "INSERT INTO BLOCKGEMS(timestamp, hashrate_in_phs, daily_reward,hoster, pool, miner) VALUES ('" + timestamp + "'," + hashrate_in_phs + "," + daily_reward + ",'" + hoster + "','" + pool + "','" + miner + "')"
    cursor.execute(query)
    cnxn.commit()

def import_data_from_csv():
    etl.get_file_from_azure("total_raw.csv")
    df = pd.read_csv('total_raw.csv', index_col=False)

    for index, row in df.iterrows():
        insert_row(str(row['timestamp']), str(row['hashrate_in_phs']), str(row['daily_reward']), str(row['hoster']), str(row['pool']), str(row['miner']))

def daily_update():
    from datetime import timedelta, date
    todays_date = date.today().strftime('%Y-%m-%d')

    df_luxor = etl.get_earnings_luxor_para()
    df_luxor = df_luxor.loc[df_luxor['timestamp'] == todays_date]
    insert_row(df_luxor['timestamp'].iloc[0],str(df_luxor['hashrate_in_phs'].iloc[0]),str(df_luxor['daily_reward'].iloc[0]),df_luxor['hoster'].iloc[0],df_luxor['pool'].iloc[0],df_luxor['miner'].iloc[0])

    df_penguintests19j = etl.get_foundry_penguintests19j()
    df_penguintests19j = df_penguintests19j.loc[df_penguintests19j['timestamp'] == todays_date]
    insert_row(df_penguintests19j['timestamp'].iloc[0],str(df_penguintests19j['hashrate_in_phs'].iloc[0]),str(df_penguintests19j['daily_reward'].iloc[0]),df_penguintests19j['hoster'].iloc[0],df_penguintests19j['pool'].iloc[0],df_penguintests19j['miner'].iloc[0])

    df_eunorths19j = etl.get_foundry_eunorths19j()
    df_eunorths19j = df_eunorths19j.loc[df_eunorths19j['timestamp'] == todays_date]
    insert_row(df_eunorths19j['timestamp'].iloc[0], str(df_eunorths19j['hashrate_in_phs'].iloc[0]), str(df_eunorths19j['daily_reward'].iloc[0]), df_eunorths19j['hoster'].iloc[0], df_eunorths19j['pool'].iloc[0],df_eunorths19j['miner'].iloc[0])

    df_eunorths19xp = etl.get_foundry_eunorths19xp()
    df_eunorths19xp = df_eunorths19xp.loc[df_eunorths19xp['timestamp'] == todays_date]
    insert_row(df_eunorths19xp['timestamp'].iloc[0], str(df_eunorths19xp['hashrate_in_phs'].iloc[0]),str(df_eunorths19xp['daily_reward'].iloc[0]), df_eunorths19xp['hoster'].iloc[0], df_eunorths19xp['pool'].iloc[0],df_eunorths19xp['miner'].iloc[0])

    df_eueasts19j = etl.get_foundry_eueasts19j()
    df_eueasts19j = df_eueasts19j.loc[df_eueasts19j['timestamp'] == todays_date]
    insert_row(df_eueasts19j['timestamp'].iloc[0], str(df_eueasts19j['hashrate_in_phs'].iloc[0]),str(df_eueasts19j['daily_reward'].iloc[0]), df_eueasts19j['hoster'].iloc[0], df_eueasts19j['pool'].iloc[0],df_eueasts19j['miner'].iloc[0])

    df_eueasts19xp = etl.get_foundry_eueasts19xp()
    df_eueasts19xp = df_eueasts19xp.loc[df_eueasts19xp['timestamp'] == todays_date]
    insert_row(df_eueasts19xp['timestamp'].iloc[0], str(df_eueasts19xp['hashrate_in_phs'].iloc[0]),str(df_eueasts19xp['daily_reward'].iloc[0]), df_eueasts19xp['hoster'].iloc[0], df_eueasts19xp['pool'].iloc[0],df_eueasts19xp['miner'].iloc[0])

    df_eusouths19j = etl.get_foundry_eusouths19j()
    df_eusouths19j = df_eusouths19j.loc[df_eusouths19j['timestamp'] == todays_date]
    insert_row(df_eusouths19j['timestamp'].iloc[0], str(df_eusouths19j['hashrate_in_phs'].iloc[0]),str(df_eusouths19j['daily_reward'].iloc[0]), df_eusouths19j['hoster'].iloc[0],df_eusouths19j['pool'].iloc[0], df_eusouths19j['miner'].iloc[0])

if __name__ == '__main__':
    import_data_from_csv()



































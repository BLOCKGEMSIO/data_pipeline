import pickle
import hmac, hashlib
import json
import pandas as pd
import requests
import dill
import cryptocmd
from requests.structures import CaseInsensitiveDict
import datetime
import matplotlib.pyplot as plt
import time

coin_type = 'BTC'  # 币种
sign_id = 'BLOCKGEMS'  # 子账号名
sign_key = '12eecb4cf25e4fa684c906fa98a803a7'  # 密钥
sign_SECRET = '73939395118c445a8608c3c6e88a9527'  # 密码
html_payment = 'https://antpool.com/api/paymentHistoryV2.htm'

class Data:
    def __init__(self, raw, timestamp):
        self.raw = raw
        self.timestamp = timestamp

class Result:
  def results(self):
      return get_data()

def get_signature():  # 签名操作
    nonce = int(time.time() * 1000)  # 毫秒时间戳
    msgs = sign_id + sign_key + str(nonce)
    ret = []
    ret.append(hmac.new(sign_SECRET.encode(encoding="utf-8"), msg=msgs.encode(encoding="utf-8"),
                        digestmod=hashlib.sha256).hexdigest().upper())  # 签名
    ret.append(nonce)  # 时间戳
    return ret


def get_earnings_antpool():  # POST
    get_file_from_azure("antpool.csv")
    api_sign = get_signature()
    post_data = {'key': sign_key, 'nonce': api_sign[1], 'signature': api_sign[0], 'coin': coin_type, 'type': 'recv',
                 'pageSize': 50}  # 这里是POST参数根据接口自行更改
    request = requests.post(html_payment, data=post_data)
    json_object_response = json.loads(request.text)
    json_object_data = json_object_response["data"]
    json_object_rows = json_object_data["rows"]
    df = pd.read_json(json.dumps(json_object_rows))
    df = df.drop(
        columns=['fppsFeeAmount', 'fppsBlockAmount', 'ppappsAmount', 'ppapplnsAmount', 'soloAmount', 'ppsAmount',
                 'hashrate_unit'])
    df = df.rename(columns={"hashrate": "hashrate_in_phs", "pplnsAmount": "daily_reward"})

    for index, row in df.iterrows():
        value = str(row['hashrate_in_phs'])

        if value.find("PH/s") != -1:
            value = value.replace('PH/s', '')
            df.at[index, 'hashrate_in_phs'] = value
        elif value.find("TH/s") != -1:
            value = 0.0
            df.at[index, 'hashrate_in_phs'] = value
        elif value.find("GH/s") != -1:
            value = 0.0
            df.at[index, 'hashrate_in_phs'] = value
        elif value.find("H/s") != -1:
            value = 0.0
            df.at[index, 'hashrate_in_phs'] = value

    df.to_csv('temp.csv', index=False)
    df_old = pd.read_csv('antpool.csv', index_col=False)
    df = pd.read_csv('temp.csv', index_col=False)
    df = df.append(df_old)
    df = df.drop_duplicates()
    df['miner'] = 'S19J'
    df['miner_hashrate'] = 100
    df['hoster'] = 'rosenenergoatom'
    df['pool'] = 'antpool'
    df = insert_zeros(df)
    df.to_csv('antpool.csv', index=False)
    df = pd.read_csv('antpool.csv', index_col=False)
    upload_file_to_azure('antpool.csv')
    return (df)


def key_in_json_old(key, json_old):
    for x in json_old:
        if x[0] == int(key):
            return True;
    return False;

def get_status_antpool():
    api_sign = get_signature()
    post_data = {'key': sign_key, 'nonce': api_sign[1], 'signature': api_sign[0], 'coin': coin_type, 'type': 'recv',
                 'pageSize': 50}  # 这里是POST参数根据接口自行更改
    request = requests.post('https://antpool.com/api/hashrate.htm', data=post_data)
    json_object_response = json.loads(request.text)
    json_object_data = json_object_response["data"]
    return json_object_data

def get_earnings_slushpool():
    get_file_from_azure('slushpool_new.json')

    with open('slushpool_new.json') as json_file:
        json_old = json.load(json_file)

    df = pd.read_csv('layout.csv', index_col=False)
    del json_old[0]
    json_df = pd.DataFrame(json_old, columns=["height", "found_at", "value", "pool_scoring_hashrate_ghps", "user_scoring_hashrate_ghps", "user_reward"])
    json_df.transpose()
    json_df = json_df.sort_values(by=['height'])
    json_df['found_at_transposed'] = pd.to_datetime(json_df['found_at']).dt.date
    uniqueValues = json_df['found_at_transposed'].unique()

    for x in uniqueValues:
        temp = json_df.loc[json_df['found_at_transposed'] == x]
        temp['user_reward'] = temp['user_reward'].astype(float)
        daily_rewards = temp.loc[:, 'user_reward'].sum()
        daily_hash_rate = temp['user_scoring_hashrate_ghps'].sum() / len(temp.index)

        df_temp = {'timestamp': x, 'hashrate_in_phs': daily_hash_rate / 1000000, 'daily_reward': float(daily_rewards)}
        df = df.append(df_temp, ignore_index=True)

    df = df.drop_duplicates()
    df['hoster'] = 'acdc'
    df['pool'] = 'slushpool'
    df['miner'] = 'S19J'
    df['miner_hashrate'] = 100
    df = insert_zeros(df)
    df.to_csv('slushpool.csv', index=False)
    df = pd.read_csv('slushpool.csv', index_col=False)
    upload_file_to_azure('slushpool.csv')

    return df

def get_earnings_luxor_para():
    from luxor import API

    get_file_from_azure('luxor.csv')

    API = API(host='https://api.beta.luxor.tech/graphql', method='POST', org='luxor',
                  key='lxk.421a09f9f4586e75c71012b666ad97d3')
    resp = API.get_hashrate_score_history("blockgems_paraguay", "BTC", 100)
    resp = json.dumps(resp)
    resp = json.loads(resp)
    resp = resp["data"]
    resp = resp["getHashrateScoreHistory"]
    resp = resp["nodes"]
    df = pd.read_csv('layout.csv', index_col=False)

    for x in resp:
        hashrate = float(x["hashrate"]) / 1000000000000000
        reward = float(x["revenue"])
        timestamp = x["date"].replace('T00:00:00+00:00', "")
        temp = {'timestamp': timestamp, 'hashrate_in_phs': hashrate, 'daily_reward': float(reward)}
        df = df.append(temp, ignore_index=True)

    df = df.drop_duplicates()
    df['hoster'] = 'infinitia'
    df['pool'] = 'luxor'
    df['miner'] = 'S19J'
    df['miner_hashrate'] = 100
    df = insert_zeros(df)
    df.to_csv('luxor.csv', index=False)
    df = pd.read_csv('luxor.csv', index_col=False)
    upload_file_to_azure('luxor.csv')

    return df

def get_earnings_luxor_nor():
    from luxor import API

    get_file_from_azure('luxor.csv')

    API = API(host='https://api.beta.luxor.tech/graphql', method='POST', org='luxor',
                  key='lxk.421a09f9f4586e75c71012b666ad97d3')
    resp = API.get_hashrate_score_history("blockgems", "BTC", 100)
    resp = json.dumps(resp)
    resp = json.loads(resp)
    resp = resp["data"]
    resp = resp["getHashrateScoreHistory"]
    resp = resp["nodes"]
    df = pd.read_csv('layout.csv', index_col=False)

    for x in resp:
        hashrate = float(x["hashrate"]) / 1000000000000000
        reward = float(x["revenue"])
        timestamp = x["date"].replace('T00:00:00+00:00', "")
        temp = {'timestamp': timestamp, 'hashrate_in_phs': hashrate, 'daily_reward': float(reward)}
        df = df.append(temp, ignore_index=True)

    df = df.drop_duplicates()
    df['hoster'] = 'acdc'
    df['pool'] = 'luxor'
    df['miner'] = 'S19J'
    df['miner_hashrate'] = 100
    df = insert_zeros(df)
    df.to_csv('luxor_nor.csv', index=False)
    df = pd.read_csv('luxor_nor.csv', index_col=False)
    upload_file_to_azure('luxor_nor.csv')

    return df

def get_foundry_penguintests19j():

    url = "https://api.foundryusapool.com/earnings/penguintests19j?startDateUnixMs=1663192800000"

    payload = {}
    headers = {
        'X-API-KEY': '390cac7d-4fc4-4f38-a539-6f5fd7da1b96'
    }

    resp = requests.request("GET", url, headers=headers, data=payload)
    resp = pd.DataFrame(json.loads(resp.text))
    df = pd.read_csv('layout.csv', index_col=False)

    for index, x in resp.iterrows():
        hashrate = float(x["hashrate"]) / 1000000
        reward = float(x["ppsBaseAmount"] + x["txFeeRewardAmount"])
        timestamp = x["startTime"].replace('T00:00:00.000+00:00', "")
        temp = {'timestamp': timestamp, 'hashrate_in_phs': hashrate, 'daily_reward': float(reward)}
        df = df.append(temp, ignore_index=True)

    df = df.drop_duplicates()
    df['hoster'] = 'penguin'
    df['pool'] = 'foundry'
    df['miner'] = 'S19J'
    df['miner_hashrate'] = 100
    df = insert_zeros(df)
    df.to_csv('penguintests19j.csv', index=False)
    df = pd.read_csv('penguintests19j.csv', index_col=False)
    upload_file_to_azure('penguintests19j.csv')

    return df

def get_foundry_eunorths19j():

    url = "https://api.foundryusapool.com/earnings/eunorths19j?startDateUnixMs=1663192800000"

    payload = {}
    headers = {
        'X-API-KEY': '8a44d8fd-b080-4bed-af38-7a22579a8226'
    }

    resp = requests.request("GET", url, headers=headers, data=payload)
    resp = pd.DataFrame(json.loads(resp.text))
    df = pd.read_csv('layout.csv', index_col=False)

    for index, x in resp.iterrows():
        hashrate = float(x["hashrate"]) / 1000000
        reward = float(x["ppsBaseAmount"] + x["txFeeRewardAmount"])
        timestamp = x["startTime"].replace('T00:00:00.000+00:00', "")
        temp = {'timestamp': timestamp, 'hashrate_in_phs': hashrate, 'daily_reward': float(reward)}
        df = df.append(temp, ignore_index=True)

    df = df.drop_duplicates()
    df['hoster'] = 'acdc'
    df['pool'] = 'foundry'
    df['miner'] = 'S19J'
    df['miner_hashrate'] = 100
    df = insert_zeros(df)
    df.to_csv('eunorths19j.csv', index=False)
    df = pd.read_csv('eunorths19j.csv', index_col=False)
    upload_file_to_azure('eunorths19j.csv')

    return df

def get_foundry_eunorths19xp():

    url = "https://api.foundryusapool.com/earnings/eunorths19xp?startDateUnixMs=1663192800000"

    payload = {}
    headers = {
        'X-API-KEY': 'c8df236b-478e-4a83-ba4f-c442a5cc3ed4'
    }

    resp = requests.request("GET", url, headers=headers, data=payload)
    resp = pd.DataFrame(json.loads(resp.text))
    df = pd.read_csv('layout.csv', index_col=False)

    for index, x in resp.iterrows():
        hashrate = float(x["hashrate"]) / 1000000
        reward = float(x["ppsBaseAmount"] + x["txFeeRewardAmount"])
        timestamp = x["startTime"].replace('T00:00:00.000+00:00', "")
        temp = {'timestamp': timestamp, 'hashrate_in_phs': hashrate, 'daily_reward': float(reward)}
        df = df.append(temp, ignore_index=True)

    df = df.drop_duplicates()
    df['hoster'] = 'acdc'
    df['pool'] = 'foundry'
    df['miner'] = 'S19XP'
    df['miner_hashrate'] = 140
    df = insert_zeros(df)
    df.to_csv('eunorths19xp.csv', index=False)
    df = pd.read_csv('eunorths19xp.csv', index_col=False)
    upload_file_to_azure('eunorths19xp.csv')

    return df

def get_foundry_eueasts19j():

    url = "https://api.foundryusapool.com/earnings/eueasts19j?startDateUnixMs=1663192800000"

    payload = {}
    headers = {
        'X-API-KEY': 'efac6b55-2c8e-472e-be71-5e81d28d8d3f'
    }

    resp = requests.request("GET", url, headers=headers, data=payload)
    resp = pd.DataFrame(json.loads(resp.text))
    df = pd.read_csv('layout.csv', index_col=False)

    for index, x in resp.iterrows():
        hashrate = float(x["hashrate"]) / 1000000
        reward = float(x["ppsBaseAmount"] + x["txFeeRewardAmount"])
        timestamp = x["startTime"].replace('T00:00:00.000+00:00', "")
        temp = {'timestamp': timestamp, 'hashrate_in_phs': hashrate, 'daily_reward': float(reward)}
        df = df.append(temp, ignore_index=True)

    df = df.drop_duplicates()
    df['hoster'] = 'rosenenergoatom'
    df['pool'] = 'foundry'
    df['miner'] = 'S19J'
    df['miner_hashrate'] = 100
    df = insert_zeros(df)
    df.to_csv('eueasts19j.csv', index=False)
    df = pd.read_csv('eueasts19j.csv', index_col=False)
    upload_file_to_azure('eueasts19j.csv')

    return df

def get_foundry_eueasts19xp():

    url = "https://api.foundryusapool.com/earnings/eueasts19xp?startDateUnixMs=1663192800000"

    payload = {}
    headers = {
        'X-API-KEY': '523741f1-1118-439a-8f7f-d45f71389739'
    }

    resp = requests.request("GET", url, headers=headers, data=payload)
    resp = pd.DataFrame(json.loads(resp.text))
    df = pd.read_csv('layout.csv', index_col=False)

    for index, x in resp.iterrows():
        hashrate = float(x["hashrate"]) / 1000000
        reward = float(x["ppsBaseAmount"] + x["txFeeRewardAmount"])
        timestamp = x["startTime"].replace('T00:00:00.000+00:00', "")
        temp = {'timestamp': timestamp, 'hashrate_in_phs': hashrate, 'daily_reward': float(reward)}
        df = df.append(temp, ignore_index=True)

    df = df.drop_duplicates()
    df['hoster'] = 'rosenenergoatom'
    df['pool'] = 'foundry'
    df['miner'] = 'S19XP'
    df['miner_hashrate'] = 140
    df = insert_zeros(df)
    df.to_csv('eueasts19xp.csv', index=False)
    df = pd.read_csv('eueasts19xp.csv', index_col=False)
    upload_file_to_azure('eueasts19xp.csv')

    return df

def daterange(date1, date2):
    from datetime import timedelta
    for n in range(int ((date2 - date1).days)+1):
        yield date1 + timedelta(n)

def insert_zeros(df):
    from datetime import timedelta, date
    df = df
    hoster = df.loc[0]['hoster']
    pool = df.loc[0]['pool']
    miner_hashrate = df.loc[0]['miner_hashrate']
    miner = df.loc[0]['miner']

    df['timestamp'] = pd.to_datetime(df['timestamp']).dt.date
    oldest_date = df.timestamp.min()
    todays_date = date.today().strftime('%Y-%m-%d')
    from datetime import datetime
    todays_date = datetime.strptime(todays_date, '%Y-%m-%d').date()
    df_dates_till_today = []

    for dt in daterange(oldest_date, todays_date):
        df_dates_till_today.append(dt.strftime("%Y-%m-%d"))

    df_av_dates = df['timestamp'].tolist()
    df_av_dates = [date_obj.strftime('%Y-%m-%d') for date_obj in df_av_dates]
    df_dates_missing = list(set(df_dates_till_today).difference(df_av_dates))

    for x in df_dates_missing:
        temp = {'timestamp': x, 'hashrate_in_phs': float(0.0), 'daily_reward': float(0.0), 'hoster': hoster, 'pool': pool, 'miner': miner, 'miner_hashrate': miner_hashrate}
        df = df.append(temp, ignore_index=True)

    return df

def get_total_earnings_raw():
    df_slush = get_earnings_slushpool()
    df_ant = get_earnings_antpool()
    df_luxor = get_earnings_luxor_para()
    df_luxor_nor = get_earnings_luxor_nor()
    df_penguintests19j = get_foundry_penguintests19j()
    df_eunorths19j = get_foundry_eunorths19j()
    df_eunorths19xp = get_foundry_eunorths19xp()
    df_eueasts19j = get_foundry_eueasts19j()
    df_eueasts19xp = get_foundry_eueasts19xp()


    df = pd.DataFrame(columns=['timestamp', 'hashrate_in_phs', 'daily_reward', 'hoster', 'pool'])
    df = df.append(df_ant)
    df = df.append(df_slush)
    df = df.append(df_luxor)
    df = df.append(df_luxor_nor)
    df = df.append(df_penguintests19j)
    df = df.append(df_eunorths19j)
    df = df.append(df_eunorths19xp)
    df = df.append(df_eueasts19j)
    df = df.append(df_eueasts19xp)
    df = get_historic_price_usd(df)
    from datetime import date
    today = str(date.today())
    df = df[df.timestamp != today]
    df = df.drop('rewards_value_at_day_of_mining_usd', 1)
    df.to_csv('total_raw.csv', index=False)
    upload_file_to_azure("total_raw.csv")
    return df

def get_current_data_USD(from_sym='BTC', to_sym='USD', exchange=''):
    url = 'https://min-api.cryptocompare.com/data/price'

    parameters = {'fsym': from_sym,
                  'tsyms': to_sym}

    if exchange:
        print('exchange: ', exchange)
        parameters['e'] = exchange

    # response comes as json
    response = requests.get(url, params=parameters)
    data = response.json()

    return data

def get_current_data_EUR(from_sym='BTC', to_sym='EUR', exchange=''):
    url = 'https://min-api.cryptocompare.com/data/price'

    parameters = {'fsym': from_sym,
                  'tsyms': to_sym}

    if exchange:
        print('exchange: ', exchange)
        parameters['e'] = exchange

    # response comes as json
    response = requests.get(url, params=parameters)
    data = response.json()

    return data

def transform_to_cummulated(df):
    df['daily_reward_cum'] = df.daily_reward.cumsum()
    return df

def add_prices(df, usd_price, eur_price):
    df['daily_reward_cum_us'] = df['daily_reward_cum'] * usd_price
    df['daily_reward_cum_eur'] = df['daily_reward_cum'] * eur_price
    df['daily_reward_us'] = df['daily_reward'] * usd_price
    df['daily_reward_eur'] = df['daily_reward'] * eur_price
    return df

def get_btc_wallet_transactions():
    your_btc_address = '3QUSvpQ6d2UptXHiKCFKJRNJSZbjF7Ga6C'  # Genesis Block
    transactions_url = 'https://blockchain.info/rawaddr/' + your_btc_address
    df = pd.read_json(transactions_url)
    return df['total_received'][0] / 100000000

def transpose(date):
    return datetime.datetime.strptime(date, "%Y-%m-%d").strftime("%d-%m-%Y")

def get_historic_price_usd(df):
    print(df.info())
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    print(df.info())
    start_date = min(df['timestamp'])
    end_date = max(df['timestamp'])
    start_date = start_date.date().strftime("%d-%m-%Y")
    end_date = end_date.date().strftime("%d-%m-%Y")

    price_data = get_price_for_date(str(start_date), str(end_date))
    price_data = price_data.rename(columns={"Date": "timestamp"})
    price_data = price_data.rename(columns={"Open": "btc_day_open_price_usd"})
    price_data = price_data.rename(columns={"High": "btc_day_high_price_usd"})
    price_data = price_data.rename(columns={"Low": "btc_day_low_price_usd"})
    price_data = price_data.rename(columns={"Close": "btc_day_close_price_usd"})
    price_data.drop(['Volume', 'Market Cap'], axis=1, inplace=True)
    price_data['timestamp'] = price_data['timestamp'].dt.strftime('%Y-%m-%d')
    price_data['timestamp'] = pd.to_datetime(price_data['timestamp'])
    df = pd.merge(df, price_data, how='left')
    df['rewards_value_at_day_of_mining_usd'] = df['daily_reward'] * df['btc_day_close_price_usd']

    return df

def get_price_for_date(start_date, end_date):
    from cryptocmd import CmcScraper
    scraper = CmcScraper("BTC", start_date, end_date)
    return scraper.get_dataframe()

def get_file_from_azure(s):
    from azure.storage.blob import BlobServiceClient

    STORAGEACCOUNTURL = "https://blockgems.blob.core.windows.net"
    STORAGEACCOUNTKEY = "s/pN8kuq//BqbT+pMysvLjeguuhw/UFvW+mHlhpR2gGykMFgI8GpfPDa70K2icBlCI6RDakck7GSO0g3aW1WwA=="
    CONTAINERNAME = "blockgems"

    blob_service_client_instance = BlobServiceClient(
        account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY)

    blob_client_instance = blob_service_client_instance.get_blob_client(
        CONTAINERNAME, s, snapshot=None)

    with open(s, "wb") as my_blob:
        download_stream = blob_client_instance.download_blob()
        my_blob.write(download_stream.readall())

def upload_file_to_azure(s):
    from azure.storage.blob import BlobServiceClient

    STORAGEACCOUNTURL = "https://blockgems.blob.core.windows.net"
    STORAGEACCOUNTKEY = "s/pN8kuq//BqbT+pMysvLjeguuhw/UFvW+mHlhpR2gGykMFgI8GpfPDa70K2icBlCI6RDakck7GSO0g3aW1WwA=="
    CONTAINERNAME = "blockgems"

    blob_service_client_instance = BlobServiceClient(
        account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY)

    blob_client_instance = blob_service_client_instance.get_blob_client(
        CONTAINERNAME, s, snapshot=None)

    with open(s, "rb") as data:
        blob_client_instance.upload_blob(data, blob_type="BlockBlob",overwrite=True)

def print_results(results):
    usd_price = results.us_btc_price
    eur_price = results.eur_btc_price

    btc_on_exchange = get_btc_wallet_transactions()
    btc_on_exchange_eur = btc_on_exchange * eur_price
    btc_in_pools = results.earnings.loc[:, 'daily_reward'].sum() - btc_on_exchange
    btc_in_pools_eur = btc_in_pools * eur_price

    print(results.earnings)
    print('')
    print('TOTAL BTC MINED: ')
    print(results.earnings.loc[:, 'daily_reward'].sum())

    print('')
    print('BTC PENDING IN POOLS: ')
    print(btc_in_pools)
    print(btc_in_pools_eur)
    print('')
    print('BTC Payed out to Exchanges: ')
    print(btc_on_exchange)
    print(btc_on_exchange_eur)

    print('')
    print('TOTAL WORTH in $: ')
    print(results.earnings.loc[:, 'daily_reward'].sum() * usd_price)
    print('')
    print('TOTAL WORTH in €: ')
    print(results.earnings.loc[:, 'daily_reward'].sum() * eur_price)
    print('')

def results(raw):
    from datetime import datetime
    today = datetime.now()
    data = Data(raw, today)

    return data

def plot_rewards_to_hashrate(earnings):
    earnings.drop(earnings.tail(1).index, inplace=True)
    earnings.drop(earnings.head(10).index, inplace=True)
    fig, ax1 = plt.subplots()
    color = 'tab:red'
    ax1.set_xlabel('days')
    ax1.set_ylabel('reward 24h in btc', color=color)
    ax1.plot(earnings['daily_reward'], color=color)
    ax1.tick_params(axis='y', labelcolor=color)
    ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis
    color = 'tab:blue'
    ax2.set_ylabel('blockgems hashrate in ph', color=color)  # we already handled the x-label with ax1
    ax2.plot(earnings['hashrate_in_phs'], color=color)
    ax2.tick_params(axis='y', labelcolor=color)
    fig.tight_layout()  # otherwise the right y-label is slightly clipped
    plt.show()

def plot_hodl_vs_sell(earnings):
    earnings.drop(earnings.tail(1).index, inplace=True)
    earnings.drop(earnings.head(10).index, inplace=True)
    fig, ax1 = plt.subplots()
    color = 'tab:red'
    ax1.set_xlabel('days')
    ax1.set_ylabel('$ SELL', color=color)
    ax1.plot(earnings['rewards_value_at_day_of_mining_usd'], color=color)
    ax1.tick_params(axis='y', labelcolor=color)
    ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis
    color = 'tab:blue'
    ax2.set_ylabel('$ HODL', color=color)  # we already handled the x-label with ax1
    ax2.plot(earnings['daily_reward_us'], color=color)
    ax2.tick_params(axis='y', labelcolor=color)
    fig.tight_layout()  # otherwise the right y-label is slightly clipped
    plt.show()

def plot_pools(raw):
    df = pd.DataFrame(columns=['timestamp', 'antpool', 'slushpool', 'luxor'])
    raw['ratio'] = raw['daily_reward'] / raw['hashrate_in_phs']
    uniqueValues = raw['timestamp'].unique()

    for x in uniqueValues:
        temp = raw.query('timestamp == "' + x + '"')
        for index, row in temp.iterrows():
            if row['pool'] == 'slushpool':
                df_1 = pd.DataFrame(
                    data={'timestamp': [row['timestamp']], 'antpool': [float('0')], 'slushpool': [float(row['ratio'])],
                          'luxor': [float('0')]})
                df = df.append(df_1)
            elif row['pool'] == 'antpool':
                df_1 = pd.DataFrame(
                    data={'timestamp': [row['timestamp']], 'antpool': [float(row['ratio'])], 'slushpool': [float('0')],
                          'luxor': [float('0')]})
                df = df.append(df_1)
            elif row['pool'] == 'luxor':
                df_1 = pd.DataFrame(
                    data={'timestamp': [row['timestamp']], 'antpool': [float('0')], 'slushpool': [float('0')],
                          'luxor': [float(row['ratio'])]})
                df = df.append(df_1)
            else:
                exit()

    uniqueValues = df['timestamp'].unique()
    df_final = pd.DataFrame(columns=['timestamp', 'antpool', 'slushpool', 'luxor'])

    for x in uniqueValues:
        temp = df.query('timestamp == "' + x + '"')
        antpool = temp.loc[:, 'antpool'].sum()
        slushpool = temp.loc[:, 'slushpool'].sum()
        luxor = temp.loc[:, 'luxor'].sum()
        df_temp = {'timestamp': x, 'antpool': float(antpool), 'slushpool': float(slushpool), 'luxor': float(luxor)}
        df_final = df_final.append(df_temp, ignore_index=True)

    df_final = df_final.sort_values(by=['timestamp'])
    df_final.drop(df_final.tail(1).index, inplace=True)
    df_final['luxor'] = df_final['luxor'].rolling(3).mean()
    df_final['slushpool'] = df_final['slushpool'].rolling(3).mean()
    df_final['antpool'] = df_final['antpool'].rolling(3).mean()
    df_final['hashrate'] = 0

    plt.xlabel("Days")
    plt.ylabel("3d SMA BTC per PHS")
    plt.plot(df_final['timestamp'], df_final['luxor'], 'r', label='LUX')
    plt.plot(df_final['timestamp'], df_final['slushpool'], 'g', label='SLU')
    plt.plot(df_final['timestamp'], df_final['antpool'], 'y', label='ANT')
    plt.legend()
    plt.show()

def etl():
    raw = get_total_earnings_raw()
    data = results(raw)

    with open('data.pickle', 'wb') as io:
        dill.dump(data, io)

    upload_file_to_azure('data.pickle')
    return data

def load_from_cache():
    get_file_from_azure('data.pickle')
    with open('data.pickle', 'rb') as io:
        data = dill.load(io)
    return data

def get_data():
    data = load_from_cache()
    if (data.timestamp < datetime.datetime.now() - datetime.timedelta(minutes=15)):
        return etl()
    return data


if __name__ == '__main__':
    etl()


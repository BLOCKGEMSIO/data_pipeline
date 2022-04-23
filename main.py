import time
import hmac, hashlib
import json
import pandas as pd
import requests
import cryptocmd
from requests.structures import CaseInsensitiveDict
import datetime
import matplotlib.pyplot as plt

coin_type = 'BTC'  # 币种
sign_id = 'BLOCKGEMS'  # 子账号名
sign_key = '12eecb4cf25e4fa684c906fa98a803a7'  # 密钥
sign_SECRET = '73939395118c445a8608c3c6e88a9527'  # 密码
html_payment = 'https://antpool.com/api/paymentHistoryV2.htm'

class Data:
    def __init__(self, total_btc, total_btc_dollar, total_btc_eur, btc_in_pools, btc_in_pools_eur, btc_on_exchange, btc_on_exchange_eur, earnings, yesterdays_reward, us_btc_price, eur_btc_price, raw):
        self.total_btc = total_btc
        self.total_btc_dollar = total_btc_dollar
        self.total_btc_eur = total_btc_eur
        self.btc_in_pools = btc_in_pools
        self.btc_in_pools_eur = btc_in_pools_eur
        self.btc_on_exchange = btc_on_exchange
        self.btc_on_exchange_eur = btc_on_exchange_eur
        self.earnings = earnings
        self.yesterdays_reward = yesterdays_reward
        self.us_btc_price = us_btc_price
        self.eur_btc_price = eur_btc_price
        self.raw = raw

class Result:
  def results(self):
      return load()

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
            value = value.replace('TH/s', '')
            value = float(value)
            value = value / 1000
            df.at[index, 'hashrate_in_phs'] = value

    df.to_csv('temp.csv', index=False)
    df_old = pd.read_csv('antpool.csv', index_col=False)
    df = pd.read_csv('temp.csv', index_col=False)
    df = df.append(df_old)
    df = df.drop_duplicates()
    df.to_csv('antpool.csv', index=False)
    df = pd.read_csv('antpool.csv', index_col=False)
    upload_file_to_azure('antpool.csv')
    return (df)


def key_in_json_old(key, json_old):
    for x in json_old:
        if x[0] == int(key):
            return True;
    return False;

def get_earnings_slushpool():
    get_file_from_azure('slushpool.json')
    url = "https://slushpool.com/stats/json/btc/"
    headers = CaseInsensitiveDict()
    headers["SlushPool-Auth-Token"] = "IX5ZydZKgFqDjU5E"
    resp = requests.get(url, headers=headers)
    json_new = json.loads(resp.text)

    with open('slushpool.json') as json_file:
        json_old = json.load(json_file)

    json_btc_new = json_new["btc"]
    json_blocks_new = json_btc_new["blocks"]

    for key, value in json_blocks_new.items():
        if key_in_json_old(key,json_old):
           continue;
        else:
            temp = json_blocks_new[key]
            user_reward = temp["user_reward"]

            if user_reward is None:
                continue;

            date = temp["date_found"]
            date = datetime.datetime.fromtimestamp(date)
            value = temp["value"]
            pool_scoring_hash_rate = temp["pool_scoring_hash_rate"]
            hashrate = ((float(user_reward) / float(value)) * pool_scoring_hash_rate) * 1.02

            item = [int(key), str(date), str(value), float(pool_scoring_hash_rate), float(hashrate), str(user_reward)]
            json_old.insert(len(json_old)-1, item)

    with open('slushpool.json', 'w') as outfile:
        json.dump(json_old, outfile)

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

    df.to_csv('slushpool.csv', index=False)
    df = pd.read_csv('slushpool.csv', index_col=False)
    upload_file_to_azure('slushpool.json')
    upload_file_to_azure('slushpool.csv')

    return df

def get_earnings_luxor():
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
        timestamp = x["date"].replace('T00:00:00+00:00',"")
        temp = {'timestamp': timestamp, 'hashrate_in_phs': hashrate, 'daily_reward': float(reward)}
        df = df.append(temp, ignore_index=True)

    df = df.drop_duplicates()
    df.to_csv('luxor.csv', index=False)
    df = pd.read_csv('luxor.csv', index_col=False)
    upload_file_to_azure('luxor.csv')

    return df

def get_total_earnings_raw():
    df_slush = get_earnings_slushpool()
    df_ant = get_earnings_antpool()
    df_luxor = get_earnings_luxor()

    df_slush = df_slush.drop_duplicates()
    df_slush['hoster'] = 'acdc'
    df_slush['pool'] = 'slushpool'
    df_ant = df_ant.drop_duplicates()
    df_ant['hoster'] = 'rosenenergoatom'
    df_ant['pool'] = 'antpool'
    df_luxor = df_luxor.drop_duplicates()
    df_luxor['hoster'] = 'infinitia'
    df_luxor['pool'] = 'luxor'

    df = pd.DataFrame(columns=['timestamp', 'hashrate_in_phs', 'daily_reward', 'hoster', 'pool'])
    df = df.append(df_ant)
    df = df.append(df_slush)
    df = df.append(df_luxor)

    df = get_historic_price_usd(df)
    df = df.drop('rewards_value_at_day_of_mining_usd', 1)
    df.to_csv('total_raw.csv', index=False)
    upload_file_to_azure("total_raw.csv")
    return df


def get_total_earnings(usd_price, eur_price):
    get_total_earnings_raw()
    df = pd.read_csv('layout.csv', index_col=False)
    df_final = pd.read_csv('layout.csv', index_col=False)
    df = df.append(get_earnings_luxor())
    df = df.append(get_earnings_slushpool())
    df = df.append(get_earnings_antpool())
    df = df.drop_duplicates()
    uniqueValues = df['timestamp'].unique()

    for x in uniqueValues:
        temp = df.query('timestamp == "' + x + '"')
        daily_rewards = temp.loc[:, 'daily_reward'].sum()
        daily_hash_rate = temp['hashrate_in_phs'].sum()
        btc_per_ph = daily_rewards / daily_hash_rate
        df_temp = {'timestamp': x, 'hashrate_in_phs': daily_hash_rate, 'daily_reward': float(daily_rewards), '24h_btc_per_ph': float(btc_per_ph)}
        df_final = df_final.append(df_temp, ignore_index=True)

    df_final = df_final.drop_duplicates().sort_values(by = 'timestamp')
    df_final = transform_to_cummulated(df_final)
    df_final = add_prices(df_final, usd_price, eur_price)
    df_final = df_final.round(5)
    df_final = get_historic_price_usd(df_final)
    df_final['daily_reward_cum_eur'] = df_final['daily_reward_cum_eur'].round(2)
    df_final['daily_reward_cum_us'] = df_final['daily_reward_cum_us'].round(2)
    df_final['hashrate_in_phs'] = df_final['hashrate_in_phs'].round(2)
    df_final['rewards_value_at_day_of_mining_usd'] = df_final['rewards_value_at_day_of_mining_usd'].round(2)
    df_final['btc_day_close_price_usd'] = df_final['btc_day_close_price_usd'].round(2)
    df_final['btc_day_low_price_usd'] = df_final['btc_day_low_price_usd'].round(2)
    df_final['btc_day_high_price_usd'] = df_final['btc_day_high_price_usd'].round(2)
    df_final['btc_day_open_price_usd'] = df_final['btc_day_open_price_usd'].round(2)
    df_final['daily_reward_eur'] = df_final['daily_reward_eur'].round(2)
    df_final['daily_reward_us'] = df_final['daily_reward_us'].round(2)
    df_final.to_csv('total.csv', index=False)
    df_final = pd.read_csv('total.csv', index_col=False)
    upload_file_to_azure('total.csv')
    return df_final


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
    start_date = min(df['timestamp'])
    end_date = max(df['timestamp'])
    price_data = get_price_for_date(transpose(start_date), transpose(end_date))
    price_data = price_data.rename(columns={"Date": "timestamp"})
    price_data = price_data.rename(columns={"Open": "btc_day_open_price_usd"})
    price_data = price_data.rename(columns={"High": "btc_day_high_price_usd"})
    price_data = price_data.rename(columns={"Low": "btc_day_low_price_usd"})
    price_data = price_data.rename(columns={"Close": "btc_day_close_price_usd"})
    price_data.drop(['Volume', 'Market Cap'], axis=1, inplace=True)
    price_data['timestamp'] = price_data['timestamp'].dt.strftime('%Y-%m-%d')
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

def results(earnings, raw, usd_price, eur_price):
    btc_on_exchange = get_btc_wallet_transactions()
    btc_on_exchange_eur = btc_on_exchange * eur_price
    btc_in_pools = earnings.loc[:, 'daily_reward'].sum() - btc_on_exchange
    btc_in_pools_eur = btc_in_pools * eur_price
    total_btc = earnings.loc[:, 'daily_reward'].sum()
    total_btc_dollar = total_btc * usd_price
    total_btc_eur = total_btc * eur_price
    yesterdays_reward = earnings['daily_reward'].iloc[-2]
    data = Data(total_btc, total_btc_dollar, total_btc_eur, btc_in_pools, btc_in_pools_eur, btc_on_exchange, btc_on_exchange_eur, earnings, yesterdays_reward, usd_price, eur_price, raw)

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
                    data={'timestamp': [row['timestamp']], 'antpool': [float('0')], 'slushpool': [float(row['ratio'])], 'luxor': [float('0')]})
                df = df.append(df_1)
            elif row['pool'] == 'antpool':
                df_1 = pd.DataFrame(
                    data={'timestamp': [row['timestamp']], 'antpool': [float(row['ratio'])], 'slushpool': [float('0')], 'luxor': [float('0')]})
                df = df.append(df_1)
            elif row['pool'] == 'luxor':
                df_1 = pd.DataFrame(
                    data={'timestamp': [row['timestamp']], 'antpool': [float('0')], 'slushpool': [float('0')], 'luxor': [float(row['ratio'])]})
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

    fig, ax1 = plt.subplots()
    color = 'tab:red'
    ax1.set_xlabel('days')
    ax1.set_ylabel('$ SELL', color=color)
    ax1.plot(df_final['luxor'], color=color)
    ax1.tick_params(axis='y', labelcolor=color)
    ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis
    color = 'tab:blue'
    ax2.set_ylabel('$ HODL', color=color)  # we already handled the x-label with ax1
    ax2.plot(df_final['slushpool'], color=color)
    ax2.tick_params(axis='y', labelcolor=color)
    fig.tight_layout()  # otherwise the right y-label is slightly clipped
    plt.show()

def load():

    get_file_from_azure('total.csv')
    df = pd.read_csv('total.csv', index_col=False)

    get_file_from_azure('total_raw.csv')
    df_raw = pd.read_csv('total.csv', index_col=False)

    us_btc_price = get_current_data_USD()['USD']
    eur_btc_price = get_current_data_EUR()['EUR']
    total_btc = df.loc[:, 'daily_reward'].sum()
    total_btc_dollar = total_btc * us_btc_price
    total_btc_eur = total_btc * eur_btc_price
    btc_on_exchange = get_btc_wallet_transactions()
    btc_on_exchange_eur = btc_on_exchange * eur_btc_price
    btc_in_pools = df.loc[:, 'daily_reward'].sum() - btc_on_exchange
    btc_in_pools_eur = btc_in_pools * eur_btc_price
    yesterdays_reward = df['daily_reward'].iloc[-2]

    return Data(total_btc, total_btc_dollar, total_btc_eur, btc_in_pools, btc_in_pools_eur, btc_on_exchange, btc_on_exchange_eur, df, yesterdays_reward, us_btc_price, eur_btc_price, df_raw)

def etl():
    us_btc_price = get_current_data_USD()['USD']
    eur_btc_price = get_current_data_EUR()['EUR']
    earnings = get_total_earnings(us_btc_price, eur_btc_price)
    raw = get_total_earnings_raw()
    return results(earnings, raw, us_btc_price, eur_btc_price)

def main():
    data = etl()
    print(data.total_btc)

    #plot_pools(data.raw)

main()

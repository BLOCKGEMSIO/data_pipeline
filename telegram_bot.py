import logging
import matplotlib.pyplot as plt
import pandas as pd
from telegram import Update, ForceReply
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext
from main import Result

# Enable logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)

logger = logging.getLogger(__name__)


# Define a few command handlers. These usually take the two arguments update and
# context.
def rewards(update: Update, context: CallbackContext) -> None:
    """Send a message when the command /start is issued."""
    user = update.effective_user

    result = Result().results()
    total_btc = str(round(result.total_btc,3))
    total_btc_eur = str(round(result.total_btc_eur,2))
    btc_price = round(float(total_btc_eur) / float(total_btc),2)
    yesterdays_reward = result.yesterdays_reward
    yesterdays_reward_eur = yesterdays_reward * btc_price
    yesterdays_reward_eur = round(yesterdays_reward_eur,2)
    yesterdays_reward_eur = str(yesterdays_reward_eur)
    yesterdays_reward = str(round(yesterdays_reward,3)).replace('.', ',', 1)
    total_btc = total_btc.replace('.', ',', 1)

    btc_on_exchange = str(round(result.btc_on_exchange,3))
    btc_on_exchange_eur = currency_format(str(round(result.btc_on_exchange_eur,2)))

    btc_in_pools = str(round(result.btc_in_pools,3))
    btc_in_pools_eur = currency_format(str(round(result.btc_in_pools_eur,2)))

    update.message.reply_text("Yesterdays Rewards:\n"
                              + yesterdays_reward +
                              " BTC = "
                              + currency_format(yesterdays_reward_eur) +
                              "€\n\n"
                              "All time rewards:\n"
                              + total_btc +
                              " BTC = "
                              + currency_format(total_btc_eur) +
                              "€ \n\nAt current BTC price of: "
                              + currency_format(str(btc_price)) +
                              "€\n\n"
                              "BTC pending in pools: "
                              + btc_on_exchange +
                              " BTC = "
                              + btc_on_exchange_eur +
                              "€\n\n"
                              "BTC payed out to exchanges: "
                              + btc_in_pools +
                              " BTC = "
                              + btc_in_pools_eur +
                              "€")

    save_rewards_plot(result.earnings)
    chat_id = update.message.chat_id
    document = open('rewards.png', 'rb')
    context.bot.send_document(chat_id, document)

def save_pools_plot(raw):
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

    plt.xlabel("Days")
    plt.ylabel("BTC per PHS (3 day SMA)")
    plt.plot(df_final['timestamp'], df_final['luxor'], 'r', label='LUX')
    plt.plot(df_final['timestamp'], df_final['slushpool'], 'g', label='SLU')
    plt.plot(df_final['timestamp'], df_final['antpool'], 'y', label='ANT')
    plt.legend()
    plt.savefig('pools.png')
    plt.clf()

def save_rewards_plot(earnings):
    earnings.drop(earnings.tail(1).index, inplace=True)
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
    plt.savefig('rewards.png')
    plt.clf()

def currency_format(s):
    s = s.replace('.', ',', 1)
    length = s.find(',')

    if length == 4:
        s = s[:1] + '.' + s[1:]
    elif length == 5:
        s = s[:2] + '.' + s[2:]
    elif length == 6:
        s = s[:3] + '.' + s[3:]
    elif length == 7:
        s = s[:1] + '.' + s[1:]
        s = s[:5] + '.' + s[5:]
    elif length == 8:
        s = s[:2] + '.' + s[2:]
        s = s[:6] + '.' + s[6:]
    elif length < 4 & length > 8:
        s = s

    return s

def hashrate(update: Update, context: CallbackContext) -> None:
    """Send a message when the command /start is issued."""

    update.message.reply_markdown_v2(
        fr'Under Construction ' + u'🚨',
        reply_markup=ForceReply(selective=True),
    )

def total(update: Update, context: CallbackContext) -> None:
    """Send a message when the command /start is issued."""

    update.message.reply_markdown_v2(
        fr'Under Construction ' + u'🚨',
        reply_markup=ForceReply(selective=True),
    )

def status(update: Update, context: CallbackContext) -> None:
    """Send a message when the command /start is issued."""

    update.message.reply_markdown_v2(
        fr'Under Construction ' + u'🚨',
        reply_markup=ForceReply(selective=True),
    )

def pools(update: Update, context: CallbackContext) -> None:
    """Send a message when the command /start is issued."""
    result = Result().results()
    save_pools_plot(result.raw)
    chat_id = update.message.chat_id
    document = open('pools.png', 'rb')
    context.bot.send_document(chat_id, document)


def uptime(update: Update, context: CallbackContext) -> None:
    """Send a message when the command /start is issued."""

    update.message.reply_markdown_v2(
        fr'Under Construction ' + u'🚨',
        reply_markup=ForceReply(selective=True),
    )

def help_command(update: Update, context: CallbackContext) -> None:
    """Send a message when the command /help is issued."""
    update.message.reply_text('/help\n\n'
                              '/rewards\n\n'
                              '/pools\n\n'
                              '/status\n\n'
                              )

def echo(update: Update, context: CallbackContext) -> None:
    """Echo the user message."""
    update.message.reply_text(update.message.text)


def main() -> None:
    """Start the bot."""
    # Create the Updater and pass it your bot's token.
    with open("token.txt") as file:
        token = file.read()

    updater = Updater(token)

    # Get the dispatcher to register handlers
    dispatcher = updater.dispatcher

    # on different commands - answer in Telegram
    dispatcher.add_handler(CommandHandler("status", status))
    dispatcher.add_handler(CommandHandler("hashrate", hashrate))
    dispatcher.add_handler(CommandHandler("rewards", rewards))
    dispatcher.add_handler(CommandHandler("total", total))
    dispatcher.add_handler(CommandHandler("pools", pools))
    dispatcher.add_handler(CommandHandler("uptime", uptime))
    dispatcher.add_handler(CommandHandler("help", help_command))

    # on non command i.e message - echo the message on Telegram
    dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, echo))

    # Start the Bot
    updater.start_polling()

    # Run the bot until you press Ctrl-C or the process receives SIGINT,
    # SIGTERM or SIGABRT. This should be used most of the time, since
    # start_polling() is non-blocking and will stop the bot gracefully.
    updater.idle()


if __name__ == '__main__':
    main()
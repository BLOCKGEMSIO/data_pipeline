import logging

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
def status(update: Update, context: CallbackContext) -> None:
    """Send a message when the command /start is issued."""
    user = update.effective_user

    result = Result().results()
    total_btc = str(result.total_btc)
    total_btc_eur = str(result.total_btc_eur)
    btc_price = float(total_btc_eur) / float(total_btc)
    yesterdays_reward = result.yesterdays_reward
    yesterdays_reward_eur = yesterdays_reward * btc_price
    yesterdays_reward_eur = round(yesterdays_reward_eur,2)
    yesterdays_reward_eur = str(yesterdays_reward_eur)
    yesterdays_reward = str(round(yesterdays_reward,3))

    update.message.reply_text("Yesterdays Rewards:\n"
                              + yesterdays_reward +
                              " BTC or "
                              + yesterdays_reward_eur +
                              " €\n\n"
                              "")

def hashrate(update: Update, context: CallbackContext) -> None:
    """Send a message when the command /start is issued."""

    update.message.reply_markdown_v2(
        fr'Under Construction ' + u'🚨',
        reply_markup=ForceReply(selective=True),
    )

def rewards(update: Update, context: CallbackContext) -> None:
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

def pools(update: Update, context: CallbackContext) -> None:
    """Send a message when the command /start is issued."""

    update.message.reply_markdown_v2(
        fr'Under Construction ' + u'🚨',
        reply_markup=ForceReply(selective=True),
    )

def uptime(update: Update, context: CallbackContext) -> None:
    """Send a message when the command /start is issued."""

    update.message.reply_markdown_v2(
        fr'Under Construction ' + u'🚨',
        reply_markup=ForceReply(selective=True),
    )

def help_command(update: Update, context: CallbackContext) -> None:
    """Send a message when the command /help is issued."""
    update.message.reply_text('/help\n\n'
                              '/hashrate\n\n'
                              '/rewards\n\n'
                              '/total\n\n'
                              '/pools\n\n'
                              '/status\n\n'
                              '/uptime\n\n'
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
    dispatcher.add_handler(CommandHandler("rewards", status))
    dispatcher.add_handler(CommandHandler("total", status))
    dispatcher.add_handler(CommandHandler("pools", status))
    dispatcher.add_handler(CommandHandler("uptime", status))
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
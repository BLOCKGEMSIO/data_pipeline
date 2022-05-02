# import telegram_bot
#
# from flask import Flask
#
# app = Flask(__name__)
#
# @app.route('/')
# def dynamic_page():
#     return telegram_bot.main()
#
# if __name__ == '__main__':
#     app.run(host='0.0.0.0', port='8000', debug=True)

from flask import Flask, request
import telegram

TOKEN = "5306263723:AAGCsQ02an-ynW0f4LtE14a9wLanAoGE7l0"
URL = "https://blockgems-telegram-bot.azurewebsites.net"

app = Flask(__name__)
bot = telegram.Bot(token=TOKEN)

# ----------------------------------
# Our public Webhook URL
# ----------------------------------
@app.route('/{}'.format(TOKEN), methods=['POST'])
def respond():
    # retrieve the message in JSON and then transform it to Telegram object
    update = telegram.Update.de_json(request.get_json(force=True), bot)

    # TODO: do something with the message

    return 'ok'

# ----------------------------------
# Our Private to 'set' our webhook URL (you should protect this URL)
# ----------------------------------
@app.route('/setwebhook', methods=['GET', 'POST'])
def set_webhook():
    s = bot.setWebhook('{URL}{HOOK}'.format(URL=URL, HOOK=TOKEN))
    if s:
        return "webhook ok"
    else:
        return "webhook failed"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port='8000', debug=True)
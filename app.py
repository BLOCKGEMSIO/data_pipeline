# import threading
# import os
# from multiprocessing import Process, Value
#
# from flask import Flask
#
# import telegram_bot
#
# app = Flask(__name__)
#
# @app.route('/')
# def dynamic_page():
#     return 'oke'
#
# if __name__ == '__main__':
#     app.run(host='0.0.0.0', port='8000', debug=True)

from flask import Flask, request
import telegram

global bot
global TOKEN
TOKEN = "5306263723:AAGCsQ02an-ynW0f4LtE14a9wLanAoGE7l0"
bot = telegram.Bot(token=TOKEN)

app = Flask(__name__)

@app.route('/{}'.format(TOKEN), methods=['POST'])
def respond():
    # retrieve the message in JSON and then transform it to Telegram object
    update = telegram.Update.de_json(request.get_json(force=True), bot)

    chat_id = update.message.chat.id
    msg_id = update.message.message_id

    # Telegram understands UTF-8, so encode text for unicode compatibility
    text = update.message.text.encode('utf-8').decode()
    print("got text message :", text)

    response = "scurr"
    bot.sendMessage(chat_id=chat_id, text=response, reply_to_message_id=msg_id)

    return 'ok'

@app.route('/setwebhook', methods=['GET', 'POST'])
def set_webhook():
    s = bot.setWebhook('{URL}{HOOK}'.format(URL="https://blockgems-telegram-bot.azurewebsites.net", HOOK=TOKEN))
    if s:
        return "webhook setup ok"
    else:
        return "webhook setup failed"

@app.route('/')
def index():
    return '.'


if __name__ == '__main__':
    app.run(threaded=True)

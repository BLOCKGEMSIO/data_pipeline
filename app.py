import telegram_bot

from flask import Flask

app = Flask(__name__)

@app.route('/')
def dynamic_page():
    return 'oke'

if __name__ == '__main__':
    telegram_bot.main()
    #app.run(host='0.0.0.0', port='8000', debug=True)


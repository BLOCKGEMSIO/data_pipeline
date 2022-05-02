import threading
import os

from flask import Flask

app = Flask(__name__)

@app.route('/')
def dynamic_page():
    download_thread = threading.Thread(target=execute, name="Bot")
    download_thread.start()
    return 'oke'

def execute():
    os.system('python telegram_bot.py')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port='8000', debug=True)


import os
from threading import Thread
from flask import Flask
from waitress import serve

app = Flask(__name__)


@app.route('/')
@app.route('/health')
def health():
    return 'OK'


def run():
    serve(app, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)),
          threads=2, connection_limit=64, channel_timeout=30)


def keep_alive():
    Thread(target=run, daemon=True).start()

import os
from flask import Flask
from threading import Thread

app = Flask('')

@app.route('/')
def home():
    return "Цой жив!"

@app.route('/health')
def health():
    return "OK"

def run():
    port = int(os.environ.get("PORT", 8080))  # <- ключевая правка
    app.run(host='0.0.0.0', port=port)

def keep_alive():
    Thread(target=run, daemon=True).start()

import asyncio
from flask import Flask
from threading import Thread

from flight_search import main_run_bot

app = Flask(__name__)

def run_bot():
    asyncio.run(main_run_bot())

# Endpoint to start the bot
@app.route('/test')
def start_bot():
    
    return "TEST!"

# WSGI entry point
if __name__ == "__main__":
    thread = Thread(target=run_bot)
    thread.start()
    app.run()

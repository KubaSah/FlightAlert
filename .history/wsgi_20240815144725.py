import asyncio
from flask import Flask, jsonify
from threading import Thread
import threading

from flight_search import main_run_bot

app = Flask(__name__)

# Global variable to track if the bot has been started
bot_started = threading.Event()

def run_bot():
    # Run the bot with asyncio
    asyncio.run(main_run_bot())

@app.route('/start')
def start_bot():
    if not bot_started.is_set():
        bot_thread = Thread(target=run_bot)
        bot_thread.start()
        bot_started.set()  # Set the event to indicate that the bot has started
        return jsonify({"status": "Bot started"}), 200
    else:
        return jsonify({"status": "Bot already running"}), 200

@app.route('/test')
def test():
    return "TEST!"

if __name__ == "__main__":
    app.run()

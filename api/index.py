from flask import Flask, request, jsonify
import telegram
import logging
import os

app = Flask(__name__)

# Basic logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TOKEN = "7911881145:AAHCjem9Kg_OgETaTVFDxcb8_ZFpaXPsbhI"
AUTHORIZED_CHAT_ID = "1285451259"  # Your chat ID

bot = telegram.Bot(token=TOKEN)

@app.route('/webhook', methods=['POST'])
def webhook():
    try:
        # Get the update from Telegram
        update = request.get_json()
        
        # Log the incoming update
        logger.info(f"Received update: {update}")
        
        # Verify this is a message update
        if 'message' not in update:
            return jsonify({"status": "ignored"}), 200
            
        message = update['message']
        chat_id = str(message['chat']['id'])
        text = message.get('text', '')
        
        # Verify authorized chat
        if chat_id != AUTHORIZED_CHAT_ID:
            logger.warning(f"Unauthorized access from chat ID: {chat_id}")
            return jsonify({"status": "unauthorized"}), 403
        
        # Simple echo response
        if text == '/start':
            bot.send_message(chat_id=chat_id, text="Bot is working! Send me a username.")
        else:
            bot.send_message(chat_id=chat_id, text=f"You sent: {text}")
        
        return jsonify({"status": "success"}), 200
        
    except Exception as e:
        logger.error(f"Error in webhook: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
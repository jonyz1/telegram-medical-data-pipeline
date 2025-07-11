import asyncio
import json
import os
from datetime import datetime
from pathlib import Path
import logging
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from dotenv import load_dotenv

# Determine project root directory (two levels up from scraper.py)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Load environment variables
load_dotenv()

# Configure logging
LOG_FILE = PROJECT_ROOT / 'logs' / 'scraper.log'
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)  # Ensure logs directory exists
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Telegram API credentials from .env
API_ID = os.getenv('TELEGRAM_API_ID')
API_HASH = os.getenv('TELEGRAM_API_HASH')
PHONE = os.getenv('TELEGRAM_PHONE')

# Data Lake base path
DATA_LAKE_PATH = PROJECT_ROOT / 'data' / 'raw' / 'telegram_messages'

# List of Telegram channels to scrape
CHANNELS = [
    'lobelia4cosmetics',
    'tikvahpharma'
]

async def initialize_client():
    """Initialize Telegram client with session handling."""
    try:
        client = TelegramClient('session', API_ID, API_HASH)
        await client.start(phone=PHONE)
        
        if not await client.is_user_authorized():
            logger.info("Authorization required. Sending code request...")
            await client.send_code_request(PHONE)
            code = input("Enter the code you received: ")
            try:
                await client.sign_in(PHONE, code)
            except SessionPasswordNeededError:
                password = input("Two-factor authentication enabled. Enter password: ")
                await client.sign_in(password=password)
        
        logger.info("Telegram client initialized successfully")
        return client
    except Exception as e:
        logger.error(f"Failed to initialize Telegram client: {str(e)}")
        raise

def get_data_lake_path(channel: str, date: str) -> Path:
    """Generate partitioned path for storing raw data."""
    return Path(DATA_LAKE_PATH) / date / channel / 'messages.json'

async def scrape_channel(client: TelegramClient, channel: str):
    """Scrape messages and images from a single Telegram channel."""
    try:
        entity = await client.get_entity(channel)
        date_str = datetime.now().strftime('%Y-%m-%d')
        output_path = get_data_lake_path(channel, date_str)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        messages_data = []
        async for message in client.iter_messages(entity, limit=100):  # Adjustable limit
            msg_data = {
                'id': message.id,
                'date': message.date.isoformat(),
                'text': message.text,
                'sender_id': message.sender_id,
                'has_media': message.media is not None,
                'media_type': None,
                'media_path': None
            }
            
            # Handle media (images)
            if message.media and hasattr(message.media, 'photo'):
                msg_data['media_type'] = 'photo'
                image_path = PROJECT_ROOT / 'data' / 'raw' / 'images' / channel / date_str
                image_path.mkdir(parents=True, exist_ok=True)
                file_name = f"{message.id}.jpg"
                file_path = image_path / file_name
                await client.download_media(message.media, file=str(file_path))
                msg_data['media_path'] = str(file_path)
                logger.info(f"Downloaded image for message {message.id} from {channel}")
            
            messages_data.append(msg_data)
        
        # Save messages to JSON
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(messages_data, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved {len(messages_data)} messages from {channel} to {output_path}")
        
    except Exception as e:
        logger.error(f"Error scraping channel {channel}: {str(e)}")

async def main():
    """Main function to scrape all specified channels."""
    client = await initialize_client()
    try:
        for channel in CHANNELS:
            logger.info(f"Starting scrape for channel: {channel}")
            await scrape_channel(client, channel)
    finally:
        await client.disconnect()
        logger.info("Telegram client disconnected")

if __name__ == "__main__":
    asyncio.run(main())
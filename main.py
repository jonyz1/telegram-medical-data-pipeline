from dotenv import load_dotenv
import os

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
TELEGRAM_API_ID = os.getenv("TELEGRAM_API_ID")

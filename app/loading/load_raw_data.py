import json
import os
from pathlib import Path
import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv
import logging

# Determine project root directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Configure logging
LOG_FILE = PROJECT_ROOT / 'logs' / 'load_raw_data.log'
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')

# Data Lake path
DATA_LAKE_PATH = PROJECT_ROOT / 'data' / 'raw' / 'telegram_messages'

def get_db_connection():
    """Establish connection to PostgreSQL."""
    try:
        conn = psycopg2.connect(
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT
        )
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {str(e)}")
        raise

def create_raw_table(conn):
    """Create raw.telegram_messages table if it doesn't exist."""
    create_table_query = """
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE TABLE IF NOT EXISTS raw.telegram_messages (
        id BIGINT,
        channel_name VARCHAR(255),
        message_date TIMESTAMP,
        message_data JSONB,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    try:
        with conn.cursor() as cur:
            cur.execute(create_table_query)
            conn.commit()
            logger.info("Created raw.telegram_messages table")
    except Exception as e:
        logger.error(f"Failed to create table: {str(e)}")
        conn.rollback()
        raise

def load_json_files():
    """Load JSON files from Data Lake into PostgreSQL."""
    conn = get_db_connection()
    try:
        create_raw_table(conn)
        with conn.cursor() as cur:
            for json_file in DATA_LAKE_PATH.rglob('messages.json'):
                channel_name = json_file.parent.name
                date_str = json_file.parent.parent.name
                logger.info(f"Processing {json_file}")
                
                with open(json_file, 'r', encoding='utf-8') as f:
                    messages = json.load(f)
                
                for msg in messages:
                    insert_query = """
                    INSERT INTO raw.telegram_messages (id, channel_name, message_date, message_data)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                    """
                    cur.execute(insert_query, (
                        msg['id'],
                        channel_name,
                        msg['date'],
                        Json(msg)
                    ))
                
                conn.commit()
                logger.info(f"Loaded {len(messages)} messages from {channel_name}/{date_str}")
    except Exception as e:
        logger.error(f"Error loading JSON files: {str(e)}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    load_json_files()


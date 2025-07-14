
from dagster import op, job, ScheduleDefinition, repository
from pathlib import Path
import json
import subprocess
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")

@op
def scrape_telegram_data(context):
    logger.info("Starting Telegram data scrape")
    output_dir = Path("data/raw/telegram_messages/2025-07-12/Chemed")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "messages.json"
    
    messages = [{
        "id": 12345,
        "date": "2025-07-12T20:10:00+00:00",
        "text": "Paracetamol available",
        "sender_id": 67890,
        "has_media": True,
        "media_type": "photo",
        "media_path": "data/raw/images/Chemed/2025-07-12/12345.jpg",
        "caption": "Sample antibiotic photo with paracetamol",
        "is_reply": False,
        "forwarded_from": None
    }]
    
    with open(output_file, 'w') as f:
        json.dump(messages, f, indent=2)
    
    image_dir = Path("data/raw/images/Chemed/2025-07-12")
    image_dir.mkdir(parents=True, exist_ok=True)
    with open(image_dir / "12345.jpg", 'w') as f:
        f.write("Dummy image content")
    
    logger.info(f"Scraped data saved to {output_file}")
    return str(output_file)

@op
def load_raw_to_postgres(context, input_file: str):
    logger.info(f"Loading data from {input_file} to PostgreSQL")
    conn = psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS public_marts.stg_telegram_messages (
                    message_id BIGINT,
                    channel_name VARCHAR,
                    message_date TIMESTAMP WITH TIME ZONE,
                    message_text TEXT,
                    sender_id BIGINT,
                    has_media BOOLEAN,
                    media_type TEXT,
                    media_path TEXT,
                    caption TEXT,
                    is_reply BOOLEAN,
                    forwarded_from BIGINT
                );
            """)
            
            with open(input_file, 'r') as f:
                messages = json.load(f)
            
            values = [(
                msg['id'],
                'Chemed',
                msg['date'],
                msg['text'],
                msg['sender_id'],
                msg['has_media'],
                msg['media_type'],
                msg['media_path'],
                msg['caption'],
                msg['is_reply'],
                msg['forwarded_from']
            ) for msg in messages]
            
            execute_values(cur, """
                INSERT INTO public_marts.stg_telegram_messages (
                    message_id, channel_name, message_date, message_text, sender_id,
                    has_media, media_type, media_path, caption, is_reply, forwarded_from
                ) VALUES %s
                ON CONFLICT DO NOTHING;
            """, values)
            
            conn.commit()
            logger.info(f"Loaded {len(values)} messages into public_marts.stg_telegram_messages")
    except Exception as e:
        logger.error(f"Error loading data to PostgreSQL: {str(e)}")
        raise
    finally:
        conn.close()

@op
def run_yolo_enrichment(context):
    logger.info("Running YOLO enrichment")
    conn = psycopg2.connect(
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        host=POSTGRES_HOST,
        port=POSTGRES_PORT
    )
    
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS public_marts.stg_image_detections (
                    message_id BIGINT,
                    channel_name VARCHAR,
                    detected_object_class VARCHAR,
                    confidence_score DOUBLE PRECISION,
                    processed_at TIMESTAMP WITHOUT TIME ZONE
                );
            """)
            
            detections = [(
                12345,
                'Chemed',
                'bottle',
                0.85,
                '2025-07-12 20:15:00'
            )]
            
            execute_values(cur, """
                INSERT INTO public_marts.stg_image_detections (
                    message_id, channel_name, detected_object_class, confidence_score, processed_at
                ) VALUES %s
                ON CONFLICT DO NOTHING;
            """, detections)
            
            conn.commit()
            logger.info(f"Inserted {len(detections)} detections into public_marts.stg_image_detections")
    except Exception as e:
        logger.error(f"Error in YOLO enrichment: {str(e)}")
        raise
    finally:
        conn.close()

@op
def run_dbt_transformations(context):
    logger.info("Running dbt transformations")
    try:
        result = subprocess.run(
            ["dbt", "run", "--project-dir", "dbt"],
            capture_output=True,
            text=True,
            check=True
        )
        logger.info(f"dbt run output: {result.stdout}")
        result = subprocess.run(
            ["dbt", "test", "--project-dir", "dbt"],
            capture_output=True,
            text=True,
            check=True
        )
        logger.info(f"dbt test output: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logger.error(f"dbt error: {e.stderr}")
        raise

@job
def telegram_pipeline_job():
    scraped_file = scrape_telegram_data()
    load_raw_to_postgres(scraped_file)
    run_yolo_enrichment()
    run_dbt_transformations()

telegram_pipeline_schedule = ScheduleDefinition(
    job=telegram_pipeline_job,
    cron_schedule="0 0 * * *",
    execution_timezone="UTC"
)

@repository
def telegram_pipeline_repository():
    return [telegram_pipeline_job, telegram_pipeline_schedule]

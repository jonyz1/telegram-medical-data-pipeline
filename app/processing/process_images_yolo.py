import os
from pathlib import Path
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
import logging
from ultralytics import YOLO
from datetime import datetime

# Determine project root directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Configure logging
LOG_FILE = PROJECT_ROOT / 'logs' / 'yolo_processing.log'
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

# Image directory
IMAGE_PATH = PROJECT_ROOT / 'data' / 'raw' / 'images'

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

def create_detections_table(conn):
    """Create raw.image_detections table if it doesn't exist."""
    create_table_query = """
    CREATE SCHEMA IF NOT EXISTS raw;
    CREATE TABLE IF NOT EXISTS raw.image_detections (
        message_id BIGINT,
        channel_name VARCHAR(255),
        detected_object_class VARCHAR(100),
        confidence_score FLOAT,
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    try:
        with conn.cursor() as cur:
            cur.execute(create_table_query)
            conn.commit()
            logger.info("Created raw.image_detections table")
    except Exception as e:
        logger.error(f"Failed to create table: {str(e)}")
        conn.rollback()
        raise

def process_images():
    """Scan images, run YOLOv8, and store results in PostgreSQL."""
    # Load YOLOv8 model
    model = YOLO('yolov8n.pt')  # Pre-trained YOLOv8 nano model
    conn = get_db_connection()
    try:
        create_detections_table(conn)
        detections = []
        
        # Scan images in data/raw/images/
        for image_file in IMAGE_PATH.rglob('*.jpg'):
            channel_name = image_file.parent.parent.name
            date_str = image_file.parent.name
            message_id = int(image_file.stem)  # Extract message_id from filename
            logger.info(f"Processing image: {image_file}")
            
            # Run YOLOv8 inference
            results = model(image_file)
            
            # Extract detections
            for result in results:
                for box in result.boxes:
                    class_id = int(box.cls)
                    class_name = model.names[class_id]
                    confidence = float(box.conf)
                    detections.append((
                        message_id,
                        channel_name,
                        class_name,
                        confidence
                    ))
            
            logger.info(f"Detected {len(detections)} objects in {image_file}")
        
        # Insert detections into PostgreSQL
        if detections:
            with conn.cursor() as cur:
                insert_query = """
                INSERT INTO raw.image_detections (
                    message_id, channel_name, detected_object_class, confidence_score
                ) VALUES (%s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
                """
                execute_batch(cur, insert_query, detections)
                conn.commit()
                logger.info(f"Inserted {len(detections)} detections into raw.image_detections")
        else:
            logger.info("No objects detected in images")
            
    except Exception as e:
        logger.error(f"Error processing images: {str(e)}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    process_images()
import psycopg2
from typing import List, Dict
import logging

# Configure logging
from app.api.database import get_db_connection, PROJECT_ROOT
LOG_FILE = PROJECT_ROOT / 'logs' / 'api.log'
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_top_products(conn, limit: int = 10) -> List[Dict]:
    """Get the top N most frequently mentioned products from text and YOLO detections."""
    logger.debug(f"Executing get_top_products with limit={limit}")
    if limit < 1:
        logger.error("Limit must be a positive integer")
        raise ValueError("Limit must be a positive integer")
    
    query = """
    WITH text_mentions AS (
        SELECT
            TRIM(product_name) AS product_name,
            COUNT(*) AS mention_count
        FROM public_marts.fct_messages,
        UNNEST(STRING_TO_ARRAY(LOWER(COALESCE(message_text, '') || ' ' || COALESCE(caption, '')), ' ')) AS product_name
        WHERE (message_text IS NOT NULL OR caption IS NOT NULL)
        AND LENGTH(TRIM(product_name)) > 0
        GROUP BY product_name
    ),
    image_mentions AS (
        SELECT
            detected_object_class AS product_name,
            COUNT(*) AS mention_count
        FROM public_marts.fct_image_detections
        WHERE detected_object_class IS NOT NULL
        GROUP BY detected_object_class
    ),
    combined AS (
        SELECT product_name, mention_count FROM text_mentions
        UNION ALL
        SELECT product_name, mention_count FROM image_mentions
    )
    SELECT
        COALESCE(product_name, 'unknown') AS product_name,
        SUM(mention_count) AS mention_count
    FROM combined
    WHERE product_name IS NOT NULL AND LENGTH(TRIM(product_name)) > 0
    GROUP BY product_name
    ORDER BY mention_count DESC
    LIMIT %s;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query, (limit,))
            results = cur.fetchall()
            if not results:
                logger.info("No products found in public_marts.fct_messages or public_marts.fct_image_detections")
                return []
            logger.debug(f"Retrieved {len(results)} top products")
            return [{"product_name": row[0], "mention_count": row[1]} for row in results]
    except psycopg2.ProgrammingError as e:
        logger.error(f"Database schema error in get_top_products: {str(e)}")
        raise
    except psycopg2.OperationalError as e:
        logger.error(f"Database connection error in get_top_products: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in get_top_products: {str(e)}")
        raise

def get_channel_activity(conn, channel_name: str) -> List[Dict]:
    """Get posting activity for a specific channel."""
    logger.debug(f"Executing get_channel_activity for channel={channel_name}")
    query = """
    SELECT
        d.date_day::DATE AS date,
        COUNT(m.message_id) AS message_count,
        SUM(CASE WHEN m.has_media AND m.media_type = 'photo' THEN 1 ELSE 0 END) AS image_count
    FROM public_marts.fct_messages m
    JOIN public_marts.dim_channels c ON m.channel_id = c.channel_id
    JOIN public_marts.dim_dates d ON m.date_id = d.date_id
    WHERE c.channel_name = %s
    GROUP BY d.date_day
    ORDER BY d.date_day;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query, (channel_name,))
            results = cur.fetchall()
            if not results:
                logger.info(f"No activity found for channel '{channel_name}'")
                return []
            logger.debug(f"Retrieved {len(results)} activity records for channel {channel_name}")
            return [{"date": row[0], "message_count": row[1], "image_count": row[2]} for row in results]
    except psycopg2.Error as e:
        logger.error(f"Database error in get_channel_activity for {channel_name}: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in get_channel_activity for {channel_name}: {str(e)}")
        raise

def search_messages(conn, query: str) -> List[Dict]:
    """Search messages and captions for a keyword."""
    logger.debug(f"Executing search_messages with query={query}")
    query = f"%{query.lower()}%"
    sql = """
    SELECT
        m.message_id,
        c.channel_name,
        m.message_text,
        m.caption,
        d.date_day::DATE AS message_date
    FROM public_marts.fct_messages m
    JOIN public_marts.dim_channels c ON m.channel_id = c.channel_id
    JOIN public_marts.dim_dates d ON m.date_id = d.date_id
    WHERE LOWER(COALESCE(m.message_text, '')) LIKE %s
        OR LOWER(COALESCE(m.caption, '')) LIKE %s;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (query, query))
            results = cur.fetchall()
            if not results:
                logger.info(f"No messages found for query '{query}'")
                return []
            logger.debug(f"Retrieved {len(results)} messages for query '{query}'")
            return [{
                "message_id": row[0],
                "channel_name": row[1],
                "message_text": row[2],
                "caption": row[3],
                "message_date": row[4]
            } for row in results]
    except psycopg2.Error as e:
        logger.error(f"Database error in search_messages for query '{query}': {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in search_messages for query '{query}': {str(e)}")
        raise
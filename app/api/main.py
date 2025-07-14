from fastapi import FastAPI, Depends, HTTPException
import psycopg2
from typing import List
from app.api.database import get_db
from app.api.schemas import TopProductsResponse, ChannelActivityResponse, MessageSearchResponse
from app.api.crud import get_top_products, get_channel_activity, search_messages
import logging

# Configure logging
from app.api.database import PROJECT_ROOT
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

app = FastAPI(title="Telegram Medical Data Pipeline API")

@app.get("/api/reports/top-products", response_model=TopProductsResponse)
def get_top_products_endpoint(limit: int = 10, conn=Depends(get_db)):
    """Get the top N most frequently mentioned products."""
    logger.debug(f"Calling get_top_products_endpoint with limit={limit}")
    try:
        products = get_top_products(conn, limit)
        logger.debug(f"Retrieved {len(products)} products")
        return {"products": products}
    except psycopg2.ProgrammingError as e:
        logger.error(f"Database schema error in top-products: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database schema error: {str(e)}")
    except psycopg2.OperationalError as e:
        logger.error(f"Database connection error in top-products: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database connection error: {str(e)}")
    except ValueError as e:
        logger.error(f"Invalid input in top-products: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Invalid input: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error in top-products: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

@app.get("/api/channels/{channel_name}/activity", response_model=ChannelActivityResponse)
def get_channel_activity(channel_name: str, conn=Depends(get_db)):
    """Get posting activity for a specific channel."""
    logger.debug(f"Calling get_channel_activity for channel={channel_name}")
    try:
        activity = get_channel_activity(conn, channel_name)
        if not activity:
            logger.info(f"No activity found for channel {channel_name}")
            raise HTTPException(status_code=404, detail=f"Channel {channel_name} not found")
        logger.debug(f"Retrieved {len(activity)} activity records")
        return {"channel_name": channel_name, "activity": activity}
    except psycopg2.Error as e:
        logger.error(f"Database error in channel activity: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error in channel activity: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

@app.get("/api/search/messages", response_model=MessageSearchResponse)
def search_messages_endpoint(query: str, conn=Depends(get_db)):
    """Search messages for a keyword."""
    logger.debug(f"Calling search_messages_endpoint with query={query}")
    try:
        messages = search_messages(conn, query)
        logger.debug(f"Retrieved {len(messages)} messages")
        return {"messages": messages}
    except psycopg2.Error as e:
        logger.error(f"Database error in search messages: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error in search messages: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")
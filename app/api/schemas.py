from pydantic import BaseModel
from datetime import datetime
from typing import List, Optional

class ProductCount(BaseModel):
    product_name: str
    mention_count: int

class ChannelActivity(BaseModel):
    date: datetime
    message_count: int
    image_count: int

class MessageSearchResult(BaseModel):
    message_id: int
    channel_name: str
    message_text: Optional[str]
    caption: Optional[str]
    message_date: datetime

class TopProductsResponse(BaseModel):
    products: List[ProductCount]

class ChannelActivityResponse(BaseModel):
    channel_name: str
    activity: List[ChannelActivity]

class MessageSearchResponse(BaseModel):
    messages: List[MessageSearchResult]
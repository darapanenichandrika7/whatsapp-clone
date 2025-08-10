from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class MessageBase(BaseModel):
    wa_id: str
    text: str
    direction: str  # 'inbound' or 'outbound'
    status: str = 'sent'  # 'sent', 'delivered', 'read'

class MessageCreate(MessageBase):
    meta_msg_id: str

class Message(MessageBase):
    id: str
    meta_msg_id: str
    timestamp: datetime

    class ConfigDict:
        from_attributes = True

class MessageStatusUpdate(BaseModel):
    meta_msg_id: str
    new_status: str
import os
import argparse
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

import socketio
import uvicorn
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
from pydantic import BaseModel
from bson import ObjectId

load_dotenv()

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---- FastAPI app ----
fastapi_app = FastAPI(title="WhatsApp Web Clone API")

# Configure CORS from environment
cors_origins = os.getenv("CORS_ORIGINS", "").split(",")
if not cors_origins or cors_origins == [""]:
    cors_origins = ["http://localhost:3000", "http://127.0.0.1:3000"]

fastapi_app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---- Socket.IO server ----
sio = socketio.AsyncServer(
    async_mode="asgi",
    cors_allowed_origins="*",
    logger=logger,
    async_handlers=True,
)

class ChatNamespace(socketio.AsyncNamespace):
    async def on_connect(self, sid, environ):
        logger.info(f"Client connected: {sid}")
        return True

    async def on_join(self, sid, data):
        wa_id = data.get("wa_id")
        if wa_id:
            await self.enter_room(sid, wa_id)
            logger.info(f"Client {sid} joined room {wa_id}")

    async def on_disconnect(self, sid):
        logger.info(f"Client disconnected: {sid}")

sio.register_namespace(ChatNamespace("/"))
socketio_app = socketio.ASGIApp(sio, fastapi_app, socketio_path="/socket.io")
app = socketio_app

# ---- Constants ----
VALID_STATUS_TRANSITIONS = {
    'sent': ['delivered', 'read'],
    'delivered': ['read'],
    'read': []  # Final state
}

# ---- DB lifecycle ----
@fastapi_app.on_event("startup")
async def startup_db_client():
    mongodb_uri = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
    mongodb_name = os.getenv("MONGODB_NAME", "whatsapp")
    
    try:
        fastapi_app.mongodb_client = AsyncIOMotorClient(mongodb_uri)
        fastapi_app.mongodb = fastapi_app.mongodb_client[mongodb_name]
        
        # Create indexes
        await fastapi_app.mongodb.processed_messages.create_index("wa_id")
        await fastapi_app.mongodb.processed_messages.create_index("meta_msg_id", unique=True)
        await fastapi_app.mongodb.processed_messages.create_index("timestamp")
        
        logger.info("MongoDB connected and indexes ensured")
    except Exception as e:
        logger.error(f"MongoDB connection failed: {str(e)}")
        raise


@fastapi_app.on_event("shutdown")
async def shutdown_db_client():
    fastapi_app.mongodb_client.close()
    logger.info("MongoDB connection closed")


# ---- Models ----
class MessageBase(BaseModel):
    wa_id: str
    text: str
    direction: str
    status: str = 'sent'


class MessageCreate(MessageBase):
    meta_msg_id: str


class MessageStatusUpdate(BaseModel):
    meta_msg_id: str
    new_status: str


# ---- Exceptions ----
class MessageNotFound(HTTPException):
    def __init__(self):
        super().__init__(status_code=404, detail="Message not found")


class InvalidStatusTransition(HTTPException):
    def __init__(self, current: str, new: str):
        detail = f"Invalid status transition: {current} → {new}"
        super().__init__(status_code=400, detail=detail)


# ---- Serialization helper ----
def serialize(value: Any) -> Any:
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat() + "Z"
    if isinstance(value, dict):
        return {k: serialize(v) for k, v in value.items()}
    if isinstance(value, list):
        return [serialize(v) for v in value]
    return value


# ---- DB helpers ----
async def db_insert_message(message_data: Dict[str, Any]) -> Dict[str, Any]:
    """Insert message with UTC timestamp"""
    doc = dict(message_data)
    doc['timestamp'] = datetime.utcnow()
    
    try:
        await fastapi_app.mongodb.processed_messages.insert_one(doc)
        saved = await fastapi_app.mongodb.processed_messages.find_one(
            {"meta_msg_id": doc["meta_msg_id"]}
        )
        return saved
    except Exception as e:
        logger.error(f"Insert failed: {str(e)}")
        raise HTTPException(500, "Database insert error")


async def db_update_message_status(meta_msg_id: str, new_status: str) -> int:
    """Update message status with validation"""
    # Get current status
    current = await fastapi_app.mongodb.processed_messages.find_one(
        {"meta_msg_id": meta_msg_id}
    )
    
    if not current:
        return 0
        
    current_status = current.get("status", "sent")
    
    # Validate transition
    if new_status not in VALID_STATUS_TRANSITIONS.get(current_status, []):
        raise InvalidStatusTransition(current_status, new_status)
    
    # Update status
    result = await fastapi_app.mongodb.processed_messages.update_one(
        {'meta_msg_id': meta_msg_id},
        {'$set': {'status': new_status}}
    )
    return result.modified_count


async def db_get_messages_by_wa_id(
    wa_id: str, 
    page: int = 1, 
    limit: int = 50
) -> List[Dict[str, Any]]:
    """Get paginated messages sorted by timestamp"""
    skip = (page - 1) * limit
    cursor = fastapi_app.mongodb.processed_messages.find({'wa_id': wa_id})
    cursor.sort('timestamp', -1).skip(skip).limit(limit)
    
    try:
        docs = await cursor.to_list(length=limit)
        return [serialize(d) for d in docs]
    except Exception as e:
        logger.error(f"Message fetch failed: {str(e)}")
        raise HTTPException(500, "Database query error")


async def db_get_all_chats() -> List[Dict[str, Any]]:
    """Get chat list with unread count"""
    pipeline = [
        {"$group": {
            "_id": "$wa_id",
            "last_message": {"$last": "$$ROOT"},
            "unread_count": {
                "$sum": {
                    "$cond": [
                        {"$and": [
                            {"$eq": ["$direction", "inbound"]},
                            {"$in": ["$status", ["sent", "delivered"]]}
                        ]},
                        1,
                        0
                    ]
                }
            }
        }},
        {"$sort": {"last_message.timestamp": -1}}
    ]
    
    try:
        cursor = fastapi_app.mongodb.processed_messages.aggregate(pipeline)
        docs = await cursor.to_list(length=100)
        return [serialize(d) for d in docs]
    except Exception as e:
        logger.error(f"Chat aggregation failed: {str(e)}")
        raise HTTPException(500, "Database aggregation error")


# ---- Endpoints ----
@fastapi_app.get("/health")
async def health_check():
    """Health check endpoint for deployment"""
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}


@fastapi_app.post("/webhook", response_model=dict)
async def process_webhook_payload(payload: dict):
    """Handle incoming webhook events"""
    try:
        event_type = payload.get("type", "").lower().strip()

        # New message
        if event_type == "message":
            message_data = {
                "wa_id": payload["wa_id"],
                "text": payload["text"],
                "meta_msg_id": payload["meta_msg_id"],
                "direction": "inbound",
                "status": payload.get("status", "delivered")
            }
            saved = await db_insert_message(message_data)
            
            # Minimal real-time payload
            await sio.emit(
                "new_message",
                {
                    "wa_id": saved["wa_id"],
                    "message": {
                        "id": str(saved["_id"]),
                        "text": saved["text"],
                        "timestamp": saved["timestamp"].isoformat() + "Z",
                        "status": saved["status"],
                        "direction": saved["direction"]
                    }
                },
                room=saved["wa_id"],
                namespace="/"
            )
            return {"status": "success", "event": "message_received"}

        # Status update
        elif event_type == "status":
            meta_msg_id = payload["meta_msg_id"]
            new_status = payload["status"]
            modified_count = await db_update_message_status(meta_msg_id, new_status)
            
            if modified_count:
                # Minimal real-time payload
                await sio.emit(
                    "status_update",
                    {
                        "meta_msg_id": meta_msg_id,
                        "new_status": new_status
                    },
                    room=payload.get("wa_id", "global"),
                    namespace="/"
                )
                return {"status": "success", "event": "status_updated"}
            raise MessageNotFound()

        else:
            raise HTTPException(400, "Invalid event type")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Webhook processing failed: {str(e)}")
        raise HTTPException(500, "Internal server error")


@fastapi_app.post("/messages", response_model=dict)
async def create_message(message: MessageCreate):
    """Create a new outgoing message"""
    try:
        md = message.dict()
        md["direction"] = "outbound"
        md["status"] = "sent"
        saved = await db_insert_message(md)
        
        # Minimal real-time payload
        await sio.emit(
            "new_message",
            {
                "wa_id": saved["wa_id"],
                "message": {
                    "id": str(saved["_id"]),
                    "text": saved["text"],
                    "timestamp": saved["timestamp"].isoformat() + "Z",
                    "status": saved["status"],
                    "direction": saved["direction"]
                }
            },
            room=saved["wa_id"],
            namespace="/"
        )
        return {"status": "success", "message_id": str(saved["_id"])}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Message creation failed: {str(e)}")
        raise HTTPException(500, "Internal server error")


@fastapi_app.put("/messages/status", response_model=dict)
async def update_message_status_endpoint(update: MessageStatusUpdate):
    """Update message status"""
    try:
        modified_count = await db_update_message_status(
            update.meta_msg_id, 
            update.new_status
        )
        
        if modified_count:
            message = await fastapi_app.mongodb.processed_messages.find_one(
                {"meta_msg_id": update.meta_msg_id}
            )
            
            # Minimal real-time payload
            await sio.emit(
                "status_update",
                {
                    "meta_msg_id": update.meta_msg_id,
                    "new_status": update.new_status
                },
                room=message["wa_id"],
                namespace="/"
            )
            return {"status": "success"}
        raise MessageNotFound()
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Status update failed: {str(e)}")
        raise HTTPException(500, "Internal server error")


@fastapi_app.get("/messages/{wa_id}")
async def get_messages(
    wa_id: str,
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=100)
):
    """Get paginated messages for a chat"""
    try:
        messages = await db_get_messages_by_wa_id(wa_id, page, limit)
        return JSONResponse(content=messages)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Message retrieval failed: {str(e)}")
        raise HTTPException(500, "Internal server error")


@fastapi_app.get("/chats")
async def get_all_chats_endpoint():
    """Get all chat conversations"""
    try:
        chats = await db_get_all_chats()
        return JSONResponse(content=chats)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Chat list retrieval failed: {str(e)}")
        raise HTTPException(500, "Internal server error")


# ---- CLI run ----
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--reload", action="store_true")
    args = parser.parse_args()

    uvicorn.run(app, host=args.host, port=args.port, reload=args.reload)
from pymongo import IndexModel, ASCENDING
from datetime import datetime

class MessageModel:
    def __init__(self, db):
        self.collection = db['processed_messages']
        self.setup_indexes()
    
    def setup_indexes(self):
        indexes = [
            IndexModel([('wa_id', ASCENDING)]),
            IndexModel([('meta_msg_id', ASCENDING)], unique=True),
            IndexModel([('timestamp', ASCENDING)])
        ]
        self.collection.create_indexes(indexes)
    
    async def insert_message(self, message_data):
        message_data['timestamp'] = datetime.utcnow()
        result = await self.collection.insert_one(message_data)
        return result.inserted_id
    
    async def update_message_status(self, meta_msg_id, new_status):
        result = await self.collection.update_one(
            {'meta_msg_id': meta_msg_id},
            {'$set': {'status': new_status}}
        )
        return result.modified_count
    
    async def get_messages_by_wa_id(self, wa_id):
        cursor = self.collection.find({'wa_id': wa_id}).sort('timestamp', 1)
        return await cursor.to_list(length=1000)
    
    async def get_all_chats(self):
        pipeline = [
            {"$group": {
                "_id": "$wa_id",
                "last_message": {"$last": "$$ROOT"},
                "unread_count": {
                    "$sum": {
                        "$cond": [{"$eq": ["$status", "delivered"]}, 1, 0]
                    }
                }
            }},
            {"$sort": {"last_message.timestamp": -1}}
        ]
        cursor = self.collection.aggregate(pipeline)
        return await cursor.to_list(length=100)
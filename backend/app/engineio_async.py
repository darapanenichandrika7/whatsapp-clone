# backend/app/engineio_async.py
import asyncio
import uvicorn
from uvicorn.config import Config
from uvicorn.server import Server

class CustomServer(Server):
    async def run(self, sockets=None):
        self.config.setup_event_loop()
        return await self.serve(sockets=sockets)

def run(app, **kwargs):
    config = Config(app, **kwargs)
    server = CustomServer(config=config)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(server.serve())
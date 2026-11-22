#!/usr/bin/env python3
"""
WebSocket server for real-time countdown sync over LAN.
Serves index.html and broadcasts timer state to all connected clients.
"""
import asyncio
import json
import os
from pathlib import Path
from http.server import SimpleHTTPRequestHandler
from socketserver import TCPServer
import threading
import websockets


# Password for authentication (set via AUTH_TOKEN env var, default for dev)
import os
AUTH_TOKEN = os.environ.get("AUTH_TOKEN", "concrete-evidence-69")

# Shared timer state
class TimerState:
    def __init__(self):
        self.total_seconds = 300  # 5 minutes default
        self.remaining = 300
        self.paused = False
        self.last_update = None  # initialized on first tick
        self._lock = asyncio.Lock()
        self.clients = set()

    async def register(self, ws):
        self.clients.add(ws)
        await self.send_state(ws)

    async def unregister(self, ws):
        self.clients.discard(ws)

    async def broadcast(self, message):
        if self.clients:
            await asyncio.gather(*[c.send(message) for c in self.clients], return_exceptions=True)

    async def send_state(self, ws):
        await ws.send(json.dumps({
            "type": "state",
            "total_seconds": self.total_seconds,
            "remaining": self.remaining,
            "paused": self.paused,
        }))

    async def set_time(self, hours, minutes, seconds):
        async with self._lock:
            self.total_seconds = hours * 3600 + minutes * 60 + seconds
            if self.total_seconds == 0:
                self.total_seconds = 300
            self.remaining = self.total_seconds
            self.paused = False
            self.last_update = asyncio.get_event_loop().time()
            await self.broadcast(json.dumps({
                "type": "state",
                "total_seconds": self.total_seconds,
                "remaining": self.remaining,
                "paused": self.paused,
            }))
            # Also send a tick immediately so clients update smoothly
            await self.broadcast(json.dumps({
                "type": "tick",
                "remaining": self.remaining,
            }))

    async def toggle_pause(self):
        async with self._lock:
            self.paused = not self.paused
            if not self.paused:
                self.last_update = asyncio.get_event_loop().time()
            else:
                # When pausing, update remaining one last time
                now = asyncio.get_event_loop().time()
                if self.last_update is not None:
                    dt = now - self.last_update
                    self.remaining = max(0, self.remaining - dt)
                    self.last_update = now
            await self.broadcast(json.dumps({
                "type": "state",
                "total_seconds": self.total_seconds,
                "remaining": self.remaining,
                "paused": self.paused,
            }))

    async def tick(self):
        """Called periodically to decrement remaining time."""
        async with self._lock:
            if not self.paused and self.remaining > 0:
                now = asyncio.get_event_loop().time()
                if self.last_update is None:
                    self.last_update = now
                    return
                dt = now - self.last_update
                self.remaining = max(0, self.remaining - dt)
                self.last_update = now
                if self.remaining <= 0:
                    self.remaining = 0
                await self.broadcast(json.dumps({
                    "type": "tick",
                    "remaining": self.remaining,
                }))


state = TimerState()


async def ws_handler(ws):
    # Require auth as first message
    try:
        auth_msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
        auth_data = json.loads(auth_msg)
        if auth_data.get("type") != "auth" or auth_data.get("token") != AUTH_TOKEN:
            await ws.send(json.dumps({"type": "error", "message": "Invalid token"}))
            await ws.close(code=4001, reason="Invalid token")
            return
    except (asyncio.TimeoutError, json.JSONDecodeError, websockets.exceptions.ConnectionClosed):
        await ws.close(code=4001, reason="Auth required")
        return

    await state.register(ws)
    # Send initial state after successful auth
    await state.send_state(ws)

    try:
        async for message in ws:
            data = json.loads(message)
            if data["type"] == "set_time":
                await state.set_time(data["hours"], data["minutes"], data["seconds"])
            elif data["type"] == "toggle_pause":
                await state.toggle_pause()
            elif data["type"] == "request_state":
                await state.send_state(ws)
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        await state.unregister(ws)


async def tick_loop():
    """Background task to decrement timer and broadcast ticks."""
    while True:
        await state.tick()
        await asyncio.sleep(0.1)  # 100ms tick rate


def run_http_server(port=8080):
    """Run HTTP server in a separate thread."""
    os.chdir(Path(__file__).parent)
    handler = SimpleHTTPRequestHandler
    with TCPServer(("", port), handler) as httpd:
        print(f"HTTP server running on http://0.0.0.0:{port}")
        httpd.serve_forever()


async def main():
    # Start HTTP server in background thread
    http_thread = threading.Thread(target=run_http_server, args=(8080,), daemon=True)
    http_thread.start()

    # Start WebSocket server
    print("WebSocket server running on ws://0.0.0.0:8081")
    async with websockets.serve(ws_handler, "0.0.0.0", 8081):
        # Run tick loop
        await tick_loop()


if __name__ == "__main__":
    asyncio.run(main())
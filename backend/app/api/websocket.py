"""WebSocket endpoint that fans live edits out to the dashboard."""
from __future__ import annotations

import asyncio
import json

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

router = APIRouter()


class _Hub:
    def __init__(self) -> None:
        self.clients: set[WebSocket] = set()

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self.clients.add(ws)
        _set_clients()

    def disconnect(self, ws: WebSocket) -> None:
        self.clients.discard(ws)
        _set_clients()

    async def broadcast(self, message: dict) -> None:
        payload = json.dumps(message, default=str)
        dead: list[WebSocket] = []
        for ws in list(self.clients):
            try:
                await ws.send_text(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.clients.discard(ws)
        if dead:
            _set_clients()


def _set_clients() -> None:
    try:
        from app.observability import WS_CLIENTS

        WS_CLIENTS.set(len(hub.clients))
    except Exception:  # noqa: BLE001
        pass


hub = _Hub()


@router.get("/live")
async def live_status() -> dict:
    return {"connected_clients": len(hub.clients)}


@router.websocket("/live")
async def live_ws(websocket: WebSocket) -> None:
    await hub.connect(websocket)
    try:
        while True:
            # Keep the connection alive; clients subscribe passively.
            await asyncio.sleep(15)
            await websocket.send_text(json.dumps({"type": "ping"}))
    except WebSocketDisconnect:
        hub.disconnect(websocket)

"""
paddleMe — LinuxCNC web pendant server.

Serves index.html and a WebSocket endpoint that:
  - Pushes machine status at ~100 ms intervals
  - Accepts command messages from the client
"""

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from typing import Any

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

# ---------------------------------------------------------------------------
# LinuxCNC import — falls back to a stub so the server can be developed and
# tested without LinuxCNC installed.
# ---------------------------------------------------------------------------
try:
    import linuxcnc  # type: ignore

    LINUXCNC_AVAILABLE = True
except ImportError:
    LINUXCNC_AVAILABLE = False
    logging.warning("linuxcnc module not found — running in stub/demo mode")


POLL_INTERVAL = 0.1  # seconds between stat polls

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger("paddleMe")


# ---------------------------------------------------------------------------
# Stub so the server runs on a dev machine without LinuxCNC
# ---------------------------------------------------------------------------
class _StubStat:
    task_state = 2      # STATE_ON
    interp_state = 1    # INTERP_IDLE
    feedrate = 1.0
    spindle = [{"override": 1.0}]

    def poll(self) -> None:
        pass


class _StubCommand:
    def auto(self, *args: Any) -> None:
        log.info("STUB cmd.auto(%s)", args)

    def feedrate(self, value: float) -> None:
        log.info("STUB cmd.feedrate(%s)", value)

    def spindleoverride(self, value: float) -> None:
        log.info("STUB cmd.spindleoverride(%s)", value)

    def state(self, value: int) -> None:
        log.info("STUB cmd.state(%s)", value)


if LINUXCNC_AVAILABLE:
    _stat = linuxcnc.stat()
    _cmd = linuxcnc.command()

    # Numeric constants
    STATE_ESTOP = linuxcnc.STATE_ESTOP
    INTERP_IDLE = linuxcnc.INTERP_IDLE
    INTERP_RUNNING = linuxcnc.INTERP_READING  # LinuxCNC exposes this as INTERP_READING
    INTERP_PAUSED = linuxcnc.INTERP_PAUSED
    INTERP_WAITING = linuxcnc.INTERP_WAITING
    MODE_AUTO = linuxcnc.MODE_AUTO
    AUTO_RUN = linuxcnc.AUTO_RUN
    AUTO_PAUSE = linuxcnc.AUTO_PAUSE
    AUTO_RESUME = linuxcnc.AUTO_RESUME
else:
    _stat = _StubStat()
    _cmd = _StubCommand()

    STATE_ESTOP = 1
    INTERP_IDLE = 1
    INTERP_RUNNING = 2
    INTERP_PAUSED = 3
    INTERP_WAITING = 4
    MODE_AUTO = 1
    AUTO_RUN = 0
    AUTO_PAUSE = 1
    AUTO_RESUME = 2


# ---------------------------------------------------------------------------
# WebSocket connection registry
# ---------------------------------------------------------------------------
_clients: set[WebSocket] = set()


async def _broadcast(payload: dict) -> None:
    message = json.dumps(payload)
    dead: set[WebSocket] = set()
    for ws in _clients:
        try:
            await ws.send_text(message)
        except Exception:
            dead.add(ws)
    _clients.difference_update(dead)


# ---------------------------------------------------------------------------
# Status labels
# ---------------------------------------------------------------------------
_TASK_STATE_LABELS = {
    1: "ESTOP",
    2: "ESTOP_RESET",
    3: "OFF",
    4: "ON",
}

_INTERP_STATE_LABELS = {
    1: "IDLE",
    2: "RUNNING",
    3: "PAUSED",
    4: "READING",
    5: "WAITING",
}


def _build_status() -> dict:
    _stat.poll()
    return {
        "type": "status",
        "task_state": _stat.task_state,
        "task_state_label": _TASK_STATE_LABELS.get(_stat.task_state, "UNKNOWN"),
        "interp_state": _stat.interp_state,
        "interp_state_label": _INTERP_STATE_LABELS.get(_stat.interp_state, "UNKNOWN"),
        "feedrate": round(_stat.feedrate, 3),
        "spindlerate": round(_stat.spindle[0]["override"], 3),
    }


# ---------------------------------------------------------------------------
# Background poll loop
# ---------------------------------------------------------------------------
async def _poll_loop() -> None:
    while True:
        try:
            status = await asyncio.to_thread(_build_status)
            if _clients:
                await _broadcast(status)
        except Exception as exc:
            log.error("Poll error: %s", exc)
        await asyncio.sleep(POLL_INTERVAL)


# ---------------------------------------------------------------------------
# Command dispatch
# ---------------------------------------------------------------------------
def _dispatch_command(msg: dict) -> str | None:
    """Execute a command from the client. Returns an error string or None."""
    cmd = msg.get("cmd")

    if cmd == "cycle_start":
        # R in AXIS — must switch to AUTO mode before issuing run
        _cmd.mode(MODE_AUTO)
        _cmd.wait_complete()
        _cmd.auto(AUTO_RUN, 0)

    elif cmd == "feed_hold":
        # P in AXIS — already in AUTO mode, no mode switch needed
        _cmd.auto(AUTO_PAUSE)

    elif cmd == "resume":
        # S in AXIS — already in AUTO mode, no mode switch needed
        _cmd.auto(AUTO_RESUME)

    elif cmd == "stop":
        # ESC in AXIS — abort motion, stay in current machine state
        _cmd.abort()

    elif cmd == "estop":
        # F1 in AXIS — assert estop only, never clear it
        _cmd.state(STATE_ESTOP)
        _cmd.wait_complete()

    elif cmd == "set_feedrate":
        value = msg.get("value")
        if not isinstance(value, (int, float)):
            return "set_feedrate requires a numeric value"
        _cmd.feedrate(max(0.0, min(float(value), 1.0)))

    elif cmd == "set_spindlerate":
        value = msg.get("value")
        if not isinstance(value, (int, float)):
            return "set_spindlerate requires a numeric value"
        _cmd.spindleoverride(max(0.0, min(float(value), 1.0)))

    else:
        return f"unknown command: {cmd!r}"

    return None


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
@asynccontextmanager
async def _lifespan(app: FastAPI):
    task = asyncio.create_task(_poll_loop())
    yield
    task.cancel()


app = FastAPI(lifespan=_lifespan)


@app.get("/")
async def index():
    return FileResponse("index.html")


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    _clients.add(ws)
    log.info("Client connected  total=%d", len(_clients))

    # Send current status immediately so the UI doesn't wait for the next poll.
    try:
        status = await asyncio.to_thread(_build_status)
        await ws.send_text(json.dumps(status))
    except Exception as exc:
        log.error("Initial status error: %s", exc)

    try:
        while True:
            raw = await ws.receive_text()
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                await ws.send_text(json.dumps({"type": "error", "message": "invalid JSON"}))
                continue

            error = await asyncio.to_thread(_dispatch_command, msg)
            if error:
                await ws.send_text(json.dumps({"type": "error", "message": error}))
            else:
                # Echo an immediate status update so the UI snaps to the new
                # values rather than waiting up to POLL_INTERVAL.
                try:
                    status = await asyncio.to_thread(_build_status)
                    await ws.send_text(json.dumps(status))
                except Exception:
                    pass

    except WebSocketDisconnect:
        pass
    finally:
        _clients.discard(ws)
        log.info("Client disconnected  total=%d", len(_clients))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    uvicorn.run(
        "server:app",
        host="0.0.0.0",
        port=8080,
        log_level="info",
    )

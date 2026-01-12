import os
import json
import time
import asyncio
import inspect
import subprocess
import shutil
import re
from pathlib import Path
from typing import Any, Dict, Optional, Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, Response, JSONResponse
from fastapi.staticfiles import StaticFiles

from aiomqtt import Client, MqttError

# BLE backend (optional)
try:
    from bleak import BleakScanner, BleakClient  # type: ignore
    BLE_AVAILABLE = True
except Exception:
    BleakScanner = None  # type: ignore
    BleakClient = None   # type: ignore
    BLE_AVAILABLE = False


# =========================
# CONFIG (hardcode-friendly)
# =========================
MQTT_HOST = os.getenv("MQTT_HOST", "13.212.179.167")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_CONTROL_TOPIC = os.getenv("MQTT_CONTROL_TOPIC", "/device/datic/control")
MQTT_STATUS_TOPIC  = os.getenv("MQTT_STATUS_TOPIC",  "/device/datic/status")

MQTT_USERNAME = os.getenv("MQTT_USERNAME", "")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")

DEFAULT_SENDER = os.getenv("DEFAULT_SENDER", "1")
DEBUG = os.getenv("DEBUG", "0") in ("1", "true", "True", "YES", "yes")

# BLE (backend gateway) — UUIDs for Ai-Thinker WB2 UUID1 service (hardcoded, can override via env)
DEFAULT_BLE_SERVICE_UUID = "55535343-fe7d-4ae5-8fa9-9fafd205e455"
DEFAULT_BLE_TX_CHAR_UUID  = "49535343-8841-43f4-a8d4-ecbe34729bb3"  # TX notify characteristic
DEFAULT_BLE_RX_CHAR_UUID  = "49535343-1e4d-4bd9-ba61-23c647249616"  # RX write characteristic

BLE_SERVICE_UUID = os.getenv("BLE_SERVICE_UUID", DEFAULT_BLE_SERVICE_UUID)
BLE_RX_CHAR_UUID = os.getenv("BLE_RX_CHAR_UUID", DEFAULT_BLE_RX_CHAR_UUID)
BLE_TX_CHAR_UUID = os.getenv("BLE_TX_CHAR_UUID", DEFAULT_BLE_TX_CHAR_UUID)


# =========================
# WIFI HELPERS (Ubuntu + NetworkManager)
# =========================
def _run_cmd(cmd, timeout: float = 12.0) -> str:
    """Run command and return stdout (strip). Raise RuntimeError on failure."""
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except FileNotFoundError:
        raise RuntimeError(f"Command not found: {cmd[0]}")
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"Command timeout: {' '.join(cmd)}")
    if p.returncode != 0:
        err = (p.stderr or p.stdout or "").strip()
        raise RuntimeError(err or f"Command failed: {' '.join(cmd)}")
    return (p.stdout or "").strip()

def _nmcli(args, allow_sudo: bool = False) -> str:
    if shutil.which("nmcli") is None:
        raise RuntimeError("nmcli chưa có. Cài: sudo apt install network-manager")
    base = ["nmcli"] + list(args)
    try:
        return _run_cmd(base)
    except RuntimeError:
        # When reading secrets, non-root may need sudo -n
        if allow_sudo and os.geteuid() != 0 and shutil.which("sudo") is not None:
            return _run_cmd(["sudo", "-n", "nmcli"] + list(args))
        raise

def get_current_wifi_ssid_psk() -> tuple[str, str]:
    """Return (ssid, psk) for current Wi‑Fi.

    Primary path: NetworkManager (nmcli) WITHOUT triggering a rescan (avoid `nmcli dev wifi`).
    Fallbacks:
      - SSID via `iw` / `iwgetid` (fast)
      - PSK from NM connection file under /etc/NetworkManager/system-connections (needs root)
      - PSK from iwd profile under /var/lib/iwd (needs root, best-effort)
    """
    # ---------- helpers ----------
    def _unescape_nm(s: str) -> str:
        return s.replace(r"\:", ":").strip()

    def _ssid_via_iw() -> str:
        # Try iwgetid first
        if shutil.which("iwgetid"):
            try:
                return _run_cmd(["iwgetid", "-r"], timeout=2.0).strip()
            except Exception:
                pass
        # Fallback: iw dev <iface> link
        if shutil.which("iw"):
            try:
                iface = _run_cmd(["bash", "-lc", "iw dev | sed -n 's/^Interface //p' | head -n1"], timeout=2.0).strip()

                if iface:
                    link = _run_cmd(["bash", "-lc", f"iw dev {iface} link | sed -n 's/^\s*SSID: //p' | head -n1"], timeout=2.0).strip()
                    return link
            except Exception:
                pass
        return ""

    def _psk_from_nmconnection(conn_name: str) -> str:
        base = Path("/etc/NetworkManager/system-connections")
        if not base.exists():
            return ""
        candidates = [base / f"{conn_name}.nmconnection", base / conn_name]
        for fp in candidates:
            try:
                if fp.exists():
                    raw = fp.read_text(encoding="utf-8", errors="ignore")
                    m = re.search(r"^psk=(.*)$", raw, flags=re.M)
                    if m:
                        return m.group(1).strip()
            except Exception:
                continue
        return ""

    def _psk_from_iwd(ssid: str) -> str:
        base = Path("/var/lib/iwd")
        if not base.exists():
            return ""
        # best-effort: exact filename match "<SSID>.psk"
        fp = base / f"{ssid}.psk"
        try:
            if fp.exists():
                raw = fp.read_text(encoding="utf-8", errors="ignore")
                m = re.search(r"^Passphrase=(.*)$", raw, flags=re.M)
                if m:
                    return m.group(1).strip()
        except Exception:
            pass
        # broader search (only accept if filename contains ssid)
        try:
            for fp2 in base.glob("*.psk"):
                try:
                    if ssid and ssid not in fp2.stem:
                        continue
                    raw = fp2.read_text(encoding="utf-8", errors="ignore")
                    m = re.search(r"^Passphrase=(.*)$", raw, flags=re.M)
                    if m and m.group(1).strip():
                        return m.group(1).strip()
                except Exception:
                    continue
        except Exception:
            pass
        return ""

    # ---------- get active connection via nmcli (no rescan) ----------
    ssid = ""
    conn = ""

    try:
        out = _nmcli(["-t", "-f", "DEVICE,TYPE,STATE,CONNECTION", "dev", "status"])
        for line in out.splitlines():
            if not line:
                continue
            parts = line.split(":")
            if len(parts) < 4:
                continue
            device, typ, st = parts[0].strip(), parts[1].strip(), parts[2].strip()
            connection = _unescape_nm(":".join(parts[3:]))
            if typ in ("wifi", "802-11-wireless", "wireless") and st == "connected":
                conn = connection
                break

        if conn:
            try:
                ssid = _nmcli(["-g", "802-11-wireless.ssid", "connection", "show", conn]).strip()
            except Exception:
                ssid = ""
            if not ssid:
                ssid = conn

    except Exception:
        pass

    # ---------- SSID fallback ----------
    if not ssid:
        ssid = _ssid_via_iw()

    if not ssid:
        raise RuntimeError("Không lấy được SSID (NetworkManager không phản hồi hoặc chưa connect Wi‑Fi)")

    # ---------- PSK via nmcli ----------
    if conn:
        try:
            psk = _nmcli(["-s", "-g", "802-11-wireless-security.psk", "connection", "show", conn], allow_sudo=True).strip()
            if psk:
                return ssid, psk
        except Exception:
            pass

        psk = _psk_from_nmconnection(conn)
        if psk:
            return ssid, psk

    # ---------- PSK fallback: iwd ----------
    psk = _psk_from_iwd(ssid)
    if psk:
        return ssid, psk

    raise RuntimeError("Không lấy được PASS/PSK (Wi‑Fi enterprise/802.1X hoặc password không lưu/không truy cập được)")

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
STATIC_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# FASTAPI
# =========================
app = FastAPI()
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
async def home():
    resp = FileResponse(str(STATIC_DIR / "index.html"))
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.get("/favicon.ico")
async def favicon():
    return Response(status_code=204)


# =========================
# WS CLIENTS
# =========================
clients: Set[WebSocket] = set()
clients_lock = asyncio.Lock()


async def broadcast(obj: Dict[str, Any]) -> None:
    msg = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    async with clients_lock:
        dead = []
        for ws in clients:
            try:
                await ws.send_text(msg)
            except Exception:
                dead.append(ws)
        for ws in dead:
            clients.discard(ws)


# =========================
# STATE
# =========================
state_lock = asyncio.Lock()
mqtt_connected = asyncio.Event()

# =========================
# BLE RUNTIME (single-user gateway)
# =========================
ble_lock = asyncio.Lock()
ble_client = None  # BleakClient | None

async def ble_set_state(**kwargs):
    async with state_lock:
        _state.setdefault("ble", {})
        _state["ble"].update(kwargs)

async def ble_disconnect():
    global ble_client
    if ble_client is not None:
        try:
            # Stop notify (optional)
            if BLE_TX_CHAR_UUID and getattr(ble_client, "is_connected", False):
                try:
                    await ble_client.stop_notify(BLE_TX_CHAR_UUID)
                except Exception:
                    pass
            if getattr(ble_client, "is_connected", False):
                await ble_client.disconnect()
        except Exception:
            pass
    ble_client = None
    await ble_set_state(connected=False, address=None, name=None, notify=False)

async def ble_scan(timeout: float = 4.0):
    if not BLE_AVAILABLE or BleakScanner is None:
        raise RuntimeError("Bleak chưa được cài. Hãy: pip install bleak")
    devs = await BleakScanner.discover(timeout=timeout)
    out = []
    for d in devs:
        out.append({"name": d.name or "", "address": d.address, "rssi": getattr(d, "rssi", None)})
    # update state snapshot (giữ ngắn để không phình)
    await ble_set_state(last_scan=out[:50])
    return out

async def ble_connect(address: str):
    global ble_client
    if not BLE_AVAILABLE or BleakClient is None:
        raise RuntimeError("Bleak chưa được cài. Hãy: pip install bleak")
    await ble_disconnect()
    c = BleakClient(address)
    await c.connect()
    ble_client = c

    # best effort: name
    name = None
    try:
        name = getattr(getattr(c, "device", None), "name", None) or getattr(c, "address", None)
    except Exception:
        name = getattr(c, "address", None)

    await ble_set_state(connected=True, address=address, name=name, notify=False)

    # optional notify (TXD)
    if BLE_TX_CHAR_UUID:
        try:
            import base64
            loop = asyncio.get_running_loop()

            def _handler(sender: int, data: bytearray):
                b = bytes(data)
                try:
                    txt = b.decode("utf-8", errors="ignore")
                except Exception:
                    txt = ""
                loop.create_task(broadcast({
                    "type": "ble_notify",
                    "uuid": BLE_TX_CHAR_UUID,
                    "data_b64": base64.b64encode(b).decode("ascii"),
                    "text": txt,
                }))

            await ble_client.start_notify(BLE_TX_CHAR_UUID, _handler)
            await ble_set_state(notify=True)
        except Exception:
            # Notify is optional; ignore if TX char not found or not supported
            await ble_set_state(notify=False)

    return True

async def ble_write(data: bytes, char_uuid: str = "", response: bool = False):
    if ble_client is None or not getattr(ble_client, "is_connected", False):
        raise RuntimeError("BLE chưa connect")
    uuid = (char_uuid or BLE_RX_CHAR_UUID or "").strip()
    if not uuid:
        raise RuntimeError("Chưa cấu hình RX characteristic UUID (BLE_RX_CHAR_UUID)")
    await ble_client.write_gatt_char(uuid, data, response=response)
    await ble_set_state(last_write_bytes=len(data), last_write_uuid=uuid)
    return True
_state: Dict[str, Any] = {
    "mqtt": {"connected": False, "host": MQTT_HOST, "port": MQTT_PORT},
    "ble": {"available": BLE_AVAILABLE, "connected": False, "address": None, "name": None, "notify": False, "service_uuid": BLE_SERVICE_UUID, "rx_uuid": BLE_RX_CHAR_UUID, "tx_uuid": BLE_TX_CHAR_UUID, "last_scan": []},
    "device": {"sender": None},  # sender trong status (thường là DEVICE_ID)
    "switch": {
        "mode": None,           # "trigger" | "repeat" | None
        "enable": None,         # 0/1 nếu trigger
        "work_time": None,      # ms nếu repeat
        "duration_time": None,  # ms nếu repeat
    },
    "lock": {"lock_mode": None},   # 0..3
    "save": {"save_mode": None},   # 0..2
    "last": {"topic": None, "ts": None, "raw": None},
}


def now_ts() -> float:
    return time.time()


async def snapshot_state() -> Dict[str, Any]:
    async with state_lock:
        return json.loads(json.dumps(_state))


async def update_from_status(topic: str, raw: str, data: Dict[str, Any]) -> None:
    # Chỉ nhận message type=status (đúng thiết kế firmware)
    t = data.get("type")
    if t is not None and str(t) != "status":
        return

    async with state_lock:
        _state["last"]["topic"] = topic
        _state["last"]["ts"] = now_ts()
        _state["last"]["raw"] = raw

        if "sender" in data:
            _state["device"]["sender"] = data.get("sender")

        mode = data.get("mode")

        if mode == "trigger":
            try:
                en = int(data.get("enable"))
                if en in (0, 1):
                    _state["switch"]["mode"] = "trigger"
                    _state["switch"]["enable"] = en
            except Exception:
                pass

        elif mode == "repeat":
            try:
                wt = int(data.get("work_time"))
                dt = int(data.get("duration_time"))
                _state["switch"]["mode"] = "repeat"
                _state["switch"]["work_time"] = wt
                _state["switch"]["duration_time"] = dt
            except Exception:
                pass

        elif mode == "lock":
            try:
                lm = int(data.get("lock_mode"))
                if 0 <= lm <= 3:
                    _state["lock"]["lock_mode"] = lm
            except Exception:
                pass

        elif mode == "save":
            try:
                sm = int(data.get("save_mode"))
                if 0 <= sm <= 2:
                    _state["save"]["save_mode"] = sm
            except Exception:
                pass

    await broadcast(await snapshot_state())


# =========================
# MQTT (1 connection, publish queue)
# =========================
publish_q: asyncio.Queue[str] = asyncio.Queue(maxsize=256)
stop_event = asyncio.Event()
mqtt_task: Optional[asyncio.Task] = None


def make_client() -> Client:
    """
    Tương thích nhiều version aiomqtt: chỉ truyền kwargs mà Client __init__ support.
    (Tránh lỗi kiểu: unexpected keyword argument 'client_id')
    """
    sig = inspect.signature(Client)
    params = sig.parameters

    kwargs: Dict[str, Any] = {}
    if "port" in params:
        kwargs["port"] = MQTT_PORT
    if "username" in params and MQTT_USERNAME:
        kwargs["username"] = MQTT_USERNAME
    if "password" in params and MQTT_PASSWORD:
        kwargs["password"] = MQTT_PASSWORD

    # keepalive naming khác nhau giữa version
    if "keepalive" in params:
        kwargs["keepalive"] = 30
    elif "keep_alive" in params:
        kwargs["keep_alive"] = 30

    return Client(MQTT_HOST, **kwargs)


async def mqtt_worker() -> None:
    while not stop_event.is_set():
        mqtt_connected.clear()
        async with state_lock:
            _state["mqtt"]["connected"] = False
        await broadcast(await snapshot_state())

        try:
            async with make_client() as client:
                await client.subscribe(MQTT_STATUS_TOPIC)

                mqtt_connected.set()
                async with state_lock:
                    _state["mqtt"]["connected"] = True

                print(f"MQTT subscribed: {MQTT_HOST}:{MQTT_PORT} topic={MQTT_STATUS_TOPIC}")
                await broadcast(await snapshot_state())

                async def rx_loop():
                    async for message in client.messages:
                        if stop_event.is_set():
                            return
                        raw = message.payload.decode("utf-8", errors="ignore").strip()
                        topic = str(message.topic)
                        try:
                            obj = json.loads(raw)
                            if isinstance(obj, dict):
                                await update_from_status(topic, raw, obj)
                        except Exception:
                            continue

                async def tx_loop():
                    while not stop_event.is_set():
                        payload = await publish_q.get()
                        await client.publish(MQTT_CONTROL_TOPIC, payload, qos=0, retain=False)
                        print("PUBLISH", MQTT_CONTROL_TOPIC, payload)

                await asyncio.gather(rx_loop(), tx_loop())

        except asyncio.CancelledError:
            return
        except MqttError as e:
            print("MQTT error, retry in 2s:", repr(e))
            await asyncio.sleep(2)
        except Exception as e:
            print("MQTT unexpected error, retry in 2s:", repr(e))
            await asyncio.sleep(2)


def _normalize_sender(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        if re.fullmatch(r"[+-]?\d+", s):
            try:
                return int(s)
            except Exception:
                return s
        return s
    return v


def build_control(msg: Dict[str, Any]) -> Optional[str]:
    sender = msg.get("sender", None)
    cmd = msg.get("cmd", None)

    if sender is None or (isinstance(sender, str) and not sender.strip()):
        sender = (_state.get("device") or {}).get("sender")

    if sender is None or (isinstance(sender, str) and not sender.strip()):
        sender = DEFAULT_SENDER

    sender = _normalize_sender(sender)

    try:
        cmd_i = int(cmd)
    except Exception:
        return None

    if not (0 <= cmd_i <= 8):
        return None

    if sender is None or (isinstance(sender, str) and not sender.strip()):
        return None

    base: Dict[str, Any] = {"sender": sender, "cmd": cmd_i}

    if cmd_i == 2:
        try:
            if "work_time" in msg:
                base["work_time"] = int(msg.get("work_time"))
            if "duration_time" in msg:
                base["duration_time"] = int(msg.get("duration_time"))
        except Exception:
            pass

    return json.dumps(base, ensure_ascii=False, separators=(",", ":"))



# =========================
# LIFECYCLE + ROUTES
# =========================
@app.on_event("startup")
async def on_startup():
    global mqtt_task
    stop_event.clear()
    mqtt_task = asyncio.create_task(mqtt_worker())


@app.on_event("shutdown")
async def on_shutdown():
    stop_event.set()
    mqtt_connected.clear()

    global mqtt_task
    if mqtt_task:
        mqtt_task.cancel()
        try:
            await mqtt_task
        except Exception:
            pass
        mqtt_task = None


@app.get("/health")
async def health():
    return JSONResponse(await snapshot_state())


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    async with clients_lock:
        clients.add(websocket)

    # push snapshot ngay khi connect
    try:
        await websocket.send_text(json.dumps(await snapshot_state(), ensure_ascii=False, separators=(",", ":")))
    except Exception:
        pass

    try:
        while True:
            raw = await websocket.receive_text()
            if DEBUG:
                print("WS RX:", raw)

            try:
                msg = json.loads(raw)
            except Exception:
                await websocket.send_text(json.dumps({"ok": False, "err": "bad_json"}, ensure_ascii=False))
                continue

            if not isinstance(msg, dict):
                await websocket.send_text(json.dumps({"ok": False, "err": "bad_type"}, ensure_ascii=False))
                continue

            # BLE gateway messages (single-user)
            if msg.get("type") == "ble":
                op = msg.get("op")
                try:
                    if op == "scan":
                        timeout = float(msg.get("timeout", 4.0))
                        res = await ble_scan(timeout=timeout)
                        await websocket.send_text(json.dumps({"type": "ble_scan_result", "devices": res}, ensure_ascii=False))
                    elif op == "connect":
                        address = str(msg.get("address") or "")
                        if not address:
                            raise RuntimeError("missing address")
                        async with ble_lock:
                            await websocket.send_text(json.dumps({"type": "ble_state", "state": "connecting", "address": address}, ensure_ascii=False))
                            await ble_connect(address)
                        await websocket.send_text(json.dumps({"type": "ble_state", "state": "connected", "address": address}, ensure_ascii=False))
                        # also push snapshot
                        await websocket.send_text(json.dumps(await snapshot_state(), ensure_ascii=False, separators=(",", ":")))
                    elif op == "disconnect":
                        async with ble_lock:
                            await ble_disconnect()
                        await websocket.send_text(json.dumps({"type": "ble_state", "state": "disconnected"}, ensure_ascii=False))
                        await websocket.send_text(json.dumps(await snapshot_state(), ensure_ascii=False, separators=(",", ":")))
                    elif op == "write_wifi":
                        # Auto send Wi-Fi credentials in format: ssid.<SSID>;pass.<PASS>
                        async with ble_lock:
                            ssid, psk = get_current_wifi_ssid_psk()
                            payload_wifi = f"ssid.{ssid};pass.{psk}".encode("utf-8")
                            await ble_write(payload_wifi, response=False)
                        await websocket.send_text(json.dumps({"type": "ble_wifi_sent", "ssid": ssid, "pass_len": len(psk)}, ensure_ascii=False))
                        await websocket.send_text(json.dumps({"type": "ble_write_ok", "nbytes": len(payload_wifi)}, ensure_ascii=False))
                        await websocket.send_text(json.dumps(await snapshot_state(), ensure_ascii=False, separators=(",", ":")))
                    elif op == "write":
                        encoding = str(msg.get("encoding", "text"))
                        payload_s = msg.get("data", "")
                        if not isinstance(payload_s, (str, bytes)):
                            raise RuntimeError("bad data")
                        if encoding == "hex":
                            data_b = bytes.fromhex(str(payload_s))
                        elif encoding == "b64":
                            import base64
                            data_b = base64.b64decode(str(payload_s))
                        else:
                            data_b = str(payload_s).encode("utf-8")
                        char_uuid = str(msg.get("char_uuid") or "")
                        async with ble_lock:
                            await ble_write(data_b, char_uuid=char_uuid, response=bool(msg.get("response", False)))
                        await websocket.send_text(json.dumps({"type": "ble_write_ok", "nbytes": len(data_b)}, ensure_ascii=False))
                        await websocket.send_text(json.dumps(await snapshot_state(), ensure_ascii=False, separators=(",", ":")))
                    else:
                        raise RuntimeError(f"unknown op: {op}")
                except Exception as e:
                    await websocket.send_text(json.dumps({"type": "ble_error", "err": str(e)}, ensure_ascii=False))
                continue

            payload = build_control(msg)
            if payload is None:
                await websocket.send_text(json.dumps({"ok": False, "err": "bad_payload"}, ensure_ascii=False))
                continue

            if not mqtt_connected.is_set():
                await websocket.send_text(json.dumps({"ok": False, "err": "mqtt_not_connected"}, ensure_ascii=False))
                continue

            try:
                publish_q.put_nowait(payload)
                if DEBUG:
                    print("WS -> MQTT QUEUE:", payload)
                await websocket.send_text(json.dumps(
                    {"ok": True, "queued": True, "payload": json.loads(payload)},
                    ensure_ascii=False
                ))
            except asyncio.QueueFull:
                await websocket.send_text(json.dumps({"ok": False, "err": "queue_full"}, ensure_ascii=False))

    except WebSocketDisconnect:
        pass
    finally:
        async with clients_lock:
            clients.discard(websocket)

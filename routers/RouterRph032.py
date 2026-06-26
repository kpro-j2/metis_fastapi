import ipaddress
import os
import threading
import time
from urllib.parse import urlparse
from typing import Dict, Optional, Tuple, Union

from fastapi import APIRouter, HTTPException, Query

from modules.device_rph032 import RPH032
from modules.local_settings import get_setting, set_setting

router = APIRouter(prefix="/rph032", tags=["rph032"])

_target_lock = threading.Lock()
_target_config: Dict[str, Dict[str, Union[int, str]]] = {}
_server_lock = threading.Lock()
_server_config: Dict[str, Dict[str, str]] = {"default": {"api_base_uri": ""}}
_cache_lock = threading.Lock()
_module_cache: Dict[str, Dict[str, Union[float, str, Dict[int, int], None]]] = {}
_poll_thread: Optional[threading.Thread] = None
_poll_interval_sec = float(os.environ.get("RPH032_POLL_INTERVAL_SEC", "1.0"))


def _persist_settings() -> None:
    with _target_lock:
        targets = {k: dict(v) for k, v in _target_config.items()}
    with _server_lock:
        servers = {k: dict(v) for k, v in _server_config.items()}
    set_setting("rph032_target_config", targets)
    set_setting("rph032_server_config", servers)


def _load_settings() -> None:
    saved_targets = get_setting("rph032_target_config", {})
    saved_servers = get_setting("rph032_server_config", {})

    if isinstance(saved_servers, dict):
        with _server_lock:
            _server_config.clear()
            _server_config["default"] = {"api_base_uri": ""}
            for sid, cfg in saved_servers.items():
                if not isinstance(sid, str) or not sid:
                    continue
                if not isinstance(cfg, dict):
                    continue
                api_base_uri = str(cfg.get("api_base_uri", "")).rstrip("/")
                _server_config[sid] = {"api_base_uri": api_base_uri}

    if isinstance(saved_targets, dict):
        with _target_lock:
            _target_config.clear()
            for module, cfg in saved_targets.items():
                if not isinstance(module, str) or not module:
                    continue
                if not isinstance(cfg, dict):
                    continue
                ip = cfg.get("ip")
                port = cfg.get("port")
                server_id = str(cfg.get("server_id", "default"))
                if ip is None or port is None:
                    continue
                try:
                    _validate_ip_port(str(ip), int(port))
                except HTTPException:
                    continue
                _target_config[module] = {
                    "server_id": server_id,
                    "ip": str(ip),
                    "port": int(port),
                }

    with _target_lock:
        modules = list(_target_config.keys())
    for module in modules:
        _init_cache_if_needed(module)
    if modules:
        _start_poller_if_needed()


def _validate_ip_port(ip: str, port: int) -> None:
    try:
        ipaddress.ip_address(ip)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"invalid ip address: {ip}") from e

    if port < 1 or port > 65535:
        raise HTTPException(status_code=400, detail="port must be in range 1..65535")


def _validate_module_id(module: str) -> None:
    if not module.strip():
        raise HTTPException(status_code=400, detail="module must be non-empty")


def _validate_server_id(server_id: str) -> None:
    if not server_id.strip():
        raise HTTPException(status_code=400, detail="server_id must be non-empty")


def _validate_api_base_uri(api_base_uri: str) -> None:
    parsed = urlparse(api_base_uri)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise HTTPException(status_code=400, detail="api_base_uri must be absolute http(s) URL")


def _resolve_target(ip: Optional[str], port: Optional[int], module: str) -> Tuple[str, int]:
    _validate_module_id(module)

    # Query parameters override configured target.
    if ip is not None and port is not None:
        _validate_ip_port(ip, port)
        return ip, port

    with _target_lock:
        cfg = _target_config.get(module)

    cfg_ip = cfg.get("ip") if cfg is not None else None
    cfg_port = cfg.get("port") if cfg is not None else None

    use_ip = ip if ip is not None else cfg_ip
    use_port = port if port is not None else cfg_port

    if use_ip is None or use_port is None:
        raise HTTPException(
            status_code=400,
            detail=f"target ip/port for module '{module}' is not configured; call /rph032/config/set/{module}/{{ip}}/{{port}} first",
        )

    _validate_ip_port(str(use_ip), int(use_port))
    return str(use_ip), int(use_port)


def _controller(ip: Optional[str], port: Optional[int], module: str) -> RPH032:
    resolved_ip, resolved_port = _resolve_target(ip, port, module)
    return RPH032(ip=resolved_ip, port=resolved_port)


def _module_server_id(module: str) -> str:
    with _target_lock:
        cfg = _target_config.get(module)
    if cfg is None:
        return "default"
    server_id = cfg.get("server_id")
    return str(server_id) if server_id is not None else "default"


def _module_api_base_uri(module: str) -> str:
    server_id = _module_server_id(module)
    with _server_lock:
        server = _server_config.get(server_id)
    if server is None:
        return ""
    return str(server.get("api_base_uri", ""))


def _get_module_config(module: str) -> Optional[Tuple[str, int]]:
    with _target_lock:
        cfg = _target_config.get(module)
    if cfg is None:
        return None
    return str(cfg["ip"]), int(cfg["port"])


def _init_cache_if_needed(module: str) -> None:
    with _cache_lock:
        if module not in _module_cache:
            _module_cache[module] = {
                "timestamp": None,
                "error": "no data",
                "voltage": {},
                "current": {},
            }


def _refresh_module_cache(module: str, ip: str, port: int) -> None:
    ctl = RPH032(ip=ip, port=port)
    voltages: Dict[int, int] = {}
    currents: Dict[int, int] = {}
    ts = time.time()

    try:
        for ch in range(1, 5):
            voltages[ch] = ctl.read_voltage(ch)
            currents[ch] = ctl.read_current(ch)
        with _cache_lock:
            _module_cache[module] = {
                "timestamp": ts,
                "error": None,
                "voltage": voltages,
                "current": currents,
            }
    except Exception as e:
        with _cache_lock:
            prev = _module_cache.get(module, {"timestamp": None, "voltage": {}, "current": {}})
            _module_cache[module] = {
                "timestamp": prev.get("timestamp"),
                "error": str(e),
                "voltage": prev.get("voltage", {}),
                "current": prev.get("current", {}),
            }


def _poll_worker() -> None:
    while True:
        with _target_lock:
            modules = [(m, str(c["ip"]), int(c["port"])) for m, c in _target_config.items()]

        for module, ip, port in modules:
            _init_cache_if_needed(module)
            _refresh_module_cache(module, ip, port)

        sleep_sec = _poll_interval_sec if _poll_interval_sec > 0 else 1.0
        time.sleep(sleep_sec)


def _start_poller_if_needed() -> None:
    global _poll_thread
    with _target_lock:
        if _poll_thread is None or not _poll_thread.is_alive():
            _poll_thread = threading.Thread(target=_poll_worker, daemon=True)
            _poll_thread.start()


_load_settings()


@router.get("/")
async def root():
    return {"message": "RPH032 control API"}


@router.get("/config/get")
async def config_get():
    # Legacy endpoint returns default module config.
    with _target_lock:
        cfg = _target_config.get("default")
    if cfg is None:
        return {"module": "default", "ip": None, "port": None}
    return {"module": "default", "ip": cfg["ip"], "port": cfg["port"]}


@router.get("/config/set/{ip}/{port}")
async def config_set(ip: str, port: int):
    # Legacy endpoint sets default module config.
    _validate_ip_port(ip, port)
    with _target_lock:
        _target_config["default"] = {"server_id": "default", "ip": ip, "port": port}
    _init_cache_if_needed("default")
    _refresh_module_cache("default", ip, port)
    _persist_settings()
    _start_poller_if_needed()
    return {"message": "ok", "module": "default", "ip": ip, "port": port}


@router.get("/config/clear")
async def config_clear():
    # Legacy endpoint clears default module config.
    with _target_lock:
        _target_config.pop("default", None)
    with _cache_lock:
        _module_cache.pop("default", None)
    _persist_settings()
    return {"message": "ok", "module": "default", "ip": None, "port": None}


@router.get("/config/list")
async def config_list():
    with _target_lock:
        items = [
            {
                "module": m,
                "server_id": c.get("server_id", "default"),
                "ip": c["ip"],
                "port": c["port"],
            }
            for m, c in _target_config.items()
        ]
    with _server_lock:
        servers = {k: v.get("api_base_uri", "") for k, v in _server_config.items()}
    with _cache_lock:
        for item in items:
            cached = _module_cache.get(str(item["module"]))
            item["timestamp"] = None if cached is None else cached.get("timestamp")
            item["error"] = None if cached is None else cached.get("error")
            item["api_base_uri"] = servers.get(str(item["server_id"]), "")
    return {"modules": items, "poll_interval_sec": _poll_interval_sec}


@router.get("/config/get/{module}")
async def config_get_module(module: str):
    _validate_module_id(module)
    with _target_lock:
        cfg = _target_config.get(module)
    if cfg is None:
        return {"module": module, "server_id": None, "api_base_uri": None, "ip": None, "port": None}
    server_id = str(cfg.get("server_id", "default"))
    with _server_lock:
        server = _server_config.get(server_id)
    api_base_uri = None if server is None else server.get("api_base_uri", "")
    return {
        "module": module,
        "server_id": server_id,
        "api_base_uri": api_base_uri,
        "ip": cfg["ip"],
        "port": cfg["port"],
    }


@router.get("/server/list")
async def server_list():
    with _server_lock:
        items = [{"server_id": sid, "api_base_uri": cfg.get("api_base_uri", "")} for sid, cfg in _server_config.items()]
    return {"servers": items}


@router.get("/server/get/{server_id}")
async def server_get(server_id: str):
    _validate_server_id(server_id)
    with _server_lock:
        cfg = _server_config.get(server_id)
    if cfg is None:
        return {"server_id": server_id, "api_base_uri": None}
    return {"server_id": server_id, "api_base_uri": cfg.get("api_base_uri", "")}


@router.get("/server/set/{server_id}/{api_base_uri:path}")
async def server_set(server_id: str, api_base_uri: str):
    _validate_server_id(server_id)
    _validate_api_base_uri(api_base_uri)
    with _server_lock:
        _server_config[server_id] = {"api_base_uri": api_base_uri.rstrip("/")}
    _persist_settings()
    return {"message": "ok", "server_id": server_id, "api_base_uri": api_base_uri.rstrip("/")}


@router.get("/server/clear/{server_id}")
async def server_clear(server_id: str):
    _validate_server_id(server_id)
    if server_id == "default":
        raise HTTPException(status_code=400, detail="default server cannot be removed")
    with _server_lock:
        _server_config.pop(server_id, None)
    _persist_settings()
    return {"message": "ok", "server_id": server_id}


@router.get("/config/set/{module}/{ip}/{port}")
async def config_set_module(module: str, ip: str, port: int):
    _validate_module_id(module)
    _validate_ip_port(ip, port)
    with _target_lock:
        _target_config[module] = {"server_id": "default", "ip": ip, "port": port}
    _init_cache_if_needed(module)
    _refresh_module_cache(module, ip, port)
    _persist_settings()
    _start_poller_if_needed()
    return {"message": "ok", "module": module, "server_id": "default", "ip": ip, "port": port}


@router.get("/config/set/{module}/server/{server_id}/{ip}/{port}")
async def config_set_module_server(module: str, server_id: str, ip: str, port: int):
    _validate_module_id(module)
    _validate_server_id(server_id)
    _validate_ip_port(ip, port)
    with _server_lock:
        if server_id not in _server_config:
            raise HTTPException(status_code=400, detail=f"unknown server_id: {server_id}")
    with _target_lock:
        _target_config[module] = {"server_id": server_id, "ip": ip, "port": port}
    _init_cache_if_needed(module)
    _refresh_module_cache(module, ip, port)
    _persist_settings()
    _start_poller_if_needed()
    return {"message": "ok", "module": module, "server_id": server_id, "ip": ip, "port": port}


@router.get("/config/clear/{module}")
async def config_clear_module(module: str):
    _validate_module_id(module)
    with _target_lock:
        _target_config.pop(module, None)
    with _cache_lock:
        _module_cache.pop(module, None)
    _persist_settings()
    return {"message": "ok", "module": module, "ip": None, "port": None}


@router.get("/{module}/remote/{state}")
async def remote(
    module: str,
    state: str,
    ip: Optional[str] = Query(default=None),
    port: Optional[int] = Query(default=None),
):
    ctl = _controller(ip, port, module)
    try:
        if state == "on":
            ctl.remote_on()
        elif state == "off":
            ctl.remote_off()
        else:
            raise HTTPException(status_code=400, detail="state must be on|off")
        cfg = _get_module_config(module)
        if cfg is not None:
            _refresh_module_cache(module, cfg[0], cfg[1])
        return {
            "message": "ok",
            "module": module,
            "server_id": _module_server_id(module),
            "api_base_uri": _module_api_base_uri(module),
            "action": f"remote {state}",
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except OSError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e


@router.get("/{module}/kill/{mode}")
async def kill(
    module: str,
    mode: str,
    ip: Optional[str] = Query(default=None),
    port: Optional[int] = Query(default=None),
):
    ctl = _controller(ip, port, module)
    try:
        if mode == "all":
            ctl.kill_all()
        elif mode == "off":
            ctl.kill_off()
        else:
            raise HTTPException(status_code=400, detail="mode must be off|all")
        cfg = _get_module_config(module)
        if cfg is not None:
            _refresh_module_cache(module, cfg[0], cfg[1])
        return {
            "message": "ok",
            "module": module,
            "server_id": _module_server_id(module),
            "api_base_uri": _module_api_base_uri(module),
            "action": f"kill {mode}",
        }
    except OSError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e


@router.get("/{module}/on/{ch}/{volt}")
async def on(
    module: str,
    ch: int,
    volt: int,
    limit: int = Query(default=100),
    ramp: int = Query(default=10),
    ip: Optional[str] = Query(default=None),
    port: Optional[int] = Query(default=None),
):
    ctl = _controller(ip, port, module)
    try:
        ctl.safe_on(ch, volt, limit, ramp)
        cfg = _get_module_config(module)
        if cfg is not None:
            _refresh_module_cache(module, cfg[0], cfg[1])
        return {
            "message": "ok",
            "module": module,
            "server_id": _module_server_id(module),
            "api_base_uri": _module_api_base_uri(module),
            "action": "on",
            "ch": ch,
            "volt": volt,
            "limit": limit,
            "ramp": ramp,
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except OSError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e


@router.get("/{module}/off/{ch}")
async def off(
    module: str,
    ch: int,
    ip: Optional[str] = Query(default=None),
    port: Optional[int] = Query(default=None),
):
    ctl = _controller(ip, port, module)
    try:
        ctl.safe_off(ch)
        cfg = _get_module_config(module)
        if cfg is not None:
            _refresh_module_cache(module, cfg[0], cfg[1])
        return {
            "message": "ok",
            "module": module,
            "server_id": _module_server_id(module),
            "api_base_uri": _module_api_base_uri(module),
            "action": "off",
            "ch": ch,
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except OSError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e


@router.get("/{module}/readv/{ch}")
async def readv(
    module: str,
    ch: int,
    ip: Optional[str] = Query(default=None),
    port: Optional[int] = Query(default=None),
):
    ctl = _controller(ip, port, module)
    try:
        voltage = ctl.read_voltage(ch)
        return {
            "module": module,
            "server_id": _module_server_id(module),
            "api_base_uri": _module_api_base_uri(module),
            "ch": ch,
            "voltage": voltage,
            "timestamp": time.time(),
            "error": None,
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except OSError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e


@router.get("/{module}/readi/{ch}")
async def readi(
    module: str,
    ch: int,
    ip: Optional[str] = Query(default=None),
    port: Optional[int] = Query(default=None),
):
    ctl = _controller(ip, port, module)
    try:
        current = ctl.read_current(ch)
        return {
            "module": module,
            "server_id": _module_server_id(module),
            "api_base_uri": _module_api_base_uri(module),
            "ch": ch,
            "current": current,
            "timestamp": time.time(),
            "error": None,
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except OSError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e


@router.get("/{module}/status/{ch}")
async def status(
    module: str,
    ch: int,
    ip: Optional[str] = Query(default=None),
    port: Optional[int] = Query(default=None),
):
    ctl = _controller(ip, port, module)
    try:
        voltage = ctl.read_voltage(ch)
        current = ctl.read_current(ch)
        set_voltage = ctl.read_set_voltage(ch)
        current_limit = ctl.read_current_limit(ch)
        ramp = ctl.read_ramp(ch)
        return {
            "module": module,
            "server_id": _module_server_id(module),
            "api_base_uri": _module_api_base_uri(module),
            "ch": ch,
            "voltage": voltage,
            "current": current,
            "set_voltage": set_voltage,
            "current_limit": current_limit,
            "ramp": ramp,
            "timestamp": time.time(),
            "error": None,
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except OSError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e


@router.get("/{module}/setting/{ch}")
async def setting(
    module: str,
    ch: int,
    ip: Optional[str] = Query(default=None),
    port: Optional[int] = Query(default=None),
):
    ctl = _controller(ip, port, module)
    try:
        return {
            "module": module,
            "server_id": _module_server_id(module),
            "api_base_uri": _module_api_base_uri(module),
            "ch": ch,
            "set_voltage": ctl.read_set_voltage(ch),
            "current_limit": ctl.read_current_limit(ch),
            "ramp": ctl.read_ramp(ch),
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except OSError as e:
        raise HTTPException(status_code=502, detail=str(e)) from e

import os

from fastapi import FastAPI, Depends, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
# import logging
from metis_fastapi.dependencies import get_redis_proxy
from modules.RedisProxy import RedisProxy
from modules.local_settings import get_setting, set_setting, get_settings_file_path
from routers import RouterSystemCommand
from routers import RouterScaler
from routers import ControlNestDAQ
from routers import RouterRph032
from routers import RouterTelemetry
# logging.basicConfig(level=logging.WARNING)
app = FastAPI()
app.include_router(ControlNestDAQ.router)
app.include_router(RouterSystemCommand.router)
app.include_router(RouterScaler.router)
app.include_router(RouterRph032.router)
app.include_router(RouterTelemetry.router)
# app.include_router(ControlBabirl.router)

cors_origins_raw = os.getenv("CORS_ALLOW_ORIGINS", "*")
cors_origins = [o.strip() for o in cors_origins_raw.split(",") if o.strip()]
allow_all_origins = len(cors_origins) == 1 and cors_origins[0] == "*"

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins if cors_origins else ["*"],
    # '*' と credentials=true は併用不可なので自動調整する。
    allow_credentials=not allow_all_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

# get from dependencies.py
aProxy = get_redis_proxy(0)
saved_redis_cfg = get_setting("redis_config", {})
if not isinstance(saved_redis_cfg, dict):
    saved_redis_cfg = {}
redis_cfg = {
    "host": str(saved_redis_cfg.get("host", os.getenv("REDIS_SERVER_HOST", "localhost"))),
    "port": int(saved_redis_cfg.get("port", int(os.getenv("REDIS_SERVER_PORT", "6379")))),
    "db": int(saved_redis_cfg.get("db", 0)),
}


def _normalize_uri(uri: str) -> str:
    return str(uri or "").strip().rstrip("/")

# Try restoring saved redis endpoint at startup; if unreachable, keep app running.
try:
    aProxy.connect(redis_cfg["host"], redis_cfg["port"], redis_cfg["db"])
except Exception:
    pass


def _redis_instance():
    r = aProxy.instance()
    if r is None:
        raise HTTPException(status_code=503, detail="Redis client is not connected")
    return r


@app.get("/redis/status")
async def redis_status():
    return {
        "connected": aProxy.isConnected(),
        "host": redis_cfg["host"],
        "port": redis_cfg["port"],
        "db": redis_cfg["db"],
        "settings_file": get_settings_file_path(),
    }


@app.get("/redis/config/get")
async def redis_config_get():
    return {
        "host": redis_cfg["host"],
        "port": redis_cfg["port"],
        "db": redis_cfg["db"],
    }


@app.get("/redis/config/set/{host}/{port}/{db}")
async def redis_config_set(host: str, port: int, db: int):
    if port < 1 or port > 65535:
        raise HTTPException(status_code=400, detail="port must be in range 1..65535")
    if db < 0:
        raise HTTPException(status_code=400, detail="db must be >= 0")

    ok = aProxy.connect(host, port, db)
    redis_cfg["host"] = host
    redis_cfg["port"] = port
    redis_cfg["db"] = db
    set_setting("redis_config", redis_cfg)
    return {
        "message": "ok" if ok else "failed",
        "connected": bool(ok),
        "host": host,
        "port": port,
        "db": db,
    }


@app.get("/ui/config/get")
async def ui_config_get():
    nestdaq_api_uri = _normalize_uri(get_setting("nestdaq_api_uri", ""))
    scaler_api_uri = _normalize_uri(get_setting("scaler_api_uri", ""))
    scaler_ips = get_setting("scaler_ips", [])
    if not isinstance(scaler_ips, list):
        scaler_ips = []
    scaler_ips = [str(v).strip() for v in scaler_ips if str(v).strip()]
    return {
        "nestdaq_api_uri": nestdaq_api_uri,
        "scaler_api_uri": scaler_api_uri,
        "scaler_ips": scaler_ips,
        "redis": {
            "host": redis_cfg["host"],
            "port": redis_cfg["port"],
            "db": redis_cfg["db"],
        },
        "settings_file": get_settings_file_path(),
    }


@app.get("/ui/config/set/nestdaq_api/{api_base_uri:path}")
async def ui_config_set_nestdaq_api(api_base_uri: str):
    value = _normalize_uri(api_base_uri)
    set_setting("nestdaq_api_uri", value)
    return {
        "message": "ok",
        "nestdaq_api_uri": value,
    }


@app.get("/ui/config/set/scaler_api/{api_base_uri:path}")
async def ui_config_set_scaler_api(api_base_uri: str):
    value = _normalize_uri(api_base_uri)
    set_setting("scaler_api_uri", value)
    return {
        "message": "ok",
        "scaler_api_uri": value,
    }


@app.get("/ui/config/set/scaler_ips/{ips:path}")
async def ui_config_set_scaler_ips(ips: str):
    values = [v.strip() for v in str(ips or "").split(",") if v.strip()]
    set_setting("scaler_ips", values)
    return {
        "message": "ok",
        "scaler_ips": values,
    }

@app.get("/")
async def root():
    return {"message": "Hello World"}
#     return {"message": aProxy.isConnected()}
@app.get("/set/{key}/{val}")
async def read_item(key: str, val: str) :
    r = _redis_instance()
    r.set(key,val)
    return {"message": "set"}

@app.get("/get/{key}")
async def read_item(key: str) :
    r = _redis_instance()
    val = r.get(key)
    if val == None :
        val = ""
    return {"message": val}
 
@app.get("/incr/{key}")
async def read_item(key: str) :
    r = _redis_instance()
    val = r.incr(key)
    if val == None :
        val = ""
    return {"message": val}
 
@app.get("/expire/{key}/{time}")
async def read_item(key: str, time: str) :
    r = _redis_instance()
    val = r.expire(key, int(time))
    if val == None :
        val = ""
    return {"message": val}

@app.get("/publish/{chnl}/{msg}")
async def read_item(chnl: str, msg: str) :
    r = _redis_instance()
    val = r.publish(chnl,msg)
    if val == None :
        val = ""
    return {"message": val}


@app.get("/items/")
async def read_item(skip: int = 0, limit: int = 10):
    return JSONResponse(content={"skip":skip, "limit":limit})

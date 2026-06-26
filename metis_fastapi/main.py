from fastapi import FastAPI, Depends
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
# import logging
from metis_fastapi.dependencies import get_redis_proxy
from modules.RedisProxy import RedisProxy
from routers import RouterSystemCommand
from routers import RouterScaler
from routers import ControlNestDAQ
# logging.basicConfig(level=logging.WARNING)
app = FastAPI()
app.include_router(ControlNestDAQ.router)
app.include_router(RouterSystemCommand.router)
app.include_router(RouterScaler.router)
# app.include_router(ControlBabirl.router)
# 
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,   # 追記により追加
#     allow_methods=["*"],      # 追記により追加
#     allow_headers=["*"]       # 追記により追加
# )
# 

# get from dependencies.py
aProxy = get_redis_proxy(0)

@app.get("/")
async def root():
    return {"message": "Hello World"}
#     return {"message": aProxy.isConnected()}
@app.get("/set/{key}/{val}")
async def read_item(key: str, val: str) :
    r = aProxy.instance()
    r.set(key,val)
    return {"message": "set"}

@app.get("/get/{key}")
async def read_item(key: str) :
    r = aProxy.instance()
    val = r.get(key)
    if val == None :
        val = ""
    return {"message": val}
 
@app.get("/incr/{key}")
async def read_item(key: str) :
    r = aProxy.instance()
    val = r.incr(key)
    if val == None :
        val = ""
    return {"message": val}
 
@app.get("/expire/{key}/{time}")
async def read_item(key: str, time: str) :
    r = aProxy.instance()
    val = r.expire(key, int(time))
    if val == None :
        val = ""
    return {"message": val}

@app.get("/publish/{chnl}/{msg}")
async def read_item(chnl: str, msg: str) :
    r = aProxy.instance()
    val = r.publish(chnl,msg)
    if val == None :
        val = ""
    return {"message": val}


@app.get("/items/")
async def read_item(skip: int = 0, limit: int = 10):
    return JSONResponse(content={"skip":skip, "limit":limit})

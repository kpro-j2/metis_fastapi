import redis
from fastapi import APIRouter, HTTPException
from modules.RedisProxy import RedisProxy
from metis_fastapi.dependencies import get_redis_proxy
router = APIRouter(
   prefix="/nestdaq",
   tags=["nestdaq"]
)



aProxy = get_redis_proxy(0)


def _rcli():
   rcli = aProxy.instance()
   if rcli is None:
      raise HTTPException(status_code=503, detail="Redis client is not connected")
   return rcli


def _to_text(v):
   if isinstance(v, bytes):
      return v.decode(errors="replace")
   return v

@router.get('/')
async def root() : 
   return {"message": "nestdaq api"}

@router.get('/status/')
async def read_status():
   rcli = _rcli()
   key_updated = rcli.keys("daq_service:*:updatedTime")
   _ = rcli.mget(key_updated)
   key_state = rcli.keys("daq_service:*:fair-mq-state")
   if not key_state:
      return {}
   val_state = rcli.mget(key_state)
   key_state = [x.decode().split(':')[2] for x in key_state ]
   val_state = [_to_text(x) for x in val_state]
   
   state = dict(zip(key_state,val_state))
   return state

@router.get("/set_path/{key}/{val:path}")
async def read_item(key: str, val:str) :
   rcli = _rcli()
   rcli.set(key,val)
   return {"message": "set_path"}

@router.get('/run_number')
async def read_run_number():
   rcli = _rcli()
   val_run_number = rcli.get("run_info:run_number")
   return {"message" : _to_text(val_run_number)}

@router.get('/run_comment')
async def read_run_comment():
   rcli = _rcli()
   val_run_comment = rcli.get("run_info:run_comment")
   return {"message" : _to_text(val_run_comment)}

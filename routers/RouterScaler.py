import datetime

import redis
from fastapi import APIRouter
from modules.ScalerFactory import ScalerFactory
from modules.HulScaler import HulScaler

router = APIRouter(prefix="/scaler", tags=["scaler"])

factory = ScalerFactory()
# HulScaler.CommandPath = "ssh ata03 "
HulScaler.CommandPath = ""


@router.get("/")
async def root():
    pass

@router.get("/suspend")
async def scaler_suspend():
    return factory.suspend()

@router.get("/resume")
async def scaler_resume():
    return factory.resume()


@router.get("/add/hul/{id}/")
async def scaler_add(id: str):
    return factory.addHulScaler(id)


@router.get("/remove/{id}")
async def scaler_remove(id: str):
    factory.removeScaler(id)
    return factory.get_info()


@router.get("/read/info/all/")
async def scaler_read_info_all():
    return factory.get_info()


@router.get("/read/info/{id}/")
async def scaler_read_info_id(id: str):
    return factory.get_info(id)


@router.get("/read/data/all")
async def scaler_read_data_all():
    return factory.get_data()


@router.get("/read/data/{id}/")
async def scaler_read_data_id(id: str):
    return factory.get_data(id)


@router.get("/read/{ip}/")
async def read_scaler(ip: str):
    #    if (ip == "10.32.2.169"):
    #        # HR-TDC

    if ip == "192.168.10.1":
        scr = []
        for i in range(64):
            scr.append({"ch": i, "val": 0})
        ts = int(datetime.datetime.timestamp(datetime.datetime.now()))
        scr[63]["val"] = ts
        return {"scr": scr, "timestamp": ts, "ip": ip}
    else:
        scr = []
        for i in range(32):
            scr.append({"ch": i, "val": 0})
        ts = int(datetime.datetime.timestamp(datetime.datetime.now()))
        scr[31]["val"] = ts
        return {"scr": scr, "timestamp": ts, "ip": ip}

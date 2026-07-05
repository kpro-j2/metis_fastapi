from fastapi import APIRouter, Query

from modules.telemetry_store import list_snapshots, save_snapshot
from routers import RouterRph032, RouterScaler


router = APIRouter(prefix="/telemetry", tags=["telemetry"])


def _collect_scaler() -> dict:
    return RouterScaler.factory.get_data_cached()


def _collect_hv() -> dict:
    return RouterRph032.export_cached_all()


@router.get("/")
async def root():
    return {"message": "telemetry api"}


@router.post("/snapshot/save")
async def snapshot_save(
    source: str = Query(default="manual"),
    note: str = Query(default=""),
):
    scaler = _collect_scaler()
    hv = _collect_hv()
    saved = save_snapshot(scaler_data=scaler, hv_data=hv, source=source, note=note)
    return {
        "message": "ok",
        "saved": saved,
        "scaler_count": len(scaler.get("data", {})) if isinstance(scaler, dict) else 0,
        "hv_count": len(hv.get("modules", {})) if isinstance(hv, dict) else 0,
    }


@router.get("/snapshot/list")
async def snapshot_list(limit: int = Query(default=100, ge=1, le=1000)):
    items = list_snapshots(limit=limit)
    return {
        "message": "ok",
        "items": items,
    }